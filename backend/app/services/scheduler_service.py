from __future__ import annotations

from datetime import datetime, UTC
import logging

from sqlalchemy.orm import Session

from app.models.task import Task
from app.repositories.job_repository import JobRepository
from app.repositories.task_repository import TaskRepository
from app.utils.cron import next_cron_time
from app.utils.time import utcnow

logger = logging.getLogger(__name__)

import heapq

class SchedulerService:
    def __init__(self, session_maker, queue_client, worker_visibility_timeout_seconds: int = 300):
        self.session_maker = session_maker
        self.job = None
        self.task = None
        self._heap = []
        self._job_cache = {}
        self.last_sync_at = None
        self.queue = queue_client
        self.worker_visibility_timeout_seconds = worker_visibility_timeout_seconds

    def sync_jobs(self):
        """同步資料庫中異動的 Job 到記憶體快取與 Heap 裡 (Tombstone 模式)。"""
        
        with self.session_maker() as db:
            jobs_repo = JobRepository(db)
            jobs = jobs_repo.list_updated_since(self.last_sync_at)
            db.expunge_all()
        for job in jobs:    
            if job.enabled:
                if job.next_fire_at.tzinfo is None:
                    job.next_fire_at = job.next_fire_at.replace(tzinfo=UTC)
                self._job_cache[str(job.id)] = job
                # 直接把新的時間 Push 進原本的 Heap 裡 (O(log N))
                # 備註：Heap 裡面可能還殘留著這個 job 的舊時間，不用理它！
                heapq.heappush(self._heap, (job.next_fire_at.timestamp(),str(job.id)))
            else:
                self._job_cache.pop(str(job.id), None)    

        self.last_sync_at = utcnow()
    
    
    def recover_orphans(self) -> int:
        """回收所有 locked_until 過期的 running Task：重設為 pending 並重送 Queue。"""
        now = utcnow()
        with self.session_maker() as db:
            task_repo = TaskRepository(db)
            tasks = task_repo.list_expired_running(now)
            for task in tasks:
                task_repo.mark_running_expired_pending(task)
                self.queue.send_task(str(task.id))
        return len(tasks)


    def dispatch_due_jobs(self) -> int:
        now = utcnow()
        counter = 0
        while self._heap:
            next_time, job_id = self._heap[0]
            if next_time > now.timestamp():
                break
            next_time, job_id = heapq.heappop(self._heap)
            job = self._job_cache.get(job_id)
            if job is None or job.next_fire_at.timestamp() != next_time:
                continue
            with self.session_maker() as db:
                self.job = JobRepository(db)
                self.task = TaskRepository(db)
                job = db.merge(job)
                
                # 執行發射
                success = self._dispatch_job(job, now)
                
                if job.next_fire_at.tzinfo is None:
                    job.next_fire_at = job.next_fire_at.replace(tzinfo=UTC)
                
                # 【不管成功或失敗】，它的 next_fire_at 都已經算好了，必須存回快取並 Push 回 Heap 排隊
                self._job_cache[str(job_id)] = job
                heapq.heappush(self._heap, (job.next_fire_at.timestamp(), str(job_id)))
                
                # 只有真正發射成功才算 counter
                if success:
                    counter += 1
        return counter    

    def _dispatch_job(self, job, now: datetime) -> bool:
        """派發單一 Job：建立 Task、送入 Queue、更新 next_fire_at。
        #   1. 檢查 concurrency_policy：
        #      - 若為 'forbid' 且該 Job 有 running task → 跳過
        #        （仍需更新 next_fire_at）→ 回傳 False
        #   2. 建立 Task(job_id=..., status='pending', trigger_type='scheduled', retry_count=0)
        #   3. self.tasks.create(task)
        #   4. 更新 job.next_fire_at = next_cron_time(...)
        #   5. self.jobs.update(job)
        #   6. self.queue.send_task(str(task.id))
        #   7. 回傳 True
        """
        job.next_fire_at = next_cron_time(job.cron_expression, now)
        self.job.update(job)
        if job.concurrency_policy == "forbid" and self.task.count_running_for_job(job.id) > 0:
            return False
        task = Task(job_id=str(job.id), status="pending", trigger_type="scheduled", retry_count=0)
        self.task.create(task)
        self.queue.send_task(str(task.id))
        return True
        
