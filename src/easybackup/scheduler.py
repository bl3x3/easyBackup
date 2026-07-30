"""APScheduler integration for task Cron expressions."""

from __future__ import annotations

import logging
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from easybackup.db import Database
from easybackup.errors import ConflictError, NotFoundError, ValidationError
from easybackup.manager import OperationManager
from easybackup.models import ScrubRequest, Task


logger = logging.getLogger(__name__)


class SchedulerService:
    def __init__(
        self,
        database: Database,
        manager: OperationManager,
        timezone: str,
        scrub_schedule: str | None,
    ):
        self.database = database
        self.manager = manager
        try:
            self.timezone = ZoneInfo(timezone)
        except ZoneInfoNotFoundError as exc:
            raise ValidationError(f"未知时区：{timezone}") from exc
        self.scheduler = AsyncIOScheduler(timezone=self.timezone)
        self.scrub_schedule = scrub_schedule
        self.validate_cron(scrub_schedule)

    def validate_cron(self, expression: str | None) -> None:
        if not expression:
            return
        try:
            CronTrigger.from_crontab(expression, timezone=self.timezone)
        except (ValueError, TypeError) as exc:
            raise ValidationError(
                f"无效 Cron 表达式 {expression!r}：{exc}"
            ) from exc

    async def _scheduled_run(self, task_id: str) -> None:
        try:
            await self.manager.start_backup(task_id)
        except ConflictError:
            logger.info("跳过任务 %s 的重叠调度", task_id)
        except Exception:
            logger.exception("调度任务 %s 失败", task_id)

    async def _scheduled_scrub(self, task_id: str) -> None:
        try:
            snapshot = self.database.latest_snapshot(task_id)
        except NotFoundError:
            logger.info("任务 %s 尚无快照，跳过周期巡检", task_id)
            return
        try:
            await self.manager.start_scrub(
                snapshot.id, ScrubRequest()
            )
        except ConflictError:
            logger.info("跳过任务 %s 的重叠周期巡检", task_id)
        except Exception:
            logger.exception("任务 %s 的周期巡检启动失败", task_id)

    def start(self) -> None:
        for task in self.database.list_tasks():
            self.refresh_task(task)
        self.scheduler.start()

    def refresh_task(self, task: Task) -> None:
        backup_job_id = f"backup:{task.id}"
        scrub_job_id = f"scrub:{task.id}"
        for job_id in (backup_job_id, scrub_job_id):
            if self.scheduler.get_job(job_id):
                self.scheduler.remove_job(job_id)
        if not task.enabled:
            return
        if task.schedule:
            self.validate_cron(task.schedule)
            trigger = CronTrigger.from_crontab(
                task.schedule, timezone=self.timezone
            )
            self.scheduler.add_job(
                self._scheduled_run,
                trigger=trigger,
                args=[task.id],
                id=backup_job_id,
                replace_existing=True,
                coalesce=True,
                max_instances=1,
                misfire_grace_time=300,
            )
        if self.scrub_schedule:
            scrub_trigger = CronTrigger.from_crontab(
                self.scrub_schedule, timezone=self.timezone
            )
            self.scheduler.add_job(
                self._scheduled_scrub,
                trigger=scrub_trigger,
                args=[task.id],
                id=scrub_job_id,
                replace_existing=True,
                coalesce=True,
                max_instances=1,
                misfire_grace_time=1800,
            )

    def remove_task(self, task_id: str) -> None:
        for job_id in (f"backup:{task_id}", f"scrub:{task_id}"):
            if self.scheduler.get_job(job_id):
                self.scheduler.remove_job(job_id)

    def next_run(self, task_id: str) -> str | None:
        job = self.scheduler.get_job(f"backup:{task_id}")
        next_time = job.next_run_time if job else None
        return next_time.isoformat() if next_time else None

    def shutdown(self) -> None:
        if self.scheduler.running:
            self.scheduler.shutdown(wait=False)
