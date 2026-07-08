# -*- coding: utf-8 -*-
"""
任务持久化存储 —— MySQL 桥接层

在数据库可用时自动将内存中的任务同步到 MySQL，
数据库不可用时降级为纯内存模式（保持向后兼容）。
"""

from __future__ import annotations
from datetime import datetime
from typing import Optional
import logging
import threading

logger = logging.getLogger("smart_agent.task_repo")


class TaskRepository:
    """
    任务仓库 —— MySQL 持久化 + 内存缓存

    单例模式，线程安全。
    数据库不可用时自动降级为纯内存模式。
    """

    _instance: Optional["TaskRepository"] = None

    def __init__(self):
        self._memory_tasks: dict[str, dict] = {}
        self._lock = threading.Lock()
        self._db_enabled = False

    @classmethod
    def get_instance(cls) -> "TaskRepository":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @property
    def db_enabled(self) -> bool:
        return self._db_enabled

    def enable_db(self):
        """启用数据库持久化"""
        self._db_enabled = True
        logger.info("任务持久化已启用 (MySQL)")

    def disable_db(self):
        """回退到内存模式"""
        self._db_enabled = False
        logger.warning("任务持久化已降级为内存模式")

    # ======== 任务 CRUD ========

    async def save_task(self, task_dict: dict):
        """保存任务到 MySQL + 内存"""
        task_id = task_dict["id"]
        with self._lock:
            self._memory_tasks[task_id] = task_dict

        if not self._db_enabled:
            return

        try:
            from ..infrastructure.models import TaskModel

            async with self._get_async_session() as session:
                existing = await session.get(TaskModel, task_id)
                if existing:
                    # 更新
                    for key, value in self._task_to_model_fields(task_dict).items():
                        setattr(existing, key, value)
                else:
                    # 插入
                    model = TaskModel(**self._task_to_model_fields(task_dict))
                    session.add(model)
                await session.commit()
        except Exception as e:
            logger.error(f"保存任务到 MySQL 失败: {e}")

    async def load_task(self, task_id: str) -> Optional[dict]:
        """从 MySQL 或内存加载任务"""
        with self._lock:
            if task_id in self._memory_tasks:
                return self._memory_tasks[task_id]

        if not self._db_enabled:
            return None

        try:
            from ..infrastructure.models import TaskModel

            async with self._get_async_session() as session:
                result = await session.get(TaskModel, task_id)
                if result:
                    return result.to_dict()
        except Exception as e:
            logger.error(f"从 MySQL 加载任务失败: {e}")
        return None

    async def list_tasks(
        self, status: str = "", limit: int = 20
    ) -> list[dict]:
        """列出任务"""
        if not self._db_enabled:
            with self._lock:
                tasks = list(self._memory_tasks.values())
                if status:
                    tasks = [t for t in tasks if t.get("status") == status]
                tasks.sort(key=lambda t: t.get("created_at", ""), reverse=True)
                return tasks[:limit]

        try:
            from ..infrastructure.models import TaskModel, TaskStatusEnum
            from sqlalchemy import select

            async with self._get_async_session() as session:
                stmt = select(TaskModel)
                if status:
                    stmt = stmt.where(TaskModel.status == status)
                stmt = stmt.order_by(TaskModel.created_at.desc()).limit(limit)
                result = await session.execute(stmt)
                models = result.scalars().all()
                return [m.to_dict() for m in models]
        except Exception as e:
            logger.error(f"从 MySQL 列出任务失败: {e}")
            with self._lock:
                tasks = list(self._memory_tasks.values())
                if status:
                    tasks = [t for t in tasks if t.get("status") == status]
                return tasks[:limit]

    async def delete_task(self, task_id: str):
        """删除任务"""
        with self._lock:
            self._memory_tasks.pop(task_id, None)

        if not self._db_enabled:
            return

        try:
            from ..infrastructure.models import TaskModel

            async with self._get_async_session() as session:
                model = await session.get(TaskModel, task_id)
                if model:
                    await session.delete(model)
                    await session.commit()
        except Exception as e:
            logger.error(f"从 MySQL 删除任务失败: {e}")

    async def add_event(self, task_id: str, event: str, data: dict = None):
        """记录任务事件"""
        if self._db_enabled:
            try:
                from ..infrastructure.models import TaskEventModel

                async with self._get_async_session() as session:
                    evt = TaskEventModel(
                        task_id=task_id,
                        event=event,
                        data=data,
                    )
                    session.add(evt)
                    await session.commit()
            except Exception as e:
                logger.error(f"保存任务事件到 MySQL 失败: {e}")

    # ======== 内部工具 ========

    @staticmethod
    def _task_to_model_fields(task_dict: dict) -> dict:
        """将 Task.to_dict() 映射回 ORM 字段"""
        return {
            "id": task_dict.get("id", ""),
            "title": task_dict.get("title", ""),
            "description": task_dict.get("description", ""),
            "status": task_dict.get("status", "pending"),
            "priority": task_dict.get("priority", 0),
            "tags": task_dict.get("tags", []),
            "assigned_agent": task_dict.get("assigned_agent"),
            "result": task_dict.get("result"),
            "error": task_dict.get("error"),
            "metadata_": task_dict.get("metadata", {}),
            "created_at": _parse_dt(task_dict.get("created_at")),
            "started_at": _parse_dt(task_dict.get("started_at")),
            "finished_at": _parse_dt(task_dict.get("finished_at")),
        }

    @staticmethod
    def _get_async_session():
        from ..infrastructure.database import _session_factory
        if _session_factory is None:
            raise RuntimeError("数据库未初始化")
        return _session_factory()


def _parse_dt(val) -> Optional[datetime]:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    try:
        return datetime.fromisoformat(str(val))
    except (ValueError, TypeError):
        return None


# 全局单例
def get_task_repo() -> TaskRepository:
    return TaskRepository.get_instance()
