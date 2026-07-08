# -*- coding: utf-8 -*-
"""
MySQL ORM 模型定义

表:
  - users          用户表（认证）
  - tasks          任务表（持久化）
  - task_events    任务事件日志
  - agent_configs   Agent 配置
  - system_logs    系统日志（结构化）
"""

from __future__ import annotations
from datetime import datetime
from typing import Optional

from sqlalchemy import (
    Column, Integer, String, Text, DateTime, Float,
    Boolean, JSON, ForeignKey, Index, Enum as SAEnum,
)
from sqlalchemy.orm import DeclarativeBase, relationship
import enum


class Base(DeclarativeBase):
    pass


# ============================================================
# 枚举
# ============================================================

class TaskStatusEnum(str, enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class UserRoleEnum(str, enum.Enum):
    ADMIN = "admin"
    USER = "user"


# ============================================================
# 用户表
# ============================================================

class UserModel(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(64), unique=True, nullable=False, index=True)
    password_hash = Column(String(256), nullable=False)
    email = Column(String(128), default="")
    role = Column(String(16), default=UserRoleEnum.USER.value)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    last_login_at = Column(DateTime, nullable=True)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "username": self.username,
            "email": self.email,
            "role": self.role,
            "is_active": self.is_active,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "last_login_at": self.last_login_at.isoformat() if self.last_login_at else None,
        }


# ============================================================
# 任务表
# ============================================================

class TaskModel(Base):
    __tablename__ = "tasks"

    id = Column(String(32), primary_key=True)
    title = Column(String(256), default="")
    description = Column(Text, default="")
    status = Column(String(16), default=TaskStatusEnum.PENDING.value, index=True)
    priority = Column(Integer, default=0)
    tags = Column(JSON, default=list)
    assigned_agent = Column(String(128), nullable=True)
    result = Column(Text, nullable=True)
    error = Column(Text, nullable=True)
    metadata_ = Column("metadata", JSON, default=dict)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # 关联事件日志
    events = relationship("TaskEventModel", back_populates="task", cascade="all, delete-orphan")

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "title": self.title,
            "description": (self.description or "")[:200],
            "status": self.status,
            "priority": self.priority,
            "tags": self.tags or [],
            "assigned_agent": self.assigned_agent,
            "result": (self.result or "")[:500],
            "error": self.error,
            "metadata": self.metadata_ or {},
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "event_log": [e.to_dict() for e in (self.events or [])[-20:]] if self.events else [],
        }


# ============================================================
# 任务事件日志
# ============================================================

class TaskEventModel(Base):
    __tablename__ = "task_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(String(32), ForeignKey("tasks.id", ondelete="CASCADE"), nullable=False, index=True)
    event = Column(String(64), nullable=False)
    data = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    task = relationship("TaskModel", back_populates="events")

    def to_dict(self) -> dict:
        return {
            "time": self.created_at.isoformat(timespec="seconds") if self.created_at else None,
            "event": self.event,
            "data": self.data,
        }


# ============================================================
# Agent 配置表
# ============================================================

class AgentConfigModel(Base):
    __tablename__ = "agent_configs"

    name = Column(String(128), primary_key=True)
    model = Column(String(64), default="deepseek-chat")
    provider = Column(String(32), default="deepseek")
    skills = Column(JSON, default=list)
    description = Column(String(512), default="")
    status = Column(String(16), default="idle")
    current_task_id = Column(String(32), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "model": self.model,
            "provider": self.provider,
            "skills": self.skills or [],
            "description": self.description,
            "status": self.status,
            "current_task_id": self.current_task_id,
        }


# ============================================================
# 系统日志表（结构化日志）
# ============================================================

class SystemLogModel(Base):
    __tablename__ = "system_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    level = Column(String(16), nullable=False, index=True)
    logger_name = Column(String(128), default="")
    message = Column(Text, nullable=False)
    extra_data = Column("extra", JSON, nullable=True)
    trace_id = Column(String(64), nullable=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)

    __table_args__ = (
        Index("ix_logs_level_time", "level", "created_at"),
    )
