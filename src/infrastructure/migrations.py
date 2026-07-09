# -*- coding: utf-8 -*-
"""
数据库迁移 —— 启动时自动建表

不需要 Alembic，简单地在启动时执行 CREATE TABLE IF NOT EXISTS
"""

from __future__ import annotations
import logging

from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy import text

from .models import Base

logger = logging.getLogger("smart_agent.migrations")

INIT_SQL = """
-- 用户表
CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(64) NOT NULL UNIQUE,
    password_hash VARCHAR(256) NOT NULL,
    email VARCHAR(128) DEFAULT '',
    role VARCHAR(16) DEFAULT 'user',
    is_active BOOLEAN DEFAULT TRUE,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    last_login_at DATETIME NULL,
    INDEX idx_users_username (username)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 任务表
CREATE TABLE IF NOT EXISTS tasks (
    id VARCHAR(32) PRIMARY KEY,
    title VARCHAR(256) DEFAULT '',
    description TEXT,
    status VARCHAR(16) DEFAULT 'pending',
    priority INT DEFAULT 0,
    tags JSON,
    assigned_agent VARCHAR(128) NULL,
    result TEXT NULL,
    error TEXT NULL,
    `metadata` JSON,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    started_at DATETIME NULL,
    finished_at DATETIME NULL,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_tasks_status (status),
    INDEX idx_tasks_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 任务事件日志
CREATE TABLE IF NOT EXISTS task_events (
    id INT AUTO_INCREMENT PRIMARY KEY,
    task_id VARCHAR(32) NOT NULL,
    event VARCHAR(64) NOT NULL,
    data JSON NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_events_task (task_id),
    FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Agent 配置表
CREATE TABLE IF NOT EXISTS agent_configs (
    name VARCHAR(128) PRIMARY KEY,
    model VARCHAR(64) DEFAULT 'deepseek-chat',
    provider VARCHAR(32) DEFAULT 'deepseek',
    skills JSON,
    description VARCHAR(512) DEFAULT '',
    system_prompt TEXT DEFAULT '',
    max_iterations INT DEFAULT 15,
    enable_planning BOOLEAN DEFAULT FALSE,
    enable_rag BOOLEAN DEFAULT TRUE,
    enable_reflection BOOLEAN DEFAULT FALSE,
    status VARCHAR(16) DEFAULT 'idle',
    current_task_id VARCHAR(32) NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- (字段补全由 Python 函数 run_column_migrations 处理，兼容所有 MySQL 版本)

-- 系统日志表
CREATE TABLE IF NOT EXISTS system_logs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    level VARCHAR(16) NOT NULL,
    logger_name VARCHAR(128) DEFAULT '',
    message TEXT NOT NULL,
    extra JSON NULL,
    trace_id VARCHAR(64) NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_logs_level_time (level, created_at),
    INDEX idx_logs_trace (trace_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


async def run_migrations(engine: AsyncEngine):
    """执行所有数据库迁移 —— 逐条执行 CREATE TABLE IF NOT EXISTS"""
    logger.info("开始数据库迁移...")

    # 方案1：使用 SQLAlchemy ORM 建表
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # 方案2：同时也执行原始 SQL（互补，确保 JSON 等类型正确）
    async with engine.begin() as conn:
        for statement in INIT_SQL.split(";"):
            stmt = statement.strip()
            if not stmt:
                continue
            try:
                await conn.execute(text(stmt + ";"))
            except Exception as e:
                logger.warning(f"SQL 执行跳过: {e}")

    # 方案3：补全旧表缺失的列（兼容所有 MySQL/MariaDB 版本）
    await _ensure_agent_configs_columns(engine)

    logger.info("数据库迁移完成 ✓")


async def _ensure_agent_configs_columns(engine: AsyncEngine):
    """确保 agent_configs 表包含所有必需列（逐列检查，兼容 MySQL 5.7+ / MariaDB）"""
    required = [
        ("system_prompt", "TEXT", None),  # TEXT 列在严格模式下不能设 DEFAULT
        ("max_iterations", "INT", "15"),
        ("enable_planning", "BOOLEAN", "FALSE"),
        ("enable_rag", "BOOLEAN", "TRUE"),
        ("enable_reflection", "BOOLEAN", "FALSE"),
    ]
    async with engine.begin() as conn:
        for col_name, col_type, col_default in required:
            try:
                check = text(
                    "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS "
                    "WHERE TABLE_SCHEMA = DATABASE() "
                    "AND TABLE_NAME = 'agent_configs' "
                    "AND COLUMN_NAME = :col"
                )
                result = await conn.execute(check, {"col": col_name})
                count = result.scalar()
                if count == 0:
                    if col_default is not None:
                        stmt = f"ALTER TABLE agent_configs ADD COLUMN {col_name} {col_type} DEFAULT {col_default}"
                    else:
                        stmt = f"ALTER TABLE agent_configs ADD COLUMN {col_name} {col_type}"
                    await conn.execute(text(stmt))
                    logger.info(f"已添加列: agent_configs.{col_name}")
            except Exception as e:
                logger.warning(f"添加列 agent_configs.{col_name} 跳过: {e}")


async def seed_default_admin(engine: AsyncEngine):
    """创建默认管理员账户"""
    import os
    import bcrypt

    admin_user = os.getenv("ADMIN_USER", "admin")
    admin_pass = os.getenv("ADMIN_PASSWORD", "admin123")
    admin_email = os.getenv("ADMIN_EMAIL", "admin@smartagent.local")

    # bcrypt 限制密码不超过 72 字节
    admin_bytes = admin_pass.encode("utf-8")
    if len(admin_bytes) > 72:
        logger.warning(f"ADMIN_PASSWORD 过长 ({len(admin_bytes)} 字节)，截断至前 72 字节")
        admin_pass = admin_bytes[:72].decode("utf-8", errors="ignore")

    password_hash = bcrypt.hashpw(admin_pass.encode("utf-8"), bcrypt.gensalt()).decode()

    from sqlalchemy import select
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
    from .models import UserModel

    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with session_factory() as session:
        result = await session.execute(
            select(UserModel).where(UserModel.username == admin_user)
        )
        existing = result.scalar_one_or_none()
        if existing is None:
            user = UserModel(
                username=admin_user,
                password_hash=password_hash,
                email=admin_email,
                role="admin",
            )
            session.add(user)
            await session.commit()
            logger.info(f"默认管理员已创建: {admin_user}")
        else:
            logger.info(f"管理员账户已存在: {admin_user}")
