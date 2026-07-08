# -*- coding: utf-8 -*-
"""
MySQL 数据库连接层 —— 异步连接池 + 会话管理

用法:
    from src.infrastructure.database import get_db_url, create_engine, get_session

    engine = create_engine()
    async with get_session() as session:
        result = await session.execute(...)
"""

from __future__ import annotations
import os
import logging
from typing import AsyncGenerator, Optional

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
    AsyncEngine,
)
logger = logging.getLogger("smart_agent.database")

_engine: Optional[AsyncEngine] = None
_session_factory: Optional[async_sessionmaker] = None


def get_db_url() -> str:
    """构建 MySQL 连接 URL（异步驱动 aiomysql）

    优先级: 环境变量 DATABASE_URL > config.yaml > 默认值
    """
    url = os.getenv("DATABASE_URL", "")
    if url:
        return url

    host = os.getenv("DB_HOST", "127.0.0.1")
    port = os.getenv("DB_PORT", "3306")
    user = os.getenv("DB_USER", "smart_agent")
    password = os.getenv("DB_PASSWORD", "")
    # MySQL 驱动限制密码不能超过 72 字节
    password_bytes = password.encode("utf-8")
    logger.info(f"DB_PASSWORD 原始长度: {len(password_bytes)} 字节")
    if len(password_bytes) > 72:
        logger.warning(f"DB_PASSWORD 过长 ({len(password_bytes)} 字节)，截断至 72 字节")
        password = password_bytes[:72].decode("utf-8", errors="ignore")
    database = os.getenv("DB_NAME", "smart_agent")

    # URL 编码，防止特殊字符导致连接串解析错误
    from urllib.parse import quote_plus
    url = f"mysql+aiomysql://{quote_plus(user)}:****@{host}:{port}/{database}?charset=utf8mb4"
    logger.info(f"数据库连接 URL: {url} (密码长度: {len(password_bytes)} 字节)")
    return f"mysql+aiomysql://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/{database}?charset=utf8mb4"


def create_engine(db_url: str | None = None, echo: bool = False) -> AsyncEngine:
    """创建异步 SQLAlchemy 引擎（连接池）"""
    global _engine, _session_factory

    url = db_url or get_db_url()

    _engine = create_async_engine(
        url,
        echo=echo,
        pool_size=10,
        max_overflow=20,
        pool_recycle=3600,
        pool_pre_ping=True,
    )
    _session_factory = async_sessionmaker(
        _engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    logger.info("数据库引擎已创建 (pool_size=10, max_overflow=20)")
    return _engine


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """获取数据库会话（依赖注入用）"""
    if _session_factory is None:
        raise RuntimeError("数据库引擎未初始化，请先调用 create_engine()")

    async with _session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def close_engine():
    """关闭数据库引擎"""
    global _engine, _session_factory
    if _engine:
        await _engine.dispose()
        _engine = None
        _session_factory = None
        logger.info("数据库引擎已关闭")


def is_db_available() -> bool:
    """检查数据库是否已配置"""
    return _engine is not None and _session_factory is not None
