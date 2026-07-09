# -*- coding: utf-8 -*-
"""
Alembic 迁移环境配置

log_model = True → 每次迁移自动检测 ORM 模型变化
"""

from logging.config import fileConfig
from sqlalchemy import engine_from_config, pool
from alembic import context

# 加载 Alembic 配置（alembic.ini）
config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# 设置目标 metadata（ORM models）
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.infrastructure.models import Base
target_metadata = Base.metadata

# 从环境变量读取数据库 URL（兼容 alembic.ini 中的 sqlalchemy.url）
from src.infrastructure.database import get_db_url
config.set_main_option("sqlalchemy.url", get_db_url())


def run_migrations_offline() -> None:
    """离线迁移模式（生成 SQL 脚本）"""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """在线迁移模式（直接执行到数据库）"""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
