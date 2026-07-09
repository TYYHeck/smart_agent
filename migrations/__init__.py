# -*- coding: utf-8 -*-
"""
Alembic 数据库迁移套件

用法:
    生成迁移脚本:
        alembic revision --autogenerate -m "描述"

    执行迁移:
        alembic upgrade head

    回滚:
        alembic downgrade -1

    查看历史:
        alembic history

或在启动时自动执行（兼容旧行为）：
    from src.infrastructure.migrations import run_migrations
"""

# Alembic 配置文件在项目根目录的 alembic.ini
# 此文件提供 env.py 所需的 metadata 引用
