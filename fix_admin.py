# -*- coding: utf-8 -*-
"""服务器端修复脚本 —— 手动创建/重置 admin 账户"""
import os
import sys
import asyncio
import bcrypt

# --- 服务器数据库配置（与 docker-compose.yml 保持一致）---
os.environ.setdefault("DB_HOST", "mysql")
os.environ.setdefault("DB_PORT", "3306")
os.environ.setdefault("DB_USER", "smart_agent")
os.environ.setdefault("DB_PASSWORD", "smart_agent_pass")
os.environ.setdefault("DB_NAME", "smart_agent")
os.environ["DATABASE_URL"] = (
    f"mysql+aiomysql://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}@"
    f"{os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_NAME']}"
    "?charset=utf8mb4"
)

ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASS = os.getenv("ADMIN_PASSWORD", "admin123")
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "admin@smartagent.local")


async def main():
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
    from sqlalchemy import select, text

    engine = create_async_engine(os.environ["DATABASE_URL"], echo=False)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with session_factory() as session:
        # 检查 admin 是否存在
        result = await session.execute(
            text("SELECT id, username, role, created_at FROM users WHERE username = :u"),
            {"u": ADMIN_USER},
        )
        row = result.fetchone()

        password_hash = bcrypt.hashpw(
            ADMIN_PASS.encode("utf-8"), bcrypt.gensalt()
        ).decode()

        if row is None:
            # 不存在 → 创建
            await session.execute(
                text(
                    "INSERT INTO users (username, password_hash, email, role, is_active) "
                    "VALUES (:u, :p, :e, 'admin', TRUE)"
                ),
                {"u": ADMIN_USER, "p": password_hash, "e": ADMIN_EMAIL},
            )
            await session.commit()
            print(f"✓ admin 账户已创建: {ADMIN_USER} / {ADMIN_PASS}")
        else:
            # 已存在 → 重置密码
            await session.execute(
                text("UPDATE users SET password_hash = :p WHERE username = :u"),
                {"p": password_hash, "u": ADMIN_USER},
            )
            await session.commit()
            print(f"✓ admin 密码已重置: {ADMIN_USER} / {ADMIN_PASS}")

    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
