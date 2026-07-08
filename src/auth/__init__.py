# -*- coding: utf-8 -*-
"""认证系统 —— JWT 生成/验证 + 密码哈希"""

from __future__ import annotations
from datetime import datetime, timedelta
from typing import Optional
import os
import logging

import bcrypt
from jose import JWTError, jwt

logger = logging.getLogger("smart_agent.auth")

# JWT 配置
SECRET_KEY = os.getenv("JWT_SECRET_KEY", "smart-agent-secret-change-in-production-2024")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("JWT_EXPIRE_MINUTES", "480"))  # 默认 8 小时


def hash_password(password: str) -> str:
    """对密码进行 bcrypt 哈希（兼容 bcrypt 4.0+）"""
    password_bytes = password.encode("utf-8")
    if len(password_bytes) > 72:
        password_bytes = password_bytes[:72]
    return bcrypt.hashpw(password_bytes, bcrypt.gensalt()).decode()


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """验证密码（兼容 bcrypt 4.0+）"""
    return bcrypt.checkpw(plain_password.encode("utf-8"), hashed_password.encode("utf-8"))


def create_access_token(
    data: dict,
    expires_delta: timedelta | None = None,
) -> str:
    """创建 JWT 访问令牌

    Args:
        data: 要编码的数据（至少包含 "sub" 即用户名）
        expires_delta: 过期时间增量，默认从配置读取
    """
    to_encode = data.copy()
    expire = datetime.utcnow() + (
        expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    )
    to_encode.update({"exp": expire, "iat": datetime.utcnow()})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def decode_access_token(token: str) -> Optional[dict]:
    """解码 JWT 令牌，返回 payload 或 None"""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except JWTError as e:
        logger.debug(f"JWT 解码失败: {e}")
        return None


def create_refresh_token(data: dict) -> str:
    """创建刷新令牌（有效期 7 天）"""
    return create_access_token(data, expires_delta=timedelta(days=7))
