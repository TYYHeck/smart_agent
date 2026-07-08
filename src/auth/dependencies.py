# -*- coding: utf-8 -*-
"""
认证依赖注入 —— FastAPI 路由保护

用法:
    @app.get("/api/protected")
    async def protected_route(current_user: UserModel = Depends(get_current_user)):
        ...
"""

from __future__ import annotations
from typing import Optional
import logging

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from . import decode_access_token
from ..infrastructure.database import get_session
from ..infrastructure.models import UserModel

logger = logging.getLogger("smart_agent.auth_deps")

# Bearer Token 提取器
security = HTTPBearer(auto_error=False)


async def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
    session: AsyncSession = Depends(get_session),
) -> UserModel:
    """获取当前认证用户（必须登录）"""
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="未提供认证令牌",
            headers={"WWW-Authenticate": "Bearer"},
        )

    payload = decode_access_token(credentials.credentials)
    if payload is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="令牌无效或已过期",
            headers={"WWW-Authenticate": "Bearer"},
        )

    username: str = payload.get("sub", "")
    if not username:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="令牌内容无效",
        )

    result = await session.execute(
        select(UserModel).where(
            UserModel.username == username,
            UserModel.is_active == True,
        )
    )
    user = result.scalar_one_or_none()

    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="用户不存在或已被禁用",
        )

    return user


async def get_optional_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
    session: AsyncSession = Depends(get_session),
) -> Optional[UserModel]:
    """获取当前用户（可选，未登录返回 None）"""
    if credentials is None:
        return None
    try:
        return await get_current_user(credentials, session)
    except HTTPException:
        return None


async def get_current_admin(
    current_user: UserModel = Depends(get_current_user),
) -> UserModel:
    """获取当前管理员用户（必须 admin 角色）"""
    if current_user.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="需要管理员权限",
        )
    return current_user
