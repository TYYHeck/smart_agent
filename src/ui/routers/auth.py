# -*- coding: utf-8 -*-
"""认证路由 —— 登录/注册/用户信息"""

from __future__ import annotations
from fastapi import APIRouter, Depends, JSONResponse
from pydantic import BaseModel, Field, field_validator

from src.auth.dependencies import get_current_user

router = APIRouter(prefix="/api/auth", tags=["认证"])


class LoginRequest(BaseModel):
    username: str = Field(..., min_length=2, max_length=64, description="用户名")
    password: str = Field(..., min_length=4, description="密码")

    @field_validator("username")
    @classmethod
    def username_not_empty(cls, v: str) -> str:
        v = v.strip()
        if not v or len(v) < 2:
            raise ValueError("用户名至少 2 个字符")
        if len(v) > 64:
            raise ValueError("用户名最长 64 个字符")
        return v

    @field_validator("password")
    @classmethod
    def password_not_empty(cls, v: str) -> str:
        if not v or len(v) < 4:
            raise ValueError("密码至少 4 个字符")
        return v


class RegisterRequest(BaseModel):
    username: str = Field(..., min_length=2, max_length=64, description="用户名")
    password: str = Field(..., min_length=6, max_length=128, description="密码")
    email: str = Field("", description="邮箱（可选）")

    @field_validator("username")
    @classmethod
    def username_not_empty(cls, v: str) -> str:
        v = v.strip()
        if not v or len(v) < 2:
            raise ValueError("用户名至少 2 个字符")
        if len(v) > 64:
            raise ValueError("用户名最长 64 个字符")
        return v

    @field_validator("password")
    @classmethod
    def password_not_empty(cls, v: str) -> str:
        if not v or len(v) < 6:
            raise ValueError("密码至少 6 个字符")
        if len(v) > 128:
            raise ValueError("密码最长 128 个字符")
        return v


@router.post("/login")
async def api_login(req: LoginRequest):
    """用户登录 —— 返回 JWT Token"""
    from src.auth import verify_password, create_access_token
    from src.infrastructure.models import UserModel
    from sqlalchemy import select
    from src.infrastructure.database import get_session

    try:
        async for session in get_session():
            result = await session.execute(
                select(UserModel).where(UserModel.username == req.username)
            )
            user = result.scalar_one_or_none()

            if user is None or not verify_password(req.password, user.password_hash):
                return JSONResponse(
                    {"ok": False, "error": "用户名或密码错误"},
                    status_code=401,
                )

            token = create_access_token(user.username)
            return {
                "ok": True,
                "access_token": token,
                "token_type": "bearer",
                "user": user.to_dict(),
            }
    except ImportError:
        return JSONResponse(
            {"ok": False, "error": "认证系统未启用（数据库未连接）"},
            status_code=503,
        )


@router.post("/register")
async def api_register(req: RegisterRequest):
    """用户注册"""
    from src.auth import hash_password
    from src.infrastructure.models import UserModel
    from sqlalchemy import select
    from src.infrastructure.database import get_session

    try:
        async for session in get_session():
            result = await session.execute(
                select(UserModel).where(UserModel.username == req.username)
            )
            if result.scalar_one_or_none() is not None:
                return JSONResponse(
                    {"ok": False, "error": "用户名已存在"},
                    status_code=409,
                )

            hashed = hash_password(req.password)
            user = UserModel(
                username=req.username,
                password_hash=hashed,
                email=req.email,
                role="user",
            )
            session.add(user)
            await session.commit()

            return {"ok": True, "user": user.to_dict()}
    except ImportError:
        return JSONResponse(
            {"ok": False, "error": "注册功能需要数据库支持"},
            status_code=503,
        )


@router.get("/me")
async def api_me(current_user = Depends(get_current_user)):
    """获取当前用户信息（需要 Bearer Token）"""
    return {"ok": True, "user": current_user.to_dict()}
