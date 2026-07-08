# -*- coding: utf-8 -*-
"""
速率限制中间件 —— 基于内存的令牌桶算法

每个 IP 每分钟最多 max_requests 次请求。
"""

from __future__ import annotations
import time
import threading
from collections import defaultdict
from typing import Callable

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse


class TokenBucket:
    """令牌桶 —— 每个桶对应一个 IP"""

    def __init__(self, rate: float, capacity: int):
        self.rate = rate          # 令牌补充速率 (个/秒)
        self.capacity = capacity  # 桶容量
        self.tokens = float(capacity)
        self.last_refill = time.monotonic()

    def consume(self, tokens: int = 1) -> bool:
        """尝试消耗 tokens 个令牌，返回是否成功"""
        now = time.monotonic()
        # 补充令牌
        elapsed = now - self.last_refill
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
        self.last_refill = now

        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        return False


class RateLimitMiddleware(BaseHTTPMiddleware):
    """
    速率限制中间件

    配置:
        max_requests: 每分钟最大请求数（默认 120）
        burst: 突发允许量（默认 30）

    白名单路径: /health, /metrics
    """

    WHITELIST = {"/health", "/metrics", "/docs", "/redoc", "/openapi.json"}

    def __init__(self, app, max_requests: int = 120, burst: int = 30):
        super().__init__(app)
        self.buckets: dict[str, TokenBucket] = {}
        self._lock = threading.Lock()
        self.max_requests = max_requests
        self.burst = burst

    def _get_bucket(self, key: str) -> TokenBucket:
        with self._lock:
            if key not in self.buckets:
                # 速率 = max_requests / 60 秒
                rate = self.max_requests / 60.0
                self.buckets[key] = TokenBucket(rate=rate, capacity=self.burst)
            return self.buckets[key]

    async def dispatch(self, request: Request, call_next: Callable):
        # 白名单路径跳过限流
        if request.url.path in self.WHITELIST:
            return await call_next(request)

        # 获取客户端 IP
        client_ip = (
            request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
            or request.headers.get("X-Real-IP", "")
            or (request.client.host if request.client else "unknown")
        )

        bucket = self._get_bucket(client_ip)

        if not bucket.consume():
            return JSONResponse(
                status_code=429,
                content={
                    "ok": False,
                    "error": f"请求过于频繁，请稍后再试 (限制: {self.max_requests}次/分钟)",
                },
            )

        response = await call_next(request)
        return response
