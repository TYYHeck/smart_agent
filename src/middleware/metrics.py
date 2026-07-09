# -*- coding: utf-8 -*-
"""
Prometheus 指标中间件 —— HTTP 请求计数、延迟、错误率（纯 ASGI 实现）

基于 prometheus_client 官方库，暴露 /metrics 端点供 Prometheus 抓取。
"""

from __future__ import annotations
import time
import re

from prometheus_client import Counter, Histogram, Gauge, generate_latest, CollectorRegistry

# ── 使用独立 Registry，避免与其他库冲突 ──
_registry = CollectorRegistry(auto_describe=True)

# ── 指标定义 ──
http_requests_total = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["method", "endpoint", "status_code"],
    registry=_registry,
)

http_request_duration_seconds = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds",
    ["method", "endpoint"],
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
    registry=_registry,
)

http_requests_in_flight = Gauge(
    "http_requests_in_flight",
    "Currently in-flight HTTP requests",
    registry=_registry,
)

# ── 路径归一化：把 /api/tasks/abc123 → /api/tasks/{id} ──
_PATH_PATTERNS = [
    (re.compile(r"/api/tasks/[a-zA-Z0-9\-]+"), "/api/tasks/{id}"),
    (re.compile(r"/api/agents/[^/]+"), "/api/agents/{name}"),
]


def _normalize_path(path: str) -> str:
    for pattern, replacement in _PATH_PATTERNS:
        path = pattern.sub(replacement, path)
    return path


class PrometheusMiddleware:
    """
    纯 ASGI Prometheus 指标中间件（不使用 BaseHTTPMiddleware，避免 aiomysql 事件循环冲突）

    收集 HTTP 请求计数、延迟、错误率。
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        http_requests_in_flight.inc()
        start_time = time.monotonic()
        method = scope.get("method", "UNKNOWN")
        path = scope.get("path", "/")
        normalized_path = _normalize_path(path)

        # 用闭包捕获响应状态码
        status_code = 500
        original_send = send

        async def _send(message):
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
            await original_send(message)

        try:
            await self.app(scope, receive, _send)
        except Exception:
            status_code = 500
            raise
        finally:
            duration = time.monotonic() - start_time
            http_requests_in_flight.dec()

            http_requests_total.labels(
                method=method, endpoint=normalized_path, status_code=str(status_code)
            ).inc()

            http_request_duration_seconds.labels(
                method=method, endpoint=normalized_path,
            ).observe(duration)


def get_metrics_text() -> str:
    """生成 Prometheus 格式的指标文本"""
    return generate_latest(_registry).decode("utf-8")
