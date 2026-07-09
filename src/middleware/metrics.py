# -*- coding: utf-8 -*-
"""
Prometheus 指标中间件 —— HTTP 请求计数、延迟、错误率

基于 prometheus_client 官方库，暴露 /metrics 端点供 Prometheus 抓取。
"""

from __future__ import annotations
import time
import re
from typing import Callable

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from prometheus_client import Counter, Histogram, Gauge, generate_latest, REGISTRY, CollectorRegistry

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


class PrometheusMiddleware(BaseHTTPMiddleware):
    """收集 HTTP 请求指标"""

    async def dispatch(self, request: Request, call_next: Callable):
        http_requests_in_flight.inc()

        start_time = time.monotonic()
        status_code = 500

        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        except Exception:
            status_code = 500
            raise
        finally:
            duration = time.monotonic() - start_time
            http_requests_in_flight.dec()

            method = request.method
            path = _normalize_path(request.url.path)

            http_requests_total.labels(
                method=method, endpoint=path, status_code=str(status_code)
            ).inc()

            http_request_duration_seconds.labels(
                method=method, endpoint=path,
            ).observe(duration)


def get_metrics_text() -> str:
    """生成 Prometheus 格式的指标文本"""
    return generate_latest(_registry).decode("utf-8")
