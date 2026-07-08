# -*- coding: utf-8 -*-
"""
Prometheus 指标中间件 —— HTTP 请求计数、延迟、错误率

暴露 /metrics 端点供 Prometheus 抓取。
"""

from __future__ import annotations
import time
from typing import Callable

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

# Prometheus 指标存储（纯内存，不引入 prometheus_client 依赖）
# 生产环境建议替换为 prometheus_client 官方库

_metrics: dict[str, dict] = {
    "http_requests_total": {},       # {method_path_status: count}
    "http_request_duration_seconds": {},  # {method_path: [durations]}
    "http_requests_in_flight": 0,
}

# 路径归一化：把 /api/tasks/abc123 → /api/tasks/{id}
import re

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
        _metrics["http_requests_in_flight"] += 1

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
            _metrics["http_requests_in_flight"] -= 1

            method = request.method
            path = _normalize_path(request.url.path)
            key = f"{method} {path} {status_code}"

            # 累计计数
            _metrics["http_requests_total"][key] = (
                _metrics["http_requests_total"].get(key, 0) + 1
            )

            # 延迟采样（保留最近 1000 条）
            dur_key = f"{method} {path}"
            if dur_key not in _metrics["http_request_duration_seconds"]:
                _metrics["http_request_duration_seconds"][dur_key] = []
            durations = _metrics["http_request_duration_seconds"][dur_key]
            durations.append(duration)
            if len(durations) > 1000:
                durations.pop(0)


def get_metrics_text() -> str:
    """生成 Prometheus 格式的指标文本"""
    lines = []

    lines.append("# HELP http_requests_total Total HTTP requests")
    lines.append("# TYPE http_requests_total counter")
    for key, count in sorted(_metrics["http_requests_total"].items()):
        lines.append(f'http_requests_total{{route="{key}"}} {count}')

    lines.append("# HELP http_request_duration_seconds HTTP request duration")
    lines.append("# TYPE http_request_duration_seconds summary")
    for key, durations in sorted(_metrics["http_request_duration_seconds"].items()):
        if not durations:
            continue
        avg = sum(durations) / len(durations)
        max_d = max(durations)
        min_d = min(durations)
        lines.append(f'http_request_duration_seconds{{route="{key}",quantile="0.5"}} {_percentile(durations, 0.5):.6f}')
        lines.append(f'http_request_duration_seconds{{route="{key}",quantile="0.9"}} {_percentile(durations, 0.9):.6f}')
        lines.append(f'http_request_duration_seconds{{route="{key}",quantile="0.99"}} {_percentile(durations, 0.99):.6f}')
        lines.append(f'http_request_duration_seconds_sum{{route="{key}"}} {sum(durations):.6f}')
        lines.append(f'http_request_duration_seconds_count{{route="{key}"}} {len(durations)}')

    lines.append("# HELP http_requests_in_flight Currently in-flight requests")
    lines.append("# TYPE http_requests_in_flight gauge")
    lines.append(f"http_requests_in_flight {_metrics['http_requests_in_flight']}")

    return "\n".join(lines) + "\n"


def _percentile(sorted_data: list[float], p: float) -> float:
    """计算百分位数"""
    if not sorted_data:
        return 0.0
    data = sorted(sorted_data)
    k = (len(data) - 1) * p
    f = int(k)
    c = k - f if f + 1 < len(data) else 0
    return data[f] + c * (data[f + 1] if f + 1 < len(data) else 0 - data[f])
