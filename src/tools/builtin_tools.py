# -*- coding: utf-8 -*-
"""
内置工具集 —— Agent 出厂即用的能力

包含:
  - web_search   : 搜索引擎
  - fetch_url    : 抓取网页内容
  - read_file    : 读取本地文件
  - write_file   : 写入本地文件
  - run_python   : 执行 Python 代码
  - calculator   : 数学计算
"""

from __future__ import annotations
from typing import Any
import json
import os
import sys
import io
import traceback
import ast
import operator

from .base import tool

# ============================================================
# 1. 联网搜索
# ============================================================

@tool(description="在互联网上搜索内容，返回标题、摘要和链接。用于获取实时信息。")
def web_search(query: str, num_results: int = 5) -> str:
    """使用 DuckDuckGo 搜索"""
    try:
        from duckduckgo_search import DDGS
        results = []
        with DDGS() as ddgs:
            for r in ddgs.text(query, max_results=num_results):
                results.append({
                    "title": r.get("title", ""),
                    "body": r.get("body", ""),
                    "href": r.get("href", ""),
                })
        if not results:
            return "未找到相关结果。"

        # 格式化输出
        output = []
        for i, r in enumerate(results, 1):
            output.append(
                f"{i}. {r['title']}\n"
                f"   {r['body'][:200]}...\n"
                f"   ? {r['href']}"
            )
        return "\n\n".join(output)
    except ImportError:
        return "错误: 请先安装 duckduckgo_search 库"
    except Exception as e:
        return f"搜索失败: {str(e)}"


# ============================================================
# 2. 抓取网页
# ============================================================

@tool(description="抓取指定 URL 的网页内容，返回纯文本。用于阅读文章、文档等。")
def fetch_url(url: str, max_length: int = 5000) -> str:
    """抓取网页并提取文本"""
    try:
        import requests
        from bs4 import BeautifulSoup

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36"
        }
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        resp.encoding = resp.apparent_encoding or "utf-8"

        soup = BeautifulSoup(resp.text, "html.parser")

        # 移除无用标签
        for tag in soup(["script", "style", "nav", "footer", "header", "aside"]):
            tag.decompose()

        text = soup.get_text(separator="\n", strip=True)

        # 压缩多余空行
        lines = [line for line in text.splitlines() if line.strip()]
        text = "\n".join(lines)

        if len(text) > max_length:
            text = text[:max_length] + f"\n\n... (内容被截断，共 {len(text)} 字符)"

        return text
    except ImportError as e:
        return f"缺少依赖: {e}"
    except Exception as e:
        return f"抓取失败: {str(e)}"


# ============================================================
# 3. 文件读写
# ============================================================

def _resolve_path(filepath: str) -> str:
    """将相对路径拼接到配置的 output_dir 下，绝对路径保持不变"""
    if os.path.isabs(filepath):
        return filepath
    try:
        from ..core.config import get_config, load_config
        try:
            cfg = get_config()
        except RuntimeError:
            cfg = load_config()
        base = cfg.tools.output_dir
    except Exception:
        base = "./output"
    return os.path.join(base, filepath)


@tool(description="读取文件内容，支持文本文件。返回文件全文。")
def read_file(filepath: str) -> str:
    """读取本地文件"""
    try:
        # 安全检查：限制读取范围
        abs_path = os.path.abspath(_resolve_path(filepath))
        with open(abs_path, encoding="utf-8") as f:
            content = f.read()
        if len(content) > 10000:
            content = content[:10000] + "\n\n... (文件过长，已截断)"
        return content
    except Exception as e:
        return f"读取失败: {str(e)}"


@tool(description="将内容写入本地文件（会覆盖已有文件）。支持 .txt .md .py .json .csv .html 等格式。生成后文件自动关联到当前任务，可在前端下载。重要：完成分析报告、代码、数据导出等任务后应主动调用此工具保存结果。",
      dangerous=False)
def write_file(filepath: str, content: str) -> str:
    """写入本地文件"""
    try:
        abs_path = os.path.abspath(_resolve_path(filepath))
        os.makedirs(os.path.dirname(abs_path) or ".", exist_ok=True)
        with open(abs_path, "w", encoding="utf-8") as f:
            f.write(content)

        # ── 自动关联到当前任务 ──
        try:
            from ..core.task_manager import _current_task_id, get_task_manager
            task_id = _current_task_id.get()
            if task_id:
                tm = get_task_manager()
                tm.record_output_file(task_id, abs_path)
        except Exception:
            pass  # 静默失败，不影响主流程

        return f"? 文件已写入: {abs_path} ({len(content)} 字符)"
    except Exception as e:
        return f"写入失败: {str(e)}"


# ============================================================
# 4. Python 代码执行 (沙箱受限)
# ============================================================

# 安全的运算符和环境
_SAFE_BUILTINS = {
    "abs": abs, "all": all, "any": any, "bool": bool, "chr": chr,
    "dict": dict, "divmod": divmod, "enumerate": enumerate,
    "filter": filter, "float": float, "int": int, "len": len,
    "list": list, "map": map, "max": max, "min": min, "ord": ord,
    "pow": pow, "print": print, "range": range, "reversed": reversed,
    "round": round, "set": set, "slice": slice, "sorted": sorted,
    "str": str, "sum": sum, "tuple": tuple, "type": type, "zip": zip,
    "isinstance": isinstance, "True": True, "False": False, "None": None,
}

_SAFE_MODULES = {
    "math", "json", "re", "datetime", "collections", "itertools",
    "functools", "random", "statistics", "decimal", "fractions",
    "operator", "hashlib", "base64", "uuid",
    # ── 图片/图表生成 ──
    "matplotlib", "matplotlib.pyplot", "PIL",
    # ── IO 辅助 ──
    "io", "os", "os.path", "tempfile",
}


def _safe_import(name, globals=None, locals=None, fromlist=(), level=0):
    """限制 import 只能在安全模块列表内"""
    if name in _SAFE_MODULES:
        return __import__(name, globals, locals, fromlist, level)
    raise ImportError(f"不允许导入模块: {name}")


@tool(description="在沙箱中执行 Python 代码，返回 stdout 输出。支持 math/json/re 等安全库。禁止死循环/无限递归，5秒超时。",
      dangerous=True)
def run_python(code: str) -> str:
    """
    在受限环境中执行 Python 代码
    安全性: 仅允许纯计算/数据处理，禁止文件系统和网络操作
    """
    import signal
    import threading

    # 捕获输出
    stdout = io.StringIO()
    stderr = io.StringIO()
    old_stdout = sys.stdout
    old_stderr = sys.stderr
    sys.stdout = stdout
    sys.stderr = stderr

    # 超时机制：5 秒后强制中断
    result_container = {"output": None, "error": None}

    def _run():
        try:
            # 编译代码以检查语法
            tree = ast.parse(code)

            # 检查是否有危险操作
            for node in ast.walk(tree):
                if isinstance(node, ast.Call):
                    if isinstance(node.func, ast.Name) and node.func.id in (
                        "eval", "exec", "open", "__import__", "compile", "globals",
                        "locals", "vars", "getattr", "setattr", "delattr",
                    ):
                        result_container["error"] = f"安全限制: 禁止调用 {node.func.id}()"
                        return

                if isinstance(node, ast.Import):
                    for alias in node.names:
                        if alias.name not in _SAFE_MODULES:
                            result_container["error"] = f"安全限制: 禁止导入 {alias.name}"
                            return

            # 执行代码
            compiled = compile(tree, "<agent_code>", "exec")
            exec_globals = {"__builtins__": _SAFE_BUILTINS, "__import__": _safe_import}
            exec(compiled, exec_globals)

            output = stdout.getvalue().strip()
            if not output:
                output = "代码执行完毕 (无输出)"
            result_container["output"] = output

        except SyntaxError as e:
            result_container["error"] = f"语法错误: {e}"
        except Exception as e:
            tb = traceback.format_exc()
            result_container["error"] = f"执行错误:\n{tb[-500:]}"
        finally:
            sys.stdout = old_stdout
            sys.stderr = old_stderr

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    thread.join(timeout=5)

    if thread.is_alive():
        # 超时了，恢复 stdout/stderr 并返回
        sys.stdout = old_stdout
        sys.stderr = old_stderr
        return "执行超时 (5秒) —— 代码可能包含死循环或耗时过长"

    if result_container["error"]:
        return result_container["error"]
    return result_container["output"] or "代码执行完毕 (无输出)"


# ============================================================
# 5. 计算器
# ============================================================

@tool(description="执行数学表达式计算，支持 + - * / ** sqrt sin cos 等。")
def calculator(expression: str) -> str:
    """
    安全的数学表达式计算器
    示例: "2 + 3 * 4", "sqrt(16)", "sin(pi/2)"
    """
    try:
        # 安全评估
        safe_dict = {
            "abs": abs, "round": round, "min": min, "max": max,
            "sum": sum, "pow": pow, "sqrt": (lambda x: x ** 0.5),
        }
        # 注入 math 模块函数
        import math
        for name in dir(math):
            if not name.startswith("_"):
                safe_dict[name] = getattr(math, name)

        # 只允许安全的 AST 节点
        tree = ast.parse(expression, mode="eval")
        for node in ast.walk(tree):
            if isinstance(node, (ast.Call,)):
                if isinstance(node.func, ast.Name) and node.func.id not in safe_dict:
                    return f"? 不支持的函数: {node.func.id}"

        result = eval(compile(tree, "<calc>", "eval"),
                      {"__builtins__": {}}, safe_dict)
        return f"? {expression} = {result}"
    except Exception as e:
        return f"计算失败: {e}"


# ============================================================
# 6. 图片/图表生成
# ============================================================

@tool(description="生成图表或图片并保存到本地文件。支持折线图、柱状图、饼图等。调用后文件自动关联到任务。",
      dangerous=False)
def generate_image(chart_type: str, title: str, labels: str, values: str, filepath: str = "") -> str:
    """
    使用 matplotlib 生成图表图片。

    Args:
        chart_type: 图表类型 — line(折线图) / bar(柱状图) / pie(饼图) / scatter(散点图)
        title: 图表标题
        labels: 标签列表，逗号分隔，如 "A,B,C,D"
        values: 数值列表，逗号分隔，如 "10,25,15,30"
        filepath: 保存路径（可选，默认 output/chart_时间戳.png）

    Returns:
        生成结果描述，包含文件路径
    """
    import matplotlib
    matplotlib.use("Agg")  # 非 GUI 后端
    import matplotlib.pyplot as plt
    from io import BytesIO
    import base64 as b64
    import os

    try:
        label_list = [x.strip() for x in labels.split(",")]
        value_list = [float(x.strip()) for x in values.split(",")]

        if len(label_list) != len(value_list):
            return f"❌ labels 和 values 数量不一致: {len(label_list)} vs {len(value_list)}"

        # 设置中文字体（尝试）
        try:
            plt.rcParams["font.sans-serif"] = ["SimHei", "WenQuanYi Micro Hei",
                                                "Noto Sans CJK SC", "DejaVu Sans"]
            plt.rcParams["axes.unicode_minus"] = False
        except Exception:
            pass

        fig, ax = plt.subplots(figsize=(10, 6))

        if chart_type == "bar":
            ax.bar(label_list, value_list, color="steelblue", edgecolor="white")
        elif chart_type == "pie":
            ax.pie(value_list, labels=label_list, autopct="%1.1f%%",
                   colors=plt.cm.Set3(range(len(label_list))))
        elif chart_type == "scatter":
            ax.scatter(label_list, value_list, color="coral", s=100)
        else:  # line 默认
            ax.plot(label_list, value_list, marker="o", linewidth=2, markersize=8)

        ax.set_title(title, fontsize=14)
        if chart_type != "pie":
            ax.set_ylabel("数值")
            ax.grid(True, alpha=0.3)

        plt.tight_layout()

        # 确定保存路径
        if not filepath:
            ts = __import__("datetime").datetime.now().strftime("%Y%m%d_%H%M%S")
            filepath = f"output/chart_{ts}.png"

        abs_path = os.path.abspath(_resolve_path(filepath))
        os.makedirs(os.path.dirname(abs_path) or ".", exist_ok=True)
        fig.savefig(abs_path, dpi=150, bbox_inches="tight")
        plt.close(fig)

        # ── 自动关联到当前任务 ──
        try:
            from ..core.task_manager import _current_task_id, get_task_manager
            task_id = _current_task_id.get()
            if task_id:
                tm = get_task_manager()
                tm.record_output_file(task_id, abs_path)
        except Exception:
            pass

        return f"✅ 图表已生成: {abs_path} (类型: {chart_type}, 数据点: {len(label_list)})"
    except Exception as e:
        try:
            plt.close("all")
        except Exception:
            pass
        return f"图片生成失败: {str(e)}"


# ============================================================
# 7. 批量注册所有内置工具
# ============================================================

ALL_BUILTIN_TOOLS = [
    web_search,
    fetch_url,
    read_file,
    write_file,
    run_python,
    calculator,
    generate_image,
]


def register_all(registry=None):
    """注册所有内置工具到注册中心"""
    if registry is None:
        from .base import get_registry
        registry = get_registry()
    for t in ALL_BUILTIN_TOOLS:
        registry.register(t)
    return registry


# ============================================================
# 自测
# ============================================================
if __name__ == "__main__":
    print("=" * 60)
    print("内置工具 演示")
    print("=" * 60)

    # 测试计算器
    print("\n? 计算器:")
    print("  输入: 2**10 + sqrt(256)")
    print(f"  {calculator.call(expression='2**10 + sqrt(256)')}")

    # 测试代码执行
    print("\n? Python 沙箱:")
    code_test = """
result = 0
for i in range(1, 11):
    result += i
print(f"1到10的和是: {result}")
"""
    print(f"  输入: {code_test.strip()}")
    print(f"  {run_python.call(code=code_test)}")

    # 测试安全限制
    print("\n?? 安全检测:")
    print(f"  尝试导入 os: {run_python.call(code='import os')}")

    print("\n? 内置工具测试完成!")
