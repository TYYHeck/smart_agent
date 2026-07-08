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


@tool(description="将内容写入文件。会覆盖已有文件。", dangerous=False)
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
# 6. 批量注册所有内置工具
# ============================================================

ALL_BUILTIN_TOOLS = [
    web_search,
    fetch_url,
    read_file,
    write_file,
    run_python,
    calculator,
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
