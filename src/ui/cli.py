# -*- coding: utf-8 -*-
"""
CLI 交互界面 —— 命令行中的 Agent 对话

特性:
  - 流式打字机效果 (通过 Agent.stream)
  - 命令系统 (/help /stats /clear /tools /rag /recall /model /task /agent)
  - Tab 命令补全与历史记录 (prompt_toolkit)
  - 颜色渲染 (Rich 库)
  - 调试模式 (显示 Agent 内部状态)
  - Agent 管理 (/agent list, /agent new)
  - 任务发布 (/task publish, /task list, /task status)
"""

from __future__ import annotations
from typing import Optional
import sys
import os
import time
from datetime import datetime
import atexit

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

# ── readline 补全 / 历史 (prompt_toolkit 不兼容时的备选) ──
try:
    import readline
    READLINE_AVAILABLE = True
except ImportError:
    try:
        import pyreadline3 as readline
        READLINE_AVAILABLE = True
    except ImportError:
        READLINE_AVAILABLE = False

# ── prompt_toolkit (优先) ──
try:
    from prompt_toolkit import PromptSession
    from prompt_toolkit.history import FileHistory
    from prompt_toolkit.completion import Completer, Completion
    from prompt_toolkit.styles import Style
    from prompt_toolkit.formatted_text import HTML
    from prompt_toolkit.output import create_output as pt_create_output
    PT_AVAILABLE = True
except ImportError:
    PT_AVAILABLE = False

try:
    from rich.console import Console
    from rich.markdown import Markdown
    from rich.panel import Panel
    from rich.table import Table
    from rich.syntax import Syntax
    from rich.live import Live
    from rich.text import Text
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False

from src.core.agent import Agent, AgentEvent
from src.core.llm import LLMConfig


# ======== prompt_toolkit 补全器 ========

class _CommandCompleter(Completer):
    """Tab 补全器：补全命令 + 子命令 + 上下文参数"""

    def __init__(self, cli: "CLI"):
        self.cli = cli

    def get_completions(self, document, complete_event):
        text = document.text_before_cursor.lstrip()
        tokens = text.split() if text else []
        word_before = document.get_word_before_cursor()

        candidates = self._get_candidates(tokens, word_before, text)

        for c in candidates:
            if c.startswith(word_before):
                yield Completion(c, start_position=-len(word_before))

    def _get_candidates(
        self, tokens: list[str], word_before: str, text: str
    ) -> list[str]:
        """根据当前输入上下文返回候选列表"""

        # 空行或刚输入 / → 所有根命令
        if not tokens or (len(tokens) == 1 and text.rstrip().endswith("/")):
            return CLI.ROOT_COMMANDS

        first = tokens[0] if tokens else ""

        # ── /task 子命令 ──
        if first == "/task":
            if len(tokens) == 1:
                return CLI.TASK_SUB
            if len(tokens) == 2 and tokens[1] not in CLI.TASK_SUB:
                return CLI.TASK_SUB
            if tokens[1] == "list" and len(tokens) >= 2:
                return ["pending", "running", "completed", "failed", "cancelled"]
            if tokens[1] in ("status", "cancel") and len(tokens) >= 2:
                return self._get_task_ids()
            if tokens[1] == "publish":
                return self._publish_opts(tokens)

        # ── /agent 子命令 ──
        if first == "/agent":
            if len(tokens) == 1:
                return CLI.AGENT_SUB
            if len(tokens) == 2 and tokens[1] not in CLI.AGENT_SUB:
                return CLI.AGENT_SUB
            if tokens[1] == "create":
                return self._agent_create_opts(tokens)
            if tokens[1] in ("update", "delete"):
                return self._get_agent_names()

        # ── /orchestrate 子命令 ──
        if first == "/orchestrate":
            if len(tokens) == 1:
                return CLI.ORCH_SUB
            if len(tokens) == 2 and tokens[1] not in CLI.ORCH_SUB:
                return CLI.ORCH_SUB

        # ── /kb 子命令 ──
        if first == "/kb":
            if len(tokens) == 1:
                return CLI.KB_SUB
            if len(tokens) == 2 and tokens[1] not in CLI.KB_SUB:
                return CLI.KB_SUB

        # ── /config 子命令 ──
        if first == "/config":
            if len(tokens) == 1:
                return CLI.CONFIG_SUB
            if len(tokens) == 2 and tokens[1] not in CLI.CONFIG_SUB:
                return CLI.CONFIG_SUB
            if tokens[1] == "llm":
                if len(tokens) == 2:
                    return CLI.CONFIG_LLM_SUB
                if tokens[2] in ("set",):
                    if len(tokens) >= 4:
                        return ["openai", "deepseek", "zhipu", "qwen", "ollama", "custom"]
                elif tokens[2] in ("models",):
                    if "--key" in tokens or "--url" in tokens:
                        return ["openai", "deepseek", "zhipu", "qwen", "ollama", "custom"]
                    if len(tokens) >= 4:
                        return ["openai", "deepseek", "zhipu", "qwen", "ollama", "custom", "--key", "--url"]
                    return CLI.CONFIG_LLM_SUB

        # ── /recall → 不补全（自由文本）──
        if first == "/recall":
            return []

        # ── /model → 补全模型名 ──
        if first == "/model":
            if len(tokens) <= 2:
                return self._get_model_ids()

        # ── 默认：补全根命令 ──
        if len(tokens) == 1 and first.startswith("/"):
            return CLI.ROOT_COMMANDS

        return []

    def _get_task_ids(self) -> list[str]:
        try:
            from src.core.task_manager import get_task_manager
            tm = get_task_manager()
            tasks = tm.list_tasks(limit=50)
            return [t["id"] for t in tasks]
        except Exception:
            return []

    def _get_model_ids(self) -> list[str]:
        try:
            models = self.cli.agent.available_models()
            return [m["id"] for m in models]
        except Exception:
            return []

    def _get_agent_names(self) -> list[str]:
        try:
            from src.core.task_manager import get_task_manager
            tm = get_task_manager()
            agents = tm.list_agents()
            return [a["name"] for a in agents]
        except Exception:
            return []

    def _publish_opts(self, tokens: list[str]) -> list[str]:
        opts = ["--agent"]
        if "--agent" in tokens:
            idx = tokens.index("--agent")
            if idx + 1 >= len(tokens):
                return self._get_agent_names()
        return opts

    def _agent_create_opts(self, tokens: list[str]) -> list[str]:
        if len(tokens) == 2:
            return self._get_model_ids()
        if len(tokens) == 3:
            model_ids = self._get_model_ids()
            if tokens[2] in model_ids:
                return ["deepseek", "openai", "dashscope"]
            return model_ids
        if len(tokens) == 4:
            return ["deepseek", "openai", "dashscope"]
        return []


# ======== Agent 工厂 ========

def _create_side_agent(
    name: str,
    model: str,
    provider: str = "deepseek",
    skills: list[str] | None = None,
    description: str = "",
    system_prompt: str = "",
    max_iterations: int = 15,
    enable_planning: bool = False,
    enable_rag: bool = True,
    enable_reflection: bool = False,
):
    """创建并注册一个独立 Agent 到 TaskManager"""
    from src.core.task_manager import get_task_manager, AgentProxy
    from src.core.llm import create_llm

    config = LLMConfig(provider=provider, model=model)
    agent = Agent()
    agent.name = name

    # ── system_prompt: 优先用户自定义，否则自动生成 ──
    sp = (system_prompt or "").strip()[:5000]
    if not sp:
        skill_desc = f"专注于{'、'.join(skills)}" if skills else "通用"
        sp = f"你是 {name}，一个{skill_desc}的 AI 助手。请用你的专业知识高效完成用户的任务。"
    agent.system_prompt = sp

    agent.max_iterations = max(1, min(max_iterations, 50))
    agent.enable_planning = enable_planning
    agent.enable_rag = enable_rag
    agent.enable_reflection = enable_reflection
    agent.init(config)

    # 注册内置工具
    from src.tools.builtin_tools import register_all
    register_all(agent.tools)
    agent._rebuild_graph()

    tm = get_task_manager()
    skill_desc = f"专注于{'、'.join(skills)}" if skills else "通用"
    proxy = AgentProxy(
        name=name,
        agent=agent,
        skills=skills or [],
        description=description or f"{skill_desc}型 Agent",
    )
    tm.register_agent(proxy)
    tm.start_dispatcher()
    return agent


# ── readline 回退补全器 ──

class _ReadlineCompleter:
    """readline 兼容的补全器（当 prompt_toolkit 不可用时）"""

    def __init__(self, cli: "CLI"):
        self.cli = cli

    def complete(self, text: str, state: int):
        """readline 补全回调"""
        try:
            line = readline.get_line_buffer()
        except Exception:
            return None
        tokens = line.lstrip().split() if line else []
        cursor_pos = readline.get_endidx() if hasattr(readline, 'get_endidx') else len(line)

        candidates = self._get_candidates(tokens, text, line)
        filtered = [c for c in candidates if c.startswith(text)] if text else list(candidates)
        seen = set()
        unique = []
        for c in filtered:
            if c not in seen:
                seen.add(c)
                unique.append(c)
        try:
            return unique[state]
        except IndexError:
            return None

    def _get_candidates(self, tokens, word_before, text):
        if not tokens or (len(tokens) == 1 and text.rstrip().endswith("/")):
            return CLI.ROOT_COMMANDS
        first = tokens[0] if tokens else ""

        if first == "/task":
            if len(tokens) == 1:
                return CLI.TASK_SUB
            if len(tokens) == 2 and tokens[1] not in CLI.TASK_SUB:
                return CLI.TASK_SUB
            if tokens[1] == "list" and len(tokens) >= 2:
                return ["pending", "running", "completed", "failed", "cancelled"]
            if tokens[1] in ("status", "cancel") and len(tokens) >= 2:
                try:
                    from src.core.task_manager import get_task_manager
                    tm = get_task_manager()
                    return [t["id"] for t in tm.list_tasks(limit=50)]
                except Exception:
                    return []
            if tokens[1] == "publish":
                return ["--agent"]

        if first == "/agent":
            if len(tokens) == 1:
                return CLI.AGENT_SUB
            if len(tokens) == 2 and tokens[1] not in CLI.AGENT_SUB:
                return CLI.AGENT_SUB

        if first == "/orchestrate":
            if len(tokens) == 1:
                return CLI.ORCH_SUB

        if first == "/kb":
            if len(tokens) == 1:
                return CLI.KB_SUB

        if first == "/model" and len(tokens) <= 2:
            try:
                models = self.cli.agent.available_models()
                return [m["id"] for m in models]
            except Exception:
                return []

        if first == "/recall":
            return []

        if first == "/config":
            if len(tokens) == 1:
                return CLI.CONFIG_SUB
            if tokens[1] == "llm" and len(tokens) <= 3:
                return CLI.CONFIG_LLM_SUB
            return []

        if len(tokens) == 1 and first.startswith("/"):
            return CLI.ROOT_COMMANDS

        return []


class CLI:
    """命令行交互界面"""

    # ── 所有支持的命令（用于 Tab 补全）──
    ROOT_COMMANDS = [
        "/help", "/?", "/exit", "/quit", "/q",
        "/debug", "/tools", "/stats", "/clear",
        "/plan", "/rag", "/reflect",
        "/recall", "/kb_stats", "/model",
        "/task", "/agent",
        "/orchestrate", "/kb", "/files", "/sysinfo", "/config",
    ]
    TASK_SUB = ["publish", "list", "status", "queue", "cancel"]
    AGENT_SUB = ["list", "register", "unregister", "create", "update", "delete", "cleanup"]
    KB_SUB = ["upload", "search", "files", "delete", "clear"]
    ORCH_SUB = ["run", "detect", "modes"]
    CONFIG_SUB = ["show", "get", "llm"]
    CONFIG_LLM_SUB = ["show", "set", "models"]

    def __init__(self, agent: Agent):
        self.agent = agent
        self.debug = False
        self.show_tool_calls = True
        self.conversation_count = 0
        self._use_readline = False

        # 设置事件回调
        self.agent.on_event = self._on_agent_event

        if RICH_AVAILABLE:
            self.console = Console()
        else:
            self.console = None

        # ── 初始化 prompt_toolkit 会话 ──
        self._setup_prompt_toolkit()

    # ======== prompt_toolkit 补全 / 历史 ========

    def _setup_prompt_toolkit(self):
        """配置 Tab 补全、历史记录与输入样式"""
        self._history_file = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            ".cli_history",
        )
        self._session = None

        # 输入样式
        self._prompt_style = Style.from_dict({
            "prompt": "bold cyan",
            "": "",
        })

        if PT_AVAILABLE:
            # 检测终端兼容性：Windows PowerShell + VSCode 终端可能有不兼容
            try:
                pt_create_output()
            except Exception as e:
                print(f"[Warn] prompt_toolkit 与当前终端不兼容 ({e})，使用回退方案")

                # ── 回退：readline / pyreadline3 ──
                if READLINE_AVAILABLE:
                    self._setup_readline_fallback()
                    return
                else:
                    print("[Warn] Tab 补全不可用（请使用传统 cmd.exe 或安装 pyreadline3）")
                    return

            self._session = PromptSession(
                history=FileHistory(self._history_file),
                completer=_CommandCompleter(self),
                style=self._prompt_style,
                complete_while_typing=False,
            )
        else:
            print("[Warn] prompt_toolkit 未安装")

            # ── 回退：readline / pyreadline3 ──
            if READLINE_AVAILABLE:
                self._setup_readline_fallback()
            else:
                print("[Warn] 安装 prompt_toolkit 可获得 Tab 补全: pip install prompt_toolkit")

    def _setup_readline_fallback(self):
        """使用 readline/pyreadline3 作为备选补全方案"""
        try:
            readline.read_history_file(self._history_file)
        except (FileNotFoundError, OSError):
            pass
        readline.set_history_length(500)
        atexit.register(lambda: readline.write_history_file(self._history_file))

        self._readline_completer = _ReadlineCompleter(self)
        readline.set_completer(self._readline_completer.complete)
        readline.parse_and_bind("tab: complete")
        readline.set_completer_delims(" \t\n;")
        self._use_readline = True

    def _on_agent_event(self, event: AgentEvent, data):
        """Agent 内部事件 → UI 显示"""
        if event == AgentEvent.TOOL_CALL and self.show_tool_calls:
            self._print_tool_call(data)
        elif event == AgentEvent.TOOL_RESULT and self.show_tool_calls:
            self._print_tool_result(data)
        elif event == AgentEvent.ERROR and self.debug:
            self._print(f"[Error] {data.get('error', 'unknown')}", style="red")

    def _print(self, text: str, style: str = ""):
        """统一输出"""
        if self.console:
            self.console.print(text, style=style or None)
        else:
            print(text)

    def _print_tool_call(self, data):
        """显示工具调用"""
        name = data.get("name", "unknown")
        args = data.get("arguments", {})
        if self.console:
            self.console.print(
                f"  [tool] [cyan]{name}[/cyan] "
                f"[dim]({str(args)[:80]})[/dim]",
            )
        else:
            print(f"  [Tool] {name}({str(args)[:80]})")

    def _print_tool_result(self, data):
        """显示工具结果"""
        result = str(data.get("result", ""))[:120]
        success = data.get("success", False)
        icon = "[green]OK[/green]" if success else "[red]FAIL[/red]"
        if self.console:
            self.console.print(f"     {icon} [dim]{result}[/dim]")
        else:
            print(f"     {'OK' if success else 'FAIL'}: {result}")

    def _print_banner(self):
        banner = r"""
  +==========================================+
  |    SmartAgent - 智能 AI 助手              |
  |    思考 · 行动 · 观察 · 学习              |
  +==========================================+
"""
        if self.console:
            self.console.print(banner, style="bold cyan")
            self.console.print(
                f"  模型: {self.agent.llm.config.model if self.agent.llm else 'N/A'}"
                f"  |  Provider: {self.agent.llm.config.provider if self.agent.llm else 'N/A'}",
                style="dim",
            )
            self.console.print(
                f"  工具: {len(self.agent.tools)} 个"
                f"  |  LangChain: {'[green]启用[/green]' if self.agent._agent_graph else '[yellow]兼容模式[/yellow]'}",
                style="dim",
            )
            modes = []
            if self.agent.enable_planning:
                modes.append("[P] 计划")
            if self.agent.enable_rag:
                modes.append("[R] RAG")
            if self.agent.enable_reflection:
                modes.append("[V] 反思")
            self.console.print(
                f"  模式: {' '.join(modes) if modes else '(默认)'}",
                style="dim",
            )
            self.console.print(
                "  输入 /help 查看命令 | /exit 退出\n",
                style="dim",
            )
        else:
            print(banner)
            print(f"  模型: {self.agent.llm.config.model if self.agent.llm else 'N/A'}")
            print(f"  输入 /help 查看命令 | /exit 退出\n")

    def _print_response(self, text: str):
        """渲染 Agent 回复"""
        if self.console:
            self.console.print()
            try:
                md = Markdown(text)
                self.console.print(md)
            except Exception:
                self.console.print(text)
            self.console.print()
        else:
            print(f"\n[Agent] {text}\n")

    def _handle_command(self, cmd: str) -> bool:
        """处理斜杠命令，返回 False 表示退出"""
        parts = cmd.split()
        command = parts[0].lower()

        if command in ("/exit", "/quit", "/q"):
            print("再见!")
            return False

        elif command in ("/help", "/?"):
            self._show_help()

        elif command == "/debug":
            self.debug = not self.debug
            print(f"[Debug] 调试模式: {'开' if self.debug else '关'}")

        elif command == "/tools":
            self._show_tools()

        elif command == "/stats":
            self._show_stats()

        elif command == "/clear":
            self.agent.memory.short.clear()
            self.agent.memory.set_system(self.agent._build_system_prompt())
            print("对话已清空")

        elif command == "/plan":
            self.agent.enable_planning = not self.agent.enable_planning
            print(f"计划模式: {'开' if self.agent.enable_planning else '关'}")

        elif command == "/rag":
            self.agent.enable_rag = not self.agent.enable_rag
            print(f"RAG 知识库: {'开' if self.agent.enable_rag else '关'}")

        elif command == "/reflect":
            self.agent.enable_reflection = not self.agent.enable_reflection
            print(f"反思模式: {'开' if self.agent.enable_reflection else '关'}")

        elif command == "/recall" and len(parts) > 1:
            query = " ".join(parts[1:])
            results = self.agent.memory.recall(query)
            if results:
                print(f"[Memory] {results}")
            else:
                print("[Memory] 没有找到相关记忆")

        elif command == "/kb_stats":
            if self.agent.knowledge:
                s = self.agent.knowledge.stats()
                print(f"[KB] 知识库: {s['chunks']} 个文档块, {s['sources']} 个来源")
            else:
                print("[KB] 知识库未启用")

        elif command == "/model":
            self._handle_model_command(parts)

        elif command == "/task":
            self._handle_task_command(parts)

        elif command == "/agent":
            self._handle_agent_command(parts)

        elif command == "/orchestrate":
            self._handle_orchestrate_command(parts)

        elif command == "/kb":
            self._handle_kb_command(parts)

        elif command == "/files":
            self._handle_files_command(parts)

        elif command == "/sysinfo":
            self._handle_sysinfo_command()

        elif command == "/config":
            self._handle_config_command(parts)

        else:
            print(f"未知命令: {command}，输入 /help 查看帮助")

        return True

    def _handle_model_command(self, parts: list[str]):
        """处理 /model 命令"""
        models = self.agent.available_models()
        if len(parts) > 1:
            target = parts[1]
            found = next((m for m in models if m["id"] == target), None)
            if found:
                self.agent.switch_model(
                    model=found["id"],
                    provider=found["provider"],
                    base_url="" if found["provider"] == "openai" else None,
                )
                print(f"[Model] 已切换到 {found['name']} ({found['id']})")
            else:
                print(f"[Model] 未知模型: {target}")
                print(f"  可用: {', '.join(m['id'] for m in models)}")
        else:
            current = self.agent.llm.config.model if self.agent.llm else "N/A"
            provider = self.agent.llm.config.provider if self.agent.llm else "N/A"
            print(f"[Model] 当前提供商: {provider} | 当前模型: {current}")
            print(f"  可用模型 (来自 {provider} API):")
            for m in models:
                mark = " <-- 当前" if m["id"] == current else ""
                print(f"    {m['id']}{mark}")
            print(f"  用法: /model <模型id>")

    def _handle_task_command(self, parts: list[str]):
        """处理 /task 命令 —— 任务发布与管理"""
        from src.core.task_manager import get_task_manager
        tm = get_task_manager()

        if len(parts) < 2:
            print("用法:")
            print("  /task publish <描述>    发布新任务")
            print("  /task list [状态]      列出任务 (pending/running/completed)")
            print("  /task status <id>      查看任务详情")
            print("  /task queue            查看队列状态")
            print("  /task cancel <id>      取消任务")
            return

        sub = parts[1].lower()

        if sub == "publish" and len(parts) > 2:
            # 支持 --agent <名称> 指定执行 Agent
            desc_parts = parts[2:]
            target = ""
            if "--agent" in desc_parts:
                idx = desc_parts.index("--agent")
                if idx + 1 < len(desc_parts):
                    target = desc_parts[idx + 1]
                    desc_parts = desc_parts[:idx] + desc_parts[idx + 2:]
            desc = " ".join(desc_parts)
            tid = tm.publish(desc, priority=5, target_agent=target)
            print(f"[Task] 任务已发布: {tid}")
            print(f"  描述: {desc[:100]}")
            if target:
                print(f"  分配至: {target}")

        elif sub == "list":
            status = parts[2] if len(parts) > 2 else ""
            tasks = tm.list_tasks(status=status, limit=20)
            if not tasks:
                print("[Task] 暂无任务")
            else:
                for t in tasks:
                    status_icon = {
                        "pending": "○", "running": "◎",
                        "completed": "●", "failed": "✕", "cancelled": "−",
                    }.get(t["status"], "?")
                    print(f"  {status_icon} [{t['id']}] {t['status']:10s} {t['title']}")

        elif sub == "status" and len(parts) > 2:
            tid = parts[2]
            task = tm.get_task(tid)
            if task:
                print(f"\n{'='*50}")
                print(f"  任务: {task['title']}")
                print(f"  状态: {task['status']}  |  Agent: {task.get('assigned_agent', '未分配')}")
                print(f"  创建: {task['created_at']}")
                if task.get('started_at'):
                    print(f"  开始: {task['started_at']}")
                if task.get('finished_at'):
                    print(f"  完成: {task['finished_at']}")
                if task.get('error'):
                    print(f"  错误: {task['error']}")
                if task.get('result'):
                    print(f"  结果: {task['result'][:300]}")

                # 执行过程
                event_log = task.get("event_log", [])
                if event_log:
                    print(f"\n  --- 执行过程 ({len(event_log)} 个事件) ---")
                    for evt in event_log:
                        evt_name = evt.get("event", "?")
                        evt_time = evt.get("time", "")[-8:]  # HH:MM:SS
                        evt_data = str(evt.get("data", ""))[:80]
                        icon = {
                            "assigned": "📌", "think_start": "🤔", "think_end": "💡",
                            "tool_call": "🔧", "tool_result": "✅",
                            "plan_created": "📋", "completed": "🏁", "error": "❌",
                        }.get(evt_name, "  ")
                        print(f"    {evt_time} {icon} {evt_name}: {evt_data}")
                print(f"{'='*50}\n")
            else:
                print(f"[Task] 未找到任务: {tid}")

        elif sub == "queue":
            status = tm.queue_status()
            print(f"[Task] 队列状态:")
            print(f"  待处理: {status['pending']}")
            print(f"  执行中: {status['running']}")
            print(f"  已完成: {status['completed']}")
            print(f"  已失败: {status['failed']}")
            print(f"  Agent: {status['agents']} 个 (空闲: {status['idle_agents']})")

        elif sub == "cancel" and len(parts) > 2:
            tid = parts[2]
            tm.cancel_task(tid)
            print(f"[Task] 任务已取消: {tid}")

    def _handle_agent_command(self, parts: list[str]):
        """处理 /agent 命令 —— Agent 管理"""
        from src.core.task_manager import get_task_manager, AgentProxy
        tm = get_task_manager()

        if len(parts) < 2:
            print("用法:")
            print("  /agent list            列出所有 Agent")
            print("  /agent register        注册当前 Agent 到任务管理器")
            print("  /agent unregister      注销当前 Agent")
            print("  /agent create <名称> <模型> [provider] [--skills <技能>] [--prompt <提示词>]")
            print("                         创建新 Agent")
            print("  /agent update <名称> --skills <技能> [--desc <描述>] [--prompt <提示词>]")
            print("                         更新 Agent 配置")
            print("  /agent delete <名称>   删除 Agent")
            print("  /agent cleanup         清理无效 Agent")
            return

        sub = parts[1].lower()

        if sub == "list":
            agents = tm.list_agents()
            if not agents:
                print("[Agent] 暂无已注册 Agent")
            else:
                for a in agents:
                    status_icon = "●" if a["status"] == "idle" else "◎"
                    task_info = f" [{a['current_task_id']}]" if a.get("current_task_id") else ""
                    skills_str = f"  [{', '.join(a.get('skills', []))}]" if a.get("skills") else ""
                    desc_str = f" — {a.get('description', '')}" if a.get("description") else ""
                    print(f"  {status_icon} {a['name']} ({a['status']}){skills_str}{desc_str}{task_info}")

        elif sub == "register":
            proxy = AgentProxy(name=self.agent.name, agent=self.agent)
            tm.register_agent(proxy)
            tm.start_dispatcher()
            print(f"[Agent] 已注册并启动调度: {self.agent.name}")

        elif sub == "create":
            if len(parts) < 4:
                print("用法: /agent create <名称> <模型> [provider] [--skills <技能1,技能2>] [--prompt <自定义提示词>]")
                print("示例: /agent create 编码师 gpt-4o openai --skills coding,shell")
                print("      /agent create 研究员 deepseek-chat --skills research,writing")
                print("      /agent create 客服 deepseek-chat --prompt '你是一个专业客服，语气温和耐心'")
                print()
                print("可用技能标签: coding, research, writing, data, file_ops, shell")
                return
            agent_name = parts[2]
            model = parts[3]

            # 解析剩余参数
            remaining = parts[4:]
            provider = self.agent.llm.provider if self.agent.llm else "deepseek"
            skills: list[str] = []
            prompt = ""

            i = 0
            while i < len(remaining):
                if remaining[i] == "--skills" and i + 1 < len(remaining):
                    skills = [s.strip() for s in remaining[i + 1].split(",")]
                    i += 2
                elif remaining[i] == "--prompt" and i + 1 < len(remaining):
                    prompt = remaining[i + 1]
                    i += 2
                elif remaining[i] not in ("--skills", "--prompt"):
                    provider = remaining[i]
                    i += 1
                else:
                    i += 1

            _create_side_agent(agent_name, model, provider, skills=skills,
                               description=f"{'、'.join(skills)}型 Agent" if skills else "通用型 Agent",
                               system_prompt=prompt)
            skill_str = f", 技能: {', '.join(skills)}" if skills else ""
            prompt_str = ", 自定义Prompt" if prompt else ""
            print(f"[Agent] 已创建并注册: {agent_name} (模型: {model}, 供应商: {provider}{skill_str}{prompt_str})")

        elif sub == "unregister":
            tm.unregister_agent(self.agent.name)
            print(f"[Agent] 已注销: {self.agent.name}")

        elif sub == "update" and len(parts) > 2:
            name = parts[2]
            if name not in tm._agents:
                print(f"[Agent] Agent 不存在: {name}")
                return
            remaining = parts[3:]
            skills = None
            description = None
            prompt = None
            i = 0
            while i < len(remaining):
                if remaining[i] == "--skills" and i + 1 < len(remaining):
                    skills = [s.strip() for s in remaining[i + 1].split(",")]
                    i += 2
                elif remaining[i] == "--desc" and i + 1 < len(remaining):
                    description = remaining[i + 1]
                    i += 2
                elif remaining[i] == "--prompt" and i + 1 < len(remaining):
                    prompt = remaining[i + 1]
                    i += 2
                else:
                    i += 1
            proxy = tm._agents[name]
            agent_obj = proxy.agent
            if skills is not None:
                proxy.skills = skills
            if description is not None:
                proxy.description = description
            if prompt is not None:
                sp = prompt.strip()[:5000]
                if not sp:
                    skill_desc = f"专注于{'、'.join(proxy.skills)}" if proxy.skills else "通用"
                    sp = f"你是 {name}，一个{skill_desc}的 AI 助手。请用你的专业知识高效完成用户的任务。"
                agent_obj.system_prompt = sp
                if hasattr(agent_obj, '_rebuild_graph'):
                    agent_obj._rebuild_graph()
            print(f"[Agent] 已更新: {name}")

        elif sub == "delete" and len(parts) > 2:
            name = parts[2]
            if name not in tm._agents:
                print(f"[Agent] Agent 不存在: {name}")
                return
            tm.unregister_agent(name)
            print(f"[Agent] 已删除: {name}")

        elif sub == "cleanup":
            removed = []
            for aname in list(tm._agents.keys()):
                if not aname or not aname.strip():
                    tm.unregister_agent(aname)
                    removed.append(repr(aname))
            print(f"[Agent] 已清理 {len(removed)} 个无效 Agent")

    def _handle_orchestrate_command(self, parts: list[str]):
        """处理 /orchestrate 命令 —— 多 Agent 编排执行"""
        from src.core.task_manager import get_task_manager
        from src.core.orchestrator import patch_task_manager, ExecutionMode
        tm = get_task_manager()

        if len(parts) < 2:
            print("用法:")
            print("  /orchestrate run <描述> [--mode <模式>] [--agents <名称1,名称2>]")
            print("  /orchestrate detect <描述>      检测最佳执行模式")
            print("  /orchestrate modes               列出所有编排模式")
            print()
            print("模式: single / parallel / pipeline / collaborative / auto")
            return

        sub = parts[1].lower()

        if sub == "modes":
            for m in ExecutionMode:
                desc = {
                    ExecutionMode.SINGLE: "单 Agent 执行，适合简单问答",
                    ExecutionMode.PARALLEL: "多 Agent 并行执行，结果汇总",
                    ExecutionMode.PIPELINE: "Agent 串行接力，适合多步骤流程",
                    ExecutionMode.COLLABORATIVE: "Agent 团队讨论互审",
                    ExecutionMode.AUTO: "系统自动选择最优模式",
                }.get(m, "")
                print(f"  {m.value:15s} {desc}")

        elif sub == "detect" and len(parts) > 2:
            desc = " ".join(parts[2:])
            if not hasattr(tm, 'detect_best_mode'):
                patch_task_manager(tm)
            detection = tm.detect_best_mode(desc)
            print(f"[Orchestrate] 任务分析: {desc[:80]}...")
            print(f"  推荐模式: {detection.get('mode', 'single').upper()}")
            print(f"  原因: {detection.get('reason', 'N/A')}")

        elif sub == "run" and len(parts) > 2:
            # 解析参数
            remaining = parts[2:]
            mode = "auto"
            agent_names = None
            i = 0
            desc_parts = []
            while i < len(remaining):
                if remaining[i] == "--mode" and i + 1 < len(remaining):
                    mode = remaining[i + 1]
                    i += 2
                elif remaining[i] == "--agents" and i + 1 < len(remaining):
                    agent_names = [n.strip() for n in remaining[i + 1].split(",")]
                    i += 2
                else:
                    desc_parts.append(remaining[i])
                    i += 1
            desc = " ".join(desc_parts)

            if not hasattr(tm, 'execute_orchestrated'):
                patch_task_manager(tm)

            print(f"[Orchestrate] 启动编排执行...")
            print(f"  任务: {desc[:80]}...")
            print(f"  模式: {mode}")
            if agent_names:
                print(f"  Agent: {', '.join(agent_names)}")
            print(f"  (使用 /task status <id> 查看结果)")
            print()

            try:
                result = tm.execute_orchestrated(
                    description=desc,
                    title=desc[:50],
                    mode=mode,
                    agent_names=agent_names,
                )
                if result.success:
                    print(f"[Orchestrate] 执行完成!")
                    print(f"  模式: {result.mode.value}")
                    print(f"  参与 Agent: {', '.join(result.agents_used)}")
                    if result.final_result:
                        self._print_response(result.final_result[:2000])
                else:
                    print(f"[Orchestrate] 执行失败: {result.error}")
            except Exception as e:
                print(f"[Orchestrate] 执行异常: {e}")

    def _handle_kb_command(self, parts: list[str]):
        """处理 /kb 命令 —— 知识库管理"""
        if self.agent.knowledge is None:
            print("[KB] 知识库未启用，请在 config.yaml 中设置 rag.enabled: true")
            return

        if len(parts) < 2:
            print("用法:")
            print("  /kb upload <文件路径>    上传文件到知识库")
            print("  /kb search <查询> [top_k] 语义搜索知识库")
            print("  /kb files               列出已上传文件")
            print("  /kb delete <source_id>  删除指定来源")
            print("  /kb clear               清空知识库")
            return

        sub = parts[1].lower()

        if sub == "upload" and len(parts) > 2:
            path = parts[2]
            if not os.path.exists(path):
                print(f"[KB] 文件不存在: {path}")
                return
            try:
                from src.rag.document_loader import DocumentLoader
                loader = DocumentLoader()
                text = loader.load(path)
                source_id = os.path.basename(path)
                count = self.agent.knowledge.add_document(text, source_id=source_id)
                print(f"[KB] 已添加: {source_id} → {count} 个文档块")
            except Exception as e:
                print(f"[KB] 上传失败: {e}")

        elif sub == "search" and len(parts) > 2:
            top_k = 5
            if len(parts) > 3 and parts[3].isdigit():
                top_k = int(parts[3])
            query = " ".join(parts[2:]).split(str(top_k))[0].strip() if len(parts) > 3 and parts[3].isdigit() else " ".join(parts[2:])
            results = self.agent.knowledge.search(query, top_k=top_k)
            if results:
                print(f"\n[KB] 检索结果 (top {len(results)}):")
                for i, r in enumerate(results, 1):
                    src = r.get("source", "?")
                    score = r.get("score", 0)
                    text = r.get("text", "")[:150]
                    print(f"  {i}. [{src}] (score: {score:.3f})")
                    print(f"     {text}...")
                print()
            else:
                print("[KB] 没有找到相关文档")

        elif sub == "files":
            stats = self.agent.knowledge.stats()
            sources = stats.get("sources_list", [])
            if sources:
                print(f"\n[KB] 已上传文件 ({len(sources)} 个):")
                for s in sources:
                    print(f"  - {s}")
                print()
            else:
                print("[KB] 暂无已上传文件")

        elif sub == "delete" and len(parts) > 2:
            source_id = parts[2]
            try:
                self.agent.knowledge.delete_source(source_id)
                print(f"[KB] 已删除来源: {source_id}")
            except Exception as e:
                print(f"[KB] 删除失败: {e}")

        elif sub == "clear":
            if len(parts) > 2 and parts[2] == "--force":
                self.agent.knowledge.clear()
                print("[KB] 知识库已清空")
            else:
                print("[KB] 确认清空? 使用 /kb clear --force 确认")

    def _handle_files_command(self, parts: list[str]):
        """处理 /files 命令 —— 输出文件管理"""
        import os as _os
        from src.core.config import get_config, load_config

        try:
            cfg = get_config()
        except RuntimeError:
            cfg = load_config()
        output_dir = _os.path.abspath(cfg.tools.output_dir)

        if not _os.path.isdir(output_dir):
            print(f"[Files] 输出目录不存在: {output_dir}")
            return

        if len(parts) < 2 or parts[1].lower() == "list":
            files = []
            for root, dirs, filenames in _os.walk(output_dir):
                for fname in filenames:
                    fpath = _os.path.join(root, fname)
                    rel = _os.path.relpath(fpath, output_dir)
                    try:
                        size = _os.path.getsize(fpath)
                    except OSError:
                        size = 0
                    files.append((rel, size))
            files.sort(key=lambda x: x[0])

            if not files:
                print("[Files] 暂无输出文件")
            else:
                print(f"\n[Files] 输出文件 ({len(files)} 个):")
                for f, s in files:
                    s_str = f"{s}B" if s < 1024 else f"{s/1024:.1f}KB" if s < 1024*1024 else f"{s/1024/1024:.1f}MB"
                    print(f"  {f:50s} {s_str:>10s}")
                print()

        elif parts[1].lower() == "preview" and len(parts) > 2:
            fname = parts[2]
            fpath = _os.path.join(output_dir, fname) if not _os.path.isabs(fname) else fname
            if not _os.path.isfile(fpath):
                print(f"[Files] 文件不存在: {fname}")
                return
            try:
                with open(fpath, encoding="utf-8") as f:
                    content = f.read()
                if self.console:
                    from rich.syntax import Syntax
                    ext = _os.path.splitext(fname)[1].lstrip(".")
                    lang_map = {"py":"python","js":"javascript","ts":"typescript","json":"json",
                               "md":"markdown","yaml":"yaml","yml":"yaml","html":"html",
                               "css":"css","sql":"sql","sh":"bash"}
                    lang = lang_map.get(ext, "text")
                    syntax = Syntax(content[:5000], lang, line_numbers=True)
                    self.console.print(syntax)
                else:
                    print(content[:5000])
            except UnicodeDecodeError:
                print(f"[Files] 二进制文件不支持预览: {fname}")

    def _handle_sysinfo_command(self):
        """处理 /sysinfo 命令 —— 系统信息"""
        import platform
        import psutil
        from datetime import datetime

        process = psutil.Process()
        mem = process.memory_info()

        db_status = "disabled"
        try:
            from src.infrastructure.database import is_db_available
            db_status = "connected" if is_db_available() else "disabled"
        except Exception:
            pass

        print(f"\n--- 系统信息 ---")
        print(f"  Python:    {platform.python_version()}")
        print(f"  平台:      {platform.platform()}")
        print(f"  CPU 核心:  {psutil.cpu_count()}")
        print(f"  内存使用:  {mem.rss / 1024 / 1024:.1f} MB")
        uptime = round((datetime.now() - datetime.fromtimestamp(process.create_time())).total_seconds())
        print(f"  运行时间:  {uptime}s ({uptime//3600}h {uptime%3600//60}m)")
        print(f"  数据库:    {db_status}")
        print(f"  Agent:     {self.agent.name}")
        if self.agent.llm:
            print(f"  模型:      {self.agent.llm.config.model}")
            print(f"  供应商:    {self.agent.llm.config.provider}")
        print(f"  工具数:    {len(self.agent.tools)}")
        print()

    def _handle_config_command(self, parts: list[str]):
        """处理 /config 命令 —— 配置读写"""
        import yaml

        config_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            "config.yaml",
        )

        if not os.path.exists(config_path):
            print("[Config] config.yaml 不存在")
            return

        with open(config_path, encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}

        if len(parts) < 2 or parts[1].lower() == "show":
            print(f"\n--- 当前配置 ---")
            for section, values in cfg.items():
                if isinstance(values, dict):
                    print(f"  [{section}]")
                    for k, v in values.items():
                        # 隐藏敏感信息
                        display_v = "***" if "key" in k.lower() or "password" in k.lower() or "secret" in k.lower() else v
                        print(f"    {k}: {display_v}")
                else:
                    print(f"  {section}: {values}")
            print()

        elif parts[1].lower() == "get" and len(parts) > 2:
            key = parts[2]
            keys = key.split(".")
            val = cfg
            for k in keys:
                val = val.get(k, {}) if isinstance(val, dict) else None
            if val is not None and not isinstance(val, dict):
                print(f"[Config] {key} = {val}")
            elif isinstance(val, dict):
                for k, v in val.items():
                    print(f"  {k}: {v}")
            else:
                print(f"[Config] 未找到: {key}")

        elif parts[1].lower() == "llm":
            self._handle_config_llm(parts[2:], cfg, config_path)

        else:
            print(f"[Config] 未知子命令: {parts[1]}")
            print("  用法: /config show | /config get <键> | /config llm <子命令>")

    def _handle_config_llm(self, parts: list[str], cfg: dict, config_path: str):
        """处理 /config llm 子命令"""
        import yaml
        from src.core.agent import Agent

        if len(parts) < 1:
            print("[Config LLM] 用法:")
            print("  /config llm show          查看当前 LLM 配置")
            print("  /config llm set <provider> <model> [--key <api_key>] [--url <base_url>]")
            print("                            设置 LLM 提供商和模型")
            print("  /config llm models [provider] [--key <api_key>] [--url <base_url>]")
            print("                            查询可用模型列表")
            print("  支持 provider: openai / deepseek / zhipu / qwen / ollama / custom")
            return

        sub = parts[0].lower()

        if sub == "show":
            llm = cfg.get("llm", {})
            print(f"\n--- LLM 配置 ---")
            print(f"  提供商:    {llm.get('provider', 'N/A')}")
            print(f"  模型:      {llm.get('model', 'N/A')}")
            key = llm.get('api_key', '')
            print(f"  API Key:   {'***' if key else '(从环境变量读取)'}")
            print(f"  Base URL:  {llm.get('base_url', '(自动)')}")
            print(f"  Temp:      {llm.get('temperature', 'N/A')}")
            print(f"  MaxTokens: {llm.get('max_tokens', 'N/A')}")
            print()

        elif sub == "set" and len(parts) >= 2:
            provider = parts[1]
            model = parts[2] if len(parts) > 2 else ""
            api_key = ""
            base_url = ""

            i = 3
            while i < len(parts):
                if parts[i] == "--key" and i + 1 < len(parts):
                    api_key = parts[i + 1]
                    i += 2
                elif parts[i] == "--url" and i + 1 < len(parts):
                    base_url = parts[i + 1]
                    i += 2
                else:
                    i += 1

            valid_providers = ["openai", "deepseek", "zhipu", "qwen", "ollama", "custom"]
            if provider not in valid_providers:
                print(f"[Config LLM] 不支持的提供商: {provider}")
                print(f"  可用: {', '.join(valid_providers)}")
                return

            # 写入配置
            cfg.setdefault("llm", {})
            cfg["llm"]["provider"] = provider
            cfg["llm"]["api_key"] = api_key
            if model:
                cfg["llm"]["model"] = model
            if base_url:
                cfg["llm"]["base_url"] = base_url
            elif "base_url" in cfg.get("llm", {}):
                cfg["llm"].pop("base_url", None)

            with open(config_path, "w", encoding="utf-8") as f:
                yaml.dump(cfg, f, allow_unicode=True, default_flow_style=False)

            print(f"[Config LLM] 已将提供商设置为 {provider}")

            # 运行时切换
            if self.agent.llm:
                actual_model = model or self.agent.llm.config.model
                try:
                    self.agent.switch_model(
                        model=actual_model,
                        provider=provider,
                        api_key=api_key or None,
                        base_url=base_url or None,
                    )
                    print(f"[Config LLM] 已切换到 {actual_model} (provider: {provider})")
                except Exception as e:
                    print(f"[Config LLM] 运行时切换失败 (需重启): {e}")

        elif sub == "models":
            # 解析参数
            provider = ""
            api_key = ""
            base_url = ""
            i = 0
            while i < len(parts):
                if i == 0 and not parts[0].startswith("--"):
                    provider = parts[0]
                    i += 1
                elif parts[i] == "--key" and i + 1 < len(parts):
                    api_key = parts[i + 1]
                    i += 2
                elif parts[i] == "--url" and i + 1 < len(parts):
                    base_url = parts[i + 1]
                    i += 2
                else:
                    i += 1

            if not provider:
                provider = cfg.get("llm", {}).get("provider", "deepseek")

            # 用当前 agent 的 key/url 兜底
            if not api_key and self.agent.llm:
                api_key = self.agent.llm.config.api_key
            if not base_url and self.agent.llm:
                base_url = self.agent.llm.config.base_url

            print(f"[Config LLM] 查询 {provider} 的模型列表...")
            models = Agent.query_models(
                provider=provider,
                api_key=api_key,
                base_url=base_url,
            )
            if models:
                print(f"{provider} 可用模型 ({len(models)} 个):")
                for m in models:
                    print(f"  - {m['id']}")
            else:
                print(f"  未获取到模型列表（请检查 API Key 和网络连接）")

        else:
            print(f"[Config LLM] 未知子命令: {sub}")
            print("  用法: /config llm show | set | models")

    def _show_help(self):
        help_text = """
        --- SmartAgent 命令列表 ---
        ─────────────────────────────────────────
        对话命令:
          /help, /?           显示此帮助
          /exit, /q           退出程序
          /clear              清空对话记忆
          /debug              切换调试模式

        工具与状态:
          /tools              列出所有已注册工具
          /stats              显示运行统计
          /kb_stats           知识库统计
          /sysinfo            显示系统信息 (CPU/内存/运行时间)

        模式切换:
          /plan               切换任务计划模式
          /rag                切换 RAG 知识库增强
          /reflect            切换自我反思模式

        模型管理:
          /model              查看当前模型
          /model <id>         切换模型

        记忆检索:
          /recall <查询>      搜索长期记忆

        任务管理:
          /task publish <描述> [--agent <名称>]  发布新任务
          /task list [状态]      列出任务
          /task status <id>      查看任务详情
          /task queue            查看队列状态
          /task cancel <id>      取消任务

        多 Agent 编排:
          /orchestrate run <描述> [--mode <模式>] [--agents <名称>]
                                 多 Agent 编排执行任务
          /orchestrate detect <描述>  检测最佳执行模式
          /orchestrate modes          列出所有编排模式

        Agent 管理:
          /agent list            列出所有 Agent
          /agent register        注册当前 Agent
          /agent unregister      注销当前 Agent
          /agent create <名称> <模型> [provider] [--skills <技能>] [--prompt <提示词>]
                                 创建新 Agent
          /agent update <名称> --skills <技能> [--desc <描述>] [--prompt <提示词>]
                                 更新 Agent 配置
          /agent delete <名称>   删除 Agent
          /agent cleanup         清理无效 Agent

        知识库管理:
          /kb upload <文件>      上传文件到知识库
          /kb search <查询>      语义搜索知识库
          /kb files              列出已上传文件
          /kb delete <来源>      删除指定来源
          /kb clear --force      清空知识库

        文件管理:
          /files list            列出输出文件
          /files preview <名称>  预览文本文件

        配置管理:
          /config show           查看当前配置
          /config get <键>       查看指定配置项
          /config llm show       查看 LLM 提供商配置
          /config llm set <provider> <model> [--key <api_key>] [--url <base_url>]
                                 设置 LLM 提供商和模型
          /config llm models [provider] [--key <api_key>]
                                 查询提供商可用模型列表
        """
        print(help_text)

    def _show_tools(self):
        print(f"\n--- 已注册工具 ({len(self.agent.tools)} 个) ---")
        for tool in self.agent.tools.list_all():
            danger = " [!]" if tool.dangerous else ""
            params = ", ".join(tool.parameters.keys())
            print(f"  - {tool.name}({params}){danger}")
            print(f"    {tool.description}")
        print()

    def _show_stats(self):
        print(f"\n--- 运行统计 ---")
        print(f"  对话轮数: {self.conversation_count}")
        print(f"  短期记忆消息数: {len(self.agent.memory.short)}")
        print(f"  工具数量: {len(self.agent.tools)}")
        print(f"  LangChain Agent: {'启用' if self.agent._agent_graph else '兼容模式'}")
        if self.agent.knowledge:
            s = self.agent.knowledge.stats()
            print(f"  知识库: {s['chunks']} 块, {s['sources']} 个来源")
        print()

    def start(self):
        """启动交互循环"""
        self._print_banner()

        while True:
            try:
                if self._session:
                    user_input = self._session.prompt(
                        HTML("<prompt>>> </prompt>"),
                    ).strip()
                else:
                    user_input = input(">> ").strip()
            except (KeyboardInterrupt, EOFError):
                print("\n再见!")
                break

            if not user_input:
                continue

            # 太短的输入提示
            if len(user_input) <= 2 and not user_input.startswith("/"):
                print("[Hint] 输入太短了，试着描述清楚你想做什么？")
                continue

            # 处理命令
            if user_input.startswith("/"):
                if not self._handle_command(user_input):
                    break
                continue

            self.conversation_count += 1

            try:
                if self.console:
                    with self.console.status("[cyan]思考中...[/cyan]"):
                        result = self.agent.run(user_input)
                else:
                    print("[Thinking] 思考中...")
                    result = self.agent.run(user_input)
            except KeyboardInterrupt:
                print("\n再见!")
                break
            except Exception as e:
                if self.debug:
                    import traceback
                    traceback.print_exc()
                print(f"[Error] 错误: {e}")
                continue

            self._print_response(result)


def main():
    """CLI 入口函数"""
    import yaml
    from src.core.llm import LLMConfig
    from src.tools.builtin_tools import register_all, ALL_BUILTIN_TOOLS
    from src.core.agent import Agent

    # 1. 加载配置
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        "config.yaml",
    )

    if os.path.exists(config_path):
        with open(config_path, encoding="utf-8") as f:
            config_data = yaml.safe_load(f)
    else:
        config_data = {}

    # 2. 创建 LLM 配置
    llm_cfg_data = config_data.get("llm", {})
    llm_config = LLMConfig(
        provider=llm_cfg_data.get("provider", "openai"),
        model=llm_cfg_data.get("model", "gpt-4o"),
        api_key=llm_cfg_data.get("api_key", ""),
        base_url=llm_cfg_data.get("base_url", ""),
        temperature=float(llm_cfg_data.get("temperature", 0.7)),
        max_tokens=int(llm_cfg_data.get("max_tokens", 4096)),
        timeout=int(llm_cfg_data.get("timeout", 60)),
    )

    # 3. 检查 API Key
    if not llm_config.resolve_api_key():
        print("=" * 60)
        print("[Error] 未检测到 API Key!")
        print()
        print("请设置以下环境变量之一:")
        print("  PowerShell: $env:OPENAI_API_KEY='sk-xxx'")
        print("  bash:       export OPENAI_API_KEY=sk-xxx")
        print()
        print("提示: OPENAI_API_KEY 可作为所有 provider 的通用 fallback")
        print()
        print("支持的提供商专用变量:")
        print("  - OPENAI_API_KEY    (OpenAI / 通用兜底)")
        print("  - DEEPSEEK_API_KEY  (DeepSeek)")
        print("  - DASHSCOPE_API_KEY (阿里通义)")
        print("  - ZHIPU_API_KEY     (智谱)")
        print(f"  当前 provider: {llm_config.provider}")
        print("=" * 60)
        return

    # 3.5. 初始化 Tracing
    from src.core.tracing import init_tracing
    tracing_cfg = config_data.get("tracing", {})
    init_tracing(
        project=tracing_cfg.get("project", "smart_agent"),
        enabled=tracing_cfg.get("enabled", False),
    )

    # 4. 创建 Agent
    agent = Agent()
    agent.init(llm_config)
    agent.system_prompt = config_data.get("agent", {}).get(
        "system_prompt",
        "你是一个智能 AI 助手，具备工具使用、文件操作、代码执行等能力。",
    )
    agent.max_iterations = config_data.get("agent", {}).get("max_iterations", 15)
    agent.verbose = config_data.get("agent", {}).get("verbose", True)

    # 5. 注册工具
    register_all(agent.tools)
    agent._rebuild_graph()

    # 5.5. 自动注册当前 Agent 到 TaskManager
    from src.core.task_manager import get_task_manager, AgentProxy
    tm = get_task_manager()
    agent_name = config_data.get("agent", {}).get("name", "SmartAgent")
    proxy = AgentProxy(
        name=agent_name,
        agent=agent,
        skills=config_data.get("agent", {}).get("skills", ["通用"]),
        description="默认 Agent，CLI 启动时自动注册",
    )
    tm.register_agent(proxy)
    tm.start_dispatcher()

    # 6. 启动 CLI
    cli = CLI(agent)
    cli.start()


if __name__ == "__main__":
    main()
