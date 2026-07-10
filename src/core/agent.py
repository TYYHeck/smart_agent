# -*- coding: utf-8 -*-
"""
Agent 核心 —— 基于 LangChain 1.x 的 ReAct 智能体引擎

架构设计:
  - 使用 langgraph.prebuilt.create_react_agent 构建 ReAct Agent (CompiledStateGraph)
  - 不再使用 AgentExecutor（LangChain 1.x 已移除）
  - 集成 Callback 体系实现事件通知、流式输出、日志记录
  - 支持计划模式（Plan-and-Execute）、RAG 增强、反思模式

LangChain 组件映射:
  - ChatOpenAI           → LLM 引擎
  - StructuredTool       → 工具定义
  - ConversationBufferMemory → 短期记忆
  - Chroma               → 长期记忆 / RAG
"""

from __future__ import annotations
from typing import Any, ClassVar, Optional, Callable, AsyncIterator, Iterator
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime
import os
import json
import traceback
import logging

from langgraph.prebuilt import create_react_agent
from langchain_core.messages import (
    BaseMessage, HumanMessage, AIMessage, SystemMessage, ToolMessage
)
from langchain_core.tools import BaseTool
from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.runnables import RunnableConfig
from langchain_core.outputs import LLMResult
from uuid import UUID

from .llm import BaseLLM, create_llm, LLMConfig
from ..tools.base import ToolRegistry, get_registry, Tool
from ..memory.memory_manager import MemoryManager
from ..rag.knowledge_base import KnowledgeBase

logger = logging.getLogger("smart_agent.agent")


# ============================================================
# Agent 状态枚举
# ============================================================

class AgentState(Enum):
    IDLE = "idle"
    THINKING = "thinking"
    ACTING = "acting"
    OBSERVING = "observing"
    FINISHED = "finished"
    ERROR = "error"


class AgentEvent(Enum):
    """Agent 事件类型 —— 用于回调/UI 推送"""
    THINK_START = "think_start"
    THINK_END = "think_end"
    TOOL_CALL = "tool_call"
    TOOL_RESULT = "tool_result"
    TASK_COMPLETE = "task_complete"
    ERROR = "error"
    PLAN_CREATED = "plan_created"
    LLM_TOKEN = "llm_token"
    INTERRUPT_REQUEST = "interrupt_request"   # Agent 请求中断（询问用户）
    INTERRUPT_RESUME = "interrupt_resume"     # Agent 从中断恢复
    AGENT_THINK = "agent_think"              # 带 Agent 名称的思考标记
    WORKFLOW_ALLOC = "workflow_alloc"         # LLM 驱动的工作流分配结果


# ============================================================
# LangChain Callback 适配器
# ============================================================

class AgentCallbackHandler(BaseCallbackHandler):
    """
    将 LangChain 内部事件桥接到 SmartAgent 的事件体系

    覆盖的回调:
      - on_llm_start/end   → THINK_START / THINK_END
      - on_tool_start/end  → TOOL_CALL / TOOL_RESULT
      - on_llm_new_token   → LLM_TOKEN (流式)
      - on_chain_error     → ERROR
    """

    def __init__(self, agent: "Agent"):
        self._agent = agent
        self._current_tool_calls: dict[str, dict] = {}

    def on_llm_start(
        self, serialized: dict[str, Any], prompts: list[str],
        *, run_id: UUID, parent_run_id: UUID | None = None,
        tags: list[str] | None = None, metadata: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        self._agent.state = AgentState.THINKING
        self._agent._emit(AgentEvent.THINK_START, {"run_id": str(run_id)})

    def on_llm_end(
        self, response: LLMResult, *, run_id: UUID,
        parent_run_id: UUID | None = None, **kwargs: Any,
    ) -> None:
        self._agent._emit(AgentEvent.THINK_END, {
            "run_id": str(run_id),
            "token_usage": str(response.llm_output) if response.llm_output else None,
        })

    def on_tool_start(
        self, serialized: dict[str, Any], input_str: str,
        *, run_id: UUID, parent_run_id: UUID | None = None,
        tags: list[str] | None = None, metadata: dict[str, Any] | None = None,
        inputs: dict[str, Any] | None = None, **kwargs: Any,
    ) -> None:
        self._agent.state = AgentState.ACTING
        tool_name = serialized.get("name", "unknown")
        self._current_tool_calls[str(run_id)] = {
            "name": tool_name,
            "input": inputs or {},
        }
        self._agent._emit(AgentEvent.TOOL_CALL, {
            "name": tool_name,
            "arguments": inputs or {},
            "run_id": str(run_id),
        })
        self._agent._log(f"[Tool] 调用工具: {tool_name}({json.dumps(inputs or {}, ensure_ascii=False)})")

    def on_tool_end(
        self, output: Any, *, run_id: UUID,
        parent_run_id: UUID | None = None, **kwargs: Any,
    ) -> None:
        self._agent.state = AgentState.OBSERVING
        call_info = self._current_tool_calls.pop(str(run_id), {})
        output_str = str(output)[:500] if output else ""
        self._agent._emit(AgentEvent.TOOL_RESULT, {
            "tool": call_info.get("name", "unknown"),
            "success": True,
            "result": output_str,
            "run_id": str(run_id),
        })
        self._agent._log(f"  [OK] 结果: {output_str[:200]}")

    def on_tool_error(
        self, error: BaseException, *, run_id: UUID,
        parent_run_id: UUID | None = None, **kwargs: Any,
    ) -> None:
        call_info = self._current_tool_calls.pop(str(run_id), {})
        self._agent._emit(AgentEvent.ERROR, {
            "tool": call_info.get("name", "unknown"),
            "error": str(error),
        })
        self._agent._log(f"  [X] 错误: {error}")

    def on_chain_error(
        self, error: BaseException, *, run_id: UUID,
        parent_run_id: UUID | None = None, **kwargs: Any,
    ) -> None:
        self._agent.state = AgentState.ERROR
        self._agent._emit(AgentEvent.ERROR, {"error": str(error)})

    def on_llm_new_token(
        self, token: str, *, chunk=None, run_id: UUID,
        parent_run_id: UUID | None = None, **kwargs: Any,
    ) -> None:
        self._agent._emit(AgentEvent.LLM_TOKEN, {"token": token})


# ============================================================
# Agent 主类
# ============================================================

@dataclass
class Agent:
    """
    智能体主体 —— 基于 LangGraph create_react_agent

    简单用法:
        agent = Agent(llm_config=LLMConfig(provider="openai"))
        agent.system_prompt = "你是智能助手"
        result = agent.run("帮我查一下今天深圳的天气")

    带回调的用法:
        def on_event(event, data):
            print(f"[{event}] {data}")

        agent.on_event = on_event
        result = agent.run("写一个快速排序")

    流式用法:
        for chunk in agent.stream("分析这个项目"):
            print(chunk, end="")
    """

    # --- 核心组件 ---
    llm: Optional[BaseLLM] = None
    tools: ToolRegistry = field(default_factory=get_registry)
    memory: MemoryManager = field(default_factory=MemoryManager)
    knowledge: Optional[KnowledgeBase] = None

    # --- LangChain 内部组件 ---
    _agent_graph: Any = None                    # CompiledStateGraph
    _callback_handler: Optional[AgentCallbackHandler] = None
    _langchain_tools: list[BaseTool] = field(default_factory=list)
    _checkpointer: Any = None                   # LangGraph MemorySaver
    _thread_id: str = ""                        # 会话线程 ID (用于 checkpoint)

    # --- 配置 ---
    name: str = "SmartAgent"
    max_iterations: int = 15
    verbose: bool = True
    system_prompt: str = "你是一个智能 AI 助手。"

    # --- 能力开关 ---
    enable_planning: bool = False
    enable_rag: bool = True
    enable_reflection: bool = False

    # --- 状态 ---
    state: AgentState = AgentState.IDLE
    current_plan: list[str] = field(default_factory=list)
    iteration_count: int = 0

    # --- 事件回调 ---
    on_event: Optional[Callable[[AgentEvent, Any], None]] = None

    # --- 中断支持 ---
    _pending_interrupt: bool = field(default=False, repr=False)
    _interrupt_answer: str | None = field(default=None, repr=False)

    # --- 指标收集 ---
    metrics: Any = field(default_factory=lambda: None)  # MetricsCollector, 懒初始化

    # ======== 事件发射 ========

    def _emit(self, event: AgentEvent, data: Any = None):
        """发射事件给回调，自动附加 Agent 名称"""
        if self.on_event:
            try:
                if isinstance(data, dict):
                    data.setdefault("agent_name", self.name)
                else:
                    data = {"data": data, "agent_name": self.name}
                self.on_event(event, data)
            except Exception:
                pass

    def request_interrupt(self, question: str) -> str | None:
        """
        请求中断：向用户提问并等待回复
        
        Args:
            question: 向用户提出的问题
            
        Returns:
            用户的回复字符串，如果超时则返回 None
        """
        self._emit(AgentEvent.INTERRUPT_REQUEST, {
            "question": question,
            "agent_name": self.name,
        })
        # 中断回复由外部通过 resume_from_interrupt() 注入
        self._pending_interrupt = True
        return None  # 实际值由外部设置

    def resume_from_interrupt(self, answer: str):
        """从中断恢复，注入用户回答"""
        self._pending_interrupt = False
        self._interrupt_answer = answer
        self._emit(AgentEvent.INTERRUPT_RESUME, {
            "answer": answer,
            "agent_name": self.name,
        })

    # 中断状态
    _pending_interrupt: bool = False
    _interrupt_answer: str | None = None

    def _log(self, msg: str):
        """日志输出"""
        if self.verbose:
            logger.info(msg)

    # ======== 初始化 ========

    def init(self, llm_config: LLMConfig):
        """
        初始化 Agent:
          1. 创建 LLM 客户端
          2. 设置系统提示词
          3. 构建 LangChain Agent Graph
        """
        self.llm = create_llm(llm_config)
        self.memory.set_system(self._build_system_prompt())
        self._callback_handler = AgentCallbackHandler(self)
        self._rebuild_graph()
        return self

    def _rebuild_graph(self):
        """
        （重新）构建 LangChain CompiledStateGraph

        每次工具集变更或模型切换时调用。
        LangGraph 中 create_react_agent 返回 CompiledStateGraph，
        不再需要 AgentExecutor 包装。
        """
        if self.llm is None:
            return

        # 1. 获取 LangChain 格式的 ChatModel
        lc_llm = self.llm.as_langchain()
        if lc_llm is None:
            logger.warning("当前 LLM 不支持 LangChain 模式，将使用兼容模式")
            self._agent_graph = None
            return

        # 2. 构建工具列表
        self._langchain_tools = self._build_langchain_tools()

        # 3. 构建 system_prompt (纯字符串即可，LangChain 1.x 自动包装为 SystemMessage)
        system_prompt = self._build_full_system_prompt()

        # 4. 创建 Agent (返回 CompiledStateGraph)
        try:
            # MemorySaver: 跨轮对话状态持久化 (checkpoint)
            from langgraph.checkpoint.memory import MemorySaver
            self._checkpointer = MemorySaver()
            self._thread_id = f"session_{self.name}_{datetime.now().strftime('%Y%m%d%H%M%S')}"

            self._agent_graph = create_react_agent(
                model=lc_llm,
                tools=self._langchain_tools,
                prompt=system_prompt,
                name=self.name,
                checkpointer=self._checkpointer,
            )
            self._log(f"[Agent] LangChain Agent Graph 构建成功 (含 MemorySaver), 工具数: {len(self._langchain_tools)}")
        except Exception as e:
            logger.warning(f"创建 LangChain Agent 失败: {e}，将使用兼容模式")
            self._agent_graph = None

    def _build_langchain_tools(self) -> list[BaseTool]:
        """将内部 Tool 转为 LangChain StructuredTool"""
        lc_tools = []
        for tool in self.tools.list_all():
            lc_tool = tool.to_langchain_tool()
            if lc_tool:
                lc_tools.append(lc_tool)
        return lc_tools

    def _build_full_system_prompt(self) -> str:
        """
        构建完整的 system prompt

        使用 LangChain ChatPromptTemplate 的标准方式：
          - system prompt = 用户配置 + 能力标注
          - 自动注入身份保护和中文输出要求
        """
        try:
            from langchain_core.prompts import ChatPromptTemplate

            template_parts: list[str] = [self.system_prompt]

            # 身份保护
            template_parts.append(
                "重要：你的名字是 SmartAgent，你不是 DeepSeek、ChatGPT 或任何其他 AI 产品。"
                "当用户问你的身份时，回答你是 SmartAgent。"
            )

            # 能力说明 —— 强制工具优先
            if self.tools and len(self.tools) > 0:
                tool_names = [t.name for t in self.tools.list_all()]
                template_parts.append(
                    f"你有 {len(self.tools)} 个工具可用: {', '.join(tool_names)}。\n"
                    "核心规则:\n"
                    "- 当任务需要实时数据、外部信息、文件操作或代码执行时，必须调用工具获取，"
                    "绝对不能只凭训练数据中的知识直接回答！\n"
                    "- 涉及具体数据、表格、数值信息时，必须先搜索验证，再整理输出。\n"
                    "- 需要生成本地文件的，用 write_file 工具写出（支持 .txt、.md、.py、.json、.csv、.html 等文本格式）。\n"
                    "- 需要生成图表/图片的，用 generate_image 工具创建（支持折线图、柱状图、饼图等）。\n"
                    "- 需要图片处理或复杂绘图的，用 run_python 工具调用 matplotlib/PIL 库。\n"
                    "- 完成任务后应主动生成分析报告文件（Markdown 格式），必要时附带图表。\n"
                    "- 输出最终总结给用户。"
                )

            if self.enable_planning:
                template_parts.append("复杂任务先分解再逐步执行。")
            if self.enable_rag:
                template_parts.append("需要背景知识时，优先使用知识库检索。")
            if self.enable_reflection:
                template_parts.append("给出最终答案前，自我检查一遍是否准确完整。")

            template_parts.append("用中文回答用户。")

            # 用 ChatPromptTemplate 组装
            system_text = "\n".join(template_parts)
            return ChatPromptTemplate.from_messages([
                ("system", system_text),
            ]).format()

        except ImportError:
            # 回退到字符串拼接
            return self._build_full_system_prompt_fallback()

    def _build_system_prompt(self) -> str:
        """构建纯文本系统提示（给 Memory 系统用）"""
        return self.system_prompt

    def _build_full_system_prompt_fallback(self) -> str:
        """字符串拼接回退方案"""
        parts = [self.system_prompt]
        parts.append(
            "\n重要：你的名字是 SmartAgent，你不是 DeepSeek、ChatGPT 或任何其他 AI 产品。"
            "当用户问你的身份时，回答你是 SmartAgent。"
        )
        if self.tools and len(self.tools) > 0:
            tool_names = [t.name for t in self.tools.list_all()]
            parts.append(
                f"\n你有 {len(self.tools)} 个工具可用: {', '.join(tool_names)}。"
                " 当任务需要实时数据、外部信息时，必须先调用工具，不能凭训练记忆回答。"
                " 需要生成本地文件的，用 write_file 工具写出。"
            )
        if self.enable_planning:
            parts.append("- 复杂任务先分解再逐步执行")
        if self.enable_rag:
            parts.append("- 需要背景知识时，优先使用知识库检索")
        if self.enable_reflection:
            parts.append("- 给出最终答案前，自我检查一遍是否准确完整")
        parts.append("- 用中文回答用户")
        return "\n".join(parts)

    # ======== 模型管理 ========

    def switch_model(self, model: str, provider: str | None = None,
                     base_url: str | None = None, api_key: str | None = None) -> str:
        """
        运行时切换模型（保留记忆和工具）
        """
        if self.llm is None:
            raise RuntimeError("Agent 尚未初始化")

        old_config = self.llm.config
        new_config = LLMConfig(
            provider=provider or old_config.provider,
            model=model,
            api_key=api_key if api_key is not None else old_config.api_key,
            base_url=base_url if base_url is not None else old_config.base_url,
            temperature=old_config.temperature,
            max_tokens=old_config.max_tokens,
            timeout=old_config.timeout,
        )
        self.llm = create_llm(new_config)
        self._rebuild_graph()
        self._log(f"[Model] 已切换到 {model} (provider: {new_config.provider})")
        return model

    # ── 各 provider 的本地兜底模型列表（API 不可达时使用）──
    _FALLBACK_MODELS: ClassVar[dict[str, list[dict]]] = {
        "deepseek": [
            {"id": "deepseek-chat", "name": "DeepSeek Chat", "provider": "deepseek"},
            {"id": "deepseek-reasoner", "name": "DeepSeek Reasoner", "provider": "deepseek"},
        ],
        "openai": [
            {"id": "gpt-4o", "name": "GPT-4o", "provider": "openai"},
            {"id": "gpt-4o-mini", "name": "GPT-4o Mini", "provider": "openai"},
            {"id": "gpt-4-turbo", "name": "GPT-4 Turbo", "provider": "openai"},
        ],
        "qwen": [
            {"id": "qwen-plus", "name": "通义千问 Plus", "provider": "qwen"},
            {"id": "qwen-max", "name": "通义千问 Max", "provider": "qwen"},
            {"id": "qwen-turbo", "name": "通义千问 Turbo", "provider": "qwen"},
        ],
        "zhipu": [
            {"id": "glm-4", "name": "智谱 GLM-4", "provider": "zhipu"},
            {"id": "glm-4-flash", "name": "智谱 GLM-4 Flash", "provider": "zhipu"},
        ],
        "ollama": [],
    }

    def available_models(self) -> list[dict]:
        """实时查询当前 provider 的可选模型列表（API 不可达时用兜底列表）"""
        if self.llm is None:
            return []

        config = self.llm.config
        provider = config.provider
        api_key = config.resolve_api_key()
        base_url = config.resolve_base_url().rstrip("/")
        current_model = config.model

        try:
            import urllib.request as urllib_req
            import json as _json

            if provider == "ollama":
                req = urllib_req.Request("http://localhost:11434/api/tags")
                resp = urllib_req.urlopen(req, timeout=5)
                data = _json.loads(resp.read().decode())
                models = []
                for m in data.get("models", []):
                    m_id = m.get("name", "")
                    if not m_id:
                        continue
                    models.append({
                        "id": m_id, "name": m_id, "provider": "ollama",
                    })
                return models or self._FALLBACK_MODELS.get(provider, [])

            # OpenAI 兼容: GET {base_url}/models
            headers = {"Authorization": f"Bearer {api_key}"}
            url = f"{base_url}/models"
            req = urllib_req.Request(url, headers=headers)
            resp = urllib_req.urlopen(req, timeout=10)
            data = _json.loads(resp.read().decode())

            models = []
            for m in data.get("data", []):
                m_id = m.get("id", "")
                if not m_id:
                    continue
                # 过滤掉明显不属于当前 provider 的模型
                # （如 openai 列表里出现的 huggingface 模型）
                if not self._belongs_to_provider(m_id, provider):
                    continue
                models.append({
                    "id": m_id, "name": m_id, "provider": provider,
                })

            if models:
                # 标记当前使用的模型
                for m in models:
                    if m["id"] == current_model:
                        m["name"] = f"{m['id']} (当前)"
                        break
                return models

        except Exception as e:
            self._log(f"[Model] API 查询模型列表失败: {e}，使用本地兜底列表")

        # 兜底列表也标记当前模型
        fallback = [dict(m) for m in self._FALLBACK_MODELS.get(provider, [])]
        for m in fallback:
            if m["id"] == current_model:
                m["name"] = f"{m['id']} (当前)"
                break
        return fallback

    @staticmethod
    def query_models(provider: str, api_key: str = "", base_url: str = "",
                     timeout: int = 10) -> list[dict]:
        """静态方法：查询任意 provider 的可用模型列表（无需 Agent 实例）"""
        import urllib.request as _urllib
        import json as _json

        if provider == "ollama":
            try:
                req = _urllib.Request("http://localhost:11434/api/tags")
                resp = _urllib.urlopen(req, timeout=5)
                data = _json.loads(resp.read().decode())
                return [{"id": m.get("name", ""), "name": m.get("name", ""), "provider": "ollama"}
                        for m in data.get("models", []) if m.get("name")]
            except Exception:
                return []

        # 构建 base_url
        if not base_url:
            base_map = {
                "openai": "https://api.openai.com/v1",
                "deepseek": "https://api.deepseek.com",
                "zhipu": "https://open.bigmodel.cn/api/paas/v4",
                "qwen": "https://dashscope.aliyuncs.com/compatible-mode/v1",
            }
            base_url = base_map.get(provider, "https://api.openai.com/v1")
        base_url = base_url.rstrip("/")

        # 解析 API key
        if not api_key:
            env_map = {"openai": "OPENAI_API_KEY", "deepseek": "DEEPSEEK_API_KEY",
                       "zhipu": "ZHIPU_API_KEY", "qwen": "DASHSCOPE_API_KEY"}
            env_key = env_map.get(provider, "OPENAI_API_KEY")
            api_key = os.environ.get(env_key, "")
        api_key = Agent._resolve_key(api_key)

        try:
            headers = {"Authorization": f"Bearer {api_key}"}
            url = f"{base_url}/models"
            req = _urllib.Request(url, headers=headers)
            resp = _urllib.urlopen(req, timeout=timeout)
            data = _json.loads(resp.read().decode())

            models = []
            for m in data.get("data", []):
                m_id = m.get("id", "")
                if not m_id:
                    continue
                if not Agent._belongs_to_provider(m_id, provider):
                    continue
                models.append({"id": m_id, "name": m_id, "provider": provider})
            return models
        except Exception:
            # 兜底
            return [dict(m) for m in Agent._FALLBACK_MODELS.get(provider, [])]

    @staticmethod
    def _resolve_key(api_key: str) -> str:
        """解析 ${VAR} 环境变量引用"""
        import re
        m = re.match(r'^\$\{(\w+)\}$', api_key.strip())
        if m:
            return os.environ.get(m.group(1), "")
        return api_key

    @staticmethod
    def _belongs_to_provider(model_id: str, provider: str) -> bool:
        """判断 model_id 是否属于当前 provider（简单过滤）"""
        prefix_map = {
            "deepseek": ["deepseek"],
            "openai": ["gpt-", "o1", "o3", "davinci", "babbage", "whisper"],
            "qwen": ["qwen"],
            "zhipu": ["glm-", "chatglm", "cogview"],
        }
        prefixes = prefix_map.get(provider, [])
        m_lower = model_id.lower()
        return any(m_lower.startswith(p) for p in prefixes)

    # ======== 主运行循环 ========

    def run(self, task: str, context: str = "") -> str:
        """
        执行用户任务

        Args:
            task: 用户任务描述
            context: 可选的附加上下文

        Returns:
            Agent 的最终回复
        """
        self.state = AgentState.THINKING
        self.iteration_count = 0
        self.current_plan = []

        self._log(f"[Task] 收到任务: {task}")
        self._emit(AgentEvent.THINK_START, {"task": task})

        # --- 0. RAG 增强 ---
        rag_context = ""
        if self.enable_rag and self.knowledge:
            try:
                kb_results = self.knowledge.search_formatted(task)
                if kb_results:
                    rag_context = kb_results
                    self._log("[RAG] 知识库检索到相关内容")
            except Exception as e:
                self._log(f"[RAG] 知识库检索失败: {e}")

        # --- 1. 计划模式 ---
        if self.enable_planning and self._task_is_complex(task):
            plan = self._make_plan(task)
            if plan:
                self.current_plan = plan
                self._emit(AgentEvent.PLAN_CREATED, {"plan": plan})

        # --- 2. 添加用户消息到记忆 ---
        from .message import Message
        self.memory.short.add_user(task)
        if rag_context:
            self.memory.short.add(Message.system(
                f"[补充知识库上下文]\n{rag_context}"
            ))

        # --- 3. 执行 Agent ---
        try:
            if self._agent_graph is not None:
                result = self._run_with_langchain(task, rag_context)
            else:
                result = self._run_with_fallback(task, rag_context)
        except Exception as e:
            error_msg = f"Agent 执行失败: {e}"
            self._log(f"[Error] {error_msg}\n{traceback.format_exc()}")
            self._emit(AgentEvent.ERROR, {"error": error_msg})
            self.state = AgentState.ERROR
            return error_msg

        # --- 4. 反思检查 ---
        if self.enable_reflection:
            result = self._reflect(result, task)

        self.state = AgentState.FINISHED
        self._log(f"[OK] 任务完成")
        self._emit(AgentEvent.TASK_COMPLETE, {
            "result": result,
            "iterations": self.iteration_count,
        })

        return result

    def _run_with_langchain(self, task: str, rag_context: str = "") -> str:
        """
        使用 LangChain CompiledStateGraph 执行任务

        LangChain 1.x 中:
          - graph.invoke({"messages": [...]}) 同步执行
          - recursion_limit 控制最大步数（每个迭代可能 model + tool + 反思等多步）
          - 返回 {"messages": [...]}，最后一条 AIMessage 包含最终答案
        """
        # 构建消息列表
        from .message import Message

        input_messages: list[BaseMessage] = []

        # 添加 RAG 上下文
        if rag_context:
            input_messages.append(HumanMessage(content=(
                f"[系统上下文]\n{rag_context}\n\n[用户任务]\n{task}"
            )))
        else:
            input_messages.append(HumanMessage(content=task))

        # 对话历史已由 LangChain 的 system_prompt 管理，
        # agent graph 自动维护消息列表

        # 估算 recursion_limit: 每个迭代可能包含 model call + tool call + 反思等多步
        # 安全值: max_iterations * 6（足够应对复杂工具调用链）
        config: dict[str, Any] = {
            "recursion_limit": self.max_iterations * 6,
            "configurable": {"thread_id": self._thread_id},
        }
        if self._callback_handler:
            config["callbacks"] = [self._callback_handler]

        # 执行
        result = self._agent_graph.invoke(  # type: ignore[union-attr]
            {"messages": input_messages},
            config=RunnableConfig(config),
        )

        # 解析结果
        messages: list[BaseMessage] = result.get("messages", [])

        # 统计工具调用次数
        tool_count = sum(1 for m in messages if isinstance(m, ToolMessage))
        self.iteration_count = tool_count

        # 提取最终回复 (最后一条 AIMessage)
        final_message = ""
        for m in reversed(messages):
            if isinstance(m, AIMessage) and m.content:
                final_message = str(m.content)
                break

        # 诊断日志
        if tool_count == 0:
            self._log(f"[Agent] ⚠️ LLM 未调用任何工具，直接给出了文本回复"
                      f" ({len(final_message)} 字符)")
        else:
            self._log(f"[Agent] 调用了 {tool_count} 次工具")

        # 将结果存入记忆
        if final_message:
            self.memory.short.add(Message.assistant(final_message))

        return final_message or "（Agent 未产生有效回复）"

    def _run_with_fallback(self, task: str, rag_context: str = "") -> str:
        """
        兼容模式：当 LLM 不支持 LangChain 时，用原始 ReAct 循环

        保留此路径以兼容自定义 LLM 提供商
        """
        from .message import Message, ToolCall, ToolResult

        self.memory.short.add_user(task)

        for i in range(self.max_iterations):
            self.iteration_count = i + 1
            self.state = AgentState.THINKING

            try:
                context_msgs = self.memory.get_context()
                llm_response = self.llm.chat(  # type: ignore[union-attr]
                    messages=context_msgs,
                    tools=self.tools.to_llm_format() if len(self.tools) > 0 else None,
                )
            except Exception as e:
                error_msg = f"LLM 调用失败: {e}"
                self._emit(AgentEvent.ERROR, {"error": error_msg})
                self.state = AgentState.ERROR
                return error_msg

            self.memory.add_message(llm_response)

            if not llm_response.tool_calls:
                self.state = AgentState.FINISHED
                return llm_response.content

            # 执行工具
            self.state = AgentState.ACTING
            for tc in llm_response.tool_calls:
                self._emit(AgentEvent.TOOL_CALL, {
                    "name": tc.name,
                    "arguments": tc.arguments,
                })
                result_data = self.tools.execute(tc.name, **tc.arguments)
                tr = ToolResult(
                    call_id=tc.id,
                    name=tc.name,
                    success=result_data.get("success", False),
                    result=result_data.get("result"),
                    error=result_data.get("error"),
                )
                self._emit(AgentEvent.TOOL_RESULT, {
                    "tool": tc.name,
                    "success": tr.success,
                })
                self.memory.add_message(Message.tool_result(tr))

        # 达到最大迭代次数
        return self._force_conclude()

    # ======== 流式执行 ========

    def stream(self, task: str) -> Iterator[str]:
        """
        流式执行任务 —— 逐 token 返回

        用法:
            for chunk in agent.stream("分析这个项目"):
                print(chunk, end="")
        """
        if self._agent_graph is None:
            # 兼容模式：非流式
            yield self.run(task)
            return

        self.state = AgentState.THINKING
        from .message import Message
        self.memory.short.add_user(task)

        try:
            input_messages: list[BaseMessage] = [HumanMessage(content=task)]
            config = RunnableConfig(
                recursion_limit=self.max_iterations * 6,
                callbacks=[self._callback_handler] if self._callback_handler else None,
            )

            # 使用 astream_events 获取细粒度流
            async def _collect():
                chunks: list[str] = []
                async for event in self._agent_graph.astream_events(  # type: ignore[union-attr]
                    {"messages": input_messages},
                    config=config,
                    version="v2",
                ):
                    kind = event.get("event", "")
                    if kind == "on_chat_model_stream":
                        chunk_data = event.get("data", {}).get("chunk", {})
                        if hasattr(chunk_data, "content") and chunk_data.content:
                            chunks.append(chunk_data.content)
                    elif kind == "on_tool_start":
                        name = event.get("name", "unknown")
                        self._emit(AgentEvent.TOOL_CALL, {"name": name})
                    elif kind == "on_tool_end":
                        name = event.get("name", "unknown")
                        self._emit(AgentEvent.TOOL_RESULT, {
                            "tool": name,
                            "success": True,
                        })
                return "".join(chunks)

            import asyncio
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                # 没有运行中的事件循环
                result = asyncio.run(_collect())
                yield result
            else:
                # 已有事件循环（如 Jupyter），用同步方式
                yield self.run(task)

        except Exception as e:
            self._emit(AgentEvent.ERROR, {"error": str(e)})
            yield f"\n[错误: {e}]"

        self.state = AgentState.FINISHED
        self._emit(AgentEvent.TASK_COMPLETE, {"result": ""})

    # ======== 流式事件 (Web SSE 专用) ========

    async def stream_events(self, task: str) -> AsyncIterator[dict]:
        """
        异步流式执行，yield 结构化事件（供 Web SSE 使用）

        事件类型: text / tool_call / tool_result / error / done

        用法:
            async for event in agent.stream_events("搜索新闻"):
                if event["type"] == "text":
                    yield f"data: {json.dumps(event)}\\n\\n"
        """
        from .message import Message

        # RAG 上下文
        rag_context = ""
        if self.enable_rag and self.knowledge:
            try:
                kb_results = self.knowledge.search_formatted(task)
                if kb_results:
                    rag_context = kb_results
            except Exception as e:
                self._log(f"[RAG] 流式知识库检索失败: {e}")

        self.state = AgentState.THINKING
        self.memory.short.add_user(task)

        # 兼容模式
        if self._agent_graph is None:
            result = self.run(task)
            yield {"type": "text", "content": result}
            yield {"type": "done"}
            return

        # LangChain 路径
        try:
            input_messages: list[BaseMessage] = []
            if rag_context:
                input_messages.append(HumanMessage(content=(
                    f"[系统上下文]\n{rag_context}\n\n[用户任务]\n{task}"
                )))
            else:
                input_messages.append(HumanMessage(content=task))

            config: dict[str, Any] = {
                "recursion_limit": self.max_iterations * 6,
                "configurable": {"thread_id": self._thread_id},
            }
            if self._callback_handler:
                config["callbacks"] = [self._callback_handler]

            full_text = ""
            tool_count = 0

            async for event in self._agent_graph.astream_events(  # type: ignore[union-attr]
                {"messages": input_messages},
                config=config,
                version="v2",
            ):
                kind = event.get("event", "")

                if kind == "on_chat_model_start":
                    # Agent 开始思考 — 发送 agent_think 事件
                    yield {
                        "type": "agent_think",
                        "agent_name": self.name,
                        "status": "started",
                    }

                elif kind == "on_chat_model_stream":
                    chunk_data = event.get("data", {}).get("chunk", {})
                    if hasattr(chunk_data, "content") and chunk_data.content:
                        full_text += chunk_data.content
                        yield {"type": "text", "content": chunk_data.content, "agent_name": self.name}

                elif kind == "on_chat_model_end":
                    # Agent 思考结束
                    yield {
                        "type": "agent_think",
                        "agent_name": self.name,
                        "status": "ended",
                    }

                elif kind == "on_tool_start":
                    tool_name = event.get("name", "unknown")
                    tool_input = event.get("data", {}).get("input", {})
                    run_id = event.get("run_id", "")
                    yield {
                        "type": "tool_call",
                        "call_id": run_id,
                        "name": tool_name,
                        "arguments": tool_input,
                        "agent_name": self.name,
                    }
                    self._emit(AgentEvent.TOOL_CALL, {"name": tool_name})

                elif kind == "on_tool_end":
                    tool_name = event.get("name", "unknown")
                    tool_output = event.get("data", {}).get("output", "")
                    run_id = event.get("run_id", "")
                    output_str = str(tool_output)[:500]
                    tool_count += 1
                    yield {
                        "type": "tool_result",
                        "call_id": run_id,
                        "name": tool_name,
                        "success": True,
                        "result": output_str,
                        "agent_name": self.name,
                    }
                    self._emit(AgentEvent.TOOL_RESULT, {
                        "tool": tool_name,
                        "success": True,
                    })

            self.iteration_count = tool_count
            if full_text:
                self.memory.short.add(Message.assistant(full_text))

            yield {"type": "done", "agent_name": self.name}

        except Exception as e:
            self._emit(AgentEvent.ERROR, {"error": str(e)})
            yield {"type": "error", "content": str(e), "agent_name": self.name}
            yield {"type": "done", "agent_name": self.name}

        self.state = AgentState.FINISHED

    # ======== 结构化输出 ========

    def run_structured(self, task: str, schema: type) -> Any:
        """
        执行任务并以 Pydantic 结构化模式输出

        Args:
            task: 用户任务
            schema: Pydantic BaseModel 子类 (如 AgentResponse)

        Returns:
            解析后的 Pydantic 实例

        Usage:
            from src.core.response_schema import AgentResponse
            result = agent.run_structured("分析这个项目", AgentResponse)
        """
        if self.llm is None:
            raise RuntimeError("Agent 尚未初始化")

        lc_model = self.llm.as_langchain()
        if lc_model is None:
            # 回退：用文本输出
            text = self.run(task)
            return schema(answer=text)

        try:
            structured_model = lc_model.with_structured_output(schema)
            response = structured_model.invoke(task)
            return response
        except Exception as e:
            self._log(f"[Structured] 结构化输出失败: {e}，回退到文本模式")
            text = self.run(task)
            return schema(answer=text)

    # ======== Metrics ========

    def get_metrics(self) -> dict:
        """获取运行指标"""
        from .metrics import MetricsCollector
        if self.metrics is None or not isinstance(self.metrics, MetricsCollector):
            return {"status": "metrics 未启用"}
        return self.metrics.summary()

    # ======== 计划模式 ========

    def _task_is_complex(self, task: str) -> bool:
        """判断任务是否需要分解"""
        complexity_keywords = [
            "先", "然后", "接着", "最后", "步骤",
            "多个", "分析", "比较", "总结", "调研",
            "写", "生成", "创建", "设计", "优化",
        ]
        return any(kw in task for kw in complexity_keywords)

    def _make_plan(self, task: str) -> list[str]:
        """让 LLM 生成任务计划"""
        if self.llm is None:
            return []

        from .message import Message
        plan_prompt = [
            Message.system(
                "你是一个任务规划器。将用户的任务分解为 3-6 个具体步骤。\n"
                "每个步骤一行，格式：数字. 步骤描述\n"
                "只输出步骤列表，不要其他内容。"
            ),
            Message.user(f"任务: {task}\n\n分解步骤:"),
        ]

        try:
            response = self.llm.chat(plan_prompt)
            steps = []
            for line in response.content.split("\n"):
                line = line.strip()
                if line and (line[0].isdigit() or line.startswith("-")):
                    steps.append(line.lstrip("0123456789.-) "))
            if steps:
                self._log(f"[Plan] 任务计划 ({len(steps)} 步):")
                for s in steps:
                    self._log(f"   → {s}")
            return steps
        except Exception as e:
            self._log(f"[Plan] 计划生成失败: {e}")
            return []

    # ======== 反思模式 ========

    def _reflect(self, answer: str, task: str) -> str:
        """让 Agent 自我检查答案质量"""
        if self.llm is None:
            return answer

        from .message import Message
        reflect_prompt = [
            Message.system(
                "你是一个严格的质量检查员。检查助手回答是否完整、准确。"
                "如果有遗漏或错误，返回改进后的完整回答；"
                "如果完美，回复 EXACT: OK"
            ),
            Message.user(f"用户任务: {task}\n\n助手回答:\n{answer}"),
        ]
        try:
            response = self.llm.chat(reflect_prompt)
            if "EXACT: OK" in response.content:
                self._log("[Reflect] 答案质量合格")
                return answer
            else:
                self._log("[Reflect] 发现改进空间，已更新答案")
                return response.content
        except Exception as e:
            self._log(f"[Reflect] 反思检查失败: {e}")
            return answer

    # ======== 强制收束 ========

    def _force_conclude(self) -> str:
        """达到最大迭代次数时，强制生成总结"""
        if self.llm is None:
            return "抱歉，任务处理超时。请尝试简化问题重试。"

        from .message import Message
        conclude_prompt = [
            Message.system(
                "你达到了最大操作次数。请基于已获得的全部信息，"
                "给用户一个尽可能完整的回答。不要提达到上限的事。"
            ),
            Message.user("请总结以上所有发现，给我最终答案。"),
        ]
        try:
            response = self.llm.chat(conclude_prompt)
            return response.content
        except Exception as e:
            self._log(f"[Conclude] 强制总结失败: {e}")
            return "抱歉，任务处理超时。请尝试简化问题重试。"


# ============================================================
# 快捷创建函数
# ============================================================

def create_agent(
    provider: str = "openai",
    model: str = "gpt-4o",
    api_key: str = "",
    base_url: str = "",
    temperature: float = 0.7,
    system_prompt: str = "你是一个智能 AI 助手。",
    max_iterations: int = 15,
    verbose: bool = True,
    enable_planning: bool = False,
    enable_rag: bool = True,
    enable_reflection: bool = False,
) -> Agent:
    """一行创建 Agent"""
    config = LLMConfig(
        provider=provider,
        model=model,
        api_key=api_key,
        base_url=base_url,
        temperature=temperature,
    )

    agent = Agent()
    agent.init(config)
    agent.system_prompt = system_prompt
    agent.max_iterations = max_iterations
    agent.verbose = verbose
    agent.enable_planning = enable_planning
    agent.enable_rag = enable_rag
    agent.enable_reflection = enable_reflection

    return agent


# ============================================================
# 自测
# ============================================================
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("=" * 60)
    print("Agent 主循环 演示 (LangChain 1.x create_agent)")
    print("=" * 60)

    import os
    if not os.getenv("OPENAI_API_KEY") and not os.getenv("DEEPSEEK_API_KEY"):
        print("\n请设置 API Key 后运行:")
        print("   $env:OPENAI_API_KEY='sk-xxx'  # PowerShell")
        print("   export OPENAI_API_KEY=sk-xxx   # bash")
        exit(0)

    import sys
    _PRJ_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    sys.path.insert(0, _PRJ_ROOT)
    from src.tools.builtin_tools import register_all
    register_all()

    agent = create_agent(
        provider=os.getenv("DEEPSEEK_API_KEY") and "deepseek" or "openai",
        model=os.getenv("DEEPSEEK_API_KEY") and "deepseek-chat" or "gpt-4o",
        system_prompt="你是一个实用高效的助手，优先使用工具获取准确信息。",
    )

    print("\n测试任务: 今年的诺贝尔物理学奖颁给了谁？\n")
    result = agent.run("今年的诺贝尔物理学奖颁给了谁？简要说明。")

    print(f"\n{'=' * 60}")
    print(f"最终回复:\n{result}")
    print(f"\n统计: {agent.iteration_count} 轮迭代")
