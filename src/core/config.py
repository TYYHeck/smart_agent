# -*- coding: utf-8 -*-
"""
统一配置管理 —— 从 config.yaml 加载，供应所有模块使用

设计原则：
  1. 单一配置源：所有模块通过 AppConfig 获取配置，不直接读 yaml
  2. 懒加载 + 单例：首次调用时解析，后续共享同一实例
  3. 类型安全：所有字段使用 dataclass 强类型，避免字符串拼写错误
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional
import os
import yaml


# ============================================================
# 子配置结构
# ============================================================

@dataclass
class LLMConfigData:
    """LLM 配置 (纯数据，与 src.core.llm.LLMConfig 互补)"""
    provider: str = "deepseek"
    model: str = "deepseek-chat"
    api_key: str = ""
    base_url: str = ""
    temperature: float = 0.7
    max_tokens: int = 4096
    timeout: int = 60


@dataclass
class FallbackConfig:
    """备用模型配置"""
    provider: str = "deepseek"
    model: str = "deepseek-chat"
    api_key: str = ""


@dataclass
class ShortTermMemoryConfig:
    """短期记忆配置"""
    max_turns: int = 20
    summarize_threshold: int = 10


@dataclass
class LongTermMemoryConfig:
    """长期记忆配置"""
    enabled: bool = True
    db_path: str = "./data/memory.db"
    collection_name: str = "agent_memory"


@dataclass
class MemoryConfig:
    """记忆系统总配置"""
    short_term: ShortTermMemoryConfig = field(default_factory=ShortTermMemoryConfig)
    long_term: LongTermMemoryConfig = field(default_factory=LongTermMemoryConfig)


@dataclass
class RAGConfig:
    """RAG 知识库配置"""
    enabled: bool = True
    embedding_model: str = "text-embedding-3-small"
    embedding_provider: str = "openai"
    chunk_size: int = 500
    chunk_overlap: int = 50
    persist_dir: str = "./data/vectordb"
    top_k: int = 5


@dataclass
class ToolsConfig:
    """工具系统配置"""
    enabled: list[str] = field(default_factory=list)
    dangerous: list[str] = field(default_factory=list)
    max_calls_per_turn: int = 10
    output_dir: str = "./output"


@dataclass
class AgentConfig:
    """Agent 行为配置"""
    name: str = "SmartAgent"
    max_iterations: int = 15
    verbose: bool = True
    system_prompt: str = (
        "【角色定义】\n"
        "你是 SmartAgent，一个基于 ReAct 架构构建的智能 AI 助手。你不是 DeepSeek、OpenAI 或其他厂商的官方助手——你只有一个身份：SmartAgent。\n"
        "你的行为风格：专业、严谨、高效，用简洁清晰的语言回应用户。\n\n"
        "【核心目标】\n"
        "1. 对用户任务进行多步骤推理与规划，确保从输入到输出的全流程闭环\n"
        "2. 保证回答准确可靠——涉及实时数据、外部信息时优先使用工具获取，不依赖训练数据凭空回答\n"
        "3. 持续优化回答质量，代码执行后检查结果是否正确，必要时修正重试\n\n"
        "【行为规则】\n"
        "- 当被问\"你是谁\"时，回答你是 SmartAgent\n"
        "- 复杂任务先分解为子任务，再逐步执行，每步确认结果\n"
        "- 搜索优先于猜测：不确定的事实必须通过工具搜索确认\n"
        "- 如果有多个可行方案，选择最高效的直接执行\n"
        "- 用中文回答用户，保持语气温和专业\n\n"
        "【资源调用】\n"
        "- 可调用工具包括：网络搜索、网页抓取、Python代码执行、文件读写、知识库检索\n"
        "- 文件操作限定在输出目录内，不得修改系统文件或配置文件\n\n"
        "【容错机制】\n"
        "- 工具调用失败时，分析错误原因后自动重试最多2次，仍失败则告知用户并提供替代方案\n"
        "- API超时或网络异常时，等待后重试一次，若仍失败则回退使用已有知识回答并注明局限性\n"
        "- 遇到无法处理的任务时，明确告知用户能力边界，不强行执行或编造结果"
    )


@dataclass
class ServerConfig:
    """Web 服务器配置"""
    host: str = "127.0.0.1"
    port: int = 8080


@dataclass
class DatabaseConfig:
    """MySQL 数据库配置"""
    host: str = "127.0.0.1"
    port: int = 3306
    user: str = "smart_agent"
    password: str = "smart_agent_pass"
    database: str = "smart_agent"
    pool_size: int = 10
    max_overflow: int = 20
    pool_recycle: int = 3600


@dataclass
class AuthConfig:
    """认证配置"""
    enabled: bool = True
    jwt_secret_key: str = ""
    jwt_expire_minutes: int = 480
    bcrypt_rounds: int = 12


@dataclass
class RateLimitConfig:
    """速率限制配置"""
    enabled: bool = True
    max_requests_per_minute: int = 120
    burst: int = 30
    whitelist_ips: list[str] = field(default_factory=list)


@dataclass
class LoggingConfig:
    """日志配置"""
    level: str = "INFO"
    dir: str = "./logs"
    json_format: bool = False
    mysql_errors: bool = True


@dataclass
class OrchestratorParallelConfig:
    """并行执行配置"""
    max_agents: int = 4
    timeout_seconds: int = 600


@dataclass
class OrchestratorPipelineConfig:
    """流水线配置"""
    max_stages: int = 5
    stage_names: list[str] = field(default_factory=lambda: [
        "分析拆解", "方案设计", "执行实施", "验证检查", "总结输出",
    ])


@dataclass
class OrchestratorCollaborativeConfig:
    """协作讨论配置"""
    rounds: int = 2
    synthesizer: str = "first"


@dataclass
class OrchestratorAutoDetectConfig:
    """自动模式检测配置"""
    complexity_threshold: int = 100
    parallel_score_min: int = 2
    pipeline_score_min: int = 2


@dataclass
class OrchestratorConfig:
    """多 Agent 编排配置"""
    default_mode: str = "auto"
    parallel: OrchestratorParallelConfig = field(default_factory=OrchestratorParallelConfig)
    pipeline: OrchestratorPipelineConfig = field(default_factory=OrchestratorPipelineConfig)
    collaborative: OrchestratorCollaborativeConfig = field(default_factory=OrchestratorCollaborativeConfig)
    auto_detect: OrchestratorAutoDetectConfig = field(default_factory=OrchestratorAutoDetectConfig)


@dataclass
class MonitoringConfig:
    """监控配置"""
    enabled: bool = True
    metrics_path: str = "/metrics"
    health_path: str = "/health"


# ============================================================
# 总配置
# ============================================================

@dataclass
class AppConfig:
    """应用总配置 —— 单例模式"""
    llm: LLMConfigData = field(default_factory=LLMConfigData)
    fallback: FallbackConfig = field(default_factory=FallbackConfig)
    memory: MemoryConfig = field(default_factory=MemoryConfig)
    rag: RAGConfig = field(default_factory=RAGConfig)
    tools: ToolsConfig = field(default_factory=ToolsConfig)
    agent: AgentConfig = field(default_factory=AgentConfig)
    server: ServerConfig = field(default_factory=ServerConfig)
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    auth: AuthConfig = field(default_factory=AuthConfig)
    rate_limit: RateLimitConfig = field(default_factory=RateLimitConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    monitoring: MonitoringConfig = field(default_factory=MonitoringConfig)
    orchestrator: OrchestratorConfig = field(default_factory=OrchestratorConfig)

    # ======== 可用模型列表 ========
    available_models: list[dict] = field(default_factory=lambda: [
        {"id": "deepseek-chat", "name": "DeepSeek Chat", "provider": "deepseek"},
        {"id": "deepseek-reasoner", "name": "DeepSeek Reasoner", "provider": "deepseek"},
        {"id": "gpt-4o", "name": "GPT-4o", "provider": "openai"},
        {"id": "gpt-4o-mini", "name": "GPT-4o Mini", "provider": "openai"},
        {"id": "gpt-4-turbo", "name": "GPT-4 Turbo", "provider": "openai"},
        {"id": "qwen-plus", "name": "通义千问 Plus", "provider": "qwen"},
        {"id": "glm-4", "name": "智谱 GLM-4", "provider": "zhipu"},
    ])


# ============================================================
# 单例管理
# ============================================================

_config_instance: Optional[AppConfig] = None
_config_path: str = ""


def _parse_config(raw: dict) -> AppConfig:
    """将 yaml 原始字典解析为 AppConfig"""
    cfg = AppConfig()

    # LLM
    llm_raw = raw.get("llm", {})
    if llm_raw:
        cfg.llm = LLMConfigData(
            provider=llm_raw.get("provider", cfg.llm.provider),
            model=llm_raw.get("model", cfg.llm.model),
            api_key=llm_raw.get("api_key", cfg.llm.api_key),
            base_url=llm_raw.get("base_url", cfg.llm.base_url),
            temperature=float(llm_raw.get("temperature", cfg.llm.temperature)),
            max_tokens=int(llm_raw.get("max_tokens", cfg.llm.max_tokens)),
            timeout=int(llm_raw.get("timeout", cfg.llm.timeout)),
        )

    # Fallback
    fb_raw = raw.get("fallback", {})
    if fb_raw:
        cfg.fallback = FallbackConfig(
            provider=fb_raw.get("provider", cfg.fallback.provider),
            model=fb_raw.get("model", cfg.fallback.model),
            api_key=fb_raw.get("api_key", cfg.fallback.api_key),
        )

    # Memory
    mem_raw = raw.get("memory", {})
    if mem_raw:
        st_raw = mem_raw.get("short_term", {})
        lt_raw = mem_raw.get("long_term", {})
        cfg.memory = MemoryConfig(
            short_term=ShortTermMemoryConfig(
                max_turns=st_raw.get("max_turns", cfg.memory.short_term.max_turns),
                summarize_threshold=st_raw.get("summarize_threshold", cfg.memory.short_term.summarize_threshold),
            ),
            long_term=LongTermMemoryConfig(
                enabled=lt_raw.get("enabled", cfg.memory.long_term.enabled),
                db_path=lt_raw.get("db_path", cfg.memory.long_term.db_path),
                collection_name=lt_raw.get("collection_name", cfg.memory.long_term.collection_name),
            ),
        )

    # RAG
    rag_raw = raw.get("rag", {})
    if rag_raw:
        cfg.rag = RAGConfig(
            enabled=rag_raw.get("enabled", cfg.rag.enabled),
            embedding_model=rag_raw.get("embedding_model", cfg.rag.embedding_model),
            embedding_provider=rag_raw.get("embedding_provider", cfg.rag.embedding_provider),
            chunk_size=rag_raw.get("chunk_size", cfg.rag.chunk_size),
            chunk_overlap=rag_raw.get("chunk_overlap", cfg.rag.chunk_overlap),
            persist_dir=rag_raw.get("persist_dir", cfg.rag.persist_dir),
            top_k=rag_raw.get("top_k", cfg.rag.top_k),
        )

    # Tools
    tools_raw = raw.get("tools", {})
    if tools_raw:
        cfg.tools = ToolsConfig(
            enabled=tools_raw.get("enabled", cfg.tools.enabled),
            dangerous=tools_raw.get("dangerous", cfg.tools.dangerous),
            max_calls_per_turn=tools_raw.get("max_calls_per_turn", cfg.tools.max_calls_per_turn),
            output_dir=tools_raw.get("output_dir", cfg.tools.output_dir),
        )

    # Agent
    agent_raw = raw.get("agent", {})
    if agent_raw:
        cfg.agent = AgentConfig(
            name=agent_raw.get("name", cfg.agent.name),
            max_iterations=agent_raw.get("max_iterations", cfg.agent.max_iterations),
            verbose=agent_raw.get("verbose", cfg.agent.verbose),
            system_prompt=agent_raw.get("system_prompt", cfg.agent.system_prompt),
        )

    # Server
    srv_raw = raw.get("server", {})
    if srv_raw:
        cfg.server = ServerConfig(
            host=srv_raw.get("host", cfg.server.host),
            port=srv_raw.get("port", cfg.server.port),
        )

    # Database
    db_raw = raw.get("database", {})
    if db_raw:
        cfg.database = DatabaseConfig(
            host=db_raw.get("host", cfg.database.host),
            port=db_raw.get("port", cfg.database.port),
            user=db_raw.get("user", cfg.database.user),
            password=db_raw.get("password", cfg.database.password),
            database=db_raw.get("database", cfg.database.database),
            pool_size=db_raw.get("pool_size", cfg.database.pool_size),
            max_overflow=db_raw.get("max_overflow", cfg.database.max_overflow),
            pool_recycle=db_raw.get("pool_recycle", cfg.database.pool_recycle),
        )

    # Auth
    auth_raw = raw.get("auth", {})
    if auth_raw:
        cfg.auth = AuthConfig(
            enabled=auth_raw.get("enabled", cfg.auth.enabled),
            jwt_secret_key=auth_raw.get("jwt_secret_key", cfg.auth.jwt_secret_key),
            jwt_expire_minutes=auth_raw.get("jwt_expire_minutes", cfg.auth.jwt_expire_minutes),
            bcrypt_rounds=auth_raw.get("bcrypt_rounds", cfg.auth.bcrypt_rounds),
        )

    # Rate Limit
    rl_raw = raw.get("rate_limit", {})
    if rl_raw:
        cfg.rate_limit = RateLimitConfig(
            enabled=rl_raw.get("enabled", cfg.rate_limit.enabled),
            max_requests_per_minute=rl_raw.get("max_requests_per_minute", cfg.rate_limit.max_requests_per_minute),
            burst=rl_raw.get("burst", cfg.rate_limit.burst),
            whitelist_ips=rl_raw.get("whitelist_ips", cfg.rate_limit.whitelist_ips),
        )

    # Logging
    log_raw = raw.get("logging", {})
    if log_raw:
        cfg.logging = LoggingConfig(
            level=log_raw.get("level", cfg.logging.level),
            dir=log_raw.get("dir", cfg.logging.dir),
            json_format=log_raw.get("json_format", cfg.logging.json_format),
            mysql_errors=log_raw.get("mysql_errors", cfg.logging.mysql_errors),
        )

    # Monitoring
    mon_raw = raw.get("monitoring", {})
    if mon_raw:
        cfg.monitoring = MonitoringConfig(
            enabled=mon_raw.get("enabled", cfg.monitoring.enabled),
            metrics_path=mon_raw.get("metrics_path", cfg.monitoring.metrics_path),
            health_path=mon_raw.get("health_path", cfg.monitoring.health_path),
        )

    # Orchestrator
    orch_raw = raw.get("orchestrator", {})
    if orch_raw:
        par_raw = orch_raw.get("parallel", {})
        pipe_raw = orch_raw.get("pipeline", {})
        collab_raw = orch_raw.get("collaborative", {})
        auto_raw = orch_raw.get("auto_detect", {})
        cfg.orchestrator = OrchestratorConfig(
            default_mode=orch_raw.get("default_mode", cfg.orchestrator.default_mode),
            parallel=OrchestratorParallelConfig(
                max_agents=par_raw.get("max_agents", cfg.orchestrator.parallel.max_agents),
                timeout_seconds=par_raw.get("timeout_seconds", cfg.orchestrator.parallel.timeout_seconds),
            ),
            pipeline=OrchestratorPipelineConfig(
                max_stages=pipe_raw.get("max_stages", cfg.orchestrator.pipeline.max_stages),
                stage_names=pipe_raw.get("stage_names", cfg.orchestrator.pipeline.stage_names),
            ),
            collaborative=OrchestratorCollaborativeConfig(
                rounds=collab_raw.get("rounds", cfg.orchestrator.collaborative.rounds),
                synthesizer=collab_raw.get("synthesizer", cfg.orchestrator.collaborative.synthesizer),
            ),
            auto_detect=OrchestratorAutoDetectConfig(
                complexity_threshold=auto_raw.get("complexity_threshold", cfg.orchestrator.auto_detect.complexity_threshold),
                parallel_score_min=auto_raw.get("parallel_score_min", cfg.orchestrator.auto_detect.parallel_score_min),
                pipeline_score_min=auto_raw.get("pipeline_score_min", cfg.orchestrator.auto_detect.pipeline_score_min),
            ),
        )

    return cfg


def load_config(config_file: str = "config.yaml") -> AppConfig:
    """加载配置文件（首次调用解析，后续返回缓存）"""
    global _config_instance, _config_path

    # 同路径返回缓存
    if _config_instance is not None and _config_path == config_file:
        return _config_instance

    if os.path.exists(config_file):
        with open(config_file, encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
    else:
        raw = {}

    _config_instance = _parse_config(raw)
    _config_path = config_file
    return _config_instance


def get_config() -> AppConfig:
    """获取当前配置（必须先调用 load_config）"""
    if _config_instance is None:
        raise RuntimeError("配置尚未加载，请先调用 load_config()")
    return _config_instance


def reload_config(config_file: str = "config.yaml") -> AppConfig:
    """强制重新加载配置"""
    global _config_instance, _config_path
    _config_instance = None
    _config_path = ""
    return load_config(config_file)
