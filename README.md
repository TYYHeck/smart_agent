# SmartAgent - 智能 AI Agent 框架

基于 **LangChain ReAct** 架构的企业级多 Agent 编排框架，支持单 Agent 对话、多 Agent 流水线协作、任务队列调度，含 JWT 认证、MySQL 持久化和 Prometheus 监控。

## 架构

```
┌─────────────────────────────────────────────────────────────┐
│                       SmartAgent v2.1                        │
│                                                             │
│  ┌──────────┐  ┌──────────┐  ┌────────────┐ ┌───────────┐ │
│  │   CLI    │  │  Web UI  │  │ Task Mgr   │ │Orchestrator│ │
│  └────┬─────┘  └────┬─────┘  └─────┬──────┘ └─────┬─────┘ │
│       │              │              │              │       │
│  ┌────▼──────────────▼──────────────▼──────────────▼─────┐ │
│  │              Agent (LangGraph create_react_agent)      │ │
│  │  ┌──────────┐  ┌──────────────────────────────────┐   │ │
│  │  │ ChatOpenAI│  │       StructuredTool (6+)       │   │ │
│  │  └──────────┘  └──────────────────────────────────┘   │ │
│  └───────────────────────────────────────────────────────┘ │
│       │              │              │                       │
│  ┌────▼────┐  ┌──────▼──────┐  ┌───▼─────┐  ┌──────────┐  │
│  │ Memory  │  │  Knowledge  │  │  Tools  │  │  MySQL   │  │
│  │(ChromaDB)│  │ Base (RAG)  │  │         │  │(持久化)  │  │
│  └─────────┘  └─────────────┘  └─────────┘  └──────────┘  │
│                                                             │
│  ┌─────────┐  ┌───────────┐  ┌──────────┐  ┌───────────┐  │
│  │JWT Auth │  │Prometheus │  │Alembic   │  │ Rate      │  │
│  │(认证)   │  │(监控)     │  │(迁移)    │  │ Limiter   │  │
│  └─────────┘  └───────────┘  └──────────┘  └───────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## 核心特性

| 特性 | 说明 |
|------|------|
| **LangGraph Agent** | 基于 `create_react_agent` + MemorySaver 的状态图引擎 |
| **多 Agent 编排** | 单Agent / 并行 / 流水线 / 协作讨论 四种执行模式 |
| **多模型支持** | OpenAI / DeepSeek / 通义千问 / 智谱GLM / Ollama |
| **工具系统** | 搜索、网页抓取、文件读写、Python执行、计算器（可扩展） |
| **记忆系统** | 短期记忆（滑动窗口）+ 长期记忆（ChromaDB） |
| **RAG 知识库** | 文档加载 → 向量化 → 语义检索 |
| **任务管理** | 发布/分配/追踪/取消 + 智能 Agent 匹配 |
| **JWT 认证** | Bearer Token 保护所有 API 端点 + 管理员角色 |
| **MySQL 持久化** | 任务、事件、Agent 配置、用户全量落盘 |
| **Alembic 迁移** | 版本化数据库 schema 管理 |
| **Prometheus 监控** | 请求计数、延迟直方图 (prometheus_client) |
| **Rate Limiting** | 可配置的请求速率限制 |
| **Docker 部署** | docker-compose 一键启动 (MySQL + App) |

## 快速开始

### 1. 克隆并安装

```bash
git clone <repo> && cd smart_agent
pip install -r requirements.txt
```

### 2. 配置环境变量

```bash
cp .env.example .env
# 编辑 .env 填写 API Key 和数据库密码
```

### 3. 启动

```bash
# Web 可视化模式
python main.py --web

# Docker 一键部署
docker-compose up -d

# 自定义端口
python main.py --web --port 9090
```

### 4. 访问

打开浏览器 `http://localhost:8080`，默认管理员: `admin` / `admin123`。

## CLI 命令

| 命令 | 说明 |
|------|------|
| `/help` | 显示帮助 |
| `/exit`, `/q` | 退出 |
| `/clear` | 清空对话 |
| `/tools` | 列出工具 |
| `/stats` | 运行统计 |
| `/model [id]` | 查看/切换模型 |
| `/plan` | 切换计划模式 |
| `/rag` | 切换 RAG |
| `/reflect` | 切换反思模式 |
| `/recall <q>` | 搜索记忆 |
| `/task publish <描述>` | 发布任务 |
| `/task list [状态]` | 任务列表 |
| `/task queue` | 队列状态 |
| `/agent list` | Agent 列表 |
| `/agent register` | 注册 Agent |

## Web API

### 认证
| 方法 | 路由 | 需要认证 | 说明 |
|------|------|----------|------|
| POST | `/api/auth/login` | 否 | 用户登录 |
| POST | `/api/auth/register` | 否 | 用户注册 |
| GET | `/api/auth/me` | 是 | 当前用户信息 |

### 对话与模型
| 方法 | 路由 | 说明 |
|------|------|------|
| POST | `/api/chat` | SSE 流式对话 |
| GET | `/api/models` | 可用模型列表 |
| POST | `/api/switch_model` | 切换模型 |
| POST | `/api/toggle_mode` | 切换模式 |

### 任务管理
| 方法 | 路由 | 说明 |
|------|------|------|
| POST | `/api/tasks/publish` | 发布任务 |
| POST | `/api/tasks/orchestrate` | 编排执行 |
| POST | `/api/tasks/orchestrate/stream` | 编排 SSE 流式 |
| POST | `/api/tasks/detect-mode` | 自动检测执行模式 |
| GET | `/api/tasks/orchestrate/modes` | 查看所有模式 |
| GET | `/api/tasks/list` | 任务列表 |
| GET | `/api/tasks/{task_id}` | 任务详情 |
| POST | `/api/tasks/{task_id}/cancel` | 取消任务 |
| POST | `/api/tasks/{task_id}/update` | 更新任务 |
| GET | `/api/tasks/queue/status` | 队列状态 |

### Agent 管理
| 方法 | 路由 | 说明 |
|------|------|------|
| GET | `/api/agents/list` | Agent 列表 |
| POST | `/api/agents/create` | 创建 Agent |
| POST | `/api/agents/{name}/update` | 更新 Agent |
| DELETE | `/api/agents/{name}` | 删除 Agent |
| POST | `/api/agents/cleanup` | 清理无效 Agent |

### 系统
| 方法 | 路由 | 说明 |
|------|------|------|
| GET | `/health` | 健康检查 |
| GET | `/metrics` | Prometheus 指标 |
| GET | `/api/system/info` | 系统信息 |
| GET | `/api/dashboard/stats` | 仪表盘统计 |
| GET | `/api/config` | 当前配置 |
| GET | `/api/config/full` | 完整配置 |
| POST | `/api/config/update` | 更新配置 |
| GET | `/api/commands` | 命令列表 |

## 多 Agent 编排模式

| 模式 | 触发条件 | 行为 |
|------|----------|------|
| **Single** | 简单问答、单步操作 | 一个 Agent 直接执行 |
| **Parallel** | "同时/分别/对比/多角度" | 多 Agent 并行，结果汇总 |
| **Pipeline** | "先...再...然后..." | Agent 串行接力 |
| **Collaborative** | "讨论/辩论/评估/决策" | 团队讨论，多轮互审 |
| **Auto** | 默认 | 自动检测描述，选择最优模式 |

## 项目结构

```
smart_agent/
├── main.py                    # 入口
├── config.yaml                # 配置文件
├── requirements.txt           # 依赖
├── docker-compose.yml         # Docker 编排
├── alembic.ini                # Alembic 迁移配置
├── .env.example               # 环境变量模板
├── README.md
├── migrations/                # Alembic 迁移脚本
│   ├── env.py
│   └── versions/
├── tests/
│   ├── conftest.py            # 共享 fixtures
│   ├── test_agent.py          # Agent 核心测试
│   ├── test_task_manager.py   # 任务管理器测试
│   ├── test_api.py            # API 端点测试
│   ├── test_tools.py          # 工具系统测试
│   └── test_message.py        # 消息系统测试
└── src/
    ├── core/
    │   ├── agent.py           # Agent 核心 (LangGraph create_react_agent)
    │   ├── llm.py             # LLM 引擎 (OpenAI + LangChain)
    │   ├── message.py         # 消息系统
    │   ├── task_manager.py    # 任务管理器 + 调度器
    │   ├── orchestrator.py    # 多 Agent 编排器
    │   └── ...
    ├── tools/
    │   ├── base.py            # 工具注册系统
    │   └── builtin_tools.py   # 内置工具 (6个)
    ├── auth/
    │   ├── __init__.py        # JWT + 密码
    │   └── dependencies.py    # FastAPI 依赖注入
    ├── middleware/
    │   ├── metrics.py         # Prometheus 指标
    │   └── rate_limiter.py    # 速率限制
    ├── infrastructure/
    │   ├── database.py        # MySQL 连接池
    │   ├── models.py          # ORM 模型
    │   ├── migrations.py      # 启动时迁移
    │   └── task_repo.py       # 任务持久化
    ├── memory/
    │   └── memory_manager.py  # 记忆系统
    ├── rag/
    │   └── knowledge_base.py  # RAG 知识库
    └── ui/
        ├── cli.py             # 命令行界面
        └── web_server.py      # Web 界面 (FastAPI + SSE)
```

## 技术栈

| 组件 | 技术 |
|------|------|
| **Agent 引擎** | LangGraph (create_react_agent) + LangChain 1.x |
| **LLM SDK** | OpenAI SDK + langchain-openai |
| **Web 框架** | FastAPI + SSE 流式推送 |
| **数据库** | MySQL 8.0 + SQLAlchemy 2.0 (async) |
| **迁移工具** | Alembic |
| **认证** | python-jose (JWT) + passlib (bcrypt) |
| **记忆** | ChromaDB |
| **监控** | prometheus_client |
| **CLI** | Rich + prompt_toolkit |
| **部署** | Docker Compose |

## 常用运维命令

```bash
# 数据库迁移
alembic revision --autogenerate -m "描述变更"
alembic upgrade head

# 运行测试
pytest tests/ -v

# 运行单个测试文件
pytest tests/test_agent.py -v

# 查看指标
curl http://localhost:8080/metrics

# 健康检查
curl http://localhost:8080/health

# 登录获取 Token
curl -X POST http://localhost:8080/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"admin123"}'

# 带 Token 调用 API
curl http://localhost:8080/api/config \
  -H "Authorization: Bearer <token>"
```
