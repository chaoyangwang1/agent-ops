# OPS AI Agent 智能告警平台 — Phase 3 详细设计

> 版本：v1.0
> 日期：2026-06-29
> 依赖：Phase 1 数据基础层 + Phase 2 Agent 核心引擎

## 1. 目标与范围

Phase 3 构建**知识增强与自动运营能力**，使平台从手动诊断升级为自动触发、知识积累、智能推送的闭环系统。

### 范围决策

| 决策项 | 选择 |
|--------|------|
| 向量数据库 | Milvus（替代 Chroma） |
| 对话持久化 | 增强 Phase 2 方案（归档 + 搜索 + 回放） |
| 自动触发 | FastAPI lifespan 集成 Kafka 消费者 |
| ChatOps 推送 | API 端点 + 飞书/钉钉 Webhook |
| 故障案例来源 | 手动导入种子数据 + 自动积累新案例 |

### 交付物

- Milvus 向量知识库 + 历史故障检索
- 自动触发分析（Kafka → Agent 自启）
- ChatOps 飞书/钉钉推送 + 诊断 API
- 对话归档/搜索/回放
- 单元测试 + 集成测试

---

## 2. 架构设计

### 2.1 总体架构

```
┌─────────────────────────────────────────────────────────┐
│                    ChatOps 层                             │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────────┐ │
│  │ 飞书 Webhook  │  │ 钉钉 Webhook  │  │ GET /diagnosis │ │
│  └──────┬───────┘  └──────┬───────┘  └───────┬───────┘ │
│         └─────────────────┼──────────────────┘          │
└───────────────────────────┼──────────────────────────────┘
                            │
┌───────────────────────────┼──────────────────────────────┐
│                    Agent 引擎层 (Phase 2)                  │
│  ┌──────────────────────────────────────────────────────┐│
│  │  假设驱动状态机 + 分层上下文 + 异常降级                ││
│  └──────────────────────┬───────────────────────────────┘│
│         ┌───────────────┼───────────────┐                │
│         ▼               ▼               ▼                │
│  ┌──────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │ 自动触发  │  │ 知识增强      │  │ 对话增强      │       │
│  │ (worker) │  │ (Milvus)     │  │ (归档/回放)   │       │
│  └──────────┘  └──────────────┘  └──────────────┘       │
└──────────────────────────────────────────────────────────┘
```

### 2.2 与 Phase 2 的关系

Phase 3 在 Phase 2 基础上增量构建：

- **新增** Milvus 知识库模块
- **新增** worker 自动触发服务
- **新增** ChatOps 通知推送
- **增强** 对话持久化（ConversationRecord 模型 + 归档）
- **增强** Agent 工具集（search_similar_incidents 工具）
- **不修改** Phase 2 的状态机核心逻辑

---

## 3. 核心模块设计

### 3.1 Milvus 向量知识库

#### Docker Compose 新增

```yaml
etcd:
  image: quay.io/coreos/etcd:v3.5.5
  environment:
    ETCD_AUTO_COMPACTION_MODE: revision
    ETCD_AUTO_COMPACTION_RETENTION: "1000"
    ETCD_QUOTA_BACKEND_BYTES: "4294967296"

milvus:
  image: milvusdb/milvus:v2.4.0
  depends_on: [etcd, minio]
  ports: ["19530:19530", "9091:9091"]
  environment:
    ETCD_ENDPOINTS: etcd:2379
    MINIO_ADDRESS: minio:9000
  volumes: [milvus_data:/var/lib/milvus]
```

#### Collection Schema

```
Collection: incidents
Fields:
  - incident_id (VARCHAR, primary, max_length=64)
  - embedding (FLOAT_VECTOR, dim=1536)
  - summary (VARCHAR, max_length=512)
  - root_cause (VARCHAR, max_length=1024)
  - service (VARCHAR, max_length=128)
  - severity (VARCHAR, max_length=16)
  - created_at (INT64)
Index: IVF_FLAT on embedding
```

#### 模块结构

```
src/knowledge/
├── __init__.py
├── milvus_client.py    # Milvus 连接、collection 管理
├── embedding.py        # LLM embedding 生成
├── incident_store.py   # 故障案例 CRUD + 相似度检索
└── seed_data.py        # 种子数据导入工具
```

### 3.2 自动触发分析

#### 架构

```
FastAPI lifespan
  └── start_agent_worker()  # 启动时创建后台任务
        └── AgentWorker.run()
              ├── consume kafka "aggregated.alerts"
              ├── 对每条聚合告警调用 diagnosis.run_diagnosis()
              ├── 诊断完成 → 通知 notifier
              └── 诊断完成 → 自动入库 incident_store
```

#### Worker 设计

```python
class AgentWorker:
    def __init__(self, diagnosis_fn, notifier, incident_store):
        self._running = False

    async def run(self):
        """主循环：消费 Kafka → 诊断 → 通知 → 入库"""
        while self._running:
            alerts = kafka.consume("aggregated.alerts", max=10, timeout=5)
            for alert in alerts:
                result = await diagnosis_fn(alert)
                await notifier.send(result)           # ChatOps 推送
                await incident_store.insert(result)   # 自动入库
            await asyncio.sleep(1)
```

### 3.3 对话增强

#### ConversationRecord 模型（PostgreSQL）

```sql
CREATE TABLE conversations (
    conversation_id VARCHAR PRIMARY KEY,
    alert_id VARCHAR,
    service VARCHAR,
    severity VARCHAR,
    status VARCHAR,           -- active/completed/timeout
    diagnosis TEXT,
    hypothesis_history JSONB,
    messages JSONB,            -- 完整对话历史
    checkpoint_id VARCHAR,     -- LangGraph checkpoint
    step_count INT,
    truncated BOOL,
    mode VARCHAR,
    duration_seconds FLOAT,
    created_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);
```

#### 功能

- **归档**：诊断完成/超时后自动写入 PostgreSQL
- **搜索**：`GET /api/v1/conversations?service=payment&severity=critical`
- **回放**：`GET /api/v1/conversations/{id}` 返回完整对话 + 诊断结果

### 3.4 ChatOps 推送

#### 飞书推送

```python
class FeishuNotifier:
    def send(self, webhook_url: str, result: DiagnosisResult):
        """推送 Markdown 格式诊断报告到飞书群"""
        # POST webhook_url with interactive card
        # 标题：服务名 + 根因摘要
        # 内容：置信度、证据链、建议
        # 按钮：确认回滚、重启服务、忽略
```

#### 钉钉推送

```python
class DingtalkNotifier:
    def send(self, webhook_url: str, result: DiagnosisResult):
        """推送 Markdown 格式诊断报告到钉钉群"""
```

#### API 端点

- `GET /api/v1/diagnosis/{id}` — 查询诊断结果
- `GET /api/v1/conversations` — 搜索历史对话
- `GET /api/v1/conversations/{id}` — 对话回放

---

## 4. 数据流

### 自动触发流程

```
Kafka (aggregated.alerts)
  │
  ▼
AgentWorker.consume()
  │
  ▼
diagnosis.run_diagnosis(alert)
  │  ┌─ formulate_hypothesis
  │  ├─ search_similar_incidents (查 Milvus)  ← 新增工具
  │  ├─ search_logs / get_topology
  │  ├─ evaluate_evidence
  │  └─ root_cause
  │
  ▼
[诊断完成后]
  ├── incident_store.insert(result)    # 自动入库
  ├── conversation.archive(result)     # 对话归档
  └── notifier.send(result)            # ChatOps 推送
```

---

## 5. 目录结构

```
src/
├── knowledge/                    # 新增
│   ├── __init__.py
│   ├── milvus_client.py
│   ├── embedding.py
│   ├── incident_store.py
│   └── seed_data.py
├── worker/                       # 新增
│   ├── __init__.py
│   └── agent_worker.py
├── chatops/                      # 新增
│   ├── __init__.py
│   ├── notifier.py
│   └── routes.py
├── conversation/                 # 新增
│   ├── __init__.py
│   ├── models.py
│   └── repository.py
└── agent/
    ├── tools.py                  # 增强：添加 search_similar_incidents
    └── diagnosis.py              # 增强：支持不阻塞的 fire-and-forget

scripts/
└── seed_incidents.py             # 新增：种子数据导入

tests/
├── test_milvus_client.py         # 新增
├── test_incident_store.py        # 新增
├── test_agent_worker.py          # 新增
├── test_notifier.py              # 新增
├── test_conversation.py          # 新增
└── integration/
    └── test_phase3_e2e.py        # 新增
```

---

## 6. 验收标准

| 验收项 | 标准 |
|--------|------|
| Milvus 连接 | collection 创建 + 索引构建成功 |
| 故障入库 | 诊断结果自动向量化写入 Milvus |
| 相似检索 | Top-K 相似故障命中（语义相关） |
| 自动触发 | Kafka 聚合告警 → Agent 自动诊断 |
| 对话归档 | 诊断完成自动写入 PostgreSQL |
| 对话搜索 | 按 service/severity 查询历史 |
| 飞书推送 | Markdown 报告推送到群聊 |
| 钉钉推送 | Markdown 报告推送到群聊 |
| 诊断 API | `GET /api/v1/diagnosis/{id}` 返回结果 |

---

本文档为 Phase 3 设计，基于 Phase 1+2 基础设施构建知识增强与自动运营能力。
