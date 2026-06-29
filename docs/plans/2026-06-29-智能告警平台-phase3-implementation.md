# OPS AI Agent 智能告警平台 — Phase 3 实施计划

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 构建知识增强与自动运营能力：Milvus 向量知识库、历史故障检索、Kafka 自动触发 Agent 诊断、飞书/钉钉 ChatOps 推送、对话归档搜索回放。

**Architecture:** 在 Phase 1+2 基础上增量构建。新增 Milvus 服务（docker-compose），pymilvus SDK 封装知识库模块，FastAPI lifespan 集成 Kafka 消费者实现自动触发，飞书/钉钉 Webhook 推送诊断报告。对话归档存 PostgreSQL 带完整搜索 API。

**Tech Stack:** Python 3.11+, Milvus 2.4, pymilvus, langchain-openai (embedding), FastAPI, Kafka, PostgreSQL, Redis

---

## Task 3.0: Milvus Docker Compose 集成 + 客户端封装

**Files:**
- Modify: `docker-compose.yml`
- Create: `src/knowledge/__init__.py`
- Create: `src/knowledge/milvus_client.py`
- Test: `tests/test_milvus_client.py`

### Step 1: 添加 Milvus 服务到 docker-compose.yml

在 `docker-compose.yml` 的 `services` 中添加：

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

在 `volumes` 中添加：

```yaml
  milvus_data: null
```

### Step 2: 启动并验证

```bash
docker compose up -d etcd milvus
docker compose ps | grep milvus
```
Expected: milvus 状态 Up

### Step 3: 安装 pymilvus

```bash
pip install pymilvus
```

### Step 4: 编写测试

```python
# tests/test_milvus_client.py
import pytest
from src.knowledge.milvus_client import MilvusManager

@pytest.fixture
def milvus():
    mgr = MilvusManager(host="localhost", port="19530")
    mgr.init_collection()
    yield mgr
    mgr.close()

def test_collection_exists(milvus):
    assert milvus.collection_name == "incidents"
    assert milvus.collection is not None

def test_insert_and_search(milvus):
    milvus.flush()  # 清空已有数据
    vectors = [[0.1] * 1536, [0.2] * 1536]
    data = {
        "incident_id": ["inc-001", "inc-002"],
        "embedding": vectors,
        "summary": ["CPU 过高导致延迟", "内存溢出导致 OOM"],
        "root_cause": ["请求量激增", "内存泄漏"],
        "service": ["payment", "order"],
        "severity": ["critical", "warning"],
        "created_at": [1000000, 2000000],
    }
    ids = milvus.insert(data)
    assert len(ids) == 2
    milvus.flush()

    results = milvus.search([0.1] * 1536, top_k=2)
    assert len(results) > 0
```

### Step 5: 验证测试失败

```bash
pytest tests/test_milvus_client.py -v
```
Expected: FAIL（模块不存在）

### Step 6: 实现 MilvusManager

```python
# src/knowledge/milvus_client.py
from pymilvus import connections, Collection, FieldSchema, CollectionSchema, DataType, utility

COLLECTION_NAME = "incidents"
VECTOR_DIM = 1536


class MilvusManager:
    def __init__(self, host: str = "localhost", port: str = "19530"):
        self.host = host
        self.port = port
        self.collection_name = COLLECTION_NAME
        connections.connect(host=host, port=port)
        self._collection = None

    @property
    def collection(self):
        if self._collection is None and utility.has_collection(self.collection_name):
            self._collection = Collection(self.collection_name)
        return self._collection

    def init_collection(self):
        """创建 collection 和索引（幂等）"""
        if utility.has_collection(self.collection_name):
            self._collection = Collection(self.collection_name)
            self._collection.load()
            return

        fields = [
            FieldSchema(name="incident_id", dtype=DataType.VARCHAR, is_primary=True, max_length=64),
            FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=VECTOR_DIM),
            FieldSchema(name="summary", dtype=DataType.VARCHAR, max_length=512),
            FieldSchema(name="root_cause", dtype=DataType.VARCHAR, max_length=1024),
            FieldSchema(name="service", dtype=DataType.VARCHAR, max_length=128),
            FieldSchema(name="severity", dtype=DataType.VARCHAR, max_length=16),
            FieldSchema(name="created_at", dtype=DataType.INT64),
        ]
        schema = CollectionSchema(fields, description="故障案例向量库")
        self._collection = Collection(self.collection_name, schema)

        index_params = {"metric_type": "IP", "index_type": "IVF_FLAT", "params": {"nlist": 128}}
        self._collection.create_index("embedding", index_params)
        self._collection.load()

    def insert(self, data: dict) -> list:
        """插入数据，返回 ID 列表"""
        ids = self._collection.insert([
            data["incident_id"],
            data["embedding"],
            data["summary"],
            data["root_cause"],
            data["service"],
            data["severity"],
            data["created_at"],
        ])
        return ids.primary_keys

    def flush(self):
        """刷新数据使其可搜索"""
        if self._collection:
            self._collection.flush()

    def search(self, query_vector: list, top_k: int = 5) -> list[dict]:
        """相似度检索"""
        if self._collection is None:
            return []
        search_params = {"metric_type": "IP", "params": {"nprobe": 10}}
        results = self._collection.search(
            [query_vector], "embedding", search_params, limit=top_k,
            output_fields=["incident_id", "summary", "root_cause", "service", "severity"]
        )
        if not results or len(results[0]) == 0:
            return []
        return [
            {"id": hit.id, "distance": hit.distance, **hit.entity._row_data}
            for hit in results[0]
        ]

    def count(self) -> int:
        if self._collection is None:
            return 0
        return self._collection.num_entities

    def close(self):
        connections.disconnect("default")
```

### Step 7: 运行测试

```bash
pytest tests/test_milvus_client.py -v
```
Expected: PASS

### Step 8: Commit

```bash
git add docker-compose.yml src/knowledge/ tests/test_milvus_client.py
git commit -m "feat: Milvus Docker 集成 + 客户端封装"
```

---

## Task 3.1: 故障案例存储 + Embedding

**Files:**
- Create: `src/knowledge/embedding.py`
- Create: `src/knowledge/incident_store.py`
- Create: `scripts/seed_incidents.py`
- Test: `tests/test_incident_store.py`

### Step 1: 编写测试

```python
# tests/test_incident_store.py
import pytest
from unittest.mock import Mock, patch
from src.knowledge.incident_store import IncidentStore

@pytest.fixture
def store():
    mock_milvus = Mock()
    mock_milvus.search.return_value = []
    mock_milvus.insert.return_value = ["id-1"]
    mock_milvus.count.return_value = 1
    return IncidentStore(milvus=mock_milvus)

def test_add_incident(store):
    """添加故障案例"""
    result = store.add_incident(
        incident_id="inc-001",
        summary="CPU 过高",
        root_cause="请求量激增",
        service="payment",
        severity="critical",
    )
    assert result is True
    assert store.milvus.insert.called

def test_search_similar(store):
    """搜索相似故障"""
    store.milvus.search.return_value = [
        {"id": "inc-001", "distance": 0.1, "summary": "CPU 过高", "root_cause": "请求量激增"}
    ]
    results = store.search_similar("支付服务 CPU 过高", top_k=3)
    assert len(results) > 0
    assert "summary" in results[0]

def test_get_incident_count(store):
    assert store.count() == 1
```

### Step 2: 验证测试失败

```bash
pytest tests/test_incident_store.py -v
```
Expected: FAIL（模块不存在）

### Step 3: 实现 embedding 模块

```python
# src/knowledge/embedding.py
from langchain_openai import OpenAIEmbeddings
from src.config import settings

_embedding_model = None


def get_embedding_model() -> OpenAIEmbeddings:
    global _embedding_model
    if _embedding_model is None:
        _embedding_model = OpenAIEmbeddings(
            base_url=settings.llm_api_base,
            api_key=settings.llm_api_key,
            model="text-embedding-3-small",
        )
    return _embedding_model


def embed_text(text: str) -> list[float]:
    """将文本向量化"""
    model = get_embedding_model()
    return model.embed_query(text)


def embed_texts(texts: list[str]) -> list[list[float]]:
    """批量向量化"""
    model = get_embedding_model()
    return model.embed_documents(texts)
```

### Step 4: 实现 incident_store

```python
# src/knowledge/incident_store.py
import time
import uuid
import logging
from src.knowledge.embedding import embed_text

logger = logging.getLogger(__name__)


class IncidentStore:
    def __init__(self, milvus):
        self.milvus = milvus

    def add_incident(self, incident_id: str = None, summary: str = "",
                     root_cause: str = "", service: str = "",
                     severity: str = "warning") -> bool:
        """添加故障案例到 Milvus"""
        try:
            text = f"{summary} {root_cause}"
            embedding = embed_text(text)
            incident_id = incident_id or str(uuid.uuid4())
            data = {
                "incident_id": [incident_id],
                "embedding": [embedding],
                "summary": [summary[:512]],
                "root_cause": [root_cause[:1024]],
                "service": [service[:128]],
                "severity": [severity[:16]],
                "created_at": [int(time.time())],
            }
            self.milvus.insert(data)
            self.milvus.flush()
            logger.info(f"故障案例已入库: {incident_id}")
            return True
        except Exception as e:
            logger.error(f"故障案例入库失败: {e}")
            return False

    def search_similar(self, query: str, top_k: int = 5) -> list[dict]:
        """搜索相似历史故障"""
        try:
            embedding = embed_text(query)
            return self.milvus.search(embedding, top_k=top_k)
        except Exception as e:
            logger.error(f"相似故障检索失败: {e}")
            return []

    def count(self) -> int:
        return self.milvus.count()
```

### Step 5: 实现种子数据导入脚本

```python
#!/usr/bin/env python3
# scripts/seed_incidents.py
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.knowledge.milvus_client import MilvusManager
from src.knowledge.incident_store import IncidentStore

SEED_DATA = [
    {"summary": "支付服务 P99 延迟超过 500ms", "root_cause": "Redis 连接池耗尽，慢查询 KEYS * 阻塞", "service": "payment", "severity": "critical"},
    {"summary": "订单服务 OOM 崩溃", "root_cause": "内存泄漏，未关闭的数据库连接累积", "service": "order", "severity": "critical"},
    {"summary": "API 网关 502 错误率飙升", "root_cause": "后端服务滚动更新期间健康检查失败", "service": "gateway", "severity": "critical"},
    {"summary": "MySQL 主从延迟超过 30s", "root_cause": "大批量写入操作导致 binlog 积压", "service": "mysql", "severity": "warning"},
    {"summary": "K8s Node NotReady", "root_cause": "节点磁盘空间耗尽，kubelet 无法写入", "service": "infra", "severity": "critical"},
]

def main():
    milvus = MilvusManager()
    milvus.init_collection()
    store = IncidentStore(milvus=milvus)

    for item in SEED_DATA:
        store.add_incident(**item)

    print(f"种子数据导入完成，共 {store.count()} 条")
    milvus.close()

if __name__ == "__main__":
    main()
```

### Step 6: 运行测试

```bash
pytest tests/test_incident_store.py -v
```
Expected: PASS

### Step 7: Commit

```bash
git add src/knowledge/embedding.py src/knowledge/incident_store.py scripts/seed_incidents.py tests/test_incident_store.py
git commit -m "feat: 故障案例存储 + Embedding + 种子数据导入"
```

---

## Task 3.2: Agent 工具增强（search_similar_incidents）

**Files:**
- Modify: `src/agent/tools.py`

### Step 1: 增强 create_agent_tools

在 `create_agent_tools` 函数中添加 `search_similar_incidents` 工具：

```python
# 在 create_agent_tools 函数末尾，return registry 之前添加：

    # search_similar_incidents 工具
    if incident_store:
        def _search_similar_incidents(description: str, top_k: int = 5) -> dict:
            results = incident_store.search_similar(description, top_k=top_k)
            return {"total": len(results), "incidents": results}

        registry.register(ToolDefinition(
            name="search_similar_incidents",
            description="搜索历史相似故障案例（向量语义检索），帮助判断是否发生过类似问题",
            parameters={
                "description": {"type": "string", "description": "故障描述（自然语言）"},
                "top_k": {"type": "integer", "description": "返回最相似的 K 条结果，默认 5"},
            },
        ), handler=_search_similar_incidents)
    else:
        registry.register(ToolDefinition(
            name="search_similar_incidents",
            description="搜索历史相似故障案例",
            parameters={"description": {"type": "string"}, "top_k": {"type": "integer"}},
        ), handler=lambda **kw: {"total": 0, "incidents": [], "note": "知识库不可用"})
```

同时更新函数签名以接受 `incident_store` 参数：

```python
def create_agent_tools(es_client=None, neo4j_client=None, incident_store=None) -> ToolRegistry:
```

### Step 2: 更新测试

```python
# tests/test_agent_tools.py 中添加：
def test_tools_count_with_incident_store():
    """包含知识库时应有 4 个工具"""
    registry = create_agent_tools()
    tools = registry.list_tools()
    assert len(tools) == 4  # search_logs, get_topology, analyze_blast_radius, search_similar_incidents
    assert "search_similar_incidents" in tools
```

### Step 3: 运行测试

```bash
pytest tests/test_agent_tools.py -v
```
Expected: 4 PASS

### Step 4: Commit

```bash
git add src/agent/tools.py tests/test_agent_tools.py
git commit -m "feat: Agent 工具集增加 search_similar_incidents"
```

---

## Task 3.3: 对话增强（归档/搜索/回放）

**Files:**
- Create: `src/conversation/__init__.py`
- Create: `src/conversation/models.py`
- Create: `src/conversation/repository.py`
- Test: `tests/test_conversation.py`

### Step 1: 编写测试

```python
# tests/test_conversation.py
import pytest
import uuid
import asyncio
from src.conversation.models import ConversationRecord
from src.conversation.repository import ConversationRepository

@pytest.fixture
async def repo():
    from src.infra.database import Database
    db = Database("postgresql://agent:agent@localhost:5432/agentops")
    await db.init()
    r = ConversationRepository(db)
    await r.init_table()
    yield r

@pytest.mark.asyncio
async def test_archive_and_get(repo):
    conv_id = str(uuid.uuid4())
    record = ConversationRecord(
        conversation_id=conv_id,
        alert_id="alert-1",
        service="payment",
        severity="critical",
        status="completed",
        diagnosis="根因：Redis 连接池耗尽",
        hypothesis_history=[{"hypothesis": "Redis 问题", "result": "confirmed"}],
        messages=[{"role": "user", "content": "告警信息"}],
        step_count=3,
        truncated=False,
        mode="full",
        duration_seconds=12.5,
    )
    await repo.archive(record)
    result = await repo.get_by_id(conv_id)
    assert result is not None
    assert result.service == "payment"
    assert "Redis" in result.diagnosis

@pytest.mark.asyncio
async def test_list_by_service(repo):
    results = await repo.list_by_service("payment", limit=10)
    assert isinstance(results, list)

@pytest.mark.asyncio
async def test_list_recent(repo):
    results = await repo.list_recent(limit=5)
    assert isinstance(results, list)
```

### Step 2: 验证测试失败

```bash
pytest tests/test_conversation.py -v
```
Expected: FAIL

### Step 3: 实现模型

```python
# src/conversation/models.py
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class ConversationRecord:
    conversation_id: str
    alert_id: str = ""
    service: str = ""
    severity: str = ""
    status: str = "active"  # active/completed/timeout
    diagnosis: str = ""
    hypothesis_history: list = field(default_factory=list)
    messages: list = field(default_factory=list)
    checkpoint_id: str = ""
    step_count: int = 0
    truncated: bool = False
    mode: str = "full"
    duration_seconds: float = 0.0
    created_at: datetime = None
    completed_at: datetime = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.utcnow()
```

### Step 4: 实现 repository

```python
# src/conversation/repository.py
import json
from src.conversation.models import ConversationRecord


class ConversationRepository:
    def __init__(self, db):
        self.db = db

    async def init_table(self):
        """初始化 conversations 表"""
        async with self.db.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS conversations (
                    conversation_id VARCHAR PRIMARY KEY,
                    alert_id VARCHAR,
                    service VARCHAR,
                    severity VARCHAR,
                    status VARCHAR DEFAULT 'active',
                    diagnosis TEXT DEFAULT '',
                    hypothesis_history JSONB DEFAULT '[]',
                    messages JSONB DEFAULT '[]',
                    checkpoint_id VARCHAR DEFAULT '',
                    step_count INT DEFAULT 0,
                    truncated BOOL DEFAULT FALSE,
                    mode VARCHAR DEFAULT 'full',
                    duration_seconds FLOAT DEFAULT 0,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    completed_at TIMESTAMPTZ
                );
                CREATE INDEX IF NOT EXISTS idx_conv_service ON conversations(service);
                CREATE INDEX IF NOT EXISTS idx_conv_severity ON conversations(severity);
                CREATE INDEX IF NOT EXISTS idx_conv_status ON conversations(status);
                CREATE INDEX IF NOT EXISTS idx_conv_created ON conversations(created_at);
            """)

    async def archive(self, record: ConversationRecord):
        async with self.db.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO conversations
                (conversation_id, alert_id, service, severity, status, diagnosis,
                 hypothesis_history, messages, checkpoint_id, step_count,
                 truncated, mode, duration_seconds, created_at, completed_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9, $10, $11, $12, $13, $14, $15)
                ON CONFLICT (conversation_id) DO UPDATE SET
                    status=$5, diagnosis=$6, hypothesis_history=$7::jsonb,
                    messages=$8::jsonb, step_count=$10, truncated=$11,
                    duration_seconds=$13, completed_at=$15
            """, record.conversation_id, record.alert_id, record.service, record.severity,
               record.status, record.diagnosis,
               json.dumps(record.hypothesis_history), json.dumps(record.messages),
               record.checkpoint_id, record.step_count,
               record.truncated, record.mode, record.duration_seconds,
               record.created_at, record.completed_at)

    async def get_by_id(self, conv_id: str) -> ConversationRecord | None:
        async with self.db.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM conversations WHERE conversation_id=$1", conv_id)
            if row is None:
                return None
            return self._row_to_record(row)

    async def list_by_service(self, service: str, limit: int = 20) -> list[ConversationRecord]:
        async with self.db.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM conversations WHERE service=$1 ORDER BY created_at DESC LIMIT $2",
                service, limit
            )
            return [self._row_to_record(r) for r in rows]

    async def list_recent(self, limit: int = 20) -> list[ConversationRecord]:
        async with self.db.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM conversations ORDER BY created_at DESC LIMIT $1", limit
            )
            return [self._row_to_record(r) for r in rows]

    @staticmethod
    def _row_to_record(row) -> ConversationRecord:
        d = dict(row)
        d["hypothesis_history"] = json.loads(d["hypothesis_history"]) if isinstance(d["hypothesis_history"], str) else d["hypothesis_history"]
        d["messages"] = json.loads(d["messages"]) if isinstance(d["messages"], str) else d["messages"]
        return ConversationRecord(**d)
```

### Step 5: 运行测试

```bash
pytest tests/test_conversation.py -v
```
Expected: 3 PASS

### Step 6: Commit

```bash
git add src/conversation/ tests/test_conversation.py
git commit -m "feat: 对话归档/搜索/回放（PostgreSQL）"
```

---

## Task 3.4: 自动触发 Worker

**Files:**
- Create: `src/worker/__init__.py`
- Create: `src/worker/agent_worker.py`
- Test: `tests/test_agent_worker.py`

### Step 1: 编写测试

```python
# tests/test_agent_worker.py
import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch
from src.worker.agent_worker import AgentWorker

@pytest.fixture
def worker():
    mock_kafka = Mock()
    mock_kafka.consume.return_value = []
    mock_diagnosis = Mock()
    mock_diagnosis.return_value = Mock(
        conversation_id="conv-1", diagnosis="测试诊断", truncated=False,
        hypothesis="测试假设", hypothesis_history=[], step_count=3,
        duration_seconds=1.5, mode="full",
    )
    mock_notifier = AsyncMock()
    mock_store = Mock()
    return AgentWorker(
        kafka=mock_kafka,
        diagnosis_fn=mock_diagnosis,
        notifier=mock_notifier,
        incident_store=mock_store,
        conversation_repo=None,
    )

@pytest.mark.asyncio
async def test_worker_processes_alert(worker):
    """Worker 收到告警后应触发诊断"""
    worker.kafka.consume.return_value = [{
        "alert_id": "agg-001",
        "severity": "critical",
        "labels": {"service": "payment"},
        "annotations": {"summary": "CPU 过高"},
    }]
    worker._running = True
    # 只运行一次循环
    tasks = []
    async def stop_after():
        await asyncio.sleep(0.5)
        worker.stop()
    await asyncio.gather(worker.run(), stop_after())
    assert worker.diagnosis_fn.called

def test_worker_start_stop(worker):
    """Worker 启动和停止"""
    worker.start()
    assert worker._running is True
    worker.stop()
    assert worker._running is False
```

### Step 2: 验证测试失败

```bash
pytest tests/test_agent_worker.py -v
```
Expected: FAIL

### Step 3: 实现 Worker

```python
# src/worker/agent_worker.py
import asyncio
import logging
from src.agent.diagnosis import DiagnosisResult
from src.config import settings

logger = logging.getLogger(__name__)


class AgentWorker:
    """后台 Worker：消费 Kafka 聚合告警 → 自动触发 Agent 诊断"""

    def __init__(self, kafka, diagnosis_fn, notifier=None,
                 incident_store=None, conversation_repo=None):
        self.kafka = kafka
        self.diagnosis_fn = diagnosis_fn
        self.notifier = notifier
        self.incident_store = incident_store
        self.conversation_repo = conversation_repo
        self._running = False

    def start(self):
        self._running = True
        logger.info("AgentWorker 已启动")

    def stop(self):
        self._running = False
        logger.info("AgentWorker 已停止")

    async def run(self):
        """主循环"""
        self.start()
        while self._running:
            try:
                alerts = self.kafka.consume(
                    settings.kafka_topic_aggregated_alerts,
                    max_messages=10,
                    timeout=5,
                )
                for alert_data in alerts:
                    await self._process_alert(alert_data)
            except Exception as e:
                logger.error(f"Worker 消费失败: {e}")
            await asyncio.sleep(1)

    async def _process_alert(self, alert_data: dict):
        """处理单条聚合告警"""
        alert = {
            "alert_id": alert_data.get("alert_id", alert_data.get("aggregation_key", "")),
            "severity": alert_data.get("severity", "warning"),
            "labels": alert_data.get("labels", {}),
            "annotations": alert_data.get("annotations", {}),
        }
        service = alert["labels"].get("service", "unknown")
        logger.info(f"自动诊断触发: service={service}")

        result = self.diagnosis_fn(alert)

        # ChatOps 推送
        if self.notifier and not result.truncated:
            try:
                await self.notifier.send(result)
            except Exception as e:
                logger.error(f"通知发送失败: {e}")

        # 自动入库
        if self.incident_store and not result.truncated:
            self.incident_store.add_incident(
                summary=alert.get("annotations", {}).get("summary", ""),
                root_cause=result.diagnosis[:1024],
                service=service,
                severity=alert.get("severity", "warning"),
            )

        # 对话归档
        if self.conversation_repo:
            from src.conversation.models import ConversationRecord
            import asyncio as aio
            from datetime import datetime
            record = ConversationRecord(
                conversation_id=result.conversation_id,
                alert_id=alert.get("alert_id", ""),
                service=service,
                severity=alert.get("severity", "warning"),
                status="timeout" if result.truncated else "completed",
                diagnosis=result.diagnosis,
                hypothesis_history=result.hypothesis_history,
                step_count=result.step_count,
                truncated=result.truncated,
                mode=result.mode,
                duration_seconds=result.duration_seconds,
                completed_at=datetime.utcnow(),
            )
            try:
                await self.conversation_repo.archive(record)
            except Exception as e:
                logger.error(f"对话归档失败: {e}")
```

### Step 4: 运行测试

```bash
pytest tests/test_agent_worker.py -v
```
Expected: 2 PASS

### Step 5: Commit

```bash
git add src/worker/ tests/test_agent_worker.py
git commit -m "feat: 自动触发 Worker（Kafka → Agent 诊断）"
```

---

## Task 3.5: ChatOps 飞书/钉钉推送

**Files:**
- Create: `src/chatops/__init__.py`
- Create: `src/chatops/notifier.py`
- Test: `tests/test_notifier.py`

### Step 1: 编写测试

```python
# tests/test_notifier.py
import pytest
from unittest.mock import patch, Mock
from src.chatops.notifier import FeishuNotifier, DingtalkNotifier
from src.agent.diagnosis import DiagnosisResult

@pytest.fixture
def sample_result():
    return DiagnosisResult(
        conversation_id="conv-1",
        diagnosis="根因：Redis 连接池耗尽。置信度：92%。建议：扩容 Redis。",
        hypothesis="Redis 问题",
        hypothesis_history=[],
        step_count=3,
        truncated=False,
        duration_seconds=12.5,
        mode="full",
    )

def test_feishu_notifier_formats_message(sample_result):
    notifier = FeishuNotifier(webhook_url="https://hooks.feishu.com/test")
    card = notifier._build_card(sample_result)
    assert "Redis" in card["content"]
    assert "92%" in card["content"]

def test_feishu_notifier_send(sample_result):
    notifier = FeishuNotifier(webhook_url="https://hooks.feishu.com/test")
    with patch("httpx.AsyncClient.post") as mock_post:
        mock_post.return_value = Mock(status_code=200, json=lambda: {"code": 0})
        import asyncio
        result = asyncio.run(notifier.send(sample_result))
        assert result is True

def test_dingtalk_notifier_formats_message(sample_result):
    notifier = DingtalkNotifier(webhook_url="https://hooks.dingtalk.com/test")
    msg = notifier._build_message(sample_result)
    assert "Redis" in msg["markdown"]["text"]
```

### Step 2: 验证测试失败

```bash
pytest tests/test_notifier.py -v
```
Expected: FAIL

### Step 3: 实现通知器

```python
# src/chatops/notifier.py
import httpx
import logging
from abc import ABC, abstractmethod
from src.agent.diagnosis import DiagnosisResult

logger = logging.getLogger(__name__)


class BaseNotifier(ABC):
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url

    @abstractmethod
    async def send(self, result: DiagnosisResult) -> bool:
        pass


class FeishuNotifier(BaseNotifier):
    """飞书 Webhook 通知器"""

    async def send(self, result: DiagnosisResult) -> bool:
        card = self._build_card(result)
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(self.webhook_url, json={"msg_type": "interactive", "card": card})
                return resp.status_code == 200
        except Exception as e:
            logger.error(f"飞书推送失败: {e}")
            return False

    def _build_card(self, result: DiagnosisResult) -> dict:
        status_emoji = "✅" if not result.truncated else "⚠️"
        return {
            "header": {"title": {"content": f"{status_emoji} OPS AI Agent 诊断报告", "tag": "plain_text"}},
            "elements": [
                {"tag": "div", "text": {"content": f"**会话 ID**: {result.conversation_id}"}},
                {"tag": "div", "text": {"content": f"**耗时**: {result.duration_seconds:.1f}s | **步数**: {result.step_count}"}},
                {"tag": "hr"},
                {"tag": "div", "text": {"content": f"**诊断结论**\n{result.diagnosis}"}},
                {"tag": "hr"},
                {"tag": "action", "actions": [
                    {"tag": "button", "text": {"content": "确认回滚"}, "type": "primary"},
                    {"tag": "button", "text": {"content": "重启服务"}, "type": "default"},
                    {"tag": "button", "text": {"content": "忽略"}, "type": "danger"},
                ]},
            ],
            "content": f"{result.diagnosis}",
        }


class DingtalkNotifier(BaseNotifier):
    """钉钉 Webhook 通知器"""

    async def send(self, result: DiagnosisResult) -> bool:
        message = self._build_message(result)
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(self.webhook_url, json=message)
                return resp.status_code == 200
        except Exception as e:
            logger.error(f"钉钉推送失败: {e}")
            return False

    def _build_message(self, result: DiagnosisResult) -> dict:
        status = "✅ 诊断完成" if not result.truncated else "⚠️ 诊断超时"
        text = (
            f"## {status}\n\n"
            f"**会话 ID**: {result.conversation_id}\n\n"
            f"**耗时**: {result.duration_seconds:.1f}s | **步数**: {result.step_count}\n\n"
            f"---\n\n"
            f"**诊断结论**\n{result.diagnosis}\n\n"
        )
        return {
            "msgtype": "markdown",
            "markdown": {"title": "OPS AI Agent 诊断报告", "text": text},
        }
```

### Step 4: 运行测试

```bash
pytest tests/test_notifier.py -v
```
Expected: 3 PASS

### Step 5: Commit

```bash
git add src/chatops/ tests/test_notifier.py
git commit -m "feat: 飞书/钉钉 ChatOps 诊断推送"
```

---

## Task 3.6: 诊断 API 端点

**Files:**
- Create: `src/chatops/routes.py`
- Modify: `src/api/main.py`

### Step 1: 实现 ChatOps API 路由

```python
# src/chatops/routes.py
from fastapi import APIRouter, Depends, HTTPException
from src.conversation.repository import ConversationRepository
from src.infra.database import Database
from src.config import settings
from src.api.dependencies import require_auth

router = APIRouter(prefix="/api/v1", tags=["diagnosis"])


def get_conversation_repo() -> ConversationRepository:
    db = Database(settings.pg_dsn)
    return ConversationRepository(db)


@router.get("/diagnosis/{conversation_id}")
async def get_diagnosis(conversation_id: str,
                        repo: ConversationRepository = Depends(get_conversation_repo),
                        auth: dict = Depends(require_auth)):
    """查询诊断结果"""
    try:
        await repo.db.init()
    except Exception:
        pass
    result = await repo.get_by_id(conversation_id)
    if result is None:
        raise HTTPException(status_code=404, detail="诊断记录不存在")
    return {
        "conversation_id": result.conversation_id,
        "status": result.status,
        "service": result.service,
        "severity": result.severity,
        "diagnosis": result.diagnosis,
        "step_count": result.step_count,
        "duration_seconds": result.duration_seconds,
        "truncated": result.truncated,
    }


@router.get("/conversations")
async def list_conversations(service: str = None, severity: str = None, limit: int = 20,
                             repo: ConversationRepository = Depends(get_conversation_repo),
                             auth: dict = Depends(require_auth)):
    """搜索历史对话"""
    try:
        await repo.db.init()
    except Exception:
        pass
    if service:
        results = await repo.list_by_service(service, limit=limit)
    else:
        results = await repo.list_recent(limit=limit)
    return [{
        "conversation_id": r.conversation_id,
        "status": r.status,
        "service": r.service,
        "severity": r.severity,
        "diagnosis": r.diagnosis[:200],
        "duration_seconds": r.duration_seconds,
        "created_at": r.created_at.isoformat() if r.created_at else None,
    } for r in results]
```

### Step 2: 在 main.py 中注册路由

```python
# 在 create_app() 函数中添加：
from src.chatops.routes import router as chatops_router
app.include_router(chatops_router)
```

### Step 3: 编写 API 测试

```python
# tests/test_chatops_api.py
import pytest
from unittest.mock import Mock, patch
from httpx import AsyncClient, ASGITransport
from src.api.main import create_app
from src.api.dependencies import get_token_service
from src.api.routes import get_kafka
from src.auth.token_service import TokenService, TokenPayload

@pytest.fixture
def token_svc():
    return TokenService(secret="test-secret", expire_hours=1)

@pytest.fixture
async def client(token_svc):
    app = create_app()
    app.dependency_overrides[get_token_service] = lambda: token_svc
    app.dependency_overrides[get_kafka] = lambda: Mock()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c

@pytest.mark.asyncio
async def test_get_diagnosis_not_found(client, token_svc):
    token = token_svc.create_token(TokenPayload(user_id="u1", role="admin", scopes=["read"]))
    resp = await client.get("/api/v1/diagnosis/nonexistent",
                            headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 404

@pytest.mark.asyncio
async def test_list_conversations(client, token_svc):
    token = token_svc.create_token(TokenPayload(user_id="u1", role="admin", scopes=["read"]))
    resp = await client.get("/api/v1/conversations",
                            headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)
```

### Step 4: 运行测试

```bash
pytest tests/test_chatops_api.py -v
```
Expected: 2 PASS

### Step 5: Commit

```bash
git add src/chatops/routes.py src/api/main.py tests/test_chatops_api.py
git commit -m "feat: 诊断查询 API（GET /diagnosis/{id}, GET /conversations）"
```

---

## Task 3.7: FastAPI Lifespan 集成 + 端到端测试

**Files:**
- Modify: `src/api/main.py`
- Create: `tests/integration/test_phase3_e2e.py`

### Step 1: 在 FastAPI lifespan 中启动 Worker

修改 `src/api/main.py` 中的 `create_app()`：

```python
from contextlib import asynccontextmanager
from src.worker.agent_worker import AgentWorker
from src.infra.kafka_client import KafkaManager
from src.knowledge.milvus_client import MilvusManager
from src.knowledge.incident_store import IncidentStore
from src.conversation.repository import ConversationRepository
from src.infra.database import Database
from src.chatops.notifier import FeishuNotifier

@asynccontextmanager
async def _lifespan(app: FastAPI):
    # 启动 Worker（可选，通过环境变量控制）
    if settings.agent_auto_trigger:
        worker = AgentWorker(
            kafka=KafkaManager(settings.kafka_bootstrap_servers),
            diagnosis_fn=lambda alert: run_diagnosis(alert),  # 简化
            notifier=FeishuNotifier(settings.feishu_webhook_url) if settings.feishu_webhook_url else None,
            incident_store=IncidentStore(milvus=MilvusManager()) if settings.milvus_enabled else None,
            conversation_repo=None,  # 在 Worker 内部创建
        )
        worker_task = asyncio.create_task(worker.run())
    yield
    if settings.agent_auto_trigger:
        worker.stop()
        worker_task.cancel()

def create_app() -> FastAPI:
    app = FastAPI(title="OPS AI Agent", version="0.2.0", lifespan=_lifespan)
    # ... 现有路由注册
```

同时在 `config.py` 中添加：

```python
# Agent Worker
agent_auto_trigger: bool = False
feishu_webhook_url: str = ""
dingtalk_webhook_url: str = ""
milvus_enabled: bool = True
milvus_host: str = "localhost"
milvus_port: str = "19530"
```

### Step 2: 编写端到端测试

```python
# tests/integration/test_phase3_e2e.py
import pytest
from unittest.mock import Mock, patch
from src.agent.diagnosis import run_diagnosis
from src.knowledge.incident_store import IncidentStore

@pytest.mark.integration
def test_diagnosis_with_similar_incidents():
    """端到端：诊断 + 相似故障检索（mock Milvus + LLM）"""
    mock_milvus = Mock()
    mock_milvus.search.return_value = [
        {"id": "inc-001", "distance": 0.1, "summary": "Redis 延迟问题",
         "root_cause": "Redis 连接池耗尽", "severity": "critical"}
    ]
    mock_milvus.insert.return_value = ["id-new"]
    mock_milvus.count.return_value = 1
    store = IncidentStore(milvus=mock_milvus)

    alert = {
        "alert_id": "e2e-001", "severity": "critical",
        "labels": {"service": "payment"},
        "annotations": {"summary": "P99 > 500ms"},
    }

    with patch("src.agent.graph.call_llm_with_retry") as mock_llm:
        mock_llm.side_effect = [
            Mock(content="需要查历史故障和日志", tool_calls=[
                {"name": "search_similar_incidents", "args": {"description": "payment 延迟", "top_k": 3}, "id": "c1"},
                {"name": "search_logs", "args": {"service": "payment", "keywords": "error"}, "id": "c2"},
            ]),
            Mock(content="评估：支持。置信度：95。确认根因。", tool_calls=None),
            Mock(content="根因：Redis 连接池耗尽（与历史故障 inc-001 相似）。置信度：95%。建议：扩容。", tool_calls=None),
        ]
        result = run_diagnosis(alert)
        assert result.step_count >= 1
        assert "Redis" in result.diagnosis

@pytest.mark.integration
def test_incident_store_auto_add():
    """诊断完成后自动入库"""
    mock_milvus = Mock()
    mock_milvus.search.return_value = []
    mock_milvus.insert.return_value = ["id-auto"]
    store = IncidentStore(milvus=mock_milvus)

    success = store.add_incident(
        summary="CPU 过高", root_cause="请求量激增",
        service="payment", severity="critical"
    )
    assert success is True
    mock_milvus.insert.assert_called_once()
```

### Step 3: 运行测试

```bash
pytest tests/integration/test_phase3_e2e.py -v -m integration
```
Expected: 2 PASS

### Step 4: Commit

```bash
git add src/api/main.py src/config.py tests/integration/test_phase3_e2e.py
git commit -m "feat: FastAPI lifespan Worker 集成 + Phase 3 E2E 测试"
```

---

## 最终验收检查清单

| 验收项 | 验证命令 |
|--------|----------|
| Milvus 客户端 | `pytest tests/test_milvus_client.py -v` |
| 故障案例存储 | `pytest tests/test_incident_store.py -v` |
| Agent 工具增强 | `pytest tests/test_agent_tools.py -v` |
| 对话归档 | `pytest tests/test_conversation.py -v` |
| 自动 Worker | `pytest tests/test_agent_worker.py -v` |
| ChatOps 推送 | `pytest tests/test_notifier.py -v` |
| 诊断 API | `pytest tests/test_chatops_api.py -v` |
| 端到端集成 | `pytest tests/integration/test_phase3_e2e.py -v -m integration` |
| 种子数据 | `python scripts/seed_incidents.py` |

---

## 目录结构总览

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
├── agent/
│   ├── tools.py                  # 增强：+ search_similar_incidents
│   └── ...
├── api/
│   └── main.py                   # 增强：lifespan Worker
└── config.py                     # 增强：Worker/Milvus 配置

scripts/
├── diagnose.py
└── seed_incidents.py             # 新增

tests/
├── test_milvus_client.py         # 新增
├── test_incident_store.py        # 新增
├── test_conversation.py          # 新增
├── test_agent_worker.py          # 新增
├── test_notifier.py              # 新增
├── test_chatops_api.py           # 新增
└── integration/
    └── test_phase3_e2e.py        # 新增
```
