# OPS AI Agent 智能告警平台 — Phase 1 实现计划

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 构建智能告警平台的数据基础层：统一采集管道、告警标准化、聚合降噪、知识图谱基础、只读工具集。

**Architecture:** 采用星型数据管道架构（分散采集 → 中心存储），以 Python 为主开发语言，Kafka 为事件总线，LangGraph 为 Agent 编排框架。Phase 1 覆盖从数据采集到告警输出的完整链路，不涉及 Agent 自动诊断。

**Tech Stack:** Python 3.11+, Kafka 3.x, Elasticsearch 8.x, Loki 3.x, VictoriaMetrics, Neo4j 5.x, PostgreSQL 15, Redis 7, Chroma, LangGraph, FastAPI, Docker Compose

---

## 前置：项目初始化

### Task 0: 项目骨架搭建

**Files:**
- Create: `pyproject.toml`
- Create: `src/__init__.py`
- Create: `src/config.py`
- Create: `docker-compose.yml`
- Create: `.env.example`
- Create: `tests/__init__.py`

**Step 1: 初始化 pyproject.toml**

```toml
[project]
name = "agent-ops"
version = "0.1.0"
description = "OPS AI Agent 智能告警平台"
requires-python = ">=3.11"
dependencies = [
    "fastapi>=0.109",
    "uvicorn[standard]>=0.27",
    "kafka-python>=2.0",
    "elasticsearch>=8.12",
    "neo4j>=5.18",
    "psycopg[binary]>=3.1",
    "redis>=5.0",
    "chromadb>=0.4",
    "langgraph>=0.0.40",
    "langchain>=0.1",
    "langchain-openai>=0.0.5",
    "httpx>=0.26",
    "pydantic>=2.6",
    "pydantic-settings>=2.1",
    "python-dotenv>=1.0",
    "prometheus-client>=0.19",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0",
    "pytest-asyncio>=0.23",
    "httpx>=0.26",
    "black>=24.0",
    "ruff>=0.1",
]

[tool.pytest.ini_options]
testpaths = ["tests"]
asyncio_mode = "auto"
```

**Step 2: 创建配置模块 `src/config.py`**

```python
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_raw_alerts: str = "raw.alerts"
    kafka_topic_aggregated_alerts: str = "aggregated.alerts"

    # Elasticsearch
    es_hosts: str = "http://localhost:9200"
    es_log_index_pattern: str = "logs-*"
    es_alert_index: str = "alerts"

    # PostgreSQL
    pg_dsn: str = "postgresql://agent:agent@localhost:5432/agentops"

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # Neo4j
    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = "password"

    # LLM
    llm_api_base: str = "https://api.openai.com/v1"
    llm_api_key: str = ""
    llm_model: str = "gpt-4o"

    # Auth
    token_secret: str = "change-me-in-production"
    token_expire_hours: int = 24

    # Agent
    agent_max_steps: int = 15
    agent_max_duration_seconds: int = 300

    model_config = {"env_file": ".env", "env_prefix": "AGENTOPS_"}

settings = Settings()
```

**Step 3: 创建 `.env.example`**

```
AGENTOPS_LLM_API_KEY=sk-xxx
AGENTOPS_TOKEN_SECRET=your-secret-here
AGENTOPS_NEO4J_PASSWORD=your-neo4j-password
```

**Step 4: 验证**

```bash
python -c "from src.config import settings; print(settings.kafka_bootstrap_servers)"
```
Expected: `localhost:9092`

**Step 5: Commit**

```bash
git init
git add -A
git commit -m "chore: 项目骨架初始化"
```

---

## Milestone 1: 核心基础设施

### Task 1.1: Docker Compose 基础设施

**Files:**
- Create: `docker-compose.yml`
- Create: `docker/kafka/init.sh`

**Step 1: 编写 docker-compose.yml**

```yaml
version: "3.8"
services:
  # ---- 消息队列 ----
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on: [zookeeper]
    ports: ["9092:9092"]
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"

  # ---- 数据库 ----
  postgres:
    image: postgres:15-alpine
    ports: ["5432:5432"]
    environment:
      POSTGRES_DB: agentops
      POSTGRES_USER: agent
      POSTGRES_PASSWORD: agent
    volumes: [pg_data:/var/lib/postgresql/data]

  redis:
    image: redis:7-alpine
    ports: ["6379:6379"]

  neo4j:
    image: neo4j:5-community
    ports: ["7474:7474", "7687:7687"]
    environment:
      NEO4J_AUTH: neo4j/password
    volumes: [neo4j_data:/data]

  # ---- 存储 ----
  elasticsearch:
    image: elasticsearch:8.12.0
    ports: ["9200:9200"]
    environment:
      discovery.type: single-node
      xpack.security.enabled: "false"
    volumes: [es_data:/usr/share/elasticsearch/data]

  minio:
    image: minio/minio:latest
    ports: ["9000:9000", "9001:9001"]
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    volumes: [minio_data:/data]

volumes:
  pg_data: null
  neo4j_data: null
  es_data: null
  minio_data: null
```

**Step 2: 启动并验证**

```bash
docker compose up -d
docker compose ps  # 所有服务状态应为 Up
```

**Step 3: Commit**

```bash
git add docker-compose.yml docker/
git commit -m "feat: 添加 Docker Compose 基础设施"
```

---

### Task 1.2: Kafka 主题初始化与连接测试

**Files:**
- Create: `src/infra/kafka_client.py`
- Test: `tests/test_kafka_client.py`

**Step 1: 编写失败测试**

```python
# tests/test_kafka_client.py
import pytest
from src.infra.kafka_client import KafkaManager

@pytest.fixture
def kafka_mgr():
    return KafkaManager(bootstrap_servers="localhost:9092")

def test_create_topics(kafka_mgr):
    topics = ["raw.alerts", "aggregated.alerts", "k8s.events"]
    kafka_mgr.create_topics(topics)
    existing = kafka_mgr.list_topics()
    for t in topics:
        assert t in existing

def test_produce_and_consume(kafka_mgr):
    topic = "raw.alerts"
    test_msg = {"alert_id": "test-001", "severity": "critical"}
    kafka_mgr.produce(topic, test_msg)
    messages = kafka_mgr.consume(topic, max_messages=1, timeout=5)
    assert len(messages) == 1
    assert messages[0]["alert_id"] == "test-001"
```

**Step 2: 验证测试失败**

```bash
pytest tests/test_kafka_client.py -v
```
Expected: FAIL (module not found)

**Step 3: 实现 KafkaManager**

```python
# src/infra/kafka_client.py
import json
from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

class KafkaManager:
    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers

    def _get_admin(self):
        return KafkaAdminClient(bootstrap_servers=self.bootstrap_servers)

    def create_topics(self, topics: list[str], num_partitions: int = 3):
        admin = self._get_admin()
        new_topics = [NewTopic(t, num_partitions, 1) for t in topics]
        try:
            admin.create_topics(new_topics)
        except TopicAlreadyExistsError:
            pass
        admin.close()

    def list_topics(self) -> set[str]:
        admin = self._get_admin()
        topics = admin.list_topics()
        admin.close()
        return set(topics)

    def produce(self, topic: str, message: dict):
        producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        producer.send(topic, message)
        producer.flush()
        producer.close()

    def consume(self, topic: str, max_messages: int = 10, timeout: int = 10) -> list[dict]:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset="earliest",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            consumer_timeout_ms=timeout * 1000,
        )
        messages = []
        for msg in consumer:
            messages.append(msg.value)
            if len(messages) >= max_messages:
                break
        consumer.close()
        return messages
```

**Step 4: 验证测试通过**

```bash
pytest tests/test_kafka_client.py -v
```
Expected: PASS

**Step 5: Commit**

```bash
git add src/infra/kafka_client.py tests/test_kafka_client.py
git commit -m "feat: Kafka 客户端封装与主题初始化"
```

---

### Task 1.3: PostgreSQL 表结构初始化

**Files:**
- Create: `src/infra/database.py`
- Create: `src/schemas/alert.py`
- Test: `tests/test_database.py`

**Step 1: 编写失败测试**

```python
# tests/test_database.py
import pytest
from src.infra.database import Database
from src.schemas.alert import AlertRecord

@pytest.fixture
async def db():
    d = Database("postgresql://agent:agent@localhost:5432/agentops")
    await d.init()
    yield d
    # cleanup handled by test

@pytest.mark.asyncio
async def test_create_and_query_alert(db):
    alert = AlertRecord(
        alert_id="test-001",
        source="prometheus",
        severity="critical",
        status="firing",
        labels={"service": "payment", "cluster": "prod"},
        annotations={"summary": "CPU > 90%"},
    )
    await db.insert_alert(alert)
    result = await db.get_alert("test-001")
    assert result is not None
    assert result.severity == "critical"

@pytest.mark.asyncio
async def test_list_active_alerts(db):
    alerts = await db.list_active_alerts(limit=10)
    assert isinstance(alerts, list)
```

**Step 2: 实现 schema 与 database 层**

```python
# src/schemas/alert.py
from pydantic import BaseModel
from datetime import datetime
from typing import Optional

class AlertRecord(BaseModel):
    alert_id: str
    source: str
    fingerprint: str = ""
    severity: str
    status: str
    labels: dict = {}
    annotations: dict = {}
    timestamp: datetime = None
    value: Optional[float] = None

    def model_post_init(self, __context):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow()
```

```python
# src/infra/database.py
import asyncpg
from src.schemas.alert import AlertRecord

class Database:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool = None

    async def init(self):
        self.pool = await asyncpg.create_pool(self.dsn, min_size=2, max_size=10)
        async with self.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS alerts (
                    alert_id VARCHAR PRIMARY KEY,
                    source VARCHAR NOT NULL,
                    fingerprint VARCHAR,
                    severity VARCHAR NOT NULL,
                    status VARCHAR NOT NULL,
                    labels JSONB DEFAULT '{}',
                    annotations JSONB DEFAULT '{}',
                    timestamp TIMESTAMPTZ DEFAULT NOW(),
                    value DOUBLE PRECISION,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS idx_alerts_severity ON alerts(severity);
                CREATE INDEX IF NOT EXISTS idx_alerts_status ON alerts(status);
                CREATE INDEX IF NOT EXISTS idx_alerts_timestamp ON alerts(timestamp);
            """)

    async def insert_alert(self, alert: AlertRecord):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO alerts (alert_id, source, fingerprint, severity, status, labels, annotations, timestamp, value)
                VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8, $9)
            """, alert.alert_id, alert.source, alert.fingerprint, alert.severity,
               alert.status, alert.labels, alert.annotations, alert.timestamp, alert.value)

    async def get_alert(self, alert_id: str) -> AlertRecord | None:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM alerts WHERE alert_id=$1", alert_id)
            if row is None:
                return None
            return AlertRecord(**dict(row))

    async def list_active_alerts(self, limit: int = 100) -> list[AlertRecord]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM alerts WHERE status='firing' ORDER BY timestamp DESC LIMIT $1", limit
            )
            return [AlertRecord(**dict(r)) for r in rows]
```

**Step 3: 运行测试**

```bash
pytest tests/test_database.py -v
```
Expected: PASS

**Step 4: Commit**

```bash
git add src/schemas/ src/infra/database.py tests/test_database.py
git commit -m "feat: PostgreSQL 告警存储层"
```

---

## Milestone 2: 数据采集管道

### Task 2.1: 告警标准化适配器

**Files:**
- Create: `src/pipeline/adapters.py`
- Test: `tests/test_adapters.py`

**Step 1: 编写测试**

```python
# tests/test_adapters.py
from src.pipeline.adapters import AlertNormalizer, UnifiedAlert

def test_normalize_prometheus_alert():
    raw = {
        "receiver": "ops",
        "status": "firing",
        "alerts": [{
            "labels": {"alertname": "HighCPU", "service": "payment"},
            "annotations": {"summary": "CPU > 90%"},
            "startsAt": "2026-06-28T10:00:00Z",
        }]
    }
    results = AlertNormalizer.normalize("prometheus", raw)
    assert len(results) == 1
    assert results[0].source == "prometheus"
    assert results[0].severity == "warning"
    assert results[0].labels["service"] == "payment"

def test_normalize_unknown_source():
    raw = {"message": "something happened"}
    results = AlertNormalizer.normalize("unknown_source", raw)
    assert len(results) == 1
    assert results[0].source == "unknown_source"
```

**Step 2: 实现适配器**

```python
# src/pipeline/adapters.py
import uuid
from datetime import datetime
from dataclasses import dataclass, field

@dataclass
class UnifiedAlert:
    alert_id: str
    source: str
    fingerprint: str
    severity: str
    status: str
    labels: dict
    annotations: dict
    timestamp: str
    value: float | None = None

class AlertNormalizer:
    SEVERITY_MAP = {"critical": "critical", "warning": "warning", "info": "info"}

    @classmethod
    def normalize(cls, source: str, raw: dict) -> list[UnifiedAlert]:
        handler = getattr(cls, f"_from_{source}", cls._from_generic)
        return handler(raw)

    @classmethod
    def _from_prometheus(cls, raw: dict) -> list[UnifiedAlert]:
        results = []
        for alert in raw.get("alerts", []):
            labels = alert.get("labels", {})
            severity = labels.get("severity", "warning")
            results.append(UnifiedAlert(
                alert_id=str(uuid.uuid4()),
                source="prometheus",
                fingerprint=labels.get("alertname", ""),
                severity=severity,
                status=raw.get("status", "firing"),
                labels=labels,
                annotations=alert.get("annotations", {}),
                timestamp=alert.get("startsAt", datetime.utcnow().isoformat()),
            ))
        return results

    @classmethod
    def _from_generic(cls, raw: dict) -> list[UnifiedAlert]:
        return [UnifiedAlert(
            alert_id=str(uuid.uuid4()),
            source="generic",
            fingerprint=raw.get("title", str(uuid.uuid4())[:8]),
            severity=raw.get("severity", "info"),
            status=raw.get("status", "firing"),
            labels=raw.get("labels", {}),
            annotations=raw.get("annotations", {}),
            timestamp=raw.get("timestamp", datetime.utcnow().isoformat()),
        )]
```

**Step 3: 运行测试**

```bash
pytest tests/test_adapters.py -v
```
Expected: PASS

**Step 4: Commit**

```bash
git add src/pipeline/adapters.py tests/test_adapters.py
git commit -m "feat: 告警标准化适配器"
```

---

### Task 2.2: Webhook 告警接入端点

**Files:**
- Create: `src/api/__init__.py`
- Create: `src/api/routes.py`
- Create: `src/api/main.py`
- Test: `tests/test_api.py`

**Step 1: 编写测试**

```python
# tests/test_api.py
import pytest
from httpx import AsyncClient, ASGITransport
from src.api.main import create_app

@pytest.fixture
async def client():
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c

@pytest.mark.asyncio
async def test_health_check(client):
    resp = await client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"

@pytest.mark.asyncio
async def test_ingest_alert(client):
    payload = {
        "source": "prometheus",
        "raw": {
            "status": "firing",
            "alerts": [{
                "labels": {"alertname": "TestAlert", "service": "test-svc"},
                "annotations": {"summary": "test"},
                "startsAt": "2026-06-28T10:00:00Z",
            }]
        }
    }
    resp = await client.post("/api/v1/alerts/ingest", json=payload)
    assert resp.status_code == 202
    data = resp.json()
    assert data["accepted"] == 1
    assert "alert_ids" in data
```

**Step 2: 实现 API**

```python
# src/api/main.py
from fastapi import FastAPI
from src.api.routes import router

def create_app() -> FastAPI:
    app = FastAPI(title="OPS AI Agent", version="0.1.0")
    app.include_router(router)
    return app

app = create_app()
```

```python
# src/api/routes.py
from fastapi import APIRouter, Depends
from src.pipeline.adapters import AlertNormalizer
from src.infra.kafka_client import KafkaManager
from src.config import settings
from pydantic import BaseModel

router = APIRouter()

class IngestRequest(BaseModel):
    source: str
    raw: dict

class IngestResponse(BaseModel):
    accepted: int
    alert_ids: list[str]

def get_kafka():
    return KafkaManager(settings.kafka_bootstrap_servers)

@router.get("/health")
async def health():
    return {"status": "ok", "version": "0.1.0"}

@router.post("/api/v1/alerts/ingest", response_model=IngestResponse, status_code=202)
async def ingest_alert(req: IngestRequest, kafka: KafkaManager = Depends(get_kafka)):
    alerts = AlertNormalizer.normalize(req.source, req.raw)
    for alert in alerts:
        kafka.produce(settings.kafka_topic_raw_alerts, alert.__dict__)
    return IngestResponse(
        accepted=len(alerts),
        alert_ids=[a.alert_id for a in alerts],
    )
```

**Step 3: 运行测试**

```bash
pytest tests/test_api.py -v
```
Expected: PASS

**Step 4: Commit**

```bash
git add src/api/ tests/test_api.py
git commit -m "feat: 告警接入 Webhook API"
```

---

### Task 2.3: 日志搜索工具（ES + Drain 采样）

**Files:**
- Create: `src/tools/__init__.py`
- Create: `src/tools/log_search.py`
- Test: `tests/test_log_search.py`

**Step 1: 编写测试**

```python
# tests/test_log_search.py
from src.tools.log_search import LogSampler

def test_drain_cluster_and_sample():
    logs = [
        "2026-06-28T10:00:00 INFO Request completed in 5ms path=/api/users",     # x500
        "2026-06-28T10:00:01 INFO Request completed in 8ms path=/api/orders",    # x300
        "2026-06-28T10:00:02 INFO health check passed",                          # x900
        "2026-06-28T10:00:03 ERROR NullPointerException at line 42",             # x3
        "2026-06-28T10:00:04 WARN connection pool exhausted, retrying",           # x2
    ]
    # 扩展为大量日志
    expanded = []
    expanded.extend([logs[0]] * 500)
    expanded.extend([logs[1]] * 300)
    expanded.extend([logs[2]] * 900)
    expanded.extend([logs[3]] * 3)
    expanded.extend([logs[4]] * 2)

    sampler = LogSampler()
    result = sampler.sample(expanded, max_return=20)

    # 低频异常日志必须全部保留
    error_logs = [r for r in result if "ERROR" in r["content"]]
    warn_logs = [r for r in result if "WARN" in r["content"]]
    assert len(error_logs) == 3   # 低频全保留
    assert len(warn_logs) == 2    # 低频全保留
    assert len(result) <= 20
    # 高频模式只保留少量样本
    health_logs = [r for r in result if "health check" in r["content"]]
    assert len(health_logs) <= 3
```

**Step 2: 实现 Drain 日志采样器**

```python
# src/tools/log_search.py
import re
from typing import List

class Drain:
    """轻量级日志模式聚类，基于 Drain3 算法简化"""
    def __init__(self, depth: int = 4, similarity_threshold: float = 0.5):
        self.depth = depth
        self.similarity_threshold = similarity_threshold
        self.clusters: dict[str, list] = {}

    @staticmethod
    def _tokenize(log: str) -> list[str]:
        return log.strip().split()

    @staticmethod
    def _get_template(tokens: list[str]) -> str:
        """将数字、IP、路径等替换为通配符"""
        result = []
        for t in tokens:
            if re.match(r'^[\d.]+$', t):       # 数字/IP
                result.append("<*>")
            elif re.match(r'^/[\w/]+$', t):     # 路径
                result.append("<path>")
            elif re.match(r'^[0-9a-f-]{36}$', t):  # UUID
                result.append("<uuid>")
            else:
                result.append(t)
        return " ".join(result)

    def match(self, log: str) -> str:
        tokens = self._tokenize(log)
        template = self._get_template(tokens)
        key = " ".join(tokens[:self.depth]) + "|" + template
        if key not in self.clusters:
            self.clusters[key] = []
        self.clusters[key].append(log)
        return key

    def get_cluster_sizes(self) -> dict[str, int]:
        return {k: len(v) for k, v in self.clusters.items()}

class LogSampler:
    def __init__(self):
        self.drain = Drain()

    def sample(self, logs: List[str], max_return: int = 30) -> List[dict]:
        if not logs:
            return []

        # 聚类
        for log in logs:
            self.drain.match(log)

        total = len(logs)
        sampled = []

        for cluster_key, entries in self.drain.clusters.items():
            ratio = len(entries) / total
            if ratio > 0.5:
                n = min(3, len(entries))
            elif ratio < 0.05:
                n = len(entries)
            else:
                n = min(2, len(entries))
            sampled.extend(entries[:n])

        # 优先保留含 ERROR/WARN 的
        priority = [l for l in logs if any(kw in l.upper() for kw in ["ERROR", "WARN", "FATAL", "CRITICAL"])]
        sampled = list(dict.fromkeys(priority + sampled))[:max_return]

        return [{
            "content": s,
            "critical": any(kw in s.upper() for kw in ["ERROR", "FATAL", "CRITICAL"]),
        } for s in sampled]
```

**Step 3: 运行测试**

```bash
pytest tests/test_log_search.py -v
```
Expected: PASS

**Step 4: Commit**

```bash
git add src/tools/log_search.py tests/test_log_search.py
git commit -m "feat: Drain 日志模式聚类与智能采样"
```

---

### Task 2.4: Elasticsearch 日志查询客户端

**Files:**
- Create: `src/infra/es_client.py`
- Test: `tests/test_es_client.py`

**Step 1: 编写测试**

```python
# tests/test_es_client.py
import pytest
from src.infra.es_client import ESLogClient

@pytest.fixture
def es():
    return ESLogClient("http://localhost:9200", index_pattern="logs-*")

@pytest.mark.asyncio
async def test_search_logs_basic(es):
    result = await es.search_logs(
        service="test-svc",
        keywords="error",
        time_range="5m",
        max_results=50,
    )
    assert isinstance(result, list)
    # 无数据时返回空列表
    assert result == [] or all("content" in r for r in result)
```

**Step 2: 实现 ES 客户端**

```python
# src/infra/es_client.py
from datetime import datetime, timedelta
from elasticsearch import AsyncElasticsearch
from src.tools.log_search import LogSampler

class ESLogClient:
    def __init__(self, hosts: str, index_pattern: str = "logs-*"):
        self.client = AsyncElasticsearch(hosts)
        self.index_pattern = index_pattern
        self.sampler = LogSampler()

    async def search_logs(
        self,
        service: str,
        keywords: str = "",
        time_range: str = "15m",
        max_results: int = 50,
    ) -> list[dict]:
        time_from = self._parse_time_range(time_range)
        query = {
            "bool": {
                "must": [
                    {"term": {"labels.service": service}},
                ],
                "filter": [{"range": {"@timestamp": {"gte": time_from.isoformat()}}}],
            }
        }
        if keywords:
            query["bool"]["must"].append({"query_string": {"query": keywords}})

        resp = await self.client.search(
            index=self.index_pattern,
            query=query,
            size=min(max_results * 10, 2000),  # 多取一些给采样器
            sort=[{"@timestamp": "desc"}],
        )

        raw_logs = [hit["_source"].get("log", hit["_source"].get("message", ""))
                    for hit in resp["hits"]["hits"]]
        return self.sampler.sample(raw_logs, max_return=max_results)

    @staticmethod
    def _parse_time_range(tr: str) -> datetime:
        num = int("".join(c for c in tr if c.isdigit()) or "15")
        if "h" in tr:
            return datetime.utcnow() - timedelta(hours=num)
        elif "d" in tr:
            return datetime.utcnow() - timedelta(days=num)
        return datetime.utcnow() - timedelta(minutes=num)

    async def close(self):
        await self.client.close()
```

**Step 3: 运行测试 (需要 ES 运行)**

```bash
docker compose up -d elasticsearch
# 等待 ES 就绪
pytest tests/test_es_client.py -v
```
Expected: PASS

**Step 4: Commit**

```bash
git add src/infra/es_client.py tests/test_es_client.py
git commit -m "feat: ES 日志查询客户端（含 Drain 采样集成）"
```

---

## Milestone 3: 告警处理管道

### Task 3.1: Kafka Streams 告警聚合引擎

**Files:**
- Create: `src/pipeline/aggregator.py`
- Test: `tests/test_aggregator.py`

**Step 1: 编写测试**

```python
# tests/test_aggregator.py
from src.pipeline.aggregator import AlertAggregator, AggregatedAlert

def make_alert(alert_id, labels, severity="critical", fingerprint=None):
    from src.pipeline.adapters import UnifiedAlert
    return UnifiedAlert(
        alert_id=alert_id, source="prometheus", fingerprint=fingerprint or alert_id,
        severity=severity, status="firing", labels=labels,
        annotations={"summary": "test"}, timestamp="2026-06-28T10:00:00Z",
    )

def test_mechanical_aggregation():
    agg = AlertAggregator(window_seconds=300)
    # 相同标签不同告警 → 应聚合
    agg.process(make_alert("a1", {"service": "payment", "alertname": "HighCPU"}))
    agg.process(make_alert("a2", {"service": "payment", "alertname": "HighCPU"}))
    agg.process(make_alert("a3", {"service": "payment", "alertname": "HighCPU"}))

    # 不同标签 → 不应聚合
    agg.process(make_alert("b1", {"service": "order", "alertname": "HighMem"}))

    results = agg.flush()
    # payment HighCPU 的 3 条应聚合为 1 条
    assert len(results) == 2
    payment_agg = [r for r in results if r.labels.get("service") == "payment"][0]
    assert payment_agg.merged_count == 3

def test_semantic_aggregation_same_node():
    """同 Node 的告警应语义聚合"""
    agg = AlertAggregator(window_seconds=300)
    agg.process(make_alert("a1", {"service": "svc-a", "node": "node-1"}))
    agg.process(make_alert("a2", {"service": "svc-b", "node": "node-1"}))
    agg.process(make_alert("a3", {"service": "svc-c", "node": "node-1"}))

    results = agg.flush()
    # 应生成一条 Node 级别的聚合告警
    assert len(results) == 1
    assert results[0].aggregation_type == "node"
```

**Step 2: 实现聚合引擎**

```python
# src/pipeline/aggregator.py
import hashlib
import json
import time
from dataclasses import dataclass, field
from collections import defaultdict
from src.pipeline.adapters import UnifiedAlert

@dataclass
class AggregatedAlert:
    aggregation_key: str
    aggregation_type: str  # "mechanical" | "node" | "deployment" | "namespace"
    severity: str
    status: str
    labels: dict
    source_alerts: list[str]  # 原始 alert_ids
    merged_count: int
    first_at: str
    last_at: str

class AlertAggregator:
    """告警聚合器：机械聚合 + 语义聚合"""

    def __init__(self, window_seconds: int = 300):
        self.window_seconds = window_seconds
        self._buffer: dict[str, list[UnifiedAlert]] = defaultdict(list)
        self._last_flush = time.time()

    def _make_key(self, alert: UnifiedAlert) -> str:
        """机械聚合 key：基于关键标签 hash"""
        key_labels = {
            k: alert.labels[k]
            for k in ["service", "alertname", "namespace"]
            if k in alert.labels
        }
        raw = json.dumps(key_labels, sort_keys=True)
        return hashlib.md5(raw.encode()).hexdigest()

    def _make_node_key(self, alert: UnifiedAlert) -> str | None:
        node = alert.labels.get("node") or alert.labels.get("instance")
        return f"node:{node}" if node else None

    def _make_deploy_key(self, alert: UnifiedAlert) -> str | None:
        deploy = alert.labels.get("deployment")
        return f"deploy:{deploy}" if deploy else None

    def process(self, alert: UnifiedAlert):
        """处理一条告警，放入缓冲区"""
        # 机械聚合
        mech_key = self._make_key(alert)
        self._buffer[mech_key].append(alert)
        # 语义聚合 key（辅助，不影响机械聚合）
        for key_fn in [self._make_node_key, self._make_deploy_key]:
            key = key_fn(alert)
            if key:
                self._buffer[key].append(alert)

    def flush(self) -> list[AggregatedAlert]:
        """刷新窗口，输出聚合告警"""
        results = []
        now = time.time()
        for key, alerts in self._buffer.items():
            if not alerts:
                continue
            agg_type = "mechanical"
            if key.startswith("node:"):
                agg_type = "node"
            elif key.startswith("deploy:"):
                agg_type = "deployment"

            # 确定最高严重级别
            severity_order = {"critical": 3, "warning": 2, "info": 1}
            max_sev = max(alerts, key=lambda a: severity_order.get(a.severity, 0))

            results.append(AggregatedAlert(
                aggregation_key=key,
                aggregation_type=agg_type,
                severity=max_sev.severity,
                status="firing",
                labels=alerts[0].labels,
                source_alerts=[a.alert_id for a in alerts],
                merged_count=len(alerts),
                first_at=min(a.timestamp for a in alerts),
                last_at=max(a.timestamp for a in alerts),
            ))

        self._buffer.clear()
        self._last_flush = now
        return results
```

**Step 3: 运行测试**

```bash
pytest tests/test_aggregator.py -v
```
Expected: PASS

**Step 4: Commit**

```bash
git add src/pipeline/aggregator.py tests/test_aggregator.py
git commit -m "feat: 机械+语义告警聚合引擎"
```

---

### Task 3.2: 告警处理管道编排

**Files:**
- Create: `src/pipeline/orchestrator.py`
- Test: `tests/test_orchestrator.py`

**Step 1: 编写测试**

```python
# tests/test_orchestrator.py
import asyncio
import pytest
from unittest.mock import Mock, patch
from src.pipeline.orchestrator import PipelineOrchestrator
from src.pipeline.adapters import UnifiedAlert

def make_alert(alert_id, labels):
    return UnifiedAlert(
        alert_id=alert_id, source="test", fingerprint="fp",
        severity="warning", status="firing", labels=labels,
        annotations={}, timestamp="2026-06-28T10:00:00Z",
    )

@pytest.mark.asyncio
async def test_pipeline_processes_alert():
    mock_kafka = Mock()
    mock_db = Mock()
    mock_es = Mock()
    orch = PipelineOrchestrator(
        kafka=mock_kafka,
        db=mock_db,
        es=mock_es,
        window_seconds=1,  # 短窗口便于测试
    )
    orch.process(make_alert("a1", {"service": "svc", "node": "n1"}))
    orch.process(make_alert("a2", {"service": "svc", "node": "n1"}))
    orch.process(make_alert("a3", {"service": "svc", "node": "n1"}))

    await asyncio.sleep(1.5)
    results = orch.flush()

    assert len(results) > 0
    # 验证写入了 Kafka
    assert mock_kafka.produce.called
    # 验证写入了 DB
    assert mock_db.insert_alert.called
```

**Step 2: 实现管道编排器**

```python
# src/pipeline/orchestrator.py
import asyncio
import logging
from src.pipeline.aggregator import AlertAggregator
from src.pipeline.adapters import UnifiedAlert
from src.infra.kafka_client import KafkaManager
from src.infra.database import Database
from src.infra.es_client import ESLogClient
from src.config import settings

logger = logging.getLogger(__name__)

class PipelineOrchestrator:
    """告警处理管道：消费 raw.alerts → 聚合 → 输出 aggregated.alerts"""

    def __init__(self, kafka: KafkaManager, db: Database, es: ESLogClient,
                 window_seconds: int = 300):
        self.kafka = kafka
        self.db = db
        self.es = es
        self.aggregator = AlertAggregator(window_seconds=window_seconds)
        self._running = False

    def process(self, alert: UnifiedAlert):
        """处理单条告警"""
        logger.info(f"Processing alert {alert.alert_id} from {alert.source}")
        self.aggregator.process(alert)

    def flush(self) -> list:
        """刷新窗口，输出聚合告警"""
        aggregated = self.aggregator.flush()
        for agg in aggregated:
            # 写入 Kafka
            self.kafka.produce(
                settings.kafka_topic_aggregated_alerts,
                agg.__dict__,
            )
            logger.info(f"Aggregated alert: key={agg.aggregation_key}, "
                        f"type={agg.aggregation_type}, count={agg.merged_count}")
        return aggregated

    async def run(self):
        """主循环：持续消费 → 处理 → 定时刷新"""
        self._running = True
        last_flush = asyncio.get_event_loop().time()

        while self._running:
            # 消费 raw.alerts
            try:
                messages = self.kafka.consume(
                    settings.kafka_topic_raw_alerts,
                    max_messages=100,
                    timeout=5,
                )
                for msg in messages:
                    alert = UnifiedAlert(**msg)
                    self.process(alert)
            except Exception as e:
                logger.error(f"消费告警失败: {e}")

            # 按窗口刷新
            now = asyncio.get_event_loop().time()
            if now - last_flush >= self.aggregator.window_seconds:
                self.flush()
                last_flush = now

            await asyncio.sleep(1)

    def stop(self):
        self._running = False
```

**Step 3: 运行测试**

```bash
pytest tests/test_orchestrator.py -v
```
Expected: PASS

**Step 4: Commit**

```bash
git add src/pipeline/orchestrator.py tests/test_orchestrator.py
git commit -m "feat: 告警处理管道编排器"
```

---

## Milestone 4: 知识图谱基础

### Task 4.1: Neo4j Schema 初始化与查询工具

**Files:**
- Create: `src/infra/neo4j_client.py`
- Test: `tests/test_neo4j_client.py`

**Step 1: 编写测试**

```python
# tests/test_neo4j_client.py
import pytest
from src.infra.neo4j_client import Neo4jClient

@pytest.fixture
def neo4j():
    client = Neo4jClient("bolt://localhost:7687", "neo4j", "password")
    client.init_schema()
    yield client
    client.close()

def test_init_schema(neo4j):
    constraints = neo4j.query("SHOW CONSTRAINTS")
    assert len(constraints) > 0

def test_merge_service_and_dependency(neo4j):
    neo4j.merge_service("payment", {"team": "platform", "namespace": "prod"})
    neo4j.merge_service("redis", {"type": "middleware"})
    neo4j.merge_dependency("payment", "redis", "USES")

    # 查询拓扑
    topo = neo4j.get_upstream_services("payment")
    assert len(topo) > 0
    assert any(d["name"] == "redis" for d in topo)

def test_blast_radius(neo4j):
    neo4j.merge_service("a", {})
    neo4j.merge_service("b", {})
    neo4j.merge_service("c", {})
    neo4j.merge_dependency("a", "b", "DEPENDS_ON")
    neo4j.merge_dependency("b", "c", "DEPENDS_ON")

    affected = neo4j.analyze_blast_radius("a", hops=2)
    assert len(affected) == 2  # b, c
```

**Step 2: 实现 Neo4j 客户端**

```python
# src/infra/neo4j_client.py
from neo4j import GraphDatabase

class Neo4jClient:
    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def query(self, cypher: str, params: dict = None) -> list[dict]:
        with self.driver.session() as session:
            result = session.run(cypher, params or {})
            return [dict(record) for record in result]

    def init_schema(self):
        """初始化约束和索引"""
        constraints = [
            "CREATE CONSTRAINT IF NOT EXISTS FOR (s:Service) REQUIRE s.name IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Node) REQUIRE n.name IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (t:Team) REQUIRE t.name IS UNIQUE",
            "CREATE INDEX IF NOT EXISTS FOR (s:SLO) ON (s.name)",
        ]
        for c in constraints:
            try:
                self.query(c)
            except Exception:
                pass  # 约束已存在

    def merge_service(self, name: str, properties: dict):
        props = ", ".join(f"{k}: ${k}" for k in properties)
        self.query(
            f"MERGE (s:Service {{name: $name}}) SET s += {{{props}}}",
            {"name": name, **properties},
        )

    def merge_dependency(self, from_svc: str, to_svc: str, relation: str):
        self.query(f"""
            MATCH (a:Service {{name: $from}})
            MATCH (b:Service {{name: $to}})
            MERGE (a)-[:{relation}]->(b)
        """, {"from": from_svc, "to": to_svc})

    def get_upstream_services(self, service: str) -> list[dict]:
        return self.query("""
            MATCH (s:Service {name: $name})-[r:DEPENDS_ON|USES]->(d)
            RETURN d.name AS name, d.type AS type, type(r) AS relation
        """, {"name": service})

    def analyze_blast_radius(self, service: str, hops: int = 3) -> list[dict]:
        return self.query(f"""
            MATCH path = (s:Service {{name: $name}})-[:DEPENDS_ON*1..{hops}]->(d:Service)
            RETURN d.name AS service, length(path) AS distance
            ORDER BY distance
        """, {"name": service})

    def find_common_dependency(self, svc_a: str, svc_b: str) -> list[dict]:
        return self.query("""
            MATCH p = shortestPath((a:Service {name: $a})-[:DEPENDS_ON*]-(b:Service {name: $b}))
            RETURN [n in nodes(p) | n.name] AS path
        """, {"a": svc_a, "b": svc_b})

    def close(self):
        self.driver.close()
```

**Step 3: 运行测试**

```bash
docker compose up -d neo4j
# 等待 Neo4j 就绪
pytest tests/test_neo4j_client.py -v
```
Expected: PASS

**Step 4: Commit**

```bash
git add src/infra/neo4j_client.py tests/test_neo4j_client.py
git commit -m "feat: Neo4j 客户端与图算法查询"
```

---

### Task 4.2: K8s Informer 同步服务

**Files:**
- Create: `src/sync/__init__.py`
- Create: `src/sync/k8s_syncer.py`
- Test: `tests/test_k8s_syncer.py`

**Step 1: 编写测试**

```python
# tests/test_k8s_syncer.py
from unittest.mock import Mock, MagicMock
from src.sync.k8s_syncer import K8sTopologySyncer

def test_sync_pod_to_neo4j():
    mock_neo4j = Mock()
    syncer = K8sTopologySyncer(mock_neo4j)

    pod_event = {
        "type": "ADDED",
        "object": {
            "metadata": {
                "name": "payment-7d8f9-abc",
                "namespace": "prod",
                "labels": {"app": "payment"},
            },
            "spec": {"nodeName": "node-1"},
            "status": {"phase": "Running"},
        }
    }
    syncer.handle_pod_event(pod_event)
    mock_neo4j.merge_service.assert_called()
    mock_neo4j.query.assert_called()  # SCHEDULED_ON 关系

def test_sync_node_to_neo4j():
    mock_neo4j = Mock()
    syncer = K8sTopologySyncer(mock_neo4j)

    syncer.handle_node_event({
        "type": "ADDED",
        "object": {
            "metadata": {"name": "node-1", "labels": {"zone": "us-east-1a"}},
            "status": {"conditions": [{"type": "Ready", "status": "True"}]},
        }
    })
    mock_neo4j.query.assert_called()
```

**Step 2: 实现 K8s 同步器**

```python
# src/sync/k8s_syncer.py
import logging
from src.infra.neo4j_client import Neo4jClient

logger = logging.getLogger(__name__)

class K8sTopologySyncer:
    """同步 K8s 拓扑到 Neo4j"""

    def __init__(self, neo4j: Neo4jClient):
        self.neo4j = neo4j

    def handle_pod_event(self, event: dict):
        obj = event["object"]
        metadata = obj["metadata"]
        namespace = metadata["namespace"]
        pod_name = metadata["name"]
        app = metadata.get("labels", {}).get("app", pod_name.split("-")[0])
        node_name = obj.get("spec", {}).get("nodeName", "")
        phase = obj.get("status", {}).get("phase", "Unknown")

        # 确保 Service 节点存在
        self.neo4j.merge_service(app, {"namespace": namespace})

        # 确保 Node 节点存在
        if node_name:
            self.neo4j.query(
                "MERGE (n:Node {name: $name})", {"name": node_name}
            )
            # Pod SCHEDULED_ON Node
            self.neo4j.query("""
                MATCH (s:Service {name: $svc})
                MATCH (n:Node {name: $node})
                MERGE (s)-[:SCHEDULED_ON]->(n)
                SET s.last_seen = datetime()
            """, {"svc": app, "node": node_name})

        logger.info(f"Synced Pod: {namespace}/{pod_name} (app={app}, node={node_name}, phase={phase})")

    def handle_node_event(self, event: dict):
        obj = event["object"]
        name = obj["metadata"]["name"]
        labels = obj["metadata"].get("labels", {})
        zone = labels.get("topology.kubernetes.io/zone", labels.get("zone", "unknown"))
        ready = any(
            c["type"] == "Ready" and c["status"] == "True"
            for c in obj.get("status", {}).get("conditions", [])
        )

        self.neo4j.query("""
            MERGE (n:Node {name: $name})
            SET n.zone = $zone, n.ready = $ready, n.last_seen = datetime()
        """, {"name": name, "zone": zone, "ready": ready})

        logger.info(f"Synced Node: {name} (zone={zone}, ready={ready})")

    def handle_deployment_event(self, event: dict):
        obj = event["object"]
        metadata = obj["metadata"]
        name = metadata["name"]
        namespace = metadata["namespace"]
        replicas = obj.get("spec", {}).get("replicas", 0)

        self.neo4j.merge_service(name, {
            "namespace": namespace,
            "kind": "Deployment",
            "replicas": replicas,
        })
        logger.info(f"Synced Deployment: {namespace}/{name} (replicas={replicas})")
```

**Step 3: 运行测试**

```bash
pytest tests/test_k8s_syncer.py -v
```
Expected: PASS

**Step 4: Commit**

```bash
git add src/sync/k8s_syncer.py tests/test_k8s_syncer.py
git commit -m "feat: K8s 拓扑同步到 Neo4j"
```

---

## Milestone 5: 工具集与 API

### Task 5.1: 工具注册中心

**Files:**
- Create: `src/tools/registry.py`
- Test: `tests/test_tools_registry.py`

**Step 1: 编写测试**

```python
# tests/test_tools_registry.py
from src.tools.registry import ToolRegistry, ToolDefinition

def test_register_and_list_tools():
    registry = ToolRegistry()
    registry.register(ToolDefinition(
        name="get_topology",
        description="查询服务拓扑",
        parameters={"service": {"type": "string"}},
    ))
    tools = registry.get_openai_schema()
    assert len(tools) == 1
    assert tools[0]["function"]["name"] == "get_topology"

def test_tool_execution():
    registry = ToolRegistry()
    called = False

    def handler(service: str):
        nonlocal called
        called = True
        return {"dependencies": ["redis", "mysql"]}

    registry.register(ToolDefinition(
        name="get_topology",
        description="查询服务拓扑",
        parameters={"service": {"type": "string"}},
    ), handler=handler)

    result = registry.execute("get_topology", {"service": "payment"})
    assert called
    assert "redis" in result["dependencies"]
```

**Step 2: 实现工具注册中心**

```python
# src/tools/registry.py
from dataclasses import dataclass, field
from typing import Callable, Any

@dataclass
class ToolDefinition:
    name: str
    description: str
    parameters: dict  # JSON Schema properties

class ToolRegistry:
    def __init__(self):
        self._tools: dict[str, ToolDefinition] = {}
        self._handlers: dict[str, Callable] = {}

    def register(self, tool: ToolDefinition, handler: Callable = None):
        self._tools[tool.name] = tool
        if handler:
            self._handlers[tool.name] = handler

    def get_openai_schema(self) -> list[dict]:
        return [{
            "type": "function",
            "function": {
                "name": t.name,
                "description": t.description,
                "parameters": {
                    "type": "object",
                    "properties": t.parameters,
                    "required": list(t.parameters.keys()),
                },
            }
        } for t in self._tools.values()]

    def execute(self, name: str, args: dict) -> Any:
        handler = self._handlers.get(name)
        if not handler:
            raise ValueError(f"未知工具: {name}")
        return handler(**args)

    def list_tools(self) -> list[str]:
        return list(self._tools.keys())
```

**Step 3: 运行测试**

```bash
pytest tests/test_tools_registry.py -v
```
Expected: PASS

**Step 4: Commit**

```bash
git add src/tools/registry.py tests/test_tools_registry.py
git commit -m "feat: 工具注册中心"
```

---

### Task 5.2: Token 认证服务

**Files:**
- Create: `src/auth/__init__.py`
- Create: `src/auth/token_service.py`
- Test: `tests/test_auth.py`

**Step 1: 编写测试**

```python
# tests/test_auth.py
import time
import pytest
from src.auth.token_service import TokenService, TokenPayload

@pytest.fixture
def token_svc():
    return TokenService(secret="test-secret", expire_hours=1)

def test_create_and_verify_token(token_svc):
    payload = TokenPayload(user_id="user-1", role="admin", scopes=["read", "write"])
    token = token_svc.create_token(payload)
    assert len(token) > 20

    decoded = token_svc.verify_token(token)
    assert decoded.user_id == "user-1"
    assert decoded.role == "admin"
    assert decoded.scopes == ["read", "write"]

def test_expired_token(token_svc):
    svc = TokenService(secret="test", expire_hours=-1)  # 立即过期
    token = svc.create_token(TokenPayload(user_id="u1", role="viewer", scopes=["read"]))
    with pytest.raises(ValueError, match="Token 已过期"):
        svc.verify_token(token)

def test_invalid_token(token_svc):
    with pytest.raises(ValueError, match="Token 无效"):
        token_svc.verify_token("invalid-token")
```

**Step 2: 实现 Token 服务**

```python
# src/auth/token_service.py
import hashlib
import hmac
import json
import time
from base64 import urlsafe_b64encode, urlsafe_b64decode
from dataclasses import dataclass

@dataclass
class TokenPayload:
    user_id: str
    role: str
    scopes: list[str]
    exp: int = 0

    def is_expired(self) -> bool:
        return self.exp > 0 and self.exp < time.time()

class TokenService:
    def __init__(self, secret: str, expire_hours: int = 24):
        self.secret = secret.encode()
        self.expire_hours = expire_hours

    def create_token(self, payload: TokenPayload) -> str:
        payload.exp = int(time.time()) + self.expire_hours * 3600
        data = json.dumps(payload.__dict__).encode()
        signature = hmac.new(self.secret, data, hashlib.sha256).hexdigest()
        token_body = urlsafe_b64encode(data).decode()
        return f"{token_body}.{signature}"

    def verify_token(self, token: str) -> TokenPayload:
        parts = token.split(".")
        if len(parts) != 2:
            raise ValueError("Token 格式无效")
        token_body, signature = parts
        try:
            data = urlsafe_b64decode(token_body)
        except Exception:
            raise ValueError("Token 无效")

        expected_sig = hmac.new(self.secret, data, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(signature, expected_sig):
            raise ValueError("Token 签名无效")

        payload = TokenPayload(**json.loads(data))
        if payload.is_expired():
            raise ValueError("Token 已过期")
        return payload
```

**Step 3: 运行测试**

```bash
pytest tests/test_auth.py -v
```
Expected: PASS

**Step 4: Commit**

```bash
git add src/auth/ tests/test_auth.py
git commit -m "feat: Token 签发与验证服务"
```

---

### Task 5.3: 认证中间件集成 API

**Files:**
- Modify: `src/api/routes.py`
- Create: `src/api/dependencies.py`
- Test: `tests/test_api_auth.py`

**Step 1: 编写测试**

```python
# tests/test_api_auth.py
import pytest
from httpx import AsyncClient, ASGITransport
from src.api.main import create_app
from src.auth.token_service import TokenService, TokenPayload

@pytest.fixture
def token_svc():
    return TokenService(secret="test-secret", expire_hours=1)

@pytest.fixture
async def client(token_svc):
    app = create_app()
    app.dependency_overrides[get_token_service] = lambda: token_svc
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c

@pytest.mark.asyncio
async def test_protected_endpoint_without_token(client):
    resp = await client.post("/api/v1/alerts/ingest", json={"source": "test", "raw": {}})
    assert resp.status_code == 401

@pytest.mark.asyncio
async def test_protected_endpoint_with_valid_token(client, token_svc):
    token = token_svc.create_token(TokenPayload(
        user_id="u1", role="admin", scopes=["write"],
    ))
    resp = await client.post(
        "/api/v1/alerts/ingest",
        json={"source": "test", "raw": {}},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 202  # 不是 401
```

**Step 2: 实现认证中间件**

```python
# src/api/dependencies.py
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from src.auth.token_service import TokenService
from src.config import settings

security = HTTPBearer()

def get_token_service() -> TokenService:
    return TokenService(settings.token_secret, settings.token_expire_hours)

async def require_auth(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    svc: TokenService = Depends(get_token_service),
) -> dict:
    """验证 Bearer Token"""
    try:
        payload = svc.verify_token(credentials.credentials)
        return {"user_id": payload.user_id, "role": payload.role, "scopes": payload.scopes}
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(e))
```

**Step 3: 更新路由**

```python
# src/api/routes.py（更新版，添加认证依赖）
from src.api.dependencies import require_auth

# 原来的 ingest_alert 增加认证依赖
@router.post("/api/v1/alerts/ingest", ...)
async def ingest_alert(
    req: IngestRequest,
    kafka: KafkaManager = Depends(get_kafka),
    auth: dict = Depends(require_auth),  # 新增
):
    ...
```

**Step 4: 运行测试**

```bash
pytest tests/test_api_auth.py -v
```
Expected: PASS

**Step 5: Commit**

```bash
git add src/api/ tests/test_api_auth.py
git commit -m "feat: API Bearer Token 认证中间件"
```

---

## Milestone 6: Agent 核心（Sprint 0 原型）

### Task 6.1: LangGraph ReAct 循环原型

**Files:**
- Create: `src/agent/__init__.py`
- Create: `src/agent/graph.py`
- Create: `src/agent/state.py`
- Test: `tests/test_agent_graph.py`

**Step 1: 编写测试**

```python
# tests/test_agent_graph.py
import pytest
from unittest.mock import Mock, patch
from src.agent.state import AgentState
from src.agent.graph import build_graph
from src.tools.registry import ToolRegistry, ToolDefinition

@pytest.fixture
def mock_tools():
    registry = ToolRegistry()
    registry.register(ToolDefinition(
        name="query_prometheus",
        description="查询 Prometheus 指标",
        parameters={"query": {"type": "string"}},
    ), handler=lambda query: {"result": "CPU 92%"})
    return registry

def test_agent_graph_compiles(mock_tools):
    graph = build_graph(mock_tools)
    assert graph is not None

def test_agent_runs_diagnosis_loop(mock_tools):
    """验证 Agent 能完成完整的诊断循环"""
    graph = build_graph(mock_tools)

    initial_state = AgentState(
        alert={
            "alert_id": "test-001",
            "severity": "critical",
            "labels": {"service": "payment"},
            "annotations": {"summary": "CPU > 90%"},
            "source": "prometheus",
        },
        messages=[],
        hypothesis="",
        hypothesis_history=[],
        evidence={},
        diagnosis="",
        action_plan={},
        active_intent="diagnose",
        pending_confirmations=[],
        protected_data=[],
        compressed_memory=[],
        mode="full",
        step_count=0,
        start_time=0,
    )

    # 使用 mock LLM 以便单元测试
    with patch("src.agent.graph.call_llm") as mock_llm:
        mock_llm.return_value = type("Response", (), {
            "content": "CPU 过高，需要进一步诊断",
            "tool_calls": None,
        })()
        result = graph.invoke(initial_state, config={"recursion_limit": 5})
        assert "messages" in result
        assert result["step_count"] >= 0
```

**Step 2: 实现 Agent 状态与图**

```python
# src/agent/state.py
from typing import TypedDict, List
from enum import Enum

class AgentMode(Enum):
    FULL = "full"
    DIAGNOSE_ONLY = "diagnose"
    NOTIFY_ONLY = "notify"

class AgentState(TypedDict):
    alert: dict
    messages: List[dict]
    hypothesis: str
    hypothesis_history: List[dict]
    evidence: dict
    diagnosis: str
    action_plan: dict
    active_intent: str
    pending_confirmations: List[str]
    protected_data: List[dict]
    compressed_memory: List[str]
    mode: str
    step_count: int
    start_time: float
```

```python
# src/agent/graph.py
import time
from langgraph.graph import StateGraph, END
from langgraph.prebuilt import ToolNode
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, FunctionMessage
from src.agent.state import AgentState, AgentMode
from src.tools.registry import ToolRegistry
from src.config import settings

MAX_STEPS = settings.agent_max_steps
MAX_DURATION = settings.agent_max_duration_seconds

SYSTEM_PROMPT = """你是运维 AI Agent，负责告警诊断。
收到告警后：1) 形成假设 2) 调用工具验证 3) 迭代直到确认根因。
诊断完成后输出结构化结论。"""

def call_llm(state: AgentState) -> dict:
    """调用 LLM（生产环境替换为真实调用）"""
    from langchain_openai import ChatOpenAI
    llm = ChatOpenAI(
        base_url=settings.llm_api_base,
        api_key=settings.llm_api_key,
        model=settings.llm_model,
    )
    llm_with_tools = llm.bind_tools(state.get("available_tools", []))

    messages = [SystemMessage(content=SYSTEM_PROMPT)]
    messages.extend(state["messages"])

    response = llm_with_tools.invoke(messages)
    state["messages"].append(response)
    return {"messages": state["messages"]}

def call_tool(state: AgentState, tool_registry: ToolRegistry) -> dict:
    """执行工具调用"""
    last_msg = state["messages"][-1]
    results = []
    for tc in last_msg.tool_calls:
        try:
            result = tool_registry.execute(tc["name"], tc["args"])
            results.append(FunctionMessage(content=str(result), name=tc["name"]))
        except Exception as e:
            results.append(FunctionMessage(
                content=f"工具错误: {e}",
                name=tc["name"],
            ))
    state["messages"].extend(results)
    state["step_count"] += 1
    return {"messages": state["messages"], "step_count": state["step_count"]}

def should_continue(state: AgentState) -> str:
    last_msg = state["messages"][-1]
    if hasattr(last_msg, "tool_calls") and last_msg.tool_calls:
        if state["step_count"] >= MAX_STEPS:
            return "timeout"
        if time.time() - state["start_time"] > MAX_DURATION:
            return "timeout"
        return "tool_executor"
    return END

def build_graph(tool_registry: ToolRegistry) -> StateGraph:
    workflow = StateGraph(AgentState)

    workflow.add_node("agent", call_llm)
    workflow.add_node("tool_executor", lambda s: call_tool(s, tool_registry))

    workflow.set_entry_point("agent")
    workflow.add_conditional_edges("agent", should_continue, {
        "tool_executor": "tool_executor",
        "timeout": END,
        END: END,
    })
    workflow.add_edge("tool_executor", "agent")

    return workflow.compile()
```

**Step 3: 运行测试**

```bash
pytest tests/test_agent_graph.py -v
```
Expected: PASS (需设置 mock)

**Step 4: Commit**

```bash
git add src/agent/ tests/test_agent_graph.py
git commit -m "feat: LangGraph ReAct 诊断循环原型"
```

---

### Task 6.2: 自身监控（Meta-monitoring）

**Files:**
- Create: `src/monitoring/__init__.py`
- Create: `src/monitoring/metrics.py`
- Test: `tests/test_monitoring.py`

**Step 1: 编写测试**

```python
# tests/test_monitoring.py
from src.monitoring.metrics import MetricsRegistry

def test_counter_increment():
    reg = MetricsRegistry()
    reg.counter("agent_analyses_total").inc()
    reg.counter("agent_analyses_total").inc()
    assert reg.counter("agent_analyses_total").value == 2

def test_histogram_observe():
    reg = MetricsRegistry()
    reg.histogram("tool_call_duration_seconds").observe(0.5)
    reg.histogram("tool_call_duration_seconds").observe(1.2)
    assert reg.histogram("tool_call_duration_seconds").count == 2
```

**Step 2: 实现指标注册**

```python
# src/monitoring/metrics.py
from prometheus_client import Counter, Histogram, Gauge, generate_latest, REGISTRY
from fastapi import APIRouter

router = APIRouter()

class MetricsRegistry:
    """平台自身监控指标注册"""

    def __init__(self):
        self._counters: dict[str, Counter] = {}
        self._histograms: dict[str, Histogram] = {}
        self._gauges: dict[str, Gauge] = {}

    def counter(self, name: str, description: str = "", labels: list[str] = None) -> "CounterWrapper":
        if name not in self._counters:
            self._counters[name] = Counter(name, description, labels or [])
        return CounterWrapper(self._counters[name])

    def histogram(self, name: str, description: str = "", labels: list[str] = None) -> "HistogramWrapper":
        if name not in self._histograms:
            self._histograms[name] = Histogram(name, description, labels or [])
        return HistogramWrapper(self._histograms[name])

    def gauge(self, name: str, description: str = "", labels: list[str] = None):
        if name not in self._gauges:
            self._gauges[name] = Gauge(name, description, labels or [])
        return self._gauges[name]

class CounterWrapper:
    def __init__(self, counter: Counter):
        self._counter = counter

    def inc(self, amount: int = 1):
        self._counter.inc(amount)

    @property
    def value(self):
        samples = list(self._counter.collect())[0].samples
        return int(samples[0].value) if samples else 0

class HistogramWrapper:
    def __init__(self, histogram: Histogram):
        self._histogram = histogram

    def observe(self, amount: float):
        self._histogram.observe(amount)

    @property
    def count(self):
        samples = list(self._histogram.collect())[0].samples
        count_samples = [s for s in samples if s.name.endswith("_count")]
        return int(count_samples[0].value) if count_samples else 0
```

**Step 3: 在 API 中添加 /metrics 端点**

```python
# src/api/routes.py 追加
from src.monitoring.metrics import generate_latest
from fastapi.responses import PlainTextResponse

@router.get("/metrics", response_class=PlainTextResponse)
async def metrics():
    return generate_latest()
```

**Step 4: 运行测试**

```bash
pytest tests/test_monitoring.py -v
```
Expected: PASS

**Step 5: Commit**

```bash
git add src/monitoring/ tests/test_monitoring.py
git commit -m "feat: Meta-monitoring 自身监控指标"
```

---

## Milestone 7: 集成与端到端测试

### Task 7.1: 端到端 Smoke Test

**Files:**
- Create: `tests/integration/test_e2e_alert_pipeline.py`

**Step 1: 编写端到端测试**

```python
# tests/integration/test_e2e_alert_pipeline.py
import pytest
import asyncio
from httpx import AsyncClient, ASGITransport
from src.api.main import create_app
from src.auth.token_service import TokenService, TokenPayload

@pytest.fixture
async def client():
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c

@pytest.mark.integration
@pytest.mark.asyncio
async def test_full_alert_ingest_pipeline(client):
    """端到端：告警接入 → 标准化 → Kafka"""
    token_svc = TokenService("test-secret", expire_hours=1)
    token = token_svc.create_token(TokenPayload(user_id="u1", role="admin", scopes=["write"]))

    payload = {
        "source": "prometheus",
        "raw": {
            "status": "firing",
            "alerts": [
                {
                    "labels": {
                        "alertname": "TestHighCPU",
                        "service": "test-svc",
                        "severity": "critical",
                    },
                    "annotations": {"summary": "CPU > 95%"},
                    "startsAt": "2026-06-28T10:00:00Z",
                }
            ],
        },
    }
    resp = await client.post(
        "/api/v1/alerts/ingest",
        json=payload,
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 202
    data = resp.json()
    assert data["accepted"] == 1

@pytest.mark.integration
@pytest.mark.asyncio
async def test_health_endpoint(client):
    resp = await client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"
```

**Step 2: 运行集成测试**

```bash
pytest tests/integration/ -v -m integration
```
Expected: PASS

**Step 3: Commit**

```bash
git add tests/integration/
git commit -m "test: 端到端告警管道集成测试"
```

---

## 最终验收检查清单

| 验收项 | 标准 | 验证命令 |
|--------|------|----------|
| 项目骨架 | `src/` 各模块可导入 | `python -c "from src.config import settings"` |
| Kafka | 主题创建 + 生产消费 | `pytest tests/test_kafka_client.py -v` |
| PostgreSQL | 告警 CRUD | `pytest tests/test_database.py -v` |
| API 认证 | 无 Token 返回 401 | `pytest tests/test_api_auth.py -v` |
| 告警标准化 | Prometheus → 统一格式 | `pytest tests/test_adapters.py -v` |
| 日志采样 | Drain 聚类正确 | `pytest tests/test_log_search.py -v` |
| 告警聚合 | 机械+语义聚合 | `pytest tests/test_aggregator.py -v` |
| Neo4j | 拓扑查询 + 爆炸半径 | `pytest tests/test_neo4j_client.py -v` |
| Agent 原型 | ReAct 循环可编译执行 | `pytest tests/test_agent_graph.py -v` |
| 自身监控 | /metrics 端点 | `curl http://localhost:8000/metrics` |
| 端到端 | 完整管道不报错 | `pytest tests/integration/ -v -m integration` |

---

## 目录结构总览

```
agent-ops/
├── pyproject.toml
├── .env.example
├── .gitignore
├── docker-compose.yml
├── docker/
│   └── kafka/
│       └── init.sh
├── src/
│   ├── __init__.py
│   ├── config.py
│   ├── api/
│   │   ├── __init__.py
│   │   ├── main.py
│   │   ├── routes.py
│   │   └── dependencies.py
│   ├── auth/
│   │   ├── __init__.py
│   │   └── token_service.py
│   ├── infra/
│   │   ├── kafka_client.py
│   │   ├── database.py
│   │   ├── es_client.py
│   │   └── neo4j_client.py
│   ├── pipeline/
│   │   ├── adapters.py
│   │   ├── aggregator.py
│   │   └── orchestrator.py
│   ├── tools/
│   │   ├── __init__.py
│   │   ├── registry.py
│   │   └── log_search.py
│   ├── sync/
│   │   ├── __init__.py
│   │   └── k8s_syncer.py
│   ├── agent/
│   │   ├── __init__.py
│   │   ├── state.py
│   │   └── graph.py
│   └── monitoring/
│       ├── __init__.py
│       └── metrics.py
└── tests/
    ├── __init__.py
    ├── test_kafka_client.py
    ├── test_database.py
    ├── test_adapters.py
    ├── test_api.py
    ├── test_api_auth.py
    ├── test_aggregator.py
    ├── test_orchestrator.py
    ├── test_log_search.py
    ├── test_es_client.py
    ├── test_neo4j_client.py
    ├── test_k8s_syncer.py
    ├── test_tools_registry.py
    ├── test_auth.py
    ├── test_agent_graph.py
    ├── test_monitoring.py
    └── integration/
        └── test_e2e_alert_pipeline.py
```
