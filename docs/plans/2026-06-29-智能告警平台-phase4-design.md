# OPS AI Agent 智能告警平台 — Phase 4 详细设计

> 版本：v1.0
> 日期：2026-06-29
> 依赖：Phase 1+2+3 全部能力

## 1. 目标与范围

Phase 4 构建**自动化执行与安全控制能力**，使 Agent 从「诊断建议」升级为「诊断+执行」的自治体。

### 范围决策

| 决策项 | 选择 |
|--------|------|
| K8s 执行方式 | 默认 Mock + 检测 kubeconfig 自动切换真实 API |
| 策略引擎 | Python 内嵌三层策略（L1 硬限制 / L2 影响评估 / L3 时间窗口） |
| 审计存储 | PostgreSQL（结构化） + Elasticsearch（全文检索）双写 |
| 高风险操作 | 强制人工审批（飞书/钉钉交互卡片 + Webhook 回调） |
| 回滚机制 | 执行前快照 + 恶化监控 + 自动回滚（失败 3 次升级人工） |

### 交付物

- 三层策略引擎（Python）
- K8s 执行器（Mock + 真实兼容）
- 回滚管理（快照 + 自动回滚）
- 审批服务（高风险操作人工审批）
- 审计服务（PG + ES 双写）
- Agent 写操作工具（restart_pod / scale / rollback）
- 单元测试 + 集成测试

---

## 2. 架构设计

```
Agent 诊断 → 输出 action_plan
                │
                ▼
┌───────────────────────────────────┐
│         策略引擎 (policy.py)        │
│  L1: 资源边界（硬限制，不可绕过）    │
│  L2: 影响评估（条件放行/审批）      │
│  L3: 时间窗口（变更窗口约束）       │
│  输出: allowed / denied / approval  │
└───────────────┬───────────────────┘
        allow   │   needs_approval
                ▼
┌───────────────────────────────────┐
│        审批服务 (approval.py)       │
│  创建审批请求 → ChatOps 推送卡片    │
│  Webhook 回调 → 批准/拒绝/超时      │
│  超时 30 分钟 → 自动拒绝            │
└───────────────┬───────────────────┘
                │ approved
                ▼
┌───────────────────────────────────┐
│      K8s 执行器 (k8s_executor.py)   │
│  restart_pod / scale / rollback    │
│  幂等 key + 执行前快照 + 回滚计划   │
│  默认 Mock，kubeconfig → 真实 API  │
└───────────────┬───────────────────┘
                │
       ┌────────┴────────┐
       ▼                 ▼
┌──────────────┐  ┌──────────────┐
│ 审计 (audit)  │  │ 回滚 (rollback)│
│ PG + ES 双写  │  │ 快照 + 回滚   │
└──────────────┘  └──────────────┘
```

---

## 3. 核心模块设计

### 3.1 策略引擎

#### 三层策略

```python
class PolicyEngine:
    # L1: 硬限制（deny）
    #   - 禁止操作 StatefulSet
    #   - 禁止操作核心中间件（kafka/redis/mysql）
    #   - 命名空间不在白名单内
    #   - 禁止 delete/scale_to_zero 操作

    # L2: 影响评估（needs_approval / allow）
    #   - 影响副本数 > 50% → 需要审批
    #   - Error Budget 耗尽 → deny
    #   - 目标服务有 P0 SLO → 需要审批

    # L3: 时间窗口（deny / allow）
    #   - 高风险操作必须在变更窗口内
    #   - 变更窗口: 工作日 10:00-18:00
    #   - 紧急窗口: 需额外标注 emergency=true
```

### 3.2 K8s 执行器

#### Mock 模式（默认）

```python
class MockK8sExecutor:
    def restart_pod(self, namespace, pod_name, idempotency_key):
        """模拟 Pod 重启，返回成功"""
    def scale_deployment(self, namespace, name, replicas, idempotency_key):
        """模拟扩缩容，记录操作"""
    def rollback_deployment(self, namespace, name, revision, idempotency_key):
        """模拟回滚"""
```

#### 真实模式（检测 kubeconfig）

```python
class RealK8sExecutor:
    def __init__(self, kubeconfig_path=None):
        # 加载 kubeconfig，创建 Kubernetes API 客户端
    def restart_pod(self, namespace, pod_name, idempotency_key):
        # kubectl rollout restart deployment/<name>
    def scale_deployment(self, namespace, name, replicas, idempotency_key):
        # kubectl scale deployment/<name> --replicas=N
    def rollback_deployment(self, namespace, name, revision, idempotency_key):
        # kubectl rollout undo deployment/<name> --to-revision=N
```

#### 执行模式自动切换

```python
def create_executor() -> K8sExecutor:
    kubeconfig = os.environ.get("KUBECONFIG") or "~/.kube/config"
    if Path(kubeconfig).expanduser().exists():
        return RealK8sExecutor(kubeconfig)
    return MockK8sExecutor()
```

### 3.3 回滚管理

```python
class RollbackManager:
    def snapshot_before(self, action, target):
        """执行前快照：记录当前副本数等状态"""

    def check_after(self, snapshot, action, max_wait_seconds=300):
        """执行后监控：指标是否恶化？是否需要回滚？"""

    def rollback(self, snapshot, action):
        """执行回滚，失败重试 3 次，仍失败升级人工"""
```

### 3.4 审批服务

#### 审批流程

```
1. 策略引擎判定 "needs_approval"
2. approval.create_request() → 存 PostgreSQL
3. ChatOps 推送审批卡片（飞书/钉钉）
   - 卡片内容：操作详情、风险等级、影响范围
   - 按钮：[批准] [拒绝]
4. Webhook 回调 POST /api/v1/approval/callback
5. 批准 → 执行器执行
6. 拒绝 / 超时 30min → 记录拒绝
```

### 3.5 审计服务

#### PostgreSQL 表

```sql
CREATE TABLE audit_logs (
    audit_id VARCHAR PRIMARY KEY,
    conversation_id VARCHAR,
    action VARCHAR,
    target_namespace VARCHAR,
    target_resource VARCHAR,
    idempotency_key VARCHAR UNIQUE,
    policy_result VARCHAR,    -- allowed/denied/approval_required
    approval_status VARCHAR,  -- approved/denied/pending
    execution_status VARCHAR, -- success/failed/rolled_back
    snapshot JSONB,
    revert_plan JSONB,
    error_message TEXT,
    executed_by VARCHAR,      -- agent-{id} / human-{id}
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);
```

#### ES 文档

```json
{
    "audit_id": "...",
    "action": "restart_pod",
    "target": "payment-7d8f9-abc",
    "outcome": "success",
    "details": "滚动重启完成，新 Pod 正常运行",
    "timestamp": "2026-06-29T10:00:00Z"
}
```

### 3.6 Agent 工具扩展

Phase 4 为 Agent 工具集新增 3 个执行工具：

```python
tools = [
    {"name": "restart_pod",
     "description": "滚动重启指定服务的 Pod",
     "risk_level": "low"},

    {"name": "scale_deployment",
     "description": "调整 Deployment 副本数",
     "risk_level": "medium"},

    {"name": "rollback_deployment",
     "description": "回滚 Deployment 到上一版本",
     "risk_level": "high", "requires_approval": true},
]
```

---

## 4. 目录结构

```
src/execution/
├── __init__.py
├── policy.py            # 三层策略引擎
├── k8s_executor.py      # K8s 执行器（Mock + 真实）
├── rollback.py          # 回滚管理
├── approval.py          # 审批服务
├── audit.py             # 审计服务（PG + ES 双写）
└── action_tools.py      # Agent 执行工具注册

tests/
├── test_policy.py
├── test_k8s_executor.py
├── test_rollback.py
├── test_approval.py
├── test_audit.py
└── integration/
    └── test_phase4_e2e.py
```

---

## 5. 验收标准

| 验收项 | 标准 |
|--------|------|
| 策略 L1 硬限制 | StatefulSet 操作被拒绝 |
| 策略 L2 影响评估 | >50% 副本操作需要审批 |
| 策略 L3 时间窗口 | 高风险操作非窗口期拒绝 |
| Mock 执行器 | restart/scale/rollback 返回成功 |
| 幂等性 | 重复 idempotency_key 不重复执行 |
| 回滚 | 执行失败自动触发回滚 |
| 审批流程 | 高风险操作 → 审批卡片 → 批准/拒绝 |
| 审计 PG | audit_logs 写入 PostgreSQL |
| 审计 ES | audit_logs 写入 Elasticsearch |
| Agent 工具 | 3 个执行工具注册到 ToolRegistry |

---

本文档为 Phase 4 设计，基于 Phase 1-3 基础设施构建自动化执行与安全控制能力。
