# OPS AI Agent 智能告警平台 — Phase 2 详细设计

> 版本：v1.0
> 日期：2026-06-28
> 依赖：Phase 1 数据基础层

## 1. 目标与范围

Phase 2 构建 **AI Agent 核心诊断引擎**，在 Phase 1 数据基础层之上，使 Agent 能够对告警进行自主分析诊断。

### 范围决策

| 决策项 | 选择 |
|--------|------|
| 工具范围 | 先用已有工具（ES 日志、Neo4j 拓扑），Prometheus/Loki/Tempo 暂用 mock |
| 触发方式 | CLI 脚本 `scripts/diagnose.py` |
| 上下文管理 | 完整四层实现 + PostgreSQL Checkpointer + Redis 会话缓存 |
| 诊断模式 | 假设驱动状态机（显式建模诊断节点和路由） |

### 交付物

- 假设驱动诊断状态机（LangGraph）
- 分层上下文管理器
- LLM 重试与降级策略
- 诊断 CLI 入口
- PostgreSQL Checkpointer + Redis 会话缓存
- 单元测试 + 集成测试

---

## 2. 架构设计

### 2.1 总体架构

```
┌─────────────────────────────────────────────────────┐
│                  CLI 入口 (scripts/diagnose.py)       │
│                   ┌──────────────────┐               │
│                   │  告警 JSON 输入    │               │
│                   └────────┬─────────┘               │
└────────────────────────────┼─────────────────────────┘
                             │
┌────────────────────────────┴─────────────────────────┐
│                    Agent 引擎层                        │
│  ┌─────────────────────────────────────────────────┐ │
│  │             假设驱动状态机 (graph.py)              │ │
│  │  START → formulate_hypothesis → tool_executor   │ │
│  │    ↑          ↓                    ↓             │ │
│  │    └── refine ◄── evaluate_evidence             │ │
│  │                        ↓                        │ │
│  │              hypothesis_confirmed                │ │
│  │                        ↓                        │ │
│  │                root_cause → END                  │ │
│  └─────────────────────────────────────────────────┘ │
│  ┌─────────────────────────────────────────────────┐ │
│  │  分层上下文 (context.py)                          │ │
│  │  L1: 系统 Prompt │ L2: protected_data           │ │
│  │  L3: compressed_memory │ L4: 当前窗口            │ │
│  └─────────────────────────────────────────────────┘ │
│  ┌─────────────────────────────────────────────────┐ │
│  │  异常处理 (resilience.py)                         │ │
│  │  LLM 重试 │ 工具降级 │ 超时控制 │ 降级模式        │ │
│  └─────────────────────────────────────────────────┘ │
└────────────────────────────┬─────────────────────────┘
                             │
┌────────────────────────────┴─────────────────────────┐
│                    已有工具层 (Phase 1)                │
│  ES 日志查询 │ Neo4j 拓扑 │ 爆炸半径 │ 工具注册中心    │
└──────────────────────────────────────────────────────┘
                             │
┌────────────────────────────┴─────────────────────────┐
│                    持久化层 (Phase 1)                  │
│  PostgreSQL (Checkpointer) │ Redis (会话缓存)         │
└──────────────────────────────────────────────────────┘
```

### 2.2 与 Phase 1 的关系

Phase 2 是 Phase 1 的 **增量增强**，不替代而是扩展：

- `src/agent/graph.py` — 重写为假设驱动状态机
- `src/agent/state.py` — 扩展 AgentState 字段
- 新增 `context.py`、`resilience.py`、`tools.py`、`diagnosis.py`
- 复用 Phase 1 的 `ToolRegistry`、`Neo4jClient`、`ESLogClient`、`Database`

---

## 3. 核心模块设计

### 3.1 假设驱动状态机

#### 节点定义

| 节点 | 类型 | 职责 |
|------|------|------|
| `formulate_hypothesis` | LLM | 分析告警，生成候选假设列表，选出最可能的假设 |
| `tool_executor` | 工具 | 执行 Agent 请求的工具调用，标记关键数据 |
| `evaluate_evidence` | LLM | 评估工具返回结果是否支持当前假设，计算置信度 |
| `refine_hypothesis` | LLM | 根据新证据修正或替换假设 |
| `root_cause` | LLM | 综合所有证据输出最终根因诊断 |
| `timeout_handler` | 逻辑 | 超时时输出阶段性结论 |

#### 状态机流转

```
START
  │
  ▼
formulate_hypothesis ──→ tool_executor
  │                          │
  │                          ▼
  │                    evaluate_evidence
  │                     /      │       \
  │            confirmed   partial    exhausted
  │               │           │          │
  │               ▼           ▼          ▼
  │          root_cause   refine     timeout_handler
  │               │           │          │
  │               ▼           │          ▼
  │              END          │        END
  │                           │
  └─────── (refine 后) ───────┘
```

#### 超时控制（双维度）

1. 最大步数：15 步（工具调用次数）
2. 最大时长：300 秒（5 分钟）

### 3.2 分层上下文管理

```
┌──────────────────────────────────────────┐
│ Layer 1: 系统 Prompt（永远保留）           │
├──────────────────────────────────────────┤
│ Layer 2: protected_data（不可压缩）        │
│  - 工具返回时自标记 critical 字段进入此层   │
│  - 滑动窗口保留最近 20 条                  │
├──────────────────────────────────────────┤
│ Layer 3: compressed_memory（可压缩）       │
│  - 已排除的假设路径摘要                    │
│  - 超出阈值时自动压缩为摘要                │
├──────────────────────────────────────────┤
│ Layer 4: 当前上下文窗口                    │
│  - 最近 5 轮对话完整保留                   │
│  - 保证当前推理连贯性                      │
└──────────────────────────────────────────┘
```

#### 压缩策略

- 当消息数 > 50 条时触发压缩
- Layer 3 中的 Agent 推理过程被 LLM 摘要为 "已排除 X，因为 Y" 格式
- protected_data 永不压缩
- 压缩后消息数约 20-30 条

### 3.3 异常处理

#### LLM 调用异常

| 错误类型 | 策略 | 示例 |
|----------|------|------|
| 瞬时错误 | 指数退避重试 3 次（2s/4s/8s） | 超时、429 限流、5xx |
| 持久错误 | 不重试，记录并通知 | 400 参数错、401 鉴权 |
| 响应格式错误 | 重试 1 次，仍失败走兜底 | JSON 解析失败 |

#### 工具执行异常

| 情况 | 策略 |
|------|------|
| 个别工具失败 | Agent 尝试替代工具或不同参数 |
| 全部工具不可用 | 降级为 NOTIFY_ONLY 模式 |
| 工具返回空结果 | 标注"无结果"（非失败），提示换条件 |

#### 降级模式

```python
class AgentMode(Enum):
    FULL = "full"               # 完整诊断
    DIAGNOSE_ONLY = "diagnose"  # 仅诊断，不执行写操作
    NOTIFY_ONLY = "notify"      # 仅通知原始告警
```

- 连续 3 次 LLM 调用失败 → NOTIFY_ONLY
- 诊断超时 → 输出阶段性结论 + `truncated: true`
- 全部工具不可用 → NOTIFY_ONLY

### 3.4 Checkpointer 持久化

- **PostgreSQL**: LangGraph `PostgresSaver` 存储 checkpoint（每次节点执行后自动保存）
- **Redis**: 活跃会话索引缓存（`conversation_id → checkpoint_id`，TTL 2 小时）
- **断点恢复**: 用户重新运行 CLI 时检测活跃会话，提示是否继续

---

## 4. 数据流

### 诊断请求流程

```
1. CLI 脚本接收告警 JSON
2. diagnosis.run_diagnosis(alert) 被调用
3. 创建 AgentState 初始状态
4. 调用 graph.invoke(state, config)
5. LangGraph 执行状态机：
   a. formulate_hypothesis: LLM 生成假设
   b. tool_executor: 执行工具（search_logs / get_topology / ...）
   c. evaluate_evidence: LLM 评估证据
   d. 循环直到 hypothesis_confirmed 或 timeout
6. 返回诊断结果 DiagnosisResult
7. CLI 打印格式化报告
```

### Checkpoint 存储流程

```
每次节点执行后:
  LangGraph → PostgresSaver.save(state, checkpoint_id)

会话恢复时:
  CLI → Redis 查询活跃会话 → PostgresSaver.load(checkpoint_id)
  → graph.invoke(state, config) 从断点继续
```

---

## 5. 目录结构

```
src/agent/
├── __init__.py
├── state.py          # AgentState TypedDict (扩展)
├── graph.py          # 假设驱动状态机 (重写)
├── context.py        # 分层上下文管理 (新增)
├── resilience.py     # 重试与降级 (新增)
├── tools.py          # Agent 工具绑定 (新增)
└── diagnosis.py      # 诊断入口函数 (新增)

scripts/
└── diagnose.py       # CLI 诊断脚本 (新增)

tests/
├── test_agent_context.py     # 上下文管理测试 (新增)
├── test_agent_resilience.py  # 异常处理测试 (新增)
├── test_agent_graph.py       # 状态机测试 (重写)
└── test_diagnosis_pipeline.py # 集成测试 (新增)
```

---

## 6. 测试策略

### 单元测试

- **context.py**: 测试分层上下文构建、压缩触发、protected_data 保护
- **resilience.py**: 测试重试逻辑（mock LLM 失败）、降级模式切换
- **graph.py**: 测试状态机编译、各节点路由（mock LLM + mock 工具）

### 集成测试

- **diagnosis_pipeline.py**: 完整诊断流程（mock LLM + 真实 ES/Neo4j），验证 Agent 能调用 ≥ 2 个工具后输出诊断

### CLI 验收测试

```bash
# 手动运行
python scripts/diagnose.py --alert '{"alert_id":"test","severity":"critical","labels":{"service":"payment"},"annotations":{"summary":"CPU > 90%"}}'
# 期望输出：结构化的诊断报告
```

---

## 7. Phase 2 验收标准

| 验收项 | 标准 |
|--------|------|
| 状态机编译 | graph.compile() 成功 |
| 完整诊断循环 | Agent 完成 3+ 步工具调用后输出诊断 |
| 上下文压缩 | 消息超过 50 条时自动压缩 |
| LLM 重试 | 瞬时错误自动重试 3 次 |
| 超时输出 | 超时后输出阶段性结论 |
| Checkpointer 持久化 | 诊断中断后可恢复 |
| 工具调用 | Agent 正确调用 ≥ 2 个不同工具 |

---

本文档为 Phase 2 设计，基于 Phase 1 基础设施增量构建 Agent 核心诊断能力。
