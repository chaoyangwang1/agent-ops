import time
import logging
from langgraph.graph import StateGraph, END
from langchain_core.messages import AIMessage, SystemMessage, HumanMessage, FunctionMessage
from langchain_openai import ChatOpenAI
from src.agent.state import AgentState, AgentMode
from src.agent.context import ContextManager, SYSTEM_PROMPT
from src.agent.resilience import ResilienceHandler, retry_with_backoff
from src.tools.registry import ToolRegistry
from src.config import settings

logger = logging.getLogger(__name__)

MAX_STEPS = settings.agent_max_steps
MAX_DURATION = settings.agent_max_duration_seconds


def _get_llm():
    """创建 LLM 客户端"""
    return ChatOpenAI(
        base_url=settings.llm_api_base,
        api_key=settings.llm_api_key,
        model=settings.llm_model,
        temperature=0.3,
    )


@retry_with_backoff(max_retries=3, base_delay=2.0)
def call_llm_with_retry(messages: list, tools: list = None):
    """调用 LLM（带重试），返回 response"""
    llm = _get_llm()
    if tools:
        llm_with_tools = llm.bind_tools(tools)
        return llm_with_tools.invoke(messages)
    return llm.invoke(messages)


# ---------- 状态机节点 ----------

def formulate_hypothesis(state: AgentState, tool_registry: ToolRegistry,
                         context_mgr: ContextManager,
                         resilience: ResilienceHandler) -> dict:
    """节点 1: 分析告警，形成初始假设"""
    tools_schema = tool_registry.get_openai_schema()
    messages, _ = context_mgr.build_messages(
        alert=state["alert"],
        reasoning_history=state.get("messages", []),
    )
    messages.append(HumanMessage(content="请分析以上告警，形成诊断假设，并决定需要调用哪些工具来验证。"))

    try:
        response = call_llm_with_retry(messages, tools=tools_schema)
        resilience.record_llm_success()
    except Exception as e:
        logger.error(f"LLM 调用失败: {e}")
        resilience.record_llm_failure()
        return {
            "messages": [AIMessage(content=f"LLM 调用失败: {e}")],
            "truncated": True,
        }

    state["messages"].append(HumanMessage(content="请分析告警并形成假设"))
    state["messages"].append(response)
    context_mgr.record_reasoning_step(HumanMessage(content="请分析告警并形成假设"))
    context_mgr.record_reasoning_step(response)

    return {
        "messages": state["messages"],
        "hypothesis": response.content[:200] if hasattr(response, "content") else "",
    }


def tool_executor(state: AgentState, tool_registry: ToolRegistry,
                  context_mgr: ContextManager) -> dict:
    """节点 2: 执行工具调用"""
    last_msg = state["messages"][-1]
    results = []

    if not hasattr(last_msg, "tool_calls") or not last_msg.tool_calls:
        return {"messages": state["messages"], "step_count": state["step_count"] + 1}

    any_success = False
    for tc in last_msg.tool_calls:
        try:
            result = tool_registry.execute(tc["name"], tc.get("args", {}))
            results.append(FunctionMessage(content=str(result), name=tc["name"]))
            is_critical = any(kw in str(result).upper() for kw in ["ERROR", "FATAL", "CRITICAL", "EXCEPTION"])
            context_mgr.add_protected({
                "tool": tc["name"],
                "data": str(result)[:500],
                "critical": is_critical,
                "timestamp": time.time(),
            })
            any_success = True
        except Exception as e:
            results.append(FunctionMessage(content=f"工具错误: {e}", name=tc["name"]))
            logger.warning(f"工具 {tc['name']} 执行失败: {e}")

    state["messages"].extend(results)
    state["step_count"] += 1
    return {
        "messages": state["messages"],
        "step_count": state["step_count"],
        "tools_failed": not any_success,
    }


def evaluate_evidence(state: AgentState, context_mgr: ContextManager,
                      resilience: ResilienceHandler) -> dict:
    """节点 3: 评估证据是否支持当前假设"""
    messages, protected = context_mgr.build_messages(
        alert=state["alert"],
        reasoning_history=state["messages"][-10:],
    )

    evidence_text = "\n".join(
        f"[{p['tool']}] {str(p['data'])[:200]}"
        for p in protected[-5:]
    )
    messages.append(HumanMessage(content=(
        f"当前假设：{state.get('hypothesis', '未确定')}\n\n"
        f"收集到的证据：\n{evidence_text}\n\n"
        f"请评估：\n"
        f"1. 证据是否充分支持当前假设？\n"
        f"2. 置信度是多少（0-100%）？\n"
        f"3. 是否需要更多证据？\n"
        f"请按格式回复：\n"
        f"评估：[支持/部分支持/不支持]\n"
        f"置信度：[0-100]\n"
        f"下一步：[确认根因/继续验证/修改假设]"
    )))

    try:
        response = call_llm_with_retry(messages, tools=None)
        resilience.record_llm_success()
    except Exception as e:
        logger.error(f"证据评估 LLM 调用失败: {e}")
        resilience.record_llm_failure()
        return {"messages": state["messages"]}

    state["messages"].append(HumanMessage(content="请评估证据"))
    state["messages"].append(response)
    context_mgr.record_reasoning_step(HumanMessage(content="请评估证据"))
    context_mgr.record_reasoning_step(response)

    return {"messages": state["messages"]}


def refine_hypothesis(state: AgentState, context_mgr: ContextManager,
                      resilience: ResilienceHandler) -> dict:
    """节点 4: 根据新证据修正假设"""
    messages, _ = context_mgr.build_messages(
        alert=state["alert"],
        reasoning_history=state["messages"][-10:],
    )
    messages.append(HumanMessage(content=(
        f"当前假设未能完全确认：{state.get('hypothesis', '')}\n"
        f"请根据已有证据修正假设，或提出新的诊断方向。"
    )))

    try:
        response = call_llm_with_retry(messages, tools=None)
        resilience.record_llm_success()
    except Exception as e:
        logger.error(f"修正假设 LLM 调用失败: {e}")
        resilience.record_llm_failure()
        return {"messages": state["messages"]}

    hypothesis_history = state.get("hypothesis_history", []) + [{
        "hypothesis": state.get("hypothesis", ""),
        "result": "refined",
        "reason": response.content[:100] if hasattr(response, "content") else "",
    }]
    context_mgr.compress_excluded_hypothesis(
        f"已修正假设「{state.get('hypothesis', '')[:80]}」→ 新方向：{response.content[:80] if hasattr(response, 'content') else ''}"
    )

    return {
        "messages": state["messages"] + [response],
        "hypothesis": response.content[:200] if hasattr(response, "content") else "",
        "hypothesis_history": hypothesis_history,
    }


def root_cause(state: AgentState, context_mgr: ContextManager,
               resilience: ResilienceHandler) -> dict:
    """节点 5: 确认根因，输出诊断报告"""
    messages, protected = context_mgr.build_messages(
        alert=state["alert"],
        reasoning_history=state["messages"][-10:],
    )
    messages.append(HumanMessage(content=(
        f"证据已充分，假设已确认。\n"
        f"请输出最终诊断报告：\n"
        f"- 根因：[具体根因描述]\n"
        f"- 置信度：[0-100%]\n"
        f"- 证据链：[关键证据]\n"
        f"- 修复建议：[具体操作]"
    )))

    try:
        response = call_llm_with_retry(messages, tools=None)
        resilience.record_llm_success()
    except Exception as e:
        response = AIMessage(content=f"诊断报告中 LLM 调用失败: {e}")

    return {
        "diagnosis": response.content if hasattr(response, "content") else str(response),
        "messages": state["messages"] + [response],
    }


def timeout_handler(state: AgentState) -> dict:
    """超时处理：输出阶段性结论"""
    hypothesis_history = state.get("hypothesis_history", [])
    excluded = [h for h in hypothesis_history if h.get("result") in ("excluded", "refined")]
    diagnosis = (
        f"[诊断超时 - 阶段性结论]\n"
        f"已排除 {len(excluded)} 个假设：\n" +
        "\n".join(f"- {h.get('hypothesis', '')[:100]}" for h in excluded) +
        f"\n当前假设：{state.get('hypothesis', '未确定')[:200]}\n"
        f"已执行 {state.get('step_count', 0)} 步"
    )
    return {
        "diagnosis": diagnosis,
        "truncated": True,
        "messages": state["messages"],
    }


# ---------- 路由函数 ----------

def route_after_formulate(state: AgentState) -> str:
    """formulate_hypothesis 之后的路由"""
    if state.get("truncated"):
        return "timeout_handler"
    last_msg = state["messages"][-1]
    if hasattr(last_msg, "tool_calls") and last_msg.tool_calls:
        return "tool_executor"
    return "evaluate_evidence"


def route_after_evaluate(state: AgentState, resilience: ResilienceHandler) -> str:
    """evaluate_evidence 之后的路由"""
    if resilience.mode == AgentMode.NOTIFY_ONLY:
        return "timeout_handler"

    last_msg = state["messages"][-1]
    content = last_msg.content.lower() if hasattr(last_msg, "content") else ""

    if "确认根因" in content or "confirmed" in content:
        return "root_cause"
    elif "修改假设" in content or "refine" in content or "不支持" in content:
        return "refine_hypothesis"
    elif hasattr(last_msg, "tool_calls") and last_msg.tool_calls:
        return "tool_executor"
    else:
        return "formulate_hypothesis"


def route_after_refine(state: AgentState) -> str:
    """refine_hypothesis 之后的路由：返回 formulate 开始新一轮"""
    return "formulate_hypothesis"


def check_timeout(state: AgentState, resilience: ResilienceHandler) -> str:
    """全局超时检查"""
    if resilience.time_exceeded(
        state.get("step_count", 0), MAX_STEPS,
        state.get("start_time", time.time()), MAX_DURATION,
    ):
        return "timeout_handler"
    return "continue"


# ---------- 构建状态机 ----------

def build_graph(tool_registry: ToolRegistry,
                context_mgr: ContextManager = None,
                resilience: ResilienceHandler = None) -> StateGraph:
    """构建假设驱动诊断状态机"""
    if context_mgr is None:
        context_mgr = ContextManager()
    if resilience is None:
        resilience = ResilienceHandler()

    workflow = StateGraph(AgentState)

    # 添加节点
    workflow.add_node("formulate_hypothesis",
                      lambda s: formulate_hypothesis(s, tool_registry, context_mgr, resilience))
    workflow.add_node("tool_executor",
                      lambda s: tool_executor(s, tool_registry, context_mgr))
    workflow.add_node("evaluate_evidence",
                      lambda s: evaluate_evidence(s, context_mgr, resilience))
    workflow.add_node("refine_hypothesis",
                      lambda s: refine_hypothesis(s, context_mgr, resilience))
    workflow.add_node("root_cause",
                      lambda s: root_cause(s, context_mgr, resilience))
    workflow.add_node("timeout_handler", timeout_handler)

    # 设置入口
    workflow.set_entry_point("formulate_hypothesis")

    # 添加边
    workflow.add_conditional_edges("formulate_hypothesis", route_after_formulate, {
        "tool_executor": "tool_executor",
        "evaluate_evidence": "evaluate_evidence",
        "timeout_handler": "timeout_handler",
    })

    workflow.add_edge("tool_executor", "evaluate_evidence")

    workflow.add_conditional_edges("evaluate_evidence",
                                   lambda s: route_after_evaluate(s, resilience), {
        "root_cause": "root_cause",
        "refine_hypothesis": "refine_hypothesis",
        "tool_executor": "tool_executor",
        "formulate_hypothesis": "formulate_hypothesis",
        "timeout_handler": "timeout_handler",
    })

    workflow.add_edge("refine_hypothesis", "formulate_hypothesis")

    # 终态
    workflow.add_edge("root_cause", END)
    workflow.add_edge("timeout_handler", END)

    return workflow.compile()
