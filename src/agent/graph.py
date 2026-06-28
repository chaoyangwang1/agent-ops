import time
from langgraph.graph import StateGraph, END
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
