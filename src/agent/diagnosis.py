import time
import uuid
import asyncio
import logging
from dataclasses import dataclass
from src.agent.state import AgentState
from src.agent.graph import build_graph
from src.agent.context import ContextManager
from src.agent.resilience import ResilienceHandler
from src.agent.tools import create_agent_tools

logger = logging.getLogger(__name__)


@dataclass
class DiagnosisResult:
    conversation_id: str
    diagnosis: str
    hypothesis: str
    hypothesis_history: list
    step_count: int
    truncated: bool
    duration_seconds: float
    mode: str


def run_diagnosis(alert: dict, es_client=None, neo4j_client=None) -> DiagnosisResult:
    """执行告警诊断（同步包装）"""
    return asyncio.run(_run_diagnosis_async(alert, es_client, neo4j_client))


async def _run_diagnosis_async(alert: dict, es_client=None, neo4j_client=None) -> DiagnosisResult:
    """异步执行告警诊断"""
    conversation_id = str(uuid.uuid4())[:8]
    context_mgr = ContextManager()
    resilience = ResilienceHandler()
    tools = create_agent_tools(es_client=es_client, neo4j_client=neo4j_client)

    graph = build_graph(tools, context_mgr=context_mgr, resilience=resilience)

    initial_state = AgentState(
        alert=alert,
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
        start_time=time.time(),
        conversation_id=conversation_id,
        llm_failures=0,
        tools_failed=False,
        truncated=False,
    )

    start = time.time()
    logger.info(f"开始诊断 {conversation_id}: {alert.get('labels', {}).get('service', 'unknown')}")

    try:
        result = graph.invoke(initial_state, config={"recursion_limit": 20})
    except Exception as e:
        logger.error(f"诊断异常: {e}")
        return DiagnosisResult(
            conversation_id=conversation_id,
            diagnosis=f"诊断异常中断: {e}",
            hypothesis="",
            hypothesis_history=[],
            step_count=0,
            truncated=True,
            duration_seconds=time.time() - start,
            mode=resilience.mode.value,
        )

    duration = time.time() - start

    return DiagnosisResult(
        conversation_id=result.get("conversation_id", conversation_id),
        diagnosis=result.get("diagnosis", ""),
        hypothesis=result.get("hypothesis", ""),
        hypothesis_history=result.get("hypothesis_history", []),
        step_count=result.get("step_count", 0),
        truncated=result.get("truncated", False),
        duration_seconds=duration,
        mode=result.get("mode", resilience.mode.value),
    )
