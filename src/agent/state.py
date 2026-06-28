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
