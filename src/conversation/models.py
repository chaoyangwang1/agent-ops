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
