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
