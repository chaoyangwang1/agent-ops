import uuid
import time
import logging
from enum import Enum

logger = logging.getLogger(__name__)
APPROVAL_TIMEOUT = 1800


class ApprovalStatus(Enum):
    PENDING = "pending"
    APPROVED = "approved"
    DENIED = "denied"
    EXPIRED = "expired"
    UNKNOWN = "unknown"


class ApprovalService:
    def __init__(self, db=None, notifier=None):
        self.db = db
        self.notifier = notifier
        self._requests: dict[str, dict] = {}

    def create_request(
        self, action: str, target: dict, risk_level: str = "medium"
    ) -> dict:
        approval_id = str(uuid.uuid4())[:8]
        req = {
            "approval_id": approval_id,
            "action": action,
            "target": target,
            "risk_level": risk_level,
            "status": "pending",
            "created_at": time.time(),
            "expires_at": time.time() + APPROVAL_TIMEOUT,
            "reason": "",
        }
        self._requests[approval_id] = req
        if self.notifier:
            try:
                self.notifier.send_approval_card(req)
            except Exception as e:
                logger.error(f"审批通知发送失败: {e}")
        logger.info(f"审批请求已创建: {approval_id} ({action})")
        return req

    def approve(self, approval_id: str) -> ApprovalStatus:
        req = self._requests.get(approval_id)
        if req is None:
            return ApprovalStatus.UNKNOWN
        if req["status"] != "pending":
            return ApprovalStatus(req["status"])
        req["status"] = "approved"
        return ApprovalStatus.APPROVED

    def deny(self, approval_id: str, reason: str = "") -> ApprovalStatus:
        req = self._requests.get(approval_id)
        if req is None:
            return ApprovalStatus.UNKNOWN
        if req["status"] != "pending":
            return ApprovalStatus(req["status"])
        req["status"] = "denied"
        req["reason"] = reason
        return ApprovalStatus.DENIED

    def check_status(self, approval_id: str) -> ApprovalStatus:
        req = self._requests.get(approval_id)
        if req is None:
            return ApprovalStatus.UNKNOWN
        if req["status"] != "pending":
            return ApprovalStatus(req["status"])
        if time.time() > req["expires_at"]:
            req["status"] = "expired"
            return ApprovalStatus.EXPIRED
        return ApprovalStatus.PENDING
