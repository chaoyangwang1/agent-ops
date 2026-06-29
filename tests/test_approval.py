import pytest
import time
from src.execution.approval import ApprovalService, ApprovalStatus


@pytest.fixture
def approval():
    return ApprovalService(db=None, notifier=None)


def test_create_approval_request(approval):
    req = approval.create_request(
        action="rollback_deployment",
        target={"namespace": "prod", "name": "payment"},
        risk_level="high",
    )
    assert req["status"] == "pending"
    assert "approval_id" in req


def test_approve_request(approval):
    req = approval.create_request(
        "restart_pod", {"namespace": "prod", "name": "svc"}
    )
    result = approval.approve(req["approval_id"])
    assert result == ApprovalStatus.APPROVED


def test_deny_request(approval):
    req = approval.create_request(
        "scale_deployment", {"namespace": "prod", "name": "svc"}, "medium"
    )
    result = approval.deny(req["approval_id"], "风险过高")
    assert result == ApprovalStatus.DENIED


def test_expired_approval(approval):
    req = approval.create_request(
        "rollback_deployment", {"namespace": "prod", "name": "svc"}, "high"
    )
    approval._requests[req["approval_id"]]["expires_at"] = time.time() - 1
    result = approval.check_status(req["approval_id"])
    assert result == ApprovalStatus.EXPIRED


def test_unknown_approval_id(approval):
    result = approval.check_status("nonexistent")
    assert result == ApprovalStatus.UNKNOWN
