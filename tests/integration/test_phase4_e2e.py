import pytest
from unittest.mock import Mock
from src.execution.policy import PolicyEngine, PolicyResult
from src.execution.k8s_executor import MockK8sExecutor
from src.execution.approval import ApprovalService, ApprovalStatus
from src.execution.audit import AuditService
from src.execution.rollback import RollbackManager


@pytest.mark.integration
def test_full_execution_pipeline():
    policy = PolicyEngine()
    executor = MockK8sExecutor()
    approval = ApprovalService()
    audit = AuditService(db=Mock(), es=Mock())
    rollback = RollbackManager(executor)

    action = {
        "action": "restart_pod",
        "resource": {"kind": "Deployment", "name": "payment", "namespace": "prod"},
        "risk_level": "low",
        "affected_replicas": 1,
        "total_replicas": 3,
        "error_budget_remaining": 0.5,
    }
    result = policy.evaluate(action)
    assert result == PolicyResult.ALLOWED

    snapshot = rollback.snapshot_before(
        "restart_pod", {"namespace": "prod", "name": "payment"}
    )
    exec_result = executor.restart_pod("prod", "payment-abc", "e2e-key-1")
    assert exec_result.status == "success"

    audit.record(
        "conv-e2e",
        "restart_pod",
        {"namespace": "prod", "name": "payment"},
        "allowed",
        "success",
        "agent-e2e",
        snapshot=snapshot,
    )
    assert audit._db.called


@pytest.mark.integration
def test_high_risk_requires_approval():
    policy = PolicyEngine(
        change_window_start=0,
        change_window_end=24,
        change_window_days=[0, 1, 2, 3, 4, 5, 6],
    )
    approval = ApprovalService()

    action = {
        "action": "rollback_deployment",
        "resource": {"kind": "Deployment", "name": "payment", "namespace": "prod"},
        "risk_level": "high",
        "affected_replicas": 1,
        "total_replicas": 3,
    }
    result = policy.evaluate(action)
    assert result == PolicyResult.NEEDS_APPROVAL

    req = approval.create_request(
        "rollback_deployment",
        {"namespace": "prod", "name": "payment"},
        "high",
    )
    status = approval.check_status(req["approval_id"])
    assert status == ApprovalStatus.PENDING

    approval.approve(req["approval_id"])
    status = approval.check_status(req["approval_id"])
    assert status == ApprovalStatus.APPROVED


@pytest.mark.integration
def test_policy_l1_blocks_dangerous_ops():
    policy = PolicyEngine()
    action = {
        "action": "restart_pod",
        "resource": {"kind": "StatefulSet", "name": "mysql", "namespace": "prod"},
        "risk_level": "low",
    }
    assert policy.evaluate(action) == PolicyResult.DENIED
    action["resource"] = {
        "kind": "Deployment",
        "name": "redis-master",
        "namespace": "prod",
    }
    assert policy.evaluate(action) == PolicyResult.DENIED
