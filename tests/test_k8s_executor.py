import pytest
from src.execution.k8s_executor import MockK8sExecutor, ExecutionResult
from src.execution.rollback import RollbackManager


@pytest.fixture
def executor():
    return MockK8sExecutor()


def test_restart_pod_success(executor):
    result = executor.restart_pod("prod", "payment-abc", "idem-001")
    assert result.status == "success"
    assert "payment-abc" in result.message


def test_scale_deployment_success(executor):
    result = executor.scale_deployment("prod", "payment", 5, "idem-002")
    assert result.status == "success"
    assert result.details["replicas"] == 5


def test_rollback_deployment_success(executor):
    result = executor.rollback_deployment("prod", "payment", 3, "idem-003")
    assert result.status == "success"


def test_idempotency_key_prevents_duplicate(executor):
    key = "idem-004"
    r1 = executor.restart_pod("prod", "svc-a", key)
    r2 = executor.restart_pod("prod", "svc-a", key)
    assert r1.status == "success"
    assert r2.status == "skipped"


def test_rollback_manager_snapshot(executor):
    mgr = RollbackManager(executor)
    snapshot = mgr.snapshot_before(action="restart_pod", target={"namespace": "prod", "name": "payment-abc"})
    assert snapshot is not None


def test_rollback_manager_rollback(executor):
    mgr = RollbackManager(executor)
    snapshot = mgr.snapshot_before("scale_deployment", {"namespace": "prod", "name": "payment"})
    result = mgr.rollback(snapshot, {"namespace": "prod", "name": "payment", "action": "scale_deployment"})
    assert result.status == "success"
