from src.execution.action_tools import create_action_tools
from src.execution.policy import PolicyEngine


def test_action_tools_registered():
    registry = create_action_tools()
    tools = registry.list_tools()
    assert "restart_pod" in tools
    assert "scale_deployment" in tools
    assert "rollback_deployment" in tools


def test_restart_pod_succeeds():
    registry = create_action_tools()
    result = registry.execute("restart_pod", {
        "namespace": "prod", "pod_name": "test-pod",
        "idempotency_key": "key-1", "_conversation_id": "test",
    })
    assert result["status"] == "success"


def test_high_risk_needs_approval():
    policy = PolicyEngine(change_window_start=0, change_window_end=24,
                          change_window_days=[0, 1, 2, 3, 4, 5, 6])
    registry = create_action_tools(policy=policy)
    result = registry.execute("rollback_deployment", {
        "namespace": "prod", "name": "payment", "revision": 1,
        "idempotency_key": "key-2", "_conversation_id": "test",
    })
    assert result["status"] == "pending_approval"
