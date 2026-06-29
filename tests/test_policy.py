from src.execution.policy import PolicyEngine, PolicyResult


def make_action(name, kind="Deployment", namespace="prod", risk="low", affected=1, total=3, error_budget=0.5):
    return {
        "action": name,
        "resource": {"kind": kind, "name": "test-svc", "namespace": namespace},
        "risk_level": risk,
        "affected_replicas": affected,
        "total_replicas": total,
        "error_budget_remaining": error_budget,
    }


def test_l1_block_statefulset():
    """L1: 禁止 StatefulSet 操作"""
    engine = PolicyEngine()
    action = make_action("restart_pod", kind="StatefulSet")
    assert engine.evaluate(action) == PolicyResult.DENIED


def test_l1_block_core_middleware():
    """L1: 禁止核心中间件"""
    engine = PolicyEngine()
    for svc in ["kafka", "redis-master", "mysql-primary"]:
        action = make_action("restart_pod", namespace="infra")
        action["resource"]["name"] = svc
        assert engine.evaluate(action) == PolicyResult.DENIED


def test_l1_block_namespace_not_allowed():
    """L1: 命名空间不在白名单"""
    engine = PolicyEngine(allowed_namespaces=["prod", "staging"])
    action = make_action("restart_pod", namespace="kube-system")
    assert engine.evaluate(action) == PolicyResult.DENIED


def test_l2_needs_approval_high_impact():
    """L2: > 50% 副本影响需要审批"""
    engine = PolicyEngine()
    action = make_action("restart_pod", affected=6, total=10)
    assert engine.evaluate(action) == PolicyResult.NEEDS_APPROVAL


def test_l2_deny_error_budget_exhausted():
    """L2: Error Budget 耗尽禁止操作"""
    engine = PolicyEngine()
    action = make_action("restart_pod", error_budget=0.03)
    assert engine.evaluate(action) == PolicyResult.DENIED


def test_l2_allow_low_risk_in_budget():
    """L2: 低影响 + Error Budget 充足 = 放行"""
    engine = PolicyEngine()
    action = make_action("restart_pod", affected=1, total=10, error_budget=0.5)
    assert engine.evaluate(action) == PolicyResult.ALLOWED


def test_l3_block_high_risk_outside_window():
    """L3: 高风险操作非窗口期拒绝"""
    engine = PolicyEngine(change_window_start=10, change_window_end=18)
    action = make_action("rollback_deployment", risk="high")
    import datetime
    now = datetime.datetime.now()
    if now.weekday() >= 5 or now.hour < 10 or now.hour >= 18:
        assert engine.evaluate(action) == PolicyResult.DENIED


def test_high_risk_always_needs_approval():
    """高风险操作一律需要审批"""
    engine = PolicyEngine()
    action = make_action("rollback_deployment", risk="high")
    result = engine.evaluate(action)
    assert result == PolicyResult.NEEDS_APPROVAL
