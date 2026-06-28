from src.pipeline.aggregator import AlertAggregator, AggregatedAlert


def make_alert(alert_id, labels, severity="critical", fingerprint=None):
    from src.pipeline.adapters import UnifiedAlert
    return UnifiedAlert(
        alert_id=alert_id, source="prometheus", fingerprint=fingerprint or alert_id,
        severity=severity, status="firing", labels=labels,
        annotations={"summary": "test"}, timestamp="2026-06-28T10:00:00Z",
    )


def test_mechanical_aggregation():
    agg = AlertAggregator(window_seconds=300)
    # 相同标签不同告警 → 应聚合
    agg.process(make_alert("a1", {"service": "payment", "alertname": "HighCPU"}))
    agg.process(make_alert("a2", {"service": "payment", "alertname": "HighCPU"}))
    agg.process(make_alert("a3", {"service": "payment", "alertname": "HighCPU"}))

    # 不同标签 → 不应聚合
    agg.process(make_alert("b1", {"service": "order", "alertname": "HighMem"}))

    results = agg.flush()
    # payment HighCPU 的 3 条应聚合为 1 条
    assert len(results) == 2
    payment_agg = [r for r in results if r.labels.get("service") == "payment"][0]
    assert payment_agg.merged_count == 3


def test_semantic_aggregation_same_node():
    """同 Node 的告警应语义聚合"""
    agg = AlertAggregator(window_seconds=300)
    agg.process(make_alert("a1", {"service": "svc-a", "node": "node-1"}))
    agg.process(make_alert("a2", {"service": "svc-b", "node": "node-1"}))
    agg.process(make_alert("a3", {"service": "svc-c", "node": "node-1"}))

    results = agg.flush()
    # 应生成一条 Node 级别的聚合告警
    assert len(results) == 1
    assert results[0].aggregation_type == "node"
