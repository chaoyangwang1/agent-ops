from src.monitoring.metrics import MetricsRegistry


def test_counter_increment():
    reg = MetricsRegistry()
    reg.counter("agent_analyses_total").inc()
    reg.counter("agent_analyses_total").inc()
    assert reg.counter("agent_analyses_total").value == 2


def test_histogram_observe():
    reg = MetricsRegistry()
    reg.histogram("tool_call_duration_seconds").observe(0.5)
    reg.histogram("tool_call_duration_seconds").observe(1.2)
    assert reg.histogram("tool_call_duration_seconds").count == 2
