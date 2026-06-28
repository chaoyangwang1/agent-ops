from src.pipeline.adapters import AlertNormalizer, UnifiedAlert


def test_normalize_prometheus_alert():
    raw = {
        "receiver": "ops",
        "status": "firing",
        "alerts": [{
            "labels": {"alertname": "HighCPU", "service": "payment"},
            "annotations": {"summary": "CPU > 90%"},
            "startsAt": "2026-06-28T10:00:00Z",
        }]
    }
    results = AlertNormalizer.normalize("prometheus", raw)
    assert len(results) == 1
    assert results[0].source == "prometheus"
    assert results[0].severity == "warning"
    assert results[0].labels["service"] == "payment"


def test_normalize_unknown_source():
    raw = {"message": "something happened"}
    results = AlertNormalizer.normalize("unknown_source", raw)
    assert len(results) == 1
    assert results[0].source == "unknown_source"
