from src.tools.log_search import LogSampler


def test_drain_cluster_and_sample():
    logs = [
        "2026-06-28T10:00:00 INFO Request completed in 5ms path=/api/users",     # x500
        "2026-06-28T10:00:01 INFO Request completed in 8ms path=/api/orders",    # x300
        "2026-06-28T10:00:02 INFO health check passed",                          # x900
        "2026-06-28T10:00:03 ERROR NullPointerException at line 42",             # x3
        "2026-06-28T10:00:04 WARN connection pool exhausted, retrying",           # x2
    ]
    # 扩展为大量日志
    expanded = []
    expanded.extend([logs[0]] * 500)
    expanded.extend([logs[1]] * 300)
    expanded.extend([logs[2]] * 900)
    expanded.extend([logs[3]] * 3)
    expanded.extend([logs[4]] * 2)

    sampler = LogSampler()
    result = sampler.sample(expanded, max_return=20)

    # 低频异常日志必须全部保留
    error_logs = [r for r in result if "ERROR" in r["content"]]
    warn_logs = [r for r in result if "WARN" in r["content"]]
    assert len(error_logs) == 3   # 低频全保留
    assert len(warn_logs) == 2    # 低频全保留
    assert len(result) <= 20
    # 高频模式只保留少量样本
    health_logs = [r for r in result if "health check" in r["content"]]
    assert len(health_logs) <= 3
