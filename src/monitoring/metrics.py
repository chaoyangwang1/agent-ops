from prometheus_client import Counter, Histogram, Gauge


class CounterWrapper:
    def __init__(self, counter: Counter):
        self._counter = counter

    def inc(self, amount: int = 1):
        self._counter.inc(amount)

    @property
    def value(self):
        samples = list(self._counter.collect())[0].samples
        return int(samples[0].value) if samples else 0


class HistogramWrapper:
    def __init__(self, histogram: Histogram):
        self._histogram = histogram

    def observe(self, amount: float):
        self._histogram.observe(amount)

    @property
    def count(self):
        samples = list(self._histogram.collect())[0].samples
        count_samples = [s for s in samples if s.name.endswith("_count")]
        return int(count_samples[0].value) if count_samples else 0


class MetricsRegistry:
    """平台自身监控指标注册"""

    def __init__(self):
        self._counters: dict[str, Counter] = {}
        self._histograms: dict[str, Histogram] = {}
        self._gauges: dict[str, Gauge] = {}

    def counter(self, name: str, description: str = "", labels: list[str] = None) -> CounterWrapper:
        if name not in self._counters:
            self._counters[name] = Counter(name, description, labels or [])
        return CounterWrapper(self._counters[name])

    def histogram(self, name: str, description: str = "", labels: list[str] = None) -> HistogramWrapper:
        if name not in self._histograms:
            self._histograms[name] = Histogram(name, description, labels or [])
        return HistogramWrapper(self._histograms[name])

    def gauge(self, name: str, description: str = "", labels: list[str] = None):
        if name not in self._gauges:
            self._gauges[name] = Gauge(name, description, labels or [])
        return self._gauges[name]
