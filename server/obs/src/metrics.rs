//! Prometheus 指标（服务端设计文档 §11）。
//!
//! 进程内一份注册表（[`metrics()`]）：各层直接更新其中的指标，`/metrics` 抓取时编码为 OpenMetrics 文本。
//! 已有统计（例如区域缓存的命中计数）用 [`Metrics::register_sampled`] 在抓取时读取，不必改成指标类型。

use std::sync::{LazyLock, Mutex, Weak};
use std::time::Duration;

use prometheus_client::collector::Collector;
use prometheus_client::encoding::{DescriptorEncoder, EncodeLabelSet, MetricEncoder, NoLabelSet};
use prometheus_client::metrics::MetricType;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{Histogram, exponential_buckets};
use prometheus_client::registry::Registry;

/// 一次 RPC 的结果。
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct RpcLabels {
    pub service: String,
    pub method: String,
    /// gRPC 状态码名（`OK`、`NOT_FOUND` …）。
    pub code: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct MethodLabels {
    pub service: String,
    pub method: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ReasonLabels {
    pub reason: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct OutcomeLabels {
    pub outcome: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct CacheLabels {
    /// 缓存所在的层：`l3`（服务端进程内）。
    pub layer: String,
    /// `hit` 或 `miss`。
    pub result: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct SinkLabels {
    /// 事件推送目标：`webhook` 或 `nats`。
    pub sink: String,
}

/// 被拒绝请求的原因（`scion_rejected_requests_total{reason}`）。
pub mod reason {
    /// 全局在途请求超过上限（load shed）。
    pub const OVERLOAD: &str = "overload";
    /// 仓库写入速率超限。
    pub const REPO_RATE: &str = "repo_rate";
    /// 调用者（租户）请求速率超限。
    pub const TENANT_RATE: &str = "tenant_rate";
    /// 调用者（租户）在途请求超过上限。
    pub const TENANT_CONCURRENCY: &str = "tenant_concurrency";
    /// 处理超过 deadline。
    pub const DEADLINE: &str = "deadline";
    /// A caller has too many open server streams.
    pub const TENANT_STREAMS: &str = "tenant_streams";
    /// A server stream reached its maximum lifetime and ended with UNAVAILABLE.
    pub const STREAM_LIFETIME: &str = "stream_lifetime";
    /// A request body inflated past the limit of its endpoint.
    pub const DECOMPRESSED_SIZE: &str = "decompressed_size";
}

type RpcHistograms = Family<MethodLabels, Histogram, fn() -> Histogram>;

fn rpc_histogram() -> Histogram {
    // 0.5 ms .. 约 16 s
    Histogram::new(exponential_buckets(0.000_5, 2.0, 16))
}

/// 进程内的全部指标。
pub struct Metrics {
    /// `scion_rpc_requests_total{service, method, code}`
    pub rpc_requests: Family<RpcLabels, Counter>,
    /// `scion_rpc_duration_seconds{service, method}`：从收到请求到响应结束（流式响应到流结束）。
    pub rpc_duration: RpcHistograms,
    /// `scion_rpc_in_flight`
    pub rpc_in_flight: Gauge,
    /// `scion_rejected_requests_total{reason}`
    pub rejected: Family<ReasonLabels, Counter>,
    /// `scion_land_batch_size`：每批合入的请求数。
    pub land_batch_size: Histogram,
    /// `scion_land_batches_total{outcome}`：`landed` / `retried`（ref 被并发修改，CAS 失败）/ `failed`。
    pub land_batches: Family<OutcomeLabels, Counter>,
    /// `scion_land_duration_seconds`：一个合入请求从排队到得到结果。
    pub land_duration: Histogram,
    /// `scion_object_cache_requests_total{layer, result}`
    pub object_cache: Family<CacheLabels, Counter>,
    /// `scion_events_delivered_total{sink}`
    pub events_delivered: Family<SinkLabels, Counter>,
    /// `scion_event_delivery_failures_total{sink}`：失败的投递尝试（之后会重试）。
    pub event_failures: Family<SinkLabels, Counter>,
    /// `scion_event_lag{sink}`：已分配的最大 seq 与已投递的 seq 之差。
    pub event_lag: Family<SinkLabels, Gauge>,
    /// `scion_pre_land_checks_total{outcome}`：`allowed` / `denied` / `error`（检查服务不可用）。
    pub pre_land: Family<OutcomeLabels, Counter>,
    registry: Mutex<Registry>,
}

impl Metrics {
    fn new() -> Self {
        let m = Self {
            rpc_requests: Family::default(),
            rpc_duration: Family::new_with_constructor(rpc_histogram),
            rpc_in_flight: Gauge::default(),
            rejected: Family::default(),
            land_batch_size: Histogram::new(exponential_buckets(1.0, 2.0, 10)),
            land_batches: Family::default(),
            land_duration: Histogram::new(exponential_buckets(0.001, 2.0, 15)),
            object_cache: Family::default(),
            events_delivered: Family::default(),
            event_failures: Family::default(),
            event_lag: Family::default(),
            pre_land: Family::default(),
            registry: Mutex::new(Registry::default()),
        };
        if let Ok(mut r) = m.registry.lock() {
            r.register(
                "scion_rpc_requests",
                "RPCs handled, by result code",
                m.rpc_requests.clone(),
            );
            r.register(
                "scion_rpc_duration_seconds",
                "RPC latency until the response ends",
                m.rpc_duration.clone(),
            );
            r.register("scion_rpc_in_flight", "RPCs being handled", m.rpc_in_flight.clone());
            r.register(
                "scion_rejected_requests",
                "Requests rejected by overload protection, rate limits or deadlines",
                m.rejected.clone(),
            );
            r.register(
                "scion_land_batch_size",
                "Land requests per batch",
                m.land_batch_size.clone(),
            );
            r.register("scion_land_batches", "Land batches, by outcome", m.land_batches.clone());
            r.register(
                "scion_land_duration_seconds",
                "Land latency from queueing to result",
                m.land_duration.clone(),
            );
            r.register(
                "scion_object_cache_requests",
                "Object cache lookups, by layer and result",
                m.object_cache.clone(),
            );
            r.register(
                "scion_events_delivered",
                "Ref events delivered, by sink",
                m.events_delivered.clone(),
            );
            r.register(
                "scion_event_delivery_failures",
                "Failed ref event delivery attempts, by sink",
                m.event_failures.clone(),
            );
            r.register(
                "scion_event_lag",
                "Ref events not yet delivered, by sink",
                m.event_lag.clone(),
            );
            r.register(
                "scion_pre_land_checks",
                "Pre-land webhook checks, by outcome",
                m.pre_land.clone(),
            );
        }
        m
    }

    /// 记一次被拒绝的请求（原因见 [`reason`]）。
    pub fn reject(&self, reason: &str) {
        self.rejected
            .get_or_create(&ReasonLabels {
                reason: reason.to_owned(),
            })
            .inc();
    }

    /// 记一次对象缓存查询。
    pub fn object_cache(&self, layer: &str, hit: bool) {
        self.object_cache
            .get_or_create(&CacheLabels {
                layer: layer.to_owned(),
                result: if hit { "hit" } else { "miss" }.to_owned(),
            })
            .inc();
    }

    /// 记一批合入的结果。
    pub fn land_batch(&self, outcome: &str, size: usize) {
        self.land_batches
            .get_or_create(&OutcomeLabels {
                outcome: outcome.to_owned(),
            })
            .inc();
        if outcome != "retried" {
            self.land_batch_size.observe(size as f64);
        }
    }

    /// 事件推送：成功投递一条、一次失败的尝试、积压量。
    pub fn event_delivered(&self, sink: &str) {
        self.events_delivered.get_or_create(&sink_labels(sink)).inc();
    }

    pub fn event_failed(&self, sink: &str) {
        self.event_failures.get_or_create(&sink_labels(sink)).inc();
    }

    pub fn event_lag(&self, sink: &str, lag: u64) {
        self.event_lag
            .get_or_create(&sink_labels(sink))
            .set(i64::try_from(lag).unwrap_or(i64::MAX));
    }

    pub fn pre_land(&self, outcome: &str) {
        self.pre_land
            .get_or_create(&OutcomeLabels {
                outcome: outcome.to_owned(),
            })
            .inc();
    }

    pub fn land_finished(&self, elapsed: Duration) {
        self.land_duration.observe(elapsed.as_secs_f64());
    }

    /// 抓取时从 `source` 读取一组样本。`source` 被释放后不再输出。
    pub fn register_sampled(&self, name: &'static str, help: &'static str, kind: SampleKind, source: Weak<SampleFn>) {
        if let Ok(mut r) = self.registry.lock() {
            r.register_collector(Box::new(Sampled {
                name,
                help,
                kind,
                source,
            }));
        }
    }

    /// 编码为 OpenMetrics 文本（`/metrics` 的响应体）。
    pub fn encode(&self) -> String {
        let mut out = String::new();
        if let Ok(r) = self.registry.lock() {
            // 写入 String 不会失败
            let _ = prometheus_client::encoding::text::encode(&mut out, &r);
        }
        out
    }
}

fn sink_labels(sink: &str) -> SinkLabels {
    SinkLabels { sink: sink.to_owned() }
}

static METRICS: LazyLock<Metrics> = LazyLock::new(Metrics::new);

/// 进程内的指标注册表。
pub fn metrics() -> &'static Metrics {
    &METRICS
}

/// 抓取时读取的指标类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SampleKind {
    Counter,
    Gauge,
}

/// 一个样本：标签与值。
#[derive(Debug, Clone, PartialEq)]
pub struct Sample {
    pub labels: Vec<(String, String)>,
    pub value: f64,
}

impl Sample {
    pub fn new(labels: &[(&str, &str)], value: f64) -> Self {
        Self {
            labels: labels.iter().map(|(k, v)| ((*k).to_owned(), (*v).to_owned())).collect(),
            value,
        }
    }
}

/// 抓取时调用，返回当前的样本。
pub type SampleFn = dyn Fn() -> Vec<Sample> + Send + Sync;

struct Sampled {
    name: &'static str,
    help: &'static str,
    kind: SampleKind,
    source: Weak<SampleFn>,
}

impl std::fmt::Debug for Sampled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sampled").field("name", &self.name).finish()
    }
}

impl Collector for Sampled {
    fn encode(&self, mut encoder: DescriptorEncoder) -> Result<(), std::fmt::Error> {
        let Some(source) = self.source.upgrade() else {
            return Ok(());
        };
        let samples = source();
        if samples.is_empty() {
            return Ok(());
        }
        let ty = match self.kind {
            SampleKind::Counter => MetricType::Counter,
            SampleKind::Gauge => MetricType::Gauge,
        };
        let mut metric = encoder.encode_descriptor(self.name, self.help, None, ty)?;
        for s in samples {
            if s.labels.is_empty() {
                encode_value(&mut metric, self.kind, s.value)?;
            } else {
                encode_value(&mut metric.encode_family(&s.labels)?, self.kind, s.value)?;
            }
        }
        Ok(())
    }
}

fn encode_value(m: &mut MetricEncoder<'_>, kind: SampleKind, v: f64) -> Result<(), std::fmt::Error> {
    match kind {
        SampleKind::Counter => m.encode_counter::<NoLabelSet, _, f64>(&v, None),
        SampleKind::Gauge => m.encode_gauge(&v),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn encodes_registered_metrics() {
        let m = metrics();
        m.reject(reason::OVERLOAD);
        m.object_cache("l3", true);
        m.object_cache("l3", false);
        m.land_batch("landed", 3);
        m.land_batch("retried", 3);
        m.land_finished(Duration::from_millis(5));
        m.event_delivered("webhook");
        m.event_failed("webhook");
        m.event_lag("webhook", 3);
        m.pre_land("denied");
        let text = m.encode();
        assert!(
            text.contains("scion_events_delivered_total{sink=\"webhook\"}"),
            "{text}"
        );
        assert!(text.contains("scion_event_delivery_failures_total{sink=\"webhook\"}"));
        assert!(text.contains("scion_event_lag{sink=\"webhook\"} 3"));
        assert!(text.contains("scion_pre_land_checks_total{outcome=\"denied\"}"));
        assert!(
            text.contains("scion_rejected_requests_total{reason=\"overload\"}"),
            "{text}"
        );
        assert!(text.contains("scion_object_cache_requests_total{layer=\"l3\",result=\"hit\"}"));
        assert!(text.contains("scion_land_batches_total{outcome=\"retried\"}"));
        assert!(text.contains("scion_land_batch_size_count"));
        assert!(text.contains("scion_land_duration_seconds_bucket"));
        assert!(text.ends_with("# EOF\n"));
    }

    #[test]
    fn sampled_metrics_follow_their_source() {
        let source: Arc<SampleFn> = Arc::new(|| vec![Sample::new(&[("node", "a")], 7.0)]);
        metrics().register_sampled(
            "scion_test_sampled",
            "test counter",
            SampleKind::Counter,
            Arc::downgrade(&source),
        );
        let gauge: Arc<SampleFn> = Arc::new(|| vec![Sample::new(&[], 2.5)]);
        metrics().register_sampled(
            "scion_test_gauge",
            "test gauge",
            SampleKind::Gauge,
            Arc::downgrade(&gauge),
        );
        let empty: Arc<SampleFn> = Arc::new(Vec::new);
        metrics().register_sampled("scion_test_empty", "nothing", SampleKind::Gauge, Arc::downgrade(&empty));
        let text = metrics().encode();
        assert!(text.contains("scion_test_sampled_total{node=\"a\"} 7.0"), "{text}");
        assert!(text.contains("scion_test_gauge 2.5"), "{text}");
        assert!(!text.contains("scion_test_empty"));
        drop(source);
        assert!(!metrics().encode().contains("scion_test_sampled"), "dropped source");
        let _ = format!(
            "{:?}",
            Sampled {
                name: "n",
                help: "h",
                kind: SampleKind::Gauge,
                source: Arc::downgrade(&gauge),
            }
        );
    }
}
