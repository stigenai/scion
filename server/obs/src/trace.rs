//! 日志与 OpenTelemetry 追踪。
//!
//! - [`init`]：日志输出到 stderr（文本或 JSON）；配置了 OTLP 时同时把 span 导出到 collector。
//!   OTLP 地址取 `--otlp-endpoint`，或标准环境变量 `OTEL_EXPORTER_OTLP_ENDPOINT` /
//!   `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`；采样等其它参数也按 OpenTelemetry 的标准环境变量。
//! - [`TraceLayer`]：每个 gRPC 请求一个 span，从 `traceparent`（W3C Trace Context）接续调用方的 trace。
//! - [`inject_or`]：向上游的请求注入当前 span 的 `traceparent`，没有时原样转发调用方的。

use std::task::{Context, Poll};

use opentelemetry::propagation::{Extractor, Injector};
use opentelemetry::trace::{TraceContextExt, TracerProvider as _};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tonic::metadata::{Ascii, MetadataKey, MetadataMap, MetadataValue};
use tower::{Layer, Service};
use tracing::Instrument;
use tracing::instrument::Instrumented;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::Layer as _;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use crate::grpc::split_path;

/// 日志与追踪的配置。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TraceConfig {
    /// OpenTelemetry 的 `service.name`。
    pub service_name: String,
    /// OTLP gRPC 地址（例如 `http://otel-collector:4317`）。
    pub otlp_endpoint: Option<String>,
    /// 日志输出 JSON。
    pub json: bool,
    /// 日志级别的环境变量（`EnvFilter` 语法），未设置时为 `info`。
    pub filter_env: String,
}

/// 是否导出到 OTLP：`Some(Some(url))` 用给定地址，`Some(None)` 由导出器按标准环境变量决定地址，`None` 不导出。
pub fn otlp_target(explicit: Option<&str>, env: impl Fn(&str) -> Option<String>) -> Option<Option<String>> {
    if let Some(e) = explicit.filter(|e| !e.is_empty()) {
        return Some(Some(e.to_owned()));
    }
    ["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "OTEL_EXPORTER_OTLP_ENDPOINT"]
        .iter()
        .any(|k| env(k).is_some_and(|v| !v.is_empty()))
        .then_some(None)
}

/// 进程退出时把尚未导出的 span 发送出去。
#[must_use = "dropping the guard flushes and stops trace export"]
pub struct TraceGuard {
    provider: Option<SdkTracerProvider>,
}

impl TraceGuard {
    pub fn exporting(&self) -> bool {
        self.provider.is_some()
    }
}

impl Drop for TraceGuard {
    fn drop(&mut self) {
        if let Some(p) = self.provider.take() {
            let _ = p.shutdown();
        }
    }
}

/// 构建 OTLP 导出（需要在 tokio 运行时中调用：导出器使用 tonic）。
fn provider(cfg: &TraceConfig, target: Option<String>) -> Result<SdkTracerProvider, String> {
    let mut b = opentelemetry_otlp::SpanExporter::builder().with_tonic();
    if let Some(url) = target {
        b = b.with_endpoint(url);
    }
    let exporter = b.build().map_err(|e| format!("OTLP exporter: {e}"))?;
    let resource = opentelemetry_sdk::Resource::builder()
        .with_service_name(cfg.service_name.clone())
        .build();
    Ok(SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .build())
}

/// 初始化全局的日志与追踪。只能调用一次；再次调用返回错误。
pub fn init(cfg: &TraceConfig) -> Result<TraceGuard, String> {
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
    let filter = tracing_subscriber::EnvFilter::try_from_env(&cfg.filter_env)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    // 写日志失败（例如日志采集进程重启导致管道断开）时直接丢弃这条日志。默认行为是改用 eprintln!
    // 报告，而 stderr 已坏时 eprintln! 会 panic，导致正在处理的请求失败。
    let fmt = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .log_internal_errors(false);
    let fmt = if cfg.json { fmt.json().boxed() } else { fmt.boxed() };
    let provider = match otlp_target(cfg.otlp_endpoint.as_deref(), |k| std::env::var(k).ok()) {
        Some(target) => Some(provider(cfg, target)?),
        None => None,
    };
    let otel = provider
        .as_ref()
        .map(|p| tracing_opentelemetry::layer().with_tracer(p.tracer("scion")));
    tracing_subscriber::registry()
        .with(filter)
        .with(fmt)
        .with(otel)
        .try_init()
        .map_err(|e| e.to_string())?;
    Ok(TraceGuard { provider })
}

struct HeaderExtractor<'a>(&'a http::HeaderMap);

impl Extractor for HeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(http::HeaderName::as_str).collect()
    }
}

struct MetadataInjector<'a>(&'a mut MetadataMap);

impl Injector for MetadataInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        if let (Ok(k), Ok(v)) = (MetadataKey::from_bytes(key.as_bytes()), MetadataValue::try_from(value)) {
            self.0.insert(k, v);
        }
    }
}

/// 注入当前 span 的 trace 上下文；当前没有有效的 trace（未启用 OTLP）时返回 false。
pub fn inject(md: &mut MetadataMap) -> bool {
    let cx = tracing::Span::current().context();
    if !cx.span().span_context().is_valid() {
        return false;
    }
    opentelemetry::global::get_text_map_propagator(|p| p.inject_context(&cx, &mut MetadataInjector(md)));
    true
}

/// 注入当前 span 的 trace 上下文；没有时转发调用方的 `traceparent`（本节点未启用追踪也不断开 trace）。
pub fn inject_or(md: &mut MetadataMap, incoming: Option<&MetadataValue<Ascii>>) {
    if !inject(md)
        && let Some(tp) = incoming
    {
        md.insert("traceparent", tp.clone());
    }
}

/// 每个 gRPC 请求一个 span（健康检查除外）。
#[derive(Debug, Clone, Copy, Default)]
pub struct TraceLayer;

impl<S> Layer<S> for TraceLayer {
    type Service = TraceService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        TraceService { inner }
    }
}

#[derive(Debug, Clone)]
pub struct TraceService<S> {
    inner: S,
}

impl<S, ReqBody> Service<http::Request<ReqBody>> for TraceService<S>
where
    S: Service<http::Request<ReqBody>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Instrumented<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<ReqBody>) -> Self::Future {
        let path = req.uri().path();
        if path.starts_with("/grpc.health.") {
            return self.inner.call(req).instrument(tracing::Span::none());
        }
        let (service, method) = split_path(path);
        let span = tracing::info_span!(
            "rpc",
            otel.name = %path.trim_start_matches('/'),
            otel.kind = "server",
            rpc.system = "grpc",
            rpc.service = %service,
            rpc.method = %method,
        );
        let parent = opentelemetry::global::get_text_map_propagator(|p| p.extract(&HeaderExtractor(req.headers())));
        let _ = span.set_parent(parent);
        let fut = span.in_scope(|| self.inner.call(req));
        fut.instrument(span)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;

    #[test]
    fn otlp_is_enabled_by_flag_or_standard_env() {
        let none = |_: &str| None;
        assert_eq!(otlp_target(None, none), None);
        assert_eq!(otlp_target(Some(""), none), None);
        assert_eq!(
            otlp_target(Some("http://c:4317"), none),
            Some(Some("http://c:4317".into()))
        );
        let env = |k: &str| (k == "OTEL_EXPORTER_OTLP_ENDPOINT").then(|| "http://e:4317".to_string());
        assert_eq!(otlp_target(None, env), Some(None));
        let empty = |_: &str| Some(String::new());
        assert_eq!(otlp_target(None, empty), None);
    }

    const PARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    /// 测试用服务：在请求的 span 内向「上游」注入 trace 上下文，返回得到的 traceparent。
    #[derive(Clone)]
    struct Echo;

    impl Service<http::Request<()>> for Echo {
        type Response = Option<String>;
        type Error = Infallible;
        type Future = std::future::Ready<Result<Option<String>, Infallible>>;
        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, _: http::Request<()>) -> Self::Future {
            let mut md = MetadataMap::new();
            inject_or(&mut md, None);
            std::future::ready(Ok(md
                .get("traceparent")
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned)))
        }
    }

    fn request(path: &str, traceparent: Option<&str>) -> http::Request<()> {
        let mut b = http::Request::builder().uri(path);
        if let Some(tp) = traceparent {
            b = b.header("traceparent", tp);
        }
        b.body(()).expect("request")
    }

    #[tokio::test]
    async fn continues_the_callers_trace() {
        opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
        let provider = SdkTracerProvider::builder().build();
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(provider.tracer("test")));
        let _guard = tracing::subscriber::set_default(subscriber);
        let mut svc = TraceLayer.layer(Echo);
        std::future::poll_fn(|cx| svc.poll_ready(cx)).await.expect("ready");
        let tp = svc
            .call(request("/scion.v1.RefService/ResolveRef", Some(PARENT)))
            .await
            .expect("call")
            .expect("injected");
        assert!(
            tp.starts_with("00-4bf92f3577b34da6a3ce929d0e0e4736-"),
            "same trace: {tp}"
        );
        assert!(!tp.contains("00f067aa0ba902b7"), "new span id");
        // 没有调用方的 trace：开始新的 trace
        let fresh = svc
            .call(request("/s.S/M", None))
            .await
            .expect("call")
            .expect("injected");
        assert!(!fresh.contains("4bf92f3577b34da6a3ce929d0e0e4736"));
        // 健康检查不建 span
        assert_eq!(
            svc.call(request("/grpc.health.v1.Health/Check", Some(PARENT)))
                .await
                .expect("call"),
            None
        );
    }

    #[test]
    fn falls_back_to_the_incoming_traceparent() {
        let mut md = MetadataMap::new();
        inject_or(&mut md, Some(&MetadataValue::from_static(PARENT)));
        assert_eq!(md.get("traceparent").map(|v| v.to_str().expect("ascii")), Some(PARENT));
        let mut md = MetadataMap::new();
        inject_or(&mut md, None);
        assert!(md.is_empty());
        let mut headers = http::HeaderMap::new();
        headers.insert("traceparent", http::HeaderValue::from_static(PARENT));
        let ex = HeaderExtractor(&headers);
        assert_eq!(ex.keys(), ["traceparent"]);
        assert_eq!(ex.get("traceparent"), Some(PARENT));
        let mut md = MetadataMap::new();
        MetadataInjector(&mut md).set("bad key", "v".into());
        assert!(md.is_empty());
    }

    #[tokio::test]
    async fn init_installs_once() {
        let cfg = TraceConfig {
            service_name: "scion-test".into(),
            otlp_endpoint: Some("http://127.0.0.1:1".into()),
            json: true,
            filter_env: "SCION_TEST_LOG".into(),
        };
        let guard = init(&cfg).expect("first init");
        assert!(guard.exporting());
        assert!(init(&TraceConfig::default()).is_err(), "already installed");
        drop(guard);
    }
}
