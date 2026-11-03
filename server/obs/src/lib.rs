//! Scion 的可观测性（服务端设计文档 §11）：Prometheus 指标、OpenTelemetry 追踪与 gRPC deadline。
//!
//! - [`metrics`]：进程内的指标注册表与 `/metrics`（[`http::serve_metrics`]）；
//! - [`grpc::MetricsLayer`]：按方法记录请求数、状态码与延迟；
//! - [`trace`]：日志与 OTLP 导出的初始化、从 `traceparent` 接续调用方的 trace、向上游注入；
//! - [`deadline::DeadlineLayer`]：取客户端 deadline 与服务端上限中较小的一个，超时返回 DEADLINE_EXCEEDED。

pub mod age;
pub mod deadline;
pub mod grpc;
pub mod http;
pub mod metrics;
pub mod streams;
pub mod trace;

pub use metrics::{Metrics, Sample, SampleKind, metrics, reason};
