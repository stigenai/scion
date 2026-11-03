//! NatsSink must report a connect failure, never Ok, when no server listens.

use std::time::Duration;

use scion_api::events::{NatsConfig, NatsSink, Sink};
use tokio::net::TcpListener;

#[tokio::test]
async fn deliver_to_closed_port_returns_connect_error() {
    // Bind, then drop the listener to free the port.
    let port = {
        let l = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        l.local_addr().expect("addr").port()
    };
    let sink = NatsSink::new(&NatsConfig {
        url: format!("nats://127.0.0.1:{port}"),
        subject_prefix: "scion".into(),
    });
    let res = tokio::time::timeout(Duration::from_secs(10), sink.deliver("ref.updated", 1, b"{}"))
        .await
        .expect("deliver must finish in 10 s");
    let err = res.expect_err("an unreachable server must not give Ok");
    assert!(err.starts_with("connect"), "got: {err}");
}
