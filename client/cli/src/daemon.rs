//! `scn daemon`：运行、查看、停止本机 daemon，以及命令连接服务端时经 daemon 转发（客户端设计文档 §2、§7）。
//!
//! 默认经 daemon 连接：daemon 没在运行时自动在后台拉起它（等待最多几秒）；拉起失败或连不上时
//! 直接连服务端。`SCION_NO_DAEMON=1` 关闭（CI 容器里通常这么做）。

use std::path::{Path, PathBuf};
use std::time::Duration;

use scion_client::{Client, ClientBuilder};
use scion_daemon::DaemonConfig;
use serde::Serialize;

use crate::output::{CliError, Render};

/// 是否经 daemon 连接。
pub fn enabled(env: impl Fn(&str) -> Option<String>) -> bool {
    !matches!(env("SCION_NO_DAEMON").as_deref().map(str::trim), Some(v) if !v.is_empty() && v != "0")
}

/// The daemon paths: `SCION_DAEMON_SOCKET` and `SCION_DAEMON_CACHE_DIR` are used as given, each one
/// alone. A path without an override comes from an absolute root. A usage error when either path is missing.
pub fn config(env: impl Fn(&str) -> Option<String>) -> Result<DaemonConfig, CliError> {
    let (socket, cache) = DaemonConfig::default_locations(&env, cfg!(target_os = "macos"));
    let set = |k: &str| env(k).filter(|s| !s.is_empty()).map(PathBuf::from);
    match (
        set("SCION_DAEMON_SOCKET").or(socket),
        set("SCION_DAEMON_CACHE_DIR").or(cache),
    ) {
        (Some(s), Some(c)) => Ok(DaemonConfig::at(s, c)),
        _ => Err(CliError::Usage(format!(
            "no daemon paths: {}, or set SCION_DAEMON_SOCKET and SCION_DAEMON_CACHE_DIR; \
             SCION_NO_DAEMON=1 turns the daemon off",
            if cfg!(target_os = "macos") {
                "set an absolute HOME (macOS ignores XDG_CACHE_HOME and XDG_RUNTIME_DIR)"
            } else {
                "set an absolute HOME (or XDG_CACHE_HOME and XDG_RUNTIME_DIR)"
            }
        ))),
    }
}

/// 经 daemon 建立连接；daemon 没在运行时拉起它。失败时返回 `None`，调用方改为直连。
pub async fn connect(builder: ClientBuilder, socket: &Path) -> Option<Client> {
    if let Ok(c) = builder.clone().daemon_socket(socket).build().await {
        return Some(c);
    }
    // 服务端本身的错误（例如协议不兼容）直连也一样；这里只处理 daemon 不可达
    if tokio::net::UnixStream::connect(socket).await.is_ok() {
        return None;
    }
    spawn().ok()?;
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if tokio::net::UnixStream::connect(socket).await.is_ok() {
            return builder.daemon_socket(socket).build().await.ok();
        }
    }
    None
}

/// 在后台拉起 `scn daemon run`（独立的进程组，终端里的 Ctrl-C 不会带走它）。
fn spawn() -> std::io::Result<()> {
    let exe = std::env::current_exe()?;
    // 只从 scn 本身拉起（例如测试二进制调用到这里时不能把自己当成 daemon 启动）
    if exe.file_stem().and_then(|s| s.to_str()) != Some("scn") {
        return Err(std::io::Error::other("not running as scn"));
    }
    let mut cmd = std::process::Command::new(exe);
    cmd.args(["daemon", "run"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }
    cmd.spawn().map(|_| ())
}

#[derive(Debug, Serialize)]
pub struct DaemonStatusOut {
    pub schema: &'static str,
    pub running: bool,
    pub socket: String,
    pub pid: Option<u32>,
    pub version: Option<String>,
}

impl Render for DaemonStatusOut {
    fn text(&self) -> String {
        if !self.running {
            return format!("daemon:    not running\nsocket:    {}", self.socket);
        }
        format!(
            "daemon:    running (pid {})\nsocket:    {}\nversion:   {}",
            self.pid.map_or_else(|| "?".to_string(), |p| p.to_string()),
            self.socket,
            self.version.as_deref().unwrap_or("?")
        )
    }
}

fn read_pid(socket: &Path) -> Option<u32> {
    std::fs::read_to_string(scion_daemon::pid_file(socket))
        .ok()?
        .trim()
        .parse()
        .ok()
}

/// 向 daemon 询问版本（不带服务端地址的 GetServerInfo 由 daemon 自己应答）。
#[cfg(unix)]
async fn daemon_version(socket: &Path) -> Option<String> {
    let p = socket.to_path_buf();
    let channel = tonic::transport::Endpoint::from_static("http://scion-daemon")
        .connect_timeout(Duration::from_secs(2))
        .connect_with_connector(tower::service_fn(move |_| {
            let p = p.clone();
            async move {
                let s = tokio::net::UnixStream::connect(p).await?;
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(s))
            }
        }))
        .await
        .ok()?;
    let info = scion_proto::v1::server_info_service_client::ServerInfoServiceClient::new(channel)
        .get_server_info(scion_proto::v1::GetServerInfoRequest::default())
        .await
        .ok()?
        .into_inner()
        .info?;
    Some(info.server_version)
}

#[cfg(not(unix))]
async fn daemon_version(_socket: &Path) -> Option<String> {
    None
}

pub async fn status(cfg: &DaemonConfig) -> DaemonStatusOut {
    let version = daemon_version(&cfg.socket).await;
    DaemonStatusOut {
        schema: "scion.cli.daemon_status/v1",
        running: version.is_some(),
        socket: cfg.socket.display().to_string(),
        pid: version.as_ref().and_then(|_| read_pid(&cfg.socket)),
        version,
    }
}

#[derive(Debug, Serialize)]
pub struct DaemonStopOut {
    pub schema: &'static str,
    pub stopped: bool,
    pub pid: Option<u32>,
}

impl Render for DaemonStopOut {
    fn text(&self) -> String {
        match (self.stopped, self.pid) {
            (true, Some(p)) => format!("daemon stopped (pid {p})"),
            _ => "daemon was not running".to_string(),
        }
    }

    const REPORTS: bool = true;
}

/// 发送 SIGTERM 并等待 socket 消失。
#[cfg(unix)]
pub async fn stop(cfg: &DaemonConfig) -> Result<DaemonStopOut, CliError> {
    let not_running = DaemonStopOut {
        schema: "scion.cli.daemon_stop/v1",
        stopped: false,
        pid: None,
    };
    let Some(pid) = read_pid(&cfg.socket) else {
        return Ok(not_running);
    };
    if daemon_version(&cfg.socket).await.is_none() {
        return Ok(not_running);
    }
    let ok = std::process::Command::new("kill")
        .args(["-TERM", &pid.to_string()])
        .status()
        .map_err(|e| CliError::Usage(format!("kill: {e}")))?
        .success();
    if !ok {
        return Err(CliError::Usage(format!("cannot signal daemon pid {pid}")));
    }
    for _ in 0..100 {
        if !cfg.socket.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Ok(DaemonStopOut {
        schema: "scion.cli.daemon_stop/v1",
        stopped: true,
        pid: Some(pid),
    })
}

#[cfg(not(unix))]
pub async fn stop(_cfg: &DaemonConfig) -> Result<DaemonStopOut, CliError> {
    Err(CliError::Usage("the daemon is only supported on Unix".into()))
}

/// 前台运行 daemon，直到收到 SIGTERM / Ctrl-C 或空闲超时。
#[cfg(unix)]
pub async fn run(cfg: DaemonConfig) -> Result<(), CliError> {
    let filter = tracing_subscriber::EnvFilter::try_from_env("SCION_LOG")
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init();
    let shutdown = async {
        let mut term = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(s) => s,
            Err(_) => return std::future::pending::<()>().await,
        };
        tokio::select! {
            _ = term.recv() => {},
            _ = tokio::signal::ctrl_c() => {},
        }
    };
    scion_daemon::run(cfg, shutdown)
        .await
        .map_err(|e| CliError::Usage(format!("daemon: {e}")))
}

#[cfg(not(unix))]
pub async fn run(_cfg: DaemonConfig) -> Result<(), CliError> {
    Err(CliError::Usage("the daemon is only supported on Unix".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(vars: &'static [(&'static str, &'static str)]) -> impl Fn(&str) -> Option<String> {
        move |k| vars.iter().find(|(n, _)| *n == k).map(|(_, v)| (*v).to_string())
    }

    #[test]
    fn enabled_unless_opted_out() {
        assert!(enabled(env(&[])));
        assert!(enabled(env(&[("SCION_NO_DAEMON", "0")])));
        assert!(enabled(env(&[("SCION_NO_DAEMON", "")])));
        assert!(!enabled(env(&[("SCION_NO_DAEMON", "1")])));
        assert!(!enabled(env(&[("SCION_NO_DAEMON", "true")])));
    }

    #[test]
    fn config_overrides() {
        let c = config(env(&[
            ("SCION_DAEMON_SOCKET", "/tmp/d.sock"),
            ("SCION_DAEMON_CACHE_DIR", "/tmp/c"),
        ]))
        .expect("config");
        assert_eq!(c.socket, PathBuf::from("/tmp/d.sock"));
        assert_eq!(c.cache_dir, PathBuf::from("/tmp/c"));
        assert!(config(env(&[])).is_err());
        let d = config(env(&[("HOME", "/h")])).expect("config");
        let (sock, objects) = if cfg!(target_os = "macos") {
            ("/h/Library/Caches/scion/daemon.sock", "/h/Library/Caches/scion/objects")
        } else {
            ("/h/.cache/scion/daemon.sock", "/h/.cache/scion/objects")
        };
        assert_eq!((d.socket, d.cache_dir), (PathBuf::from(sock), PathBuf::from(objects)));
        // each override stands alone; an empty one selects the default; the cache is checked too
        let c = config(env(&[
            ("HOME", "/h"),
            ("SCION_DAEMON_CACHE_DIR", "rel"),
            ("SCION_DAEMON_SOCKET", ""),
        ]))
        .expect("config");
        assert_eq!((c.socket, c.cache_dir), (PathBuf::from(sock), PathBuf::from("rel")));
        let e = config(env(&[("HOME", "/h"), ("SCION_DAEMON_CACHE_DIR", "")])).expect("config");
        assert_eq!(e.cache_dir, PathBuf::from(objects));
        let s = config(env(&[("HOME", "rel"), ("SCION_DAEMON_SOCKET", "/o/d.sock")]));
        assert!(
            s.is_err(),
            "a valid socket does not make a relative HOME usable for the cache"
        );
        let msg = config(env(&[])).expect_err("no paths").to_string();
        assert_eq!(msg.contains("macOS ignores"), cfg!(target_os = "macos"), "{msg}");
        // a relative override is kept as given
        let r = config(env(&[
            ("SCION_DAEMON_SOCKET", "d.sock"),
            ("SCION_DAEMON_CACHE_DIR", "c"),
        ]))
        .expect("config");
        assert_eq!(r.socket, PathBuf::from("d.sock"));
    }

    #[test]
    fn render_status_and_stop() {
        let mut s = DaemonStatusOut {
            schema: "x",
            running: false,
            socket: "/s".into(),
            pid: None,
            version: None,
        };
        assert!(s.text().contains("not running"));
        s.running = true;
        s.pid = Some(7);
        s.version = Some("scn-daemon 0.1.0".into());
        assert!(s.text().contains("pid 7") && s.text().contains("scn-daemon"));
        let stopped = DaemonStopOut {
            schema: "x",
            stopped: true,
            pid: Some(7),
        };
        assert!(stopped.text().contains("pid 7"));
        let none = DaemonStopOut {
            schema: "x",
            stopped: false,
            pid: None,
        };
        assert!(none.text().contains("not running"));
    }

    #[test]
    fn spawn_refuses_outside_scn() {
        assert!(spawn().is_err(), "the test binary is not scn");
    }

    #[tokio::test]
    async fn status_and_stop_without_daemon() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = DaemonConfig::at(dir.path().join("d.sock"), dir.path().join("objects"));
        let s = status(&cfg).await;
        assert!(!s.running && s.pid.is_none());
        // pid 文件残留但 daemon 不在：什么都不做
        std::fs::write(scion_daemon::pid_file(&cfg.socket), "999999").expect("pid");
        assert_eq!(read_pid(&cfg.socket), Some(999999));
        let out = stop(&cfg).await.expect("stop");
        assert!(!out.stopped);
    }
}
