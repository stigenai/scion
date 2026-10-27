//! `scn login` / `scn logout` / `scn whoami`，以及各命令连接时使用的凭据。

use std::time::Duration;

use scion_client::{Auth, Client, DeviceLoginPoll};
use serde::Serialize;

use crate::commands::Ctx;
use crate::creds;
use crate::output::{CliError, Render};

/// 连接 `endpoint` 时使用的凭据：`SCION_TOKEN` 环境变量，其次是 `scn login` 保存的凭据。
pub fn auth_for(endpoint: &str, env: impl Fn(&str) -> Option<String>) -> Auth {
    if let Some(t) = env("SCION_TOKEN")
        .map(|t| t.trim().to_owned())
        .filter(|t| !t.is_empty())
    {
        return Auth::Exchange(t);
    }
    match creds::load(&creds::backend(&env), endpoint) {
        Some(secret) => Auth::Exchange(secret),
        None => Auth::None,
    }
}

#[derive(Debug, Serialize)]
pub struct LoginOut {
    pub schema: &'static str,
    pub endpoint: String,
    pub subject: String,
    /// 凭据保存的位置。
    pub stored_in: String,
}

impl Render for LoginOut {
    fn text(&self) -> String {
        format!(
            "logged in to {} as {} (credentials saved to {})",
            self.endpoint, self.subject, self.stored_in
        )
    }

    const REPORTS: bool = true;
}

/// 直接连接服务端（不经 daemon，登录本身不需要缓存）。
async fn direct(endpoint: &str, auth: Auth) -> Result<Client, CliError> {
    Ok(Client::builder()
        .endpoint(endpoint)
        .auth(auth)
        .client_name("scn")
        .tls(scion_client::Tls::from_env()?)
        .connect_timeout(Duration::from_secs(5))
        .build()
        .await?)
}

/// 用已有的令牌登录：校验后保存。
pub async fn login_with_token(ctx: &Ctx, token: &str) -> Result<LoginOut, CliError> {
    let endpoint = ctx.endpoint_or_default();
    let token = token.trim();
    if token.is_empty() {
        return Err(CliError::Usage("empty token".into()));
    }
    let c = direct(&endpoint, Auth::Exchange(token.to_owned())).await?;
    let me = c.who_am_i().await?;
    if me.subject == "anonymous" {
        return Err(CliError::Usage("the server did not accept the token".into()));
    }
    let stored_in = creds::store(&creds::backend(|k| std::env::var(k).ok()), &endpoint, token)
        .map_err(|e| CliError::Usage(format!("saving credentials: {e}")))?;
    Ok(LoginOut {
        schema: "scion.cli.login/v1",
        endpoint,
        subject: me.subject,
        stored_in,
    })
}

/// OIDC device flow：提示用户在浏览器中完成登录，轮询到完成后保存刷新令牌。
pub async fn login_device(ctx: &Ctx, mut prompt: impl FnMut(&str)) -> Result<LoginOut, CliError> {
    let endpoint = ctx.endpoint_or_default();
    let c = direct(&endpoint, Auth::None).await?;
    if !c.has_feature(scion_proto::features::AUTH_DEVICE) {
        return Err(CliError::Usage(
            "this server does not support browser login; use `scn login --token TOKEN`".into(),
        ));
    }
    let login = c.start_device_login().await?;
    let url = login
        .verification_uri_complete
        .clone()
        .unwrap_or_else(|| login.verification_uri.clone());
    prompt(&format!(
        "Open {url} in a browser and enter the code {} to log in.",
        login.user_code
    ));
    let mut interval = Duration::from_secs(u64::from(login.interval_seconds));
    loop {
        tokio::time::sleep(interval).await;
        match c.poll_device_login(&login.device_code).await? {
            DeviceLoginPoll::Pending => {}
            DeviceLoginPoll::SlowDown => interval += Duration::from_secs(5),
            DeviceLoginPoll::Denied => return Err(CliError::Usage("login was denied".into())),
            DeviceLoginPoll::Expired => {
                return Err(CliError::Usage("login code expired; run `scn login` again".into()));
            }
            DeviceLoginPoll::Complete { subject, refresh_token } => {
                let stored_in = creds::store(&creds::backend(|k| std::env::var(k).ok()), &endpoint, &refresh_token)
                    .map_err(|e| CliError::Usage(format!("saving credentials: {e}")))?;
                return Ok(LoginOut {
                    schema: "scion.cli.login/v1",
                    endpoint,
                    subject,
                    stored_in,
                });
            }
        }
    }
}

#[derive(Debug, Serialize)]
pub struct LogoutOut {
    pub schema: &'static str,
    pub endpoint: String,
    pub removed: bool,
}

impl Render for LogoutOut {
    fn text(&self) -> String {
        if self.removed {
            format!("logged out of {}", self.endpoint)
        } else {
            format!("no saved credentials for {}", self.endpoint)
        }
    }

    const REPORTS: bool = true;
}

pub fn logout(ctx: &Ctx) -> Result<LogoutOut, CliError> {
    let endpoint = ctx.endpoint_or_default();
    let removed = creds::remove(&creds::backend(|k| std::env::var(k).ok()), &endpoint)
        .map_err(|e| CliError::Usage(format!("removing credentials: {e}")))?;
    Ok(LogoutOut {
        schema: "scion.cli.logout/v1",
        endpoint,
        removed,
    })
}

#[derive(Debug, Serialize)]
pub struct WhoAmIOut {
    pub schema: &'static str,
    pub endpoint: String,
    pub subject: String,
    pub groups: Vec<String>,
    pub auth_required: bool,
}

impl Render for WhoAmIOut {
    fn text(&self) -> String {
        let mut s = format!("{} on {}", self.subject, self.endpoint);
        if !self.groups.is_empty() {
            s.push_str(&format!(" (groups: {})", self.groups.join(", ")));
        }
        if self.subject == "anonymous" && self.auth_required {
            s.push_str("\nhint: run `scn login`");
        }
        s
    }
}

pub async fn whoami(ctx: &Ctx) -> Result<WhoAmIOut, CliError> {
    let endpoint = ctx.endpoint_or_default();
    let c = crate::commands::connect(&endpoint, None).await?;
    let me = c.who_am_i().await?;
    Ok(WhoAmIOut {
        schema: "scion.cli.whoami/v1",
        endpoint,
        subject: me.subject,
        groups: me.groups,
        auth_required: me.auth_required,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credential_precedence() {
        let dir = tempfile::tempdir().expect("tempdir");
        let file = dir.path().join("c.json");
        let f = file.display().to_string();
        let b = creds::Backend::File(file);
        creds::store(&b, "http://s", "saved").expect("store");
        let env = |token: Option<&'static str>| {
            let f = f.clone();
            move |k: &str| match k {
                "SCION_TOKEN" => token.map(str::to_owned),
                "SCION_CREDENTIALS_FILE" => Some(f.clone()),
                _ => None,
            }
        };
        assert_eq!(auth_for("http://s", env(Some(" env "))), Auth::Exchange("env".into()));
        assert_eq!(auth_for("http://s", env(Some(""))), Auth::Exchange("saved".into()));
        assert_eq!(auth_for("http://other", env(None)), Auth::None);
    }

    #[test]
    fn render_outputs() {
        let l = LoginOut {
            schema: "x",
            endpoint: "http://s".into(),
            subject: "alice".into(),
            stored_in: "the system keychain".into(),
        };
        assert!(l.text().contains("alice") && l.text().contains("keychain"));
        let mut o = LogoutOut {
            schema: "x",
            endpoint: "http://s".into(),
            removed: true,
        };
        assert!(o.text().starts_with("logged out"));
        o.removed = false;
        assert!(o.text().starts_with("no saved"));
        let mut w = WhoAmIOut {
            schema: "x",
            endpoint: "http://s".into(),
            subject: "anonymous".into(),
            groups: vec![],
            auth_required: true,
        };
        assert!(w.text().contains("scn login"));
        w.subject = "bob".into();
        w.groups = vec!["eng".into()];
        assert!(w.text().contains("groups: eng") && !w.text().contains("hint"));
    }
}
