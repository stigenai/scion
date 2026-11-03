//! Scion 的认证与鉴权（服务端设计文档 §3.7、§9）。

pub mod config;
pub mod external;
pub mod github;
pub mod guard;
pub mod oidc;
pub mod orgs;
pub mod policy;
pub mod remote;
pub mod token;

#[cfg(test)]
mod revalidation_windows;

#[cfg(any(test, feature = "testing"))]
pub mod testing;

pub use config::AuthConfig;
pub use guard::{AuthError, Guard, GuardConfig, POLICY_KEY};
pub use policy::{Action, Grant, Policy, Principal, Request};
