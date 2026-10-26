//! Scion 官方 Rust SDK。
//!
//! 只依赖公开协议（`scion-proto`）和对象模型（`scion-object`）：SDK 能做到的，
//! 任何语言照着协议也能做到。
//!
//! ```no_run
//! # async fn demo() -> scion_client::Result<()> {
//! use scion_client::{Auth, Client};
//! let c = Client::builder()
//!     .endpoint("http://127.0.0.1:7080")
//!     .repo("mono")
//!     .auth(Auth::from_env())
//!     .build()
//!     .await?;
//! println!("server protocol {}", c.server_info().protocol_version);
//! # Ok(()) }
//! ```

mod client;
mod error;
mod history;
mod identity;
mod info;
mod ops;
mod orgs;
mod repos;
mod watch;

pub use client::{Auth, Client, ClientBuilder, ReadEndpoints, RedactedObject, RedactedPage, Tls, UPSTREAM_HEADER};
pub use error::{ConflictPath, Error, MergeConflict, Result};
pub use history::{
    ArchiveFormat, BlameCommitInfo, BlameInfo, FileChangeInfo, LastCommitsPage, LogItem, LogPage, Revision,
    TreeStatsInfo,
};
pub use identity::{DeviceLogin, DeviceLoginPoll, Identity};
pub use info::{Limits, ProtocolVersion, ReadEndpoint, Role, ServerInfo};
pub use ops::{
    ChangesetBuilder, CreatedChangeset, DirEntry, LAND_ATTEMPTS, Landed, LandedChangeset, Person, RefLogEntry, RefTip,
    Signature,
};
pub use orgs::{MemberInfo, OrgInfo};
pub use repos::{NewRepo, RefEventInfo, RefEventPage, RepoInfo, RepoPage};
pub use scion_object::{EntryMode, ObjectId, ObjectKind, REBASED_FROM_EXTENSION};
/// gRPC 状态码，出现在 [`Error::Rpc`] 中。
pub use tonic::Code;
pub use watch::{RefChange, RefWatch};
