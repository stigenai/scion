//! 写路径核心（服务端设计文档 §6）：创建提交、服务端 rebase 提交栈、把 ref 移到新提交。
//!
//! - [`ChangesetWriter`]：校验并创建提交（建树、写对象、写索引）；
//! - [`prepare`]：把待合入的提交栈 rebase 到 ref 当前提交上，必要时三方合并；
//! - [`Lander`]：合入一个请求，读 ref → rebase → 条件更新，冲突时重试。
//!
//! 与协议无关：错误用 [`LandError`] 表示，由 API 层映射为 gRPC 状态。

mod coordinator;
mod create;
mod land;
mod pending;
mod record;
mod stack;

pub use coordinator::{ClusterConfig, Coordinator, CoordinatorConfig};
pub use create::{Built, ChangesetWriter};
pub use land::{LandOutcome, LandRequest, Lander, check_land};
pub use stack::{Landed, Prepared, StackOptions, prepare};

use scion_merge::{Conflict, MergeError};
use scion_object::ObjectId;
use scion_query::QueryError;
use scion_tree::TreeError;
use thiserror::Error;

/// 写路径的错误。
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum LandError {
    #[error("repository {0:?} not found")]
    RepoNotFound(String),
    #[error("invalid ref name {0:?}")]
    InvalidRef(String),
    /// Tags (`tags/…`) never move: create them once, delete and recreate to change one.
    #[error("{0:?} is a tag; tags cannot move")]
    TagImmutable(String),
    /// 引用的对象（父提交、文件内容、待合入的提交）还没有上传。
    #[error("{} objects not uploaded", .0.len())]
    ObjectsNotUploaded(Vec<ObjectId>),
    /// delta 本身有问题：无法应用到父提交上、大小与实际不符等。
    #[error("invalid changeset at {path:?}: {message}")]
    InvalidChangeset { path: String, message: String },
    /// 只允许快进时，提交不是 ref 当前提交的后继。
    #[error("not a fast-forward of {tip}")]
    NotFastForward { tip: ObjectId },
    /// rebase 时 `changeset`（待合入栈中的原提交）与 ref 上的改动冲突。
    #[error("changeset {changeset} conflicts on {} paths", conflicts.len())]
    Conflict {
        changeset: ObjectId,
        conflicts: Vec<Conflict>,
    },
    /// 待合入的提交与 ref 没有共同历史。
    #[error("changeset {tip} shares no history with the ref")]
    UnrelatedHistory { tip: ObjectId },
    /// 待合入的提交栈中有合并提交（服务端 rebase 只处理线性的提交栈）。
    #[error("changeset {0} in the stack is a merge; land it as a fast-forward or rebase it locally")]
    MergeInStack(ObjectId),
    /// 一次合入的提交（快进、新建 ref 或 rebase）超过上限，客户端应分段合入。
    #[error("more than {0} changesets to land at once; land them in smaller steps")]
    StackTooDeep(usize),
    /// 同一个 request id 被用于参数不同的请求。
    #[error("request id {0:?} was already used for a different land request")]
    RequestIdReused(String),
    /// 集群模式下这个 ref 由另一个节点（`leader`，即它的地址）处理，请求应转给它。
    #[error("ref is handled by {leader}")]
    NotLeader { leader: String },
    /// 合入队列已满（过载保护），稍后重试。
    #[error("land queue for the ref is full")]
    Overloaded,
    /// 条件更新多次失败（ref 写入竞争激烈），可重试。
    #[error("ref changed concurrently too many times")]
    Contention,
    /// 存储中的数据损坏或缺失（不可重试）。
    #[error("corrupt data: {0}")]
    Corrupt(String),
    /// 存储暂时不可用（可重试）。
    #[error("storage unavailable: {0}")]
    Unavailable(String),
    /// 需要读取的 blob（例如 rebase 时三方合并的文件）已被 redact。
    #[error("blob {id} is redacted ({redaction_id})")]
    Redacted { id: ObjectId, redaction_id: String },
}

pub type Result<T> = std::result::Result<T, LandError>;

impl From<scion_store::Error> for LandError {
    fn from(e: scion_store::Error) -> Self {
        use scion_store::Error as E;
        match e {
            E::RepoNotFound(r) => Self::RepoNotFound(r),
            E::RefVersionMismatch { .. } => Self::Contention,
            E::Backend(m) => Self::Unavailable(m),
            E::Redacted { id, redaction_id } => Self::Redacted { id, redaction_id },
            other => Self::Corrupt(other.to_string()),
        }
    }
}

impl From<TreeError> for LandError {
    fn from(e: TreeError) -> Self {
        match e {
            TreeError::DeleteMissing(ref p) | TreeError::DeleteDirectory(ref p) | TreeError::PathConflict(ref p) => {
                Self::InvalidChangeset {
                    path: p.clone(),
                    message: e.to_string(),
                }
            }
            TreeError::BadTree(..) => Self::Corrupt(e.to_string()),
            TreeError::Storage(m) => Self::Unavailable(m),
        }
    }
}

impl From<MergeError> for LandError {
    fn from(e: MergeError) -> Self {
        match e {
            MergeError::Tree(t) => t.into(),
            MergeError::MissingBlob(_) => Self::Corrupt(e.to_string()),
            MergeError::Storage(m) => Self::Unavailable(m),
            MergeError::Redacted { id, redaction_id } => Self::Redacted { id, redaction_id },
        }
    }
}

impl From<QueryError> for LandError {
    fn from(e: QueryError) -> Self {
        match e {
            QueryError::Store(s) => s.into(),
            QueryError::Tree(t) => t.into(),
            QueryError::NotFound(id) => Self::ObjectsNotUploaded(vec![id]),
            other => Self::Corrupt(other.to_string()),
        }
    }
}

/// Refs under this prefix are tags: `scn tag v1.0` creates `tags/v1.0`.
pub const TAG_PREFIX: &str = "tags/";

pub fn is_tag(name: &str) -> bool {
    name.starts_with(TAG_PREFIX)
}

/// ref 名规则与路径相同（`main`、`feature/x`），不允许空串、`.`、`..` 等。
pub fn validate_ref_name(name: &str) -> Result<()> {
    scion_object::validate_path(name).map_err(|_| LandError::InvalidRef(name.to_owned()))
}

#[cfg(test)]
mod tests;
