//! 输出（文本 / JSON）、错误与退出码。

use scion_client::{Code, Role, ServerInfo};
use scion_proto::v1::ErrorReason;
use serde::Serialize;

pub const EXIT_GENERAL: u8 = 1;
pub const EXIT_USAGE: u8 = 2;
pub const EXIT_CONFLICT: u8 = 3;
pub const EXIT_RETRYABLE: u8 = 4;
pub const EXIT_AUTH: u8 = 5;

/// What stopped on conflicts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConflictOp {
    Sync,
    Merge,
    /// A revert or cherry-pick of this commit.
    Pick(scion_wc::PickKind, String),
    /// `stash apply` / `pop` of this entry.
    Stash(usize),
}

/// 命令失败的原因。
#[derive(Debug)]
pub enum CliError {
    Client(scion_client::Error),
    Wc(scion_wc::Error),
    Import(scion_gitimport::Error),
    Usage(String),
    /// A sync, merge, revert or cherry-pick stopped on conflicts; the files hold conflict markers.
    Conflicted {
        op: ConflictOp,
        reference: String,
        conflicts: Vec<ConflictOut>,
    },
}

impl From<scion_gitimport::Error> for CliError {
    fn from(e: scion_gitimport::Error) -> Self {
        match e {
            scion_gitimport::Error::Client(c) => CliError::Client(c),
            other => CliError::Import(other),
        }
    }
}

impl From<scion_client::Error> for CliError {
    fn from(e: scion_client::Error) -> Self {
        CliError::Client(e)
    }
}

impl From<scion_wc::Error> for CliError {
    fn from(e: scion_wc::Error) -> Self {
        match e {
            scion_wc::Error::Client(c) => CliError::Client(c),
            other => CliError::Wc(other),
        }
    }
}

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CliError::Client(e) => e.fmt(f),
            CliError::Wc(e) => e.fmt(f),
            CliError::Import(e) => e.fmt(f),
            CliError::Usage(m) => f.write_str(m),
            CliError::Conflicted {
                op: ConflictOp::Merge, ..
            } => f.write_str("automatic merge failed; fix conflicts and then commit the result"),
            CliError::Conflicted {
                op: ConflictOp::Pick(kind, commit),
                ..
            } => write!(
                f,
                "could not {} {}; fix conflicts and then commit the result",
                match kind {
                    scion_wc::PickKind::Revert => "revert",
                    scion_wc::PickKind::CherryPick => "apply",
                },
                &commit[..commit.len().min(12)]
            ),
            CliError::Conflicted {
                op: ConflictOp::Stash(n),
                ..
            } => write!(
                f,
                "could not apply stash@{{{n}}} cleanly; fix the conflicts in the files"
            ),
            CliError::Conflicted { reference, .. } => write!(
                f,
                "could not rebase your commit onto {reference}; fix conflicts and then commit the result"
            ),
        }
    }
}

impl CliError {
    fn client(&self) -> Option<&scion_client::Error> {
        match self {
            CliError::Client(e) => Some(e),
            _ => None,
        }
    }

    /// 稳定的退出码（客户端设计文档 §8.2）。
    pub fn exit_code(&self) -> u8 {
        match self {
            CliError::Usage(_) | CliError::Wc(scion_wc::Error::Sparse(_)) => EXIT_USAGE,
            CliError::Conflicted { .. }
            | CliError::Wc(
                scion_wc::Error::Conflict(_) | scion_wc::Error::MergeConflicts(_) | scion_wc::Error::Unresolved(_),
            ) => EXIT_CONFLICT,
            CliError::Wc(_) | CliError::Import(_) => EXIT_GENERAL,
            CliError::Client(e) => match e {
                scion_client::Error::Rpc {
                    code: Code::Unauthenticated | Code::PermissionDenied,
                    ..
                } => EXIT_AUTH,
                e if e.reason() == Some(ErrorReason::MergeConflict) => EXIT_CONFLICT,
                e if e.is_retryable() => EXIT_RETRYABLE,
                _ => EXIT_GENERAL,
            },
        }
    }

    /// 下一步该做什么。
    pub fn hint(&self) -> Option<&'static str> {
        if self.exit_code() == EXIT_AUTH {
            return Some("set SCION_TOKEN or run `scn login`");
        }
        match self {
            CliError::Client(scion_client::Error::Connect { .. }) => {
                Some("check that the server is running and --endpoint / SCION_ENDPOINT is correct")
            }
            CliError::Client(scion_client::Error::IncompatibleProtocol { .. }) => {
                Some("upgrade scn to a version matching the server's protocol")
            }
            CliError::Client(e) if e.reason() == Some(ErrorReason::NotFastForward) => Some(
                "the ref has moved and you have uncommitted changes; commit or discard them and `scn land` again \
                 (the server rebases), or run `scn sync`",
            ),
            CliError::Client(e) if e.reason() == Some(ErrorReason::MergeConflict) => Some(
                "the ref changed the same lines or files; run `scn sync` to bring them into your commit, \
                 fix the files it marks, `scn resolve FILE`, `scn commit` and `scn land` again",
            ),
            CliError::Client(e) if e.reason() == Some(ErrorReason::RepoNotFound) => {
                Some("check the repository name, or create it with `scn repo create NAME`")
            }
            CliError::Client(e) if e.reason() == Some(ErrorReason::PreLandRejected) => {
                Some("the server's pre-land check rejected this change; fix what the message says and land again")
            }
            CliError::Client(e) if e.reason() == Some(ErrorReason::RepoDeleted) => {
                Some("the repository was deleted; restore it with `scn repo restore NAME`, or wait until it is purged")
            }
            CliError::Wc(scion_wc::Error::Dirty) => {
                Some("run `scn commit -m <message>` first, or `scn stash` to put the changes aside")
            }
            CliError::Wc(scion_wc::Error::NothingToLand) => Some("run `scn commit -m <message>` first"),
            CliError::Wc(scion_wc::Error::NotAWorkingCopy(_)) => Some("run `scn clone <repo>` first, or pass --repo"),
            CliError::Wc(scion_wc::Error::PendingCommit) => Some("run `scn land` or `scn uncommit` first"),
            CliError::Wc(scion_wc::Error::MergeConflicts(_)) => {
                Some("nothing was changed; resolve the overlap on one branch (or merge the other way) and merge again")
            }
            CliError::Wc(scion_wc::Error::NoPendingCommit) => {
                Some("only a commit made with `scn commit` and not yet landed can be changed")
            }
            CliError::Import(scion_gitimport::Error::Diverged { .. }) => {
                Some("the git history was rewritten; import into a new ref with --ref")
            }
            CliError::Conflicted {
                op: ConflictOp::Stash(_),
                ..
            } => Some(
                "the files hold conflict markers (other conflicts keep the current file): fix them as local \
                 changes; the stash entry is kept, so `scn stash drop` it when done",
            ),
            CliError::Conflicted {
                op: ConflictOp::Sync, ..
            } => Some(
                "the files now hold conflict markers: fix them, then `scn resolve PATH` \
                 (or --ours / --theirs), then `scn commit`; `scn sync --abort` goes back to before the sync",
            ),
            CliError::Conflicted { .. } | CliError::Wc(scion_wc::Error::Conflict(_)) => Some(
                "the files now hold conflict markers: fix them, then `scn resolve PATH` \
                 (or --ours / --theirs), then `scn commit`; `scn merge --abort`, `scn revert --abort` or \
                 `scn cherry-pick --abort` give up instead",
            ),
            CliError::Wc(scion_wc::Error::NoChangesIn(_)) => Some(
                "paths after `scn commit` limit it to the changes in them (`scn status` lists the changes); \
                 the message goes after -m",
            ),
            CliError::Wc(scion_wc::Error::PartialFinish) => {
                Some("run `scn commit` without paths to finish, then commit further changes on their own")
            }
            CliError::Wc(scion_wc::Error::NoStash(_)) => Some("`scn stash list` lists the entries"),
            CliError::Wc(scion_wc::Error::Detached(_)) => {
                Some("`scn switch BRANCH` goes back to a branch; `scn switch -c NAME` starts one at this commit")
            }
            CliError::Wc(scion_wc::Error::NoSyncInProgress) => {
                Some("only a sync that stopped on conflicts can be aborted; `scn status` shows what is in progress")
            }
            CliError::Wc(scion_wc::Error::Unresolved(_)) => {
                Some("finish the conflicts first: `scn status` lists them, `scn resolve` marks them done")
            }
            CliError::Wc(scion_wc::Error::MarkersRemain(_)) => {
                Some("edit out the <<<<<<< / >>>>>>> sections, or pass --force, --ours or --theirs")
            }
            _ => None,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ErrorOut {
    pub schema: &'static str,
    pub error: String,
    pub reason: Option<&'static str>,
    pub retryable: bool,
    pub exit_code: u8,
    pub hint: Option<&'static str>,
    /// MERGE_CONFLICT 时的冲突路径。
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub conflicts: Vec<ConflictOut>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ConflictOut {
    pub path: String,
    pub kind: String,
}

pub fn error_out(err: &CliError) -> ErrorOut {
    ErrorOut {
        schema: "scion.cli.error/v1",
        error: err.to_string(),
        reason: err.client().and_then(|e| e.reason()).map(scion_proto::reason_name),
        retryable: err.client().is_some_and(|e| e.is_retryable()),
        exit_code: err.exit_code(),
        hint: err.hint(),
        conflicts: match err {
            CliError::Conflicted { conflicts, .. } => conflicts.clone(),
            _ => err
                .client()
                .and_then(|e| e.merge_conflict())
                .map(|c| {
                    c.paths
                        .into_iter()
                        .map(|p| ConflictOut {
                            path: p.path,
                            kind: p.kind,
                        })
                        .collect()
                })
                .unwrap_or_default(),
        },
    }
}

/// git's name for a conflict kind, as in `CONFLICT (content): Merge conflict in PATH`.
pub fn git_conflict_kind(kind: &str) -> &str {
    match kind {
        "add_add" => "add/add",
        "modify_delete" | "delete_modify" => "modify/delete",
        "path" => "file/directory",
        "binary" | "too_large" | "symlink" => "content",
        other => other,
    }
}

/// 每个命令的输出：JSON 形式由 `Serialize` 给出，文本形式由 `text()` 给出。
pub trait Render: Serialize {
    fn text(&self) -> String;
}

pub fn render<T: Render>(v: &T, json: bool) -> String {
    if json {
        serde_json::to_string(v).unwrap_or_else(|e| format!("{{\"error\":\"failed to encode JSON: {e}\"}}"))
    } else {
        v.text()
    }
}

#[derive(Debug, Serialize)]
pub struct VersionOut {
    pub schema: &'static str,
    pub client_version: &'static str,
    pub protocol_version: &'static str,
}

pub fn version_out() -> VersionOut {
    VersionOut {
        schema: "scion.cli.version/v1",
        client_version: env!("CARGO_PKG_VERSION"),
        protocol_version: scion_proto::PROTOCOL_VERSION,
    }
}

impl Render for VersionOut {
    fn text(&self) -> String {
        format!("scn {} (protocol {})", self.client_version, self.protocol_version)
    }
}

#[derive(Debug, Serialize)]
pub struct DoctorOut {
    pub schema: &'static str,
    pub endpoint: String,
    pub ok: bool,
    pub protocol_version: String,
    pub server_version: String,
    pub role: &'static str,
    pub features: Vec<String>,
    pub max_batch_objects: u32,
    pub max_chunk_bytes: u64,
    pub read_endpoints: Vec<String>,
    /// 读取对象经过的地址：daemon、区域缓存或服务端本身。
    pub objects: String,
}

pub fn role_str(r: Role) -> &'static str {
    match r {
        Role::Server => "server",
        Role::Cache => "cache",
        Role::Daemon => "daemon",
        Role::Unknown => "unknown",
    }
}

pub fn doctor_out(endpoint: &str, info: &ServerInfo) -> DoctorOut {
    DoctorOut {
        schema: "scion.cli.doctor/v1",
        endpoint: endpoint.to_string(),
        ok: true,
        protocol_version: info.protocol_version.to_string(),
        server_version: info.server_version.clone(),
        role: role_str(info.role),
        features: info.features.iter().cloned().collect(),
        max_batch_objects: info.limits.max_batch_objects,
        max_chunk_bytes: info.limits.max_chunk_bytes,
        read_endpoints: info
            .read_endpoints
            .iter()
            .map(|e| format!("{} ({})", e.url, e.region))
            .collect(),
        objects: endpoint.to_string(),
    }
}

impl Render for DoctorOut {
    fn text(&self) -> String {
        let mut s = format!(
            "endpoint:  {}\nstatus:    ok\nprotocol:  {}\nserver:    {} ({})\nfeatures:  {}\nlimits:    batch={} chunk={}B",
            self.endpoint,
            self.protocol_version,
            self.server_version,
            self.role,
            if self.features.is_empty() {
                "-".to_string()
            } else {
                self.features.join(", ")
            },
            self.max_batch_objects,
            self.max_chunk_bytes,
        );
        for e in &self.read_endpoints {
            s.push_str(&format!("\nread via:  {e}"));
        }
        s.push_str(&format!("\nobjects:   {}", self.objects));
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scion_client::{Limits, ProtocolVersion, ReadEndpoint};
    use std::collections::{BTreeSet, HashMap};

    fn rpc(code: Code, reason: Option<ErrorReason>) -> CliError {
        CliError::Client(scion_client::Error::Rpc {
            code,
            reason,
            message: "m".into(),
            metadata: HashMap::new(),
        })
    }

    pub(crate) fn info() -> ServerInfo {
        ServerInfo {
            protocol_version: ProtocolVersion { major: 1, minor: 0 },
            features: BTreeSet::from(["objects.get".to_string()]),
            limits: Limits {
                max_batch_objects: 1000,
                max_chunk_bytes: 1024,
                max_changes_per_changeset: 1,
                max_inline_bytes: 1,
                max_upload_object_bytes: 1,
            },
            read_endpoints: vec![ReadEndpoint {
                url: "http://cache".into(),
                region: "bj".into(),
            }],
            server_version: "0.1.0".into(),
            role: Role::Server,
        }
    }

    #[test]
    fn exit_codes_follow_contract() {
        assert_eq!(rpc(Code::Unauthenticated, None).exit_code(), EXIT_AUTH);
        assert_eq!(rpc(Code::PermissionDenied, None).exit_code(), EXIT_AUTH);
        assert_eq!(
            rpc(Code::Aborted, Some(ErrorReason::MergeConflict)).exit_code(),
            EXIT_CONFLICT
        );
        assert_eq!(rpc(Code::Unavailable, None).exit_code(), EXIT_RETRYABLE);
        assert_eq!(
            CliError::Client(scion_client::Error::Connect {
                endpoint: "e".into(),
                message: "m".into()
            })
            .exit_code(),
            EXIT_RETRYABLE
        );
        assert_eq!(
            rpc(Code::NotFound, Some(ErrorReason::RepoNotFound)).exit_code(),
            EXIT_GENERAL
        );
        assert_eq!(
            CliError::Wc(scion_wc::Error::Conflict(vec!["a".into()])).exit_code(),
            EXIT_CONFLICT
        );
        assert_eq!(CliError::Wc(scion_wc::Error::Dirty).exit_code(), EXIT_GENERAL);
        assert_eq!(
            CliError::Wc(scion_wc::Error::MergeConflicts(vec![])).exit_code(),
            EXIT_CONFLICT
        );
        assert_eq!(CliError::Usage("x".into()).exit_code(), EXIT_USAGE);
    }

    #[test]
    fn wc_client_errors_unwrap_to_client() {
        let e: CliError = scion_wc::Error::Client(scion_client::Error::MissingRepo).into();
        assert!(matches!(e, CliError::Client(scion_client::Error::MissingRepo)));
        let e: CliError = scion_wc::Error::Dirty.into();
        assert!(matches!(e, CliError::Wc(_)));
    }

    #[test]
    fn hints_for_common_failures() {
        let connect = CliError::Client(scion_client::Error::Connect {
            endpoint: "e".into(),
            message: "m".into(),
        });
        assert!(connect.hint().expect("hint").contains("--endpoint"));
        let incompat = CliError::Client(scion_client::Error::IncompatibleProtocol {
            server: "2.0".into(),
            client: "1.0",
        });
        assert!(incompat.hint().is_some());
        assert!(
            rpc(Code::Unauthenticated, None)
                .hint()
                .expect("hint")
                .contains("SCION_TOKEN")
        );
        assert!(
            rpc(Code::Aborted, Some(ErrorReason::NotFastForward))
                .hint()
                .expect("hint")
                .contains("scn sync")
        );
        assert!(rpc(Code::NotFound, Some(ErrorReason::RepoNotFound)).hint().is_some());
        assert!(
            rpc(Code::FailedPrecondition, Some(ErrorReason::PreLandRejected))
                .hint()
                .is_some()
        );
        assert!(
            rpc(Code::AlreadyExists, Some(ErrorReason::RepoDeleted))
                .hint()
                .is_some_and(|h| h.contains("scn repo restore"))
        );
        for e in [
            scion_wc::Error::Dirty,
            scion_wc::Error::NothingToLand,
            scion_wc::Error::NotAWorkingCopy("/x".into()),
            scion_wc::Error::Conflict(vec!["a".into()]),
            scion_wc::Error::NoPendingCommit,
            scion_wc::Error::PendingCommit,
            scion_wc::Error::MergeConflicts(vec![]),
            scion_wc::Error::Unresolved(vec![]),
            scion_wc::Error::MarkersRemain(vec![]),
            scion_wc::Error::NoSyncInProgress,
            scion_wc::Error::NoStash(1),
            scion_wc::Error::Detached("c1".into()),
            scion_wc::Error::NoChangesIn(vec![]),
            scion_wc::Error::PartialFinish,
        ] {
            assert!(CliError::Wc(e).hint().is_some());
        }
        assert!(CliError::Client(scion_client::Error::MissingRepo).hint().is_none());
        assert!(CliError::Usage("u".into()).hint().is_none());
    }

    #[test]
    fn import_errors_map() {
        let e: CliError = scion_gitimport::Error::Client(scion_client::Error::MissingRepo).into();
        assert!(matches!(e, CliError::Client(_)));
        let e: CliError = scion_gitimport::Error::Git("not a git repository".into()).into();
        assert_eq!(e.exit_code(), EXIT_GENERAL);
        assert_eq!(e.to_string(), "not a git repository");
        let id = scion_client::ObjectId::of_blob(b"x");
        let e: CliError = scion_gitimport::Error::Diverged {
            reference: "main".into(),
            current: id,
        }
        .into();
        assert!(e.hint().expect("hint").contains("--ref"));
    }

    #[test]
    fn error_out_fields() {
        let e = error_out(&rpc(Code::NotFound, Some(ErrorReason::ObjectMissing)));
        assert_eq!(e.schema, "scion.cli.error/v1");
        assert_eq!(e.reason, Some("OBJECT_MISSING"));
        assert!(!e.retryable);
        assert_eq!(e.exit_code, EXIT_GENERAL);
        let e = error_out(&CliError::Wc(scion_wc::Error::Dirty));
        assert_eq!(e.reason, None);
        assert!(e.error.contains("uncommitted"));
        assert_eq!(error_out(&CliError::Usage("bad".into())).error, "bad");
        assert!(e.conflicts.is_empty());
        // 冲突详情
        let md = std::collections::HashMap::from([
            ("path.0".to_string(), "f".to_string()),
            ("kind.0".to_string(), "content".to_string()),
        ]);
        let conflict = CliError::Client(scion_client::Error::Rpc {
            code: Code::Aborted,
            reason: Some(ErrorReason::MergeConflict),
            message: "conflict".into(),
            metadata: md,
        });
        let e = error_out(&conflict);
        assert_eq!((e.exit_code, e.reason), (EXIT_CONFLICT, Some("MERGE_CONFLICT")));
        assert_eq!(
            e.conflicts,
            vec![ConflictOut {
                path: "f".into(),
                kind: "content".into()
            }]
        );
        let hint = conflict.hint().expect("hint");
        assert!(
            hint.contains("`scn sync`") && hint.contains("`scn land` again"),
            "{hint}"
        );
        assert!(serde_json::to_string(&e).expect("json").contains("\"conflicts\""));
    }

    #[test]
    fn local_conflicts() {
        let c = |op| CliError::Conflicted {
            op,
            reference: "main".into(),
            conflicts: vec![ConflictOut {
                path: "a".into(),
                kind: "add_add".into(),
            }],
        };
        assert_eq!(
            c(ConflictOp::Merge).to_string(),
            "automatic merge failed; fix conflicts and then commit the result"
        );
        assert!(
            c(ConflictOp::Sync)
                .to_string()
                .starts_with("could not rebase your commit onto main;")
        );
        assert_eq!(
            c(ConflictOp::Stash(1)).to_string(),
            "could not apply stash@{1} cleanly; fix the conflicts in the files"
        );
        assert!(
            error_out(&c(ConflictOp::Stash(1)))
                .hint
                .expect("hint")
                .contains("scn stash drop")
        );
        let pick = |kind| c(ConflictOp::Pick(kind, "c1aaaaaaaaaaaaaaaa".into())).to_string();
        assert!(pick(scion_wc::PickKind::Revert).starts_with("could not revert c1aaaaaaaaaa;"));
        assert!(pick(scion_wc::PickKind::CherryPick).starts_with("could not apply c1aaaaaaaaaa;"));
        let e = error_out(&c(ConflictOp::Sync));
        assert_eq!((e.exit_code, e.conflicts.len()), (EXIT_CONFLICT, 1));
        assert!(e.hint.expect("hint").contains("scn sync --abort"));
        assert!(
            error_out(&c(ConflictOp::Merge))
                .hint
                .expect("hint")
                .contains("scn merge --abort")
        );
        for (kind, git) in [
            ("content", "content"),
            ("add_add", "add/add"),
            ("modify_delete", "modify/delete"),
            ("delete_modify", "modify/delete"),
            ("path", "file/directory"),
            ("binary", "content"),
        ] {
            assert_eq!(git_conflict_kind(kind), git);
        }
    }

    #[test]
    fn doctor_render_text_and_json() {
        let d = doctor_out("http://x", &info());
        assert_eq!(d.role, "server");
        let text = render(&d, false);
        assert!(text.contains("protocol:  1.0"));
        assert!(text.contains("features:  objects.get"));
        assert!(text.contains("read via:  http://cache (bj)"));
        let json: serde_json::Value = serde_json::from_str(&render(&d, true)).expect("json");
        assert_eq!(json["schema"], "scion.cli.doctor/v1");
        assert_eq!(json["max_batch_objects"], 1000);
    }

    #[test]
    fn doctor_text_without_features() {
        let mut i = info();
        i.features.clear();
        i.read_endpoints.clear();
        let text = doctor_out("http://x", &i).text();
        assert!(text.contains("features:  -"));
        assert!(!text.contains("read via"));
    }

    #[test]
    fn role_str_all() {
        assert_eq!(role_str(Role::Cache), "cache");
        assert_eq!(role_str(Role::Daemon), "daemon");
        assert_eq!(role_str(Role::Unknown), "unknown");
    }

    #[test]
    fn version_render() {
        let v = version_out();
        assert_eq!(v.schema, "scion.cli.version/v1");
        assert!(render(&v, false).starts_with("scn "));
    }
}

/// git's subject of a commit message: its first paragraph, lines joined by spaces.
pub fn subject(message: &str) -> String {
    message
        .lines()
        .skip_while(|l| l.trim().is_empty())
        .take_while(|l| !l.trim().is_empty())
        .map(str::trim)
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod subject_tests {
    use super::subject;

    #[test]
    fn first_paragraph_joined() {
        assert_eq!(subject("one line"), "one line");
        assert_eq!(subject("1. a\n2. b\n\nbody\n"), "1. a 2. b");
        assert_eq!(subject("\n\n  title  \n"), "title");
        assert_eq!(subject(""), "");
    }
}
