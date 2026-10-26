//! 两个后端共用的小工具。

use std::time::{SystemTime, UNIX_EPOCH};

use scion_object::ObjectId;
use scion_store::{Error, Result};

/// 当前 Unix 毫秒时间（仅用于审计字段）。
pub(crate) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// 数据库错误统一映射为可重试的后端错误；细节只进日志，不返回给调用方。
pub(crate) fn backend(context: &'static str, e: sqlx::Error) -> Error {
    tracing::warn!(error = %e, context, "metastore error");
    Error::Backend(format!("{context}: {}", short(&e)))
}

/// 错误摘要：不含连接串等可能敏感的细节。
fn short(e: &sqlx::Error) -> String {
    match e {
        sqlx::Error::Database(d) => format!("database error {}", d.code().unwrap_or_default()),
        sqlx::Error::PoolTimedOut => "connection pool timed out".into(),
        sqlx::Error::Io(_) => "i/o error".into(),
        other => other.to_string().split(':').next().unwrap_or("error").to_string(),
    }
}

pub(crate) fn is_unique_violation(e: &sqlx::Error) -> bool {
    e.as_database_error().is_some_and(|d| d.is_unique_violation())
}

pub(crate) fn version_to_db(v: u64) -> Result<i64> {
    i64::try_from(v).map_err(|_| Error::Backend(format!("ref version {v} exceeds i64")))
}

pub(crate) fn version_from_db(v: i64) -> Result<u64> {
    u64::try_from(v).map_err(|_| Error::Backend(format!("corrupt ref version {v}")))
}

pub(crate) fn id_from_db(b: &[u8]) -> Result<ObjectId> {
    ObjectId::from_bytes(b).map_err(|e| Error::Backend(format!("corrupt object id in metastore: {e}")))
}

/// 单条 SQL 中最多绑定的对象个数（SQLite 默认参数上限 32766，PG 65535，留足余量）。
pub(crate) const MAX_IDS_PER_QUERY: usize = 500;

/// FindMissing 的公共部分：输入去重保序，减去已存在集合。
pub(crate) fn subtract_present(ids: &[ObjectId], present: &std::collections::HashSet<Vec<u8>>) -> Vec<ObjectId> {
    let mut seen = std::collections::HashSet::new();
    ids.iter()
        .filter(|id| !present.contains(id.to_bytes().as_slice()) && seen.insert(**id))
        .copied()
        .collect()
}

pub(crate) fn size_to_db(size: u64) -> Result<i64> {
    i64::try_from(size).map_err(|_| Error::Backend(format!("object size {size} exceeds i64")))
}

/// A pack offset or length as stored (4 bytes: packs are at most a few dozen MiB).
pub(crate) fn pack_range_to_db(v: u64) -> Result<i32> {
    i32::try_from(v).map_err(|_| Error::Backend(format!("pack offset or length {v} exceeds 2 GiB")))
}

/// Pack locations joined with pack names, offsets widened for decoding.
pub(crate) const LOCATION_SELECT: &str = "SELECT l.id, p.name, CAST(l.pack_offset AS BIGINT), CAST(l.len AS BIGINT) \
     FROM pack_locations l JOIN packs p ON p.pack_id = l.pack_id WHERE l.id IN (";

/// A pack location row: `(id, pack, pack_offset, len)`.
pub(crate) type LocationRow = (Vec<u8>, Vec<u8>, i64, i64);

pub(crate) fn location_from_db(row: LocationRow) -> Result<(ObjectId, scion_store::PackLocation)> {
    let (id, pack, offset, len) = row;
    let pack: [u8; 32] = pack
        .as_slice()
        .try_into()
        .map_err(|_| Error::Backend(format!("corrupt pack id of {} bytes", pack.len())))?;
    let num = |v: i64| u64::try_from(v).map_err(|_| Error::Backend(format!("corrupt pack range {v}")));
    Ok((
        id_from_db(&id)?,
        scion_store::PackLocation {
            pack,
            offset: num(offset)?,
            len: num(len)?,
        },
    ))
}

/// 把多个 ObjectId 依次拼接存入一列。
pub(crate) fn ids_to_db(ids: &[ObjectId]) -> Vec<u8> {
    ids.iter().flat_map(|id| id.to_bytes()).collect()
}

pub(crate) fn ids_from_db(b: &[u8]) -> Result<Vec<ObjectId>> {
    if b.len() % scion_object::ID_LEN != 0 {
        return Err(Error::Backend(format!("corrupt id list of {} bytes", b.len())));
    }
    b.chunks(scion_object::ID_LEN).map(id_from_db).collect()
}

/// changesets 表的一行：cs_id, root_tree, parents, generation, fp_depth, fp_jump。
pub(crate) type ChangesetRow = (Vec<u8>, Vec<u8>, Vec<u8>, i64, i64, Option<Vec<u8>>);

/// 两个后端共用的列清单，与 [`ChangesetRow`] 顺序一致。
pub(crate) const CHANGESET_COLUMNS: &str = "cs_id, root_tree, parents, generation, fp_depth, fp_jump";

pub(crate) fn record_from_row(row: ChangesetRow) -> Result<scion_store::ChangesetRecord> {
    let (id, root, parents, generation, fp_depth, fp_jump) = row;
    Ok(scion_store::ChangesetRecord {
        id: id_from_db(&id)?,
        root_tree: id_from_db(&root)?,
        parents: ids_from_db(&parents)?,
        generation: version_from_db(generation)?,
        fp_depth: version_from_db(fp_depth)?,
        fp_jump: fp_jump.as_deref().map(id_from_db).transpose()?,
    })
}

/// redactions 表（连同仓库名）的一行，与 [`REDACTION_COLUMNS`] 顺序一致。
pub(crate) type RedactionRow = (
    String,
    Vec<u8>,
    String,
    String,
    String,
    i64,
    bool,
    Option<i64>,
    Option<i64>,
    i64,
);

/// 查询 redaction 时的列（`r` 为 repos，`x` 为 redactions）。
pub(crate) const REDACTION_COLUMNS: &str = "r.name, x.object_id, x.redaction_id, x.reason, x.created_by, \
     x.created_at_ms, x.active, x.purge_after_ms, x.purged_at_ms, x.seq";

pub(crate) fn redaction_from_row(row: RedactionRow) -> Result<scion_store::Redaction> {
    let (repo, id, redaction_id, reason, created_by, created_at_ms, active, purge_after_ms, purged_at_ms, seq) = row;
    Ok(scion_store::Redaction {
        repo,
        id: id_from_db(&id)?,
        redaction_id,
        reason,
        created_by,
        created_at_ms,
        active,
        purge_after_ms,
        purged_at_ms,
        seq: version_from_db(seq)?,
    })
}

/// tokens 表的一行。
pub(crate) type OrgRow = (String, String, i64, String);
pub(crate) type OrgMemberRow = (String, String, String, i64, String);

pub(crate) fn org_from_row((name, visibility, created_at_ms, created_by): OrgRow) -> scion_store::OrgRecord {
    scion_store::OrgRecord {
        name,
        visibility,
        created_at_ms,
        created_by,
    }
}

pub(crate) fn member_from_row((org, subject, role, added_at_ms, added_by): OrgMemberRow) -> scion_store::OrgMember {
    scion_store::OrgMember {
        org,
        subject,
        role,
        added_at_ms,
        added_by,
    }
}

pub(crate) type TokenRow = (String, Vec<u8>, String, String, String, String, i64, Option<i64>);

/// 组名按行保存（组名不能包含换行）。
pub(crate) fn join_groups(groups: &[String]) -> String {
    groups.join("\n")
}

pub(crate) fn token_from_row(row: TokenRow) -> Result<scion_store::TokenRecord> {
    let (id, hash, kind, subject, groups, description, created_at_ms, expires_at_ms) = row;
    let hash: [u8; 32] = hash
        .as_slice()
        .try_into()
        .map_err(|_| Error::Backend(format!("token {id} has a malformed hash")))?;
    Ok(scion_store::TokenRecord {
        id,
        hash,
        kind,
        subject,
        groups: groups
            .split('\n')
            .filter(|g| !g.is_empty())
            .map(str::to_owned)
            .collect(),
        description,
        created_at_ms,
        expires_at_ms,
    })
}

pub(crate) fn redact_url(url: &str) -> String {
    let Some((scheme, rest)) = url.split_once("://") else {
        return url.to_string();
    };
    let Some((userinfo, host)) = rest.split_once('@') else {
        return url.to_string();
    };
    match userinfo.split_once(':') {
        Some((user, _)) => format!("{scheme}://{user}:***@{host}"),
        None => url.to_string(),
    }
}

/// `repos` 表中仓库属性的列（顺序与 [`RepoRow`] 一致）。
pub const REPO_COLUMNS: &str = "name, default_branch, description, created_at_ms, deleted_at_ms, forked_from";

pub type RepoRow = (String, String, String, i64, Option<i64>, Option<String>);

pub fn repo_from_row(r: RepoRow) -> scion_store::RepoInfo {
    let (name, default_branch, description, created_at_ms, deleted_at_ms, forked_from) = r;
    scion_store::RepoInfo {
        name,
        default_branch,
        description,
        created_at_ms,
        deleted_at_ms,
        forked_from,
    }
}

/// 按仓库存数据的表及其主键中 `repo_id` 之后的列：清除已删除的仓库时分批删除。
pub const REPO_TABLES: &[(&str, &str)] = &[
    ("refs", "name"),
    ("ref_log", "name, version"),
    ("tag_annotations", "name"),
    ("landed", "ref, change_id"),
    ("idempotency", "request_id"),
    ("redactions", "object_id"),
    ("derived", "kind, version, key"),
    ("path_changes", "path, generation, cs_id"),
    ("changesets", "cs_id"),
    ("objects_index", "id"),
];

/// fork 时复制的表及其列（不含 `repo_id`），按顺序复制。redaction 另行复制（要分配新的 seq）。
///
/// PostgreSQL 的 READ COMMITTED 下每条 `INSERT … SELECT` 各自取快照，所以 ref 必须**最先**复制：写入顺序是
/// 对象与索引 → 提交记录 → ref，先拿到的 ref 能到达的数据在之后的复制中一定已经存在；之后才落盘的数据只会多复制，
/// 不会缺。不用 REPEATABLE READ：fork 事务里要更新序号计数器，长事务期间只要有合入就会序列化失败。
pub const FORK_TABLES: &[(&str, &str)] = &[
    ("refs", "name, target, version"),
    ("objects_index", "id, size"),
    (
        "changesets",
        "cs_id, root_tree, first_parent, generation, parents, fp_depth, fp_jump",
    ),
    ("derived", "kind, version, key, value"),
    ("path_changes", "path, cs_id, generation, deleted"),
    (
        "tag_annotations",
        "name, target, tagger_name, tagger_email, time_ms, tz_offset_min, message, git_object",
    ),
];

/// 一批删除的行数（每次清除调用最多删除这么多行）。
pub fn purge_limit(batch: usize) -> i64 {
    i64::try_from(batch.max(1)).unwrap_or(i64::MAX)
}

/// 列出仓库时 `limit` 转为 SQL 参数。
pub fn list_limit(limit: usize) -> i64 {
    i64::try_from(limit).unwrap_or(i64::MAX)
}

/// `ref_events` 的列（顺序与 [`RefEventRow`] 一致）。
pub const REF_EVENT_COLUMNS: &str = "seq, repo, ref, old_target, new_target, version, created_at_ms";

pub type RefEventRow = (i64, String, String, Option<Vec<u8>>, Option<Vec<u8>>, i64, i64);

pub fn ref_event_from_row(r: RefEventRow) -> Result<scion_store::RefEvent> {
    let (seq, repo, name, old, new, version, created_at_ms) = r;
    Ok(scion_store::RefEvent {
        seq: version_from_db(seq)?,
        repo,
        name,
        old_target: old.as_deref().map(id_from_db).transpose()?,
        new_target: new.as_deref().map(id_from_db).transpose()?,
        version: version_from_db(version)?,
        created_at_ms,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repo_rows_and_limits() {
        let r = repo_from_row(("a".into(), "main".into(), "d".into(), 5, Some(9), Some("src".into())));
        assert_eq!(
            (r.name.as_str(), r.deleted_at_ms, r.forked_from.as_deref()),
            ("a", Some(9), Some("src"))
        );
        assert_eq!(purge_limit(0), 1);
        assert_eq!(purge_limit(usize::MAX), i64::MAX);
        assert_eq!(list_limit(usize::MAX), i64::MAX);
        assert_eq!(list_limit(3), 3);
        assert!(REPO_TABLES.iter().any(|(t, _)| *t == "objects_index"));
        assert!(FORK_TABLES.iter().all(|(t, _)| REPO_TABLES.iter().any(|(r, _)| r == t)));
    }

    #[test]
    fn now_ms_is_after_2020() {
        assert!(now_ms() > 1_577_836_800_000);
    }

    #[test]
    fn version_conversions() {
        assert_eq!(version_to_db(7), Ok(7));
        assert!(matches!(version_to_db(u64::MAX), Err(Error::Backend(_))));
        assert_eq!(version_from_db(7), Ok(7));
        assert!(matches!(version_from_db(-1), Err(Error::Backend(_))));
    }

    #[test]
    fn id_from_db_roundtrip_and_corrupt() {
        let id = ObjectId::of_blob(b"x");
        assert_eq!(id_from_db(&id.to_bytes()), Ok(id));
        assert!(matches!(id_from_db(b"short"), Err(Error::Backend(_))));
    }

    #[test]
    fn subtract_present_keeps_order_and_dedups() {
        let (a, b, c) = (
            ObjectId::of_blob(b"a"),
            ObjectId::of_blob(b"b"),
            ObjectId::of_blob(b"c"),
        );
        let present = std::collections::HashSet::from([b.to_bytes().to_vec()]);
        assert_eq!(subtract_present(&[c, b, a, c], &present), vec![c, a]);
        assert_eq!(subtract_present(&[], &present), vec![]);
    }

    #[test]
    fn record_from_row_cases() {
        let id = ObjectId::hash(scion_object::ObjectKind::Changeset, b"c");
        let row = |jump: Option<Vec<u8>>, depth: i64| -> ChangesetRow {
            (
                id.to_bytes().to_vec(),
                id.to_bytes().to_vec(),
                ids_to_db(&[id]),
                3,
                depth,
                jump,
            )
        };
        let rec = record_from_row(row(Some(id.to_bytes().to_vec()), 2)).expect("ok");
        assert_eq!((rec.fp_depth, rec.fp_jump, rec.parents.len()), (2, Some(id), 1));
        assert_eq!(record_from_row(row(None, 0)).expect("ok").fp_jump, None);
        assert!(matches!(record_from_row(row(Some(vec![1]), 1)), Err(Error::Backend(_))));
        assert!(matches!(record_from_row(row(None, -1)), Err(Error::Backend(_))));
    }

    #[test]
    fn id_lists_roundtrip() {
        let ids = vec![ObjectId::of_blob(b"a"), ObjectId::of_blob(b"b")];
        assert_eq!(ids_from_db(&ids_to_db(&ids)), Ok(ids));
        assert_eq!(ids_from_db(&[]), Ok(vec![]));
        assert!(matches!(ids_from_db(&[1, 2, 3]), Err(Error::Backend(_))));
    }

    #[test]
    fn size_to_db_bounds() {
        assert_eq!(size_to_db(0), Ok(0));
        assert!(matches!(size_to_db(u64::MAX), Err(Error::Backend(_))));
    }

    #[test]
    fn redact_url_hides_password_only() {
        assert_eq!(redact_url("postgres://u:pw@h:5432/db"), "postgres://u:***@h:5432/db");
        assert_eq!(redact_url("postgres://u@h/db"), "postgres://u@h/db");
        assert_eq!(redact_url("postgres://h/db"), "postgres://h/db");
        assert_eq!(redact_url("sqlite::memory:"), "sqlite::memory:");
    }

    #[test]
    fn backend_error_is_short() {
        let e = backend("ctx", sqlx::Error::PoolTimedOut);
        assert_eq!(e, Error::Backend("ctx: connection pool timed out".into()));
        let e = backend("ctx", sqlx::Error::Io(std::io::Error::other("secret path")));
        assert!(!e.to_string().contains("secret"));
        let e = backend("ctx", sqlx::Error::RowNotFound);
        assert!(matches!(e, Error::Backend(m) if m.starts_with("ctx: ")));
    }

    #[test]
    fn unique_violation_false_for_non_db_errors() {
        assert!(!is_unique_violation(&sqlx::Error::RowNotFound));
    }

    #[test]
    fn token_rows_round_trip_and_reject_bad_hashes() {
        let groups = vec!["a".to_string(), "b".to_string()];
        let row: TokenRow = (
            "t".into(),
            vec![1; 32],
            "pat".into(),
            "alice".into(),
            join_groups(&groups),
            "d".into(),
            5,
            None,
        );
        let t = token_from_row(row.clone()).expect("ok");
        assert_eq!((t.groups, t.hash), (groups, [1; 32]));
        let mut no_groups = row.clone();
        no_groups.4 = String::new();
        assert!(token_from_row(no_groups).expect("ok").groups.is_empty());
        let mut bad = row;
        bad.1 = vec![1; 3];
        assert!(token_from_row(bad).is_err());
    }
}
