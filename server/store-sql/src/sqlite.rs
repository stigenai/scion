//! Lite 档：SQLite（WAL 模式）。

use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;

use async_trait::async_trait;
use scion_object::ObjectId;
use scion_store::{
    ChangesetRecord, Error, IdempotencyRecord, IndexedObject, LandedChange, Lease, MetaStore, OrgMember, OrgRecord,
    PackLocation, PackedObject, PathChange, PathHistoryEntry, Redaction, RedactionFilter, RefEvent, RefExpect,
    RefLogEntry, RefState, RepoFilter, RepoInfo, RepoPatch, Result, TokenRecord, prefix_upper_bound,
    validate_repo_name,
};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions, SqliteSynchronous};

use crate::common::{
    CHANGESET_COLUMNS, ChangesetRow, FORK_TABLES, LOCATION_SELECT, LocationRow, MAX_IDS_PER_QUERY, REDACTION_COLUMNS,
    REF_EVENT_COLUMNS, REPO_COLUMNS, REPO_TABLES, RedactionRow, RefEventRow, RepoRow, backend, id_from_db, ids_to_db,
    is_unique_violation, list_limit, location_from_db, now_ms, pack_range_to_db, purge_limit, record_from_row,
    redaction_from_row, ref_event_from_row, repo_from_row, size_to_db, subtract_present, version_from_db,
    version_to_db,
};

/// 写锁等待上限；SQLite 同一时刻只有一个写者，其余写者在此期间排队。
const BUSY_TIMEOUT: Duration = Duration::from_secs(10);

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations/sqlite");

pub struct SqliteMetaStore {
    pool: SqlitePool,
}

impl SqliteMetaStore {
    /// 打开（不存在则创建）数据库文件并执行迁移。URL 形如 `sqlite:///var/lib/scion/meta.db`。
    pub async fn connect(url: &str) -> Result<Self> {
        let opts = SqliteConnectOptions::from_str(url).map_err(|e| backend("parse sqlite url", e))?;
        Self::connect_with(opts).await
    }

    pub async fn connect_with(opts: SqliteConnectOptions) -> Result<Self> {
        let opts = opts
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            // FULL：每次提交都 fsync，断电也不丢已确认的 land
            .synchronous(SqliteSynchronous::Full)
            .foreign_keys(true)
            .busy_timeout(BUSY_TIMEOUT);
        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .connect_with(opts)
            .await
            .map_err(|e| backend("open sqlite", e))?;
        MIGRATOR
            .run(&pool)
            .await
            .map_err(|e| backend("migrate sqlite", e.into()))?;
        Ok(Self { pool })
    }

    async fn repo_id(&self, repo: &str) -> Result<i64> {
        sqlx::query_scalar::<_, i64>("SELECT repo_id FROM repos WHERE name = ? AND deleted_at_ms IS NULL")
            .bind(repo)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| backend("lookup repo", e))?
            .ok_or_else(|| Error::RepoNotFound(repo.to_owned()))
    }

    async fn current_version(&self, repo_id: i64, name: &str) -> Result<Option<u64>> {
        sqlx::query_scalar::<_, i64>("SELECT version FROM refs WHERE repo_id = ? AND name = ?")
            .bind(repo_id)
            .bind(name)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| backend("read ref version", e))?
            .map(version_from_db)
            .transpose()
    }

    /// 名字已被占用时的错误：正常的仓库或已删除、尚未清除的仓库。
    async fn name_taken(&self, repo: &str) -> Error {
        match sqlx::query_scalar::<_, Option<i64>>("SELECT deleted_at_ms FROM repos WHERE name = ?1")
            .bind(repo)
            .fetch_optional(&self.pool)
            .await
        {
            Ok(Some(Some(_))) => Error::RepoDeleted(repo.to_owned()),
            Ok(_) => Error::RepoAlreadyExists(repo.to_owned()),
            Err(e) => backend("lookup repo", e),
        }
    }

    /// 在事务中追加一条 ref 变化（分配下一个 seq）。
    async fn insert_ref_event(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        repo: &str,
        name: &str,
        old: Option<&[u8]>,
        new: Option<&[u8]>,
        version: u64,
    ) -> Result<()> {
        let seq: i64 = sqlx::query_scalar("UPDATE ref_event_seq SET seq = seq + 1 WHERE id = 1 RETURNING seq")
            .fetch_one(&mut **tx)
            .await
            .map_err(|e| backend("allocate ref event seq", e))?;
        sqlx::query(
            "INSERT INTO ref_events (seq, repo, ref, old_target, new_target, version, created_at_ms) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        )
        .bind(seq)
        .bind(repo)
        .bind(name)
        .bind(old)
        .bind(new)
        .bind(version_to_db(version)?)
        .bind(now_ms())
        .execute(&mut **tx)
        .await
        .map_err(|e| backend("append ref event", e))?;
        Ok(())
    }
}

#[async_trait]
impl MetaStore for SqliteMetaStore {
    async fn create_repo(&self, repo: &str) -> Result<()> {
        self.create_repo_with(&RepoInfo::new(repo, now_ms())).await
    }

    async fn repo_exists(&self, repo: &str) -> Result<bool> {
        match self.repo_id(repo).await {
            Ok(_) => Ok(true),
            Err(Error::RepoNotFound(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    async fn list_repos(&self) -> Result<Vec<String>> {
        // 按 UTF-8 字节序排序，与内存实现一致
        let mut names = sqlx::query_scalar::<_, String>("SELECT name FROM repos WHERE deleted_at_ms IS NULL")
            .fetch_all(&self.pool)
            .await
            .map_err(|e| backend("list repos", e))?;
        names.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        Ok(names)
    }

    async fn get_ref(&self, repo: &str, name: &str) -> Result<Option<RefState>> {
        let repo_id = self.repo_id(repo).await?;
        let row =
            sqlx::query_as::<_, (Vec<u8>, i64)>("SELECT target, version FROM refs WHERE repo_id = ? AND name = ?")
                .bind(repo_id)
                .bind(name)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| backend("get ref", e))?;
        row.map(|(target, version)| {
            Ok(RefState {
                target: id_from_db(&target)?,
                version: version_from_db(version)?,
            })
        })
        .transpose()
    }

    async fn ref_log(&self, repo: &str, name: &str, before: Option<u64>, limit: usize) -> Result<Vec<RefLogEntry>> {
        let repo_id = self.repo_id(repo).await?;
        let rows: Vec<(i64, Vec<u8>, i64)> = sqlx::query_as("SELECT version, target, updated_at_ms FROM ref_log WHERE repo_id = ?1 AND name = ?2 AND version < ?3 ORDER BY version DESC LIMIT ?4")
            .bind(repo_id)
            .bind(name)
            .bind(before.map_or(Ok(i64::MAX), version_to_db)?)
            .bind(i64::try_from(limit).unwrap_or(i64::MAX))
            .fetch_all(&self.pool)
            .await
            .map_err(|e| backend("read ref log", e))?;
        rows.into_iter()
            .map(|(version, target, updated_at_ms)| {
                Ok(RefLogEntry {
                    version: version_from_db(version)?,
                    target: id_from_db(&target)?,
                    updated_at_ms,
                })
            })
            .collect()
    }

    async fn land_commit(
        &self,
        repo: &str,
        name: &str,
        expect: RefExpect,
        target: ObjectId,
        landed: &[LandedChange],
        requests: &[IdempotencyRecord],
    ) -> Result<RefState> {
        let repo_id = self.repo_id(repo).await?;
        let target_bytes = target.to_bytes().to_vec();
        let mut tx = self.pool.begin().await.map_err(|e| backend("begin", e))?;
        // 第一条语句就是写操作，SQLite 直接取得写锁，避免「先读后写」的锁升级冲突
        let (affected, new_version) = match expect {
            RefExpect::Absent => {
                let r = sqlx::query(
                    "INSERT INTO refs (repo_id, name, target, version) VALUES (?, ?, ?, 1) \
                     ON CONFLICT (repo_id, name) DO NOTHING",
                )
                .bind(repo_id)
                .bind(name)
                .bind(&target_bytes)
                .execute(&mut *tx)
                .await
                .map_err(|e| backend("create ref", e))?;
                (r.rows_affected(), 1)
            }
            RefExpect::Version(v) => {
                let next = v
                    .checked_add(1)
                    .ok_or_else(|| Error::Backend("ref version overflow".into()))?;
                let r = sqlx::query(
                    "UPDATE refs SET target = ?, version = version + 1 WHERE repo_id = ? AND name = ? AND version = ?",
                )
                .bind(&target_bytes)
                .bind(repo_id)
                .bind(name)
                .bind(version_to_db(v)?)
                .execute(&mut *tx)
                .await
                .map_err(|e| backend("update ref", e))?;
                (r.rows_affected(), next)
            }
        };
        if affected != 1 {
            tx.rollback().await.map_err(|e| backend("rollback", e))?;
            let current = self.current_version(repo_id, name).await?;
            return Err(Error::RefVersionMismatch {
                name: name.to_owned(),
                current,
            });
        }
        sqlx::query("INSERT INTO ref_log (repo_id, name, version, target, updated_at_ms) VALUES (?, ?, ?, ?, ?)")
            .bind(repo_id)
            .bind(name)
            .bind(version_to_db(new_version)?)
            .bind(&target_bytes)
            .bind(now_ms())
            .execute(&mut *tx)
            .await
            .map_err(|e| backend("append ref log", e))?;

        // 事件：变化前的提交取自审计日志中的上一版本（新建时没有）
        let old: Option<Vec<u8>> = match new_version.checked_sub(1).filter(|v| *v > 0) {
            Some(prev) => {
                sqlx::query_scalar("SELECT target FROM ref_log WHERE repo_id = ?1 AND name = ?2 AND version = ?3")
                    .bind(repo_id)
                    .bind(name)
                    .bind(version_to_db(prev)?)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(|e| backend("read ref log", e))?
            }
            None => None,
        };
        Self::insert_ref_event(&mut tx, repo, name, old.as_deref(), Some(&target_bytes), new_version).await?;
        let now = now_ms();
        for l in landed {
            sqlx::query(
                "INSERT INTO landed (repo_id, ref, change_id, changeset, landed, ref_version, landed_at_ms) \
                 VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (repo_id, ref, change_id) DO NOTHING",
            )
            .bind(repo_id)
            .bind(name)
            .bind(l.change_id.to_vec())
            .bind(l.changeset.to_bytes().to_vec())
            .bind(l.landed.to_bytes().to_vec())
            .bind(version_to_db(l.ref_version)?)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(|e| backend("record landed change", e))?;
        }
        for r in requests {
            sqlx::query(
                "INSERT INTO idempotency (repo_id, request_id, fingerprint, response, created_at_ms) \
                 VALUES (?, ?, ?, ?, ?) ON CONFLICT (repo_id, request_id) DO NOTHING",
            )
            .bind(repo_id)
            .bind(&r.request_id)
            .bind(&r.fingerprint)
            .bind(&r.response)
            .bind(r.created_at_ms)
            .execute(&mut *tx)
            .await
            .map_err(|e| backend("record request result", e))?;
        }
        tx.commit().await.map_err(|e| backend("commit", e))?;
        Ok(RefState {
            target,
            version: new_version,
        })
    }

    async fn landed_changes(&self, repo: &str, name: &str, change_ids: &[[u8; 16]]) -> Result<Vec<LandedChange>> {
        let repo_id = self.repo_id(repo).await?;
        let mut out = Vec::new();
        for chunk in change_ids.chunks(MAX_IDS_PER_QUERY) {
            let mut qb = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
                "SELECT change_id, changeset, landed, ref_version FROM landed WHERE repo_id = ",
            );
            qb.push_bind(repo_id)
                .push(" AND ref = ")
                .push_bind(name)
                .push(" AND change_id IN (");
            let mut sep = qb.separated(", ");
            for c in chunk {
                sep.push_bind(c.to_vec());
            }
            qb.push(")");
            let rows: Vec<(Vec<u8>, Vec<u8>, Vec<u8>, i64)> = qb
                .build_query_as()
                .fetch_all(&self.pool)
                .await
                .map_err(|e| backend("landed changes", e))?;
            for (change_id, changeset, landed, version) in rows {
                out.push(LandedChange {
                    change_id: change_id
                        .as_slice()
                        .try_into()
                        .map_err(|_| Error::Backend(format!("corrupt change id ({} bytes)", change_id.len())))?,
                    changeset: id_from_db(&changeset)?,
                    landed: id_from_db(&landed)?,
                    ref_version: version_from_db(version)?,
                });
            }
        }
        Ok(out)
    }

    async fn get_idempotency(&self, repo: &str, request_id: &str) -> Result<Option<IdempotencyRecord>> {
        let repo_id = self.repo_id(repo).await?;
        let row: Option<(Vec<u8>, Vec<u8>, i64)> = sqlx::query_as(
            "SELECT fingerprint, response, created_at_ms FROM idempotency WHERE repo_id = ? AND request_id = ?",
        )
        .bind(repo_id)
        .bind(request_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| backend("get request result", e))?;
        Ok(row.map(|(fingerprint, response, created_at_ms)| IdempotencyRecord {
            request_id: request_id.to_owned(),
            fingerprint,
            response,
            created_at_ms,
        }))
    }

    async fn acquire_lease(&self, scope: &str, holder: &str, ttl_ms: i64, now_ms: i64) -> Result<Lease> {
        // 条件 upsert：没有、已过期或自己持有时写入；否则不变。随后读出当前租约
        sqlx::query(
            "INSERT INTO leases (scope, holder, token, expires_at_ms) VALUES (?1, ?2, 1, ?3) \
             ON CONFLICT (scope) DO UPDATE SET \
               token = CASE WHEN leases.holder = excluded.holder THEN leases.token ELSE leases.token + 1 END, \
               holder = excluded.holder, expires_at_ms = excluded.expires_at_ms \
             WHERE leases.holder = excluded.holder OR leases.expires_at_ms <= ?4",
        )
        .bind(scope)
        .bind(holder)
        .bind(now_ms.saturating_add(ttl_ms))
        .bind(now_ms)
        .execute(&self.pool)
        .await
        .map_err(|e| backend("acquire lease", e))?;
        let (holder, token, expires_at_ms): (String, i64, i64) =
            sqlx::query_as("SELECT holder, token, expires_at_ms FROM leases WHERE scope = ?")
                .bind(scope)
                .fetch_one(&self.pool)
                .await
                .map_err(|e| backend("read lease", e))?;
        Ok(Lease {
            holder,
            token: version_from_db(token)?,
            expires_at_ms,
        })
    }

    async fn put_token(&self, t: &TokenRecord) -> Result<()> {
        sqlx::query(
            "INSERT INTO tokens (id, hash, kind, subject, groups_text, description, created_at_ms, expires_at_ms) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&t.id)
        .bind(&t.hash[..])
        .bind(&t.kind)
        .bind(&t.subject)
        .bind(crate::common::join_groups(&t.groups))
        .bind(&t.description)
        .bind(t.created_at_ms)
        .bind(t.expires_at_ms)
        .execute(&self.pool)
        .await
        .map_err(|e| backend("put token", e))?;
        Ok(())
    }

    async fn get_token(&self, hash: &[u8; 32]) -> Result<Option<TokenRecord>> {
        let row: Option<crate::common::TokenRow> = sqlx::query_as(
            "SELECT id, hash, kind, subject, groups_text, description, created_at_ms, expires_at_ms \
             FROM tokens WHERE hash = ?",
        )
        .bind(&hash[..])
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| backend("get token", e))?;
        row.map(crate::common::token_from_row).transpose()
    }

    async fn list_tokens(&self, subject: Option<&str>) -> Result<Vec<TokenRecord>> {
        let rows: Vec<crate::common::TokenRow> = sqlx::query_as(
            "SELECT id, hash, kind, subject, groups_text, description, created_at_ms, expires_at_ms \
             FROM tokens WHERE ? IS NULL OR subject = ? ORDER BY created_at_ms, id",
        )
        .bind(subject)
        .bind(subject)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| backend("list tokens", e))?;
        rows.into_iter().map(crate::common::token_from_row).collect()
    }

    async fn delete_token(&self, id: &str) -> Result<bool> {
        let r = sqlx::query("DELETE FROM tokens WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| backend("delete token", e))?;
        Ok(r.rows_affected() > 0)
    }

    async fn create_org(&self, org: &OrgRecord) -> Result<()> {
        let r = sqlx::query(
            "INSERT INTO orgs (name, visibility, created_at_ms, created_by) VALUES (?1, ?2, ?3, ?4) \
             ON CONFLICT (name) DO NOTHING",
        )
        .bind(&org.name)
        .bind(&org.visibility)
        .bind(org.created_at_ms)
        .bind(&org.created_by)
        .execute(&self.pool)
        .await
        .map_err(|e| backend("create org", e))?;
        if r.rows_affected() == 0 {
            return Err(scion_store::Error::OrgAlreadyExists(org.name.clone()));
        }
        Ok(())
    }

    async fn get_org(&self, name: &str) -> Result<Option<OrgRecord>> {
        let row: Option<crate::common::OrgRow> =
            sqlx::query_as("SELECT name, visibility, created_at_ms, created_by FROM orgs WHERE name = ?1")
                .bind(name)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| backend("get org", e))?;
        Ok(row.map(crate::common::org_from_row))
    }

    async fn list_orgs(&self) -> Result<Vec<OrgRecord>> {
        let rows: Vec<crate::common::OrgRow> =
            sqlx::query_as("SELECT name, visibility, created_at_ms, created_by FROM orgs ORDER BY name")
                .fetch_all(&self.pool)
                .await
                .map_err(|e| backend("list orgs", e))?;
        Ok(rows.into_iter().map(crate::common::org_from_row).collect())
    }

    async fn set_org_visibility(&self, name: &str, visibility: &str) -> Result<bool> {
        let r = sqlx::query("UPDATE orgs SET visibility = ?1 WHERE name = ?2")
            .bind(visibility)
            .bind(name)
            .execute(&self.pool)
            .await
            .map_err(|e| backend("set org visibility", e))?;
        Ok(r.rows_affected() > 0)
    }

    async fn delete_org(&self, name: &str) -> Result<bool> {
        // Members go with the organization (ON DELETE CASCADE)
        let r = sqlx::query("DELETE FROM orgs WHERE name = ?1")
            .bind(name)
            .execute(&self.pool)
            .await
            .map_err(|e| backend("delete org", e))?;
        Ok(r.rows_affected() > 0)
    }

    async fn put_org_member(&self, m: &OrgMember) -> Result<()> {
        // A role change keeps when and by whom the member was added
        let r = sqlx::query(
            "INSERT INTO org_members (org, subject, role, added_at_ms, added_by) \
             SELECT ?1, ?2, ?3, ?4, ?5 WHERE EXISTS (SELECT 1 FROM orgs WHERE name = ?1) \
             ON CONFLICT (org, subject) DO UPDATE SET role = excluded.role",
        )
        .bind(&m.org)
        .bind(&m.subject)
        .bind(&m.role)
        .bind(m.added_at_ms)
        .bind(&m.added_by)
        .execute(&self.pool)
        .await
        .map_err(|e| backend("put org member", e))?;
        if r.rows_affected() == 0 {
            return Err(scion_store::Error::OrgNotFound(m.org.clone()));
        }
        Ok(())
    }

    async fn delete_org_member(&self, org: &str, subject: &str) -> Result<bool> {
        let r = sqlx::query("DELETE FROM org_members WHERE org = ?1 AND subject = ?2")
            .bind(org)
            .bind(subject)
            .execute(&self.pool)
            .await
            .map_err(|e| backend("delete org member", e))?;
        Ok(r.rows_affected() > 0)
    }

    async fn list_org_members(&self, org: &str) -> Result<Vec<OrgMember>> {
        let rows: Vec<crate::common::OrgMemberRow> = sqlx::query_as(
            "SELECT org, subject, role, added_at_ms, added_by FROM org_members WHERE org = ?1 ORDER BY subject",
        )
        .bind(org)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| backend("list org members", e))?;
        Ok(rows.into_iter().map(crate::common::member_from_row).collect())
    }

    async fn list_memberships(&self, subject: &str) -> Result<Vec<OrgMember>> {
        let rows: Vec<crate::common::OrgMemberRow> = sqlx::query_as(
            "SELECT org, subject, role, added_at_ms, added_by FROM org_members WHERE subject = ?1 ORDER BY org",
        )
        .bind(subject)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| backend("list memberships", e))?;
        Ok(rows.into_iter().map(crate::common::member_from_row).collect())
    }

    async fn get_setting(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let row: Option<(Vec<u8>,)> = sqlx::query_as("SELECT value FROM settings WHERE key = ?")
            .bind(key)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| backend("get setting", e))?;
        Ok(row.map(|(v,)| v))
    }

    async fn put_setting(&self, key: &str, value: &[u8]) -> Result<()> {
        sqlx::query(
            "INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value",
        )
        .bind(key)
        .bind(value)
        .execute(&self.pool)
        .await
        .map_err(|e| backend("put setting", e))?;
        Ok(())
    }

    async fn put_redactions(&self, entries: &[Redaction]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut repo_ids = std::collections::HashMap::new();
        for e in entries {
            if !repo_ids.contains_key(&e.repo) {
                repo_ids.insert(e.repo.clone(), self.repo_id(&e.repo).await?);
            }
        }
        let n = i64::try_from(entries.len()).map_err(|_| Error::Backend("too many redactions".into()))?;
        let mut tx = self.pool.begin().await.map_err(|e| backend("begin", e))?;
        // 计数器的行锁让写入按 seq 顺序提交
        let last: i64 = sqlx::query_scalar("UPDATE redaction_seq SET seq = seq + ? WHERE id = 1 RETURNING seq")
            .bind(n)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| backend("allocate redaction seq", e))?;
        for (i, e) in entries.iter().enumerate() {
            let seq = last - n + 1 + i as i64;
            sqlx::query(
                "INSERT INTO redactions (repo_id, object_id, redaction_id, reason, created_by, created_at_ms, \
                 active, purge_after_ms, purged_at_ms, seq) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
                 ON CONFLICT (repo_id, object_id) DO UPDATE SET redaction_id = excluded.redaction_id, \
                 reason = excluded.reason, created_by = excluded.created_by, created_at_ms = excluded.created_at_ms, \
                 active = excluded.active, purge_after_ms = excluded.purge_after_ms, \
                 purged_at_ms = excluded.purged_at_ms, seq = excluded.seq",
            )
            .bind(repo_ids[&e.repo])
            .bind(e.id.to_bytes().to_vec())
            .bind(&e.redaction_id)
            .bind(&e.reason)
            .bind(&e.created_by)
            .bind(e.created_at_ms)
            .bind(e.active)
            .bind(e.purge_after_ms)
            .bind(e.purged_at_ms)
            .bind(seq)
            .execute(&mut *tx)
            .await
            .map_err(|e| backend("put redaction", e))?;
        }
        tx.commit().await.map_err(|e| backend("commit", e))
    }

    async fn get_redactions(&self, repo: &str, ids: &[ObjectId]) -> Result<Vec<Redaction>> {
        let repo_id = self.repo_id(repo).await?;
        let mut out = Vec::new();
        let mut unique = ids.to_vec();
        unique.sort_unstable_by_key(|id| id.to_bytes());
        unique.dedup();
        for chunk in unique.chunks(MAX_IDS_PER_QUERY) {
            let mut qb = sqlx::QueryBuilder::<sqlx::Sqlite>::new(format!(
                "SELECT {} FROM redactions x JOIN repos r ON r.repo_id = x.repo_id WHERE x.repo_id = ",
                REDACTION_COLUMNS
            ));
            qb.push_bind(repo_id).push(" AND x.object_id IN (");
            let mut sep = qb.separated(", ");
            for id in chunk {
                sep.push_bind(id.to_bytes().to_vec());
            }
            qb.push(")");
            let rows: Vec<RedactionRow> = qb
                .build_query_as()
                .fetch_all(&self.pool)
                .await
                .map_err(|e| backend("get redactions", e))?;
            for row in rows {
                out.push(redaction_from_row(row)?);
            }
        }
        Ok(out)
    }

    async fn list_redactions(&self, filter: &RedactionFilter) -> Result<Vec<Redaction>> {
        let mut qb = sqlx::QueryBuilder::<sqlx::Sqlite>::new(format!(
            "SELECT {} FROM redactions x JOIN repos r ON r.repo_id = x.repo_id WHERE x.seq > ",
            REDACTION_COLUMNS
        ));
        qb.push_bind(version_to_db(filter.after_seq)?);
        if let Some(repo) = &filter.repo {
            qb.push(" AND r.name = ").push_bind(repo.clone());
        }
        if let Some(rid) = &filter.redaction_id {
            qb.push(" AND x.redaction_id = ").push_bind(rid.clone());
        }
        if let Some(t) = filter.purge_due_at {
            qb.push(" AND x.active = ")
                .push_bind(true)
                .push(" AND x.purged_at_ms IS NULL AND x.purge_after_ms <= ")
                .push_bind(t);
        }
        qb.push(" ORDER BY x.seq");
        if filter.limit > 0 {
            qb.push(" LIMIT ")
                .push_bind(i64::try_from(filter.limit).unwrap_or(i64::MAX));
        }
        let rows: Vec<RedactionRow> = qb
            .build_query_as()
            .fetch_all(&self.pool)
            .await
            .map_err(|e| backend("list redactions", e))?;
        rows.into_iter().map(redaction_from_row).collect()
    }

    async fn object_repos(&self, id: &ObjectId) -> Result<Vec<String>> {
        // 逐个仓库按主键探测：objects_index 没有按 id 的索引（写入热路径上不值得多维护一个）
        sqlx::query_scalar(
            "SELECT r.name FROM repos r WHERE r.deleted_at_ms IS NULL AND EXISTS \
             (SELECT 1 FROM objects_index o WHERE o.repo_id = r.repo_id AND o.id = ?) ORDER BY r.name",
        )
        .bind(id.to_bytes().to_vec())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| backend("object repos", e))
    }

    async fn list_leases(&self) -> Result<Vec<(String, Lease)>> {
        let rows: Vec<(String, String, i64, i64)> =
            sqlx::query_as("SELECT scope, holder, token, expires_at_ms FROM leases ORDER BY scope")
                .fetch_all(&self.pool)
                .await
                .map_err(|e| backend("list leases", e))?;
        rows.into_iter()
            .map(|(scope, holder, token, expires_at_ms)| {
                Ok((
                    scope,
                    Lease {
                        holder,
                        token: version_from_db(token)?,
                        expires_at_ms,
                    },
                ))
            })
            .collect()
    }

    async fn create_repo_with(&self, info: &RepoInfo) -> Result<()> {
        validate_repo_name(&info.name)?;
        let res = sqlx::query(
            "INSERT INTO repos (name, created_at_ms, default_branch, description, forked_from) VALUES (?1, ?2, ?3, ?4, ?5)",
        )
        .bind(&info.name)
        .bind(info.created_at_ms)
        .bind(&info.default_branch)
        .bind(&info.description)
        .bind(&info.forked_from)
        .execute(&self.pool)
        .await;
        match res {
            Ok(_) => Ok(()),
            Err(e) if is_unique_violation(&e) => Err(self.name_taken(&info.name).await),
            Err(e) => Err(backend("create repo", e)),
        }
    }

    async fn get_repo(&self, repo: &str) -> Result<Option<RepoInfo>> {
        let row: Option<RepoRow> = sqlx::query_as(&format!("SELECT {REPO_COLUMNS} FROM repos WHERE name = ?1"))
            .bind(repo)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| backend("get repo", e))?;
        Ok(row.map(repo_from_row))
    }

    async fn list_repo_infos(&self, filter: &RepoFilter) -> Result<Vec<RepoInfo>> {
        let state = if filter.deleted { "IS NOT NULL" } else { "IS NULL" };
        let after = filter.after.clone().unwrap_or_default();
        let rows: Vec<RepoRow> = sqlx::query_as(&format!(
            "SELECT {REPO_COLUMNS} FROM repos WHERE deleted_at_ms {state} \
             AND name >= ?1 AND name < ?2 AND name > ?3 ORDER BY name LIMIT ?4"
        ))
        .bind(&filter.prefix)
        .bind(prefix_upper_bound(&filter.prefix))
        .bind(after)
        .bind(list_limit(filter.limit))
        .fetch_all(&self.pool)
        .await
        .map_err(|e| backend("list repos", e))?;
        Ok(rows.into_iter().map(repo_from_row).collect())
    }

    async fn update_repo(&self, repo: &str, patch: &RepoPatch) -> Result<RepoInfo> {
        let row: Option<RepoRow> = sqlx::query_as(&format!(
            "UPDATE repos SET default_branch = COALESCE(?1, default_branch), description = COALESCE(?2, description) \
             WHERE name = ?3 AND deleted_at_ms IS NULL RETURNING {REPO_COLUMNS}"
        ))
        .bind(&patch.default_branch)
        .bind(&patch.description)
        .bind(repo)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| backend("update repo", e))?;
        row.map(repo_from_row)
            .ok_or_else(|| Error::RepoNotFound(repo.to_owned()))
    }

    async fn delete_repo(&self, repo: &str, now_ms: i64) -> Result<()> {
        let done = sqlx::query("UPDATE repos SET deleted_at_ms = ?1 WHERE name = ?2 AND deleted_at_ms IS NULL")
            .bind(now_ms)
            .bind(repo)
            .execute(&self.pool)
            .await
            .map_err(|e| backend("delete repo", e))?;
        if done.rows_affected() == 0 {
            return Err(Error::RepoNotFound(repo.to_owned()));
        }
        Ok(())
    }

    async fn restore_repo(&self, repo: &str) -> Result<()> {
        let done = sqlx::query(
            "UPDATE repos SET deleted_at_ms = NULL WHERE name = ?1 AND deleted_at_ms IS NOT NULL AND NOT purging",
        )
        .bind(repo)
        .execute(&self.pool)
        .await
        .map_err(|e| backend("restore repo", e))?;
        if done.rows_affected() == 0 {
            let purging: Option<bool> =
                sqlx::query_scalar("SELECT purging FROM repos WHERE name = ?1 AND deleted_at_ms IS NOT NULL")
                    .bind(repo)
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(|e| backend("restore repo", e))?;
            return Err(match purging {
                Some(true) => Error::RepoPurging(repo.to_owned()),
                _ => Error::RepoNotFound(repo.to_owned()),
            });
        }
        Ok(())
    }

    async fn purge_repo(&self, repo: &str, batch: usize) -> Result<bool> {
        // 每一批一个事务。第一句就是写：锁住仓库行、确认仍是已删除，并标记清除已开始（之后恢复会失败）。
        // 恢复与清除因此不会交错：要么恢复先提交、这里找不到已删除的仓库，要么清除先开始、恢复被拒绝。
        let mut tx = self.pool.begin().await.map_err(|e| backend("begin", e))?;
        let id: Option<i64> = sqlx::query_scalar(
            "UPDATE repos SET purging = TRUE WHERE name = ?1 AND deleted_at_ms IS NOT NULL RETURNING repo_id",
        )
        .bind(repo)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| backend("lock deleted repo", e))?;
        let Some(id) = id else {
            tx.rollback().await.map_err(|e| backend("rollback", e))?;
            return Err(Error::RepoNotFound(repo.to_owned()));
        };
        // 每次只删一张表中的一批行，单个事务保持短小
        for (table, key) in REPO_TABLES {
            let done = sqlx::query(&format!(
                "DELETE FROM {table} WHERE repo_id = ?1 AND ({key}) IN \
                 (SELECT {key} FROM {table} WHERE repo_id = ?1 LIMIT ?2)"
            ))
            .bind(id)
            .bind(purge_limit(batch))
            .execute(&mut *tx)
            .await
            .map_err(|e| backend("purge repo", e))?;
            if done.rows_affected() > 0 {
                tx.commit().await.map_err(|e| backend("commit", e))?;
                return Ok(false);
            }
        }
        sqlx::query("DELETE FROM repos WHERE repo_id = ?1 AND deleted_at_ms IS NOT NULL")
            .bind(id)
            .execute(&mut *tx)
            .await
            .map_err(|e| backend("purge repo", e))?;
        tx.commit().await.map_err(|e| backend("commit", e))?;
        Ok(true)
    }

    async fn fork_repo(&self, source: &str, target: &str, now_ms: i64) -> Result<()> {
        validate_repo_name(target)?;
        let mut tx = self.pool.begin().await.map_err(|e| backend("begin", e))?;
        let src: Option<(i64, String, String)> = sqlx::query_as(
            "SELECT repo_id, default_branch, description FROM repos WHERE name = ?1 AND deleted_at_ms IS NULL",
        )
        .bind(source)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| backend("lookup repo", e))?;
        let Some((src_id, default_branch, description)) = src else {
            return Err(Error::RepoNotFound(source.to_owned()));
        };
        let existing: Option<Option<i64>> = sqlx::query_scalar("SELECT deleted_at_ms FROM repos WHERE name = ?1")
            .bind(target)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| backend("lookup repo", e))?;
        match existing {
            Some(None) => return Err(Error::RepoAlreadyExists(target.to_owned())),
            Some(Some(_)) => return Err(Error::RepoDeleted(target.to_owned())),
            None => {}
        }
        let dst_id: i64 = sqlx::query_scalar(
            "INSERT INTO repos (name, created_at_ms, default_branch, description, forked_from) \
             VALUES (?1, ?2, ?3, ?4, ?5) RETURNING repo_id",
        )
        .bind(target)
        .bind(now_ms)
        .bind(&default_branch)
        .bind(&description)
        .bind(source)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| {
            if is_unique_violation(&e) {
                Error::RepoAlreadyExists(target.to_owned())
            } else {
                backend("fork repo", e)
            }
        })?;
        for (table, cols) in FORK_TABLES {
            sqlx::query(&format!(
                "INSERT INTO {table} (repo_id, {cols}) SELECT ?1, {cols} FROM {table} WHERE repo_id = ?2"
            ))
            .bind(dst_id)
            .bind(src_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| backend("fork repo", e))?;
        }
        // 每个复制过来的 ref 一条创建事件
        let refs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM refs WHERE repo_id = ?1")
            .bind(dst_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| backend("fork events", e))?;
        if refs > 0 {
            let last: i64 = sqlx::query_scalar("UPDATE ref_event_seq SET seq = seq + ?1 WHERE id = 1 RETURNING seq")
                .bind(refs)
                .fetch_one(&mut *tx)
                .await
                .map_err(|e| backend("allocate ref event seq", e))?;
            sqlx::query(
                "INSERT INTO ref_events (seq, repo, ref, old_target, new_target, version, created_at_ms) \
                 SELECT ?1 + ROW_NUMBER() OVER (ORDER BY name), ?2, name, NULL, target, version, ?3 \
                 FROM refs WHERE repo_id = ?4",
            )
            .bind(last - refs)
            .bind(target)
            .bind(now_ms)
            .bind(dst_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| backend("fork events", e))?;
        }
        // 有效的 redaction 一并复制，按对象顺序分配新的 seq（缓存层按 seq 增量同步）
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM redactions WHERE repo_id = ?1 AND active")
            .bind(src_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| backend("fork redactions", e))?;
        if n > 0 {
            let last: i64 = sqlx::query_scalar("UPDATE redaction_seq SET seq = seq + ?1 WHERE id = 1 RETURNING seq")
                .bind(n)
                .fetch_one(&mut *tx)
                .await
                .map_err(|e| backend("allocate redaction seq", e))?;
            sqlx::query(
                "INSERT INTO redactions (repo_id, object_id, redaction_id, reason, created_by, created_at_ms, \
                 active, purge_after_ms, purged_at_ms, seq) \
                 SELECT ?1, object_id, redaction_id, reason, created_by, created_at_ms, active, purge_after_ms, \
                 purged_at_ms, ?2 + ROW_NUMBER() OVER (ORDER BY object_id) \
                 FROM redactions WHERE repo_id = ?3 AND active",
            )
            .bind(dst_id)
            .bind(last - n)
            .bind(src_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| backend("fork redactions", e))?;
        }
        tx.commit().await.map_err(|e| backend("commit", e))
    }

    async fn list_ref_events(&self, after_seq: u64, limit: usize, repo: Option<&str>) -> Result<Vec<RefEvent>> {
        let filter = if repo.is_some() { "AND repo = ?3" } else { "" };
        let rows: Vec<RefEventRow> = sqlx::query_as(&format!(
            "SELECT {REF_EVENT_COLUMNS} FROM ref_events WHERE seq > ?1 {filter} ORDER BY seq LIMIT ?2"
        ))
        .bind(version_to_db(after_seq)?)
        .bind(list_limit(limit))
        .bind(repo)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| backend("list ref events", e))?;
        rows.into_iter().map(ref_event_from_row).collect()
    }

    async fn ref_event_bounds(&self) -> Result<(Option<u64>, u64)> {
        let (lo, hi): (Option<i64>, i64) =
            sqlx::query_as("SELECT (SELECT MIN(seq) FROM ref_events), seq FROM ref_event_seq WHERE id = 1")
                .fetch_one(&self.pool)
                .await
                .map_err(|e| backend("ref event bounds", e))?;
        Ok((lo.map(version_from_db).transpose()?, version_from_db(hi)?))
    }

    async fn prune_ref_events(&self, before_ms: i64) -> Result<u64> {
        let done = sqlx::query("DELETE FROM ref_events WHERE created_at_ms < ?1")
            .bind(before_ms)
            .execute(&self.pool)
            .await
            .map_err(|e| backend("prune ref events", e))?;
        Ok(done.rows_affected())
    }

    async fn delete_ref(&self, repo: &str, name: &str, expect: Option<u64>) -> Result<()> {
        let repo_id = self.repo_id(repo).await?;
        let mut tx = self.pool.begin().await.map_err(|e| backend("begin", e))?;
        let deleted: Option<(Vec<u8>, i64)> = match expect {
            Some(v) => sqlx::query_as(
                "DELETE FROM refs WHERE repo_id = ?1 AND name = ?2 AND version = ?3 RETURNING target, version",
            )
            .bind(repo_id)
            .bind(name)
            .bind(version_to_db(v)?),
            None => sqlx::query_as("DELETE FROM refs WHERE repo_id = ?1 AND name = ?2 RETURNING target, version")
                .bind(repo_id)
                .bind(name),
        }
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| backend("delete ref", e))?;
        let Some((old_target, old_version)) = deleted else {
            tx.rollback().await.map_err(|e| backend("rollback", e))?;
            return Err(Error::RefVersionMismatch {
                name: name.to_owned(),
                current: self.current_version(repo_id, name).await?,
            });
        };
        Self::insert_ref_event(
            &mut tx,
            repo,
            name,
            Some(&old_target),
            None,
            version_from_db(old_version)?,
        )
        .await?;
        // 重新创建的 ref 版本从 1 开始，旧的审计记录会与之冲突：随 ref 一起删除
        sqlx::query("DELETE FROM ref_log WHERE repo_id = ?1 AND name = ?2")
            .bind(repo_id)
            .bind(name)
            .execute(&mut *tx)
            .await
            .map_err(|e| backend("delete ref log", e))?;
        tx.commit().await.map_err(|e| backend("commit", e))
    }

    async fn release_lease(&self, scope: &str, holder: &str) -> Result<()> {
        sqlx::query("UPDATE leases SET expires_at_ms = 0 WHERE scope = ? AND holder = ?")
            .bind(scope)
            .bind(holder)
            .execute(&self.pool)
            .await
            .map_err(|e| backend("release lease", e))?;
        Ok(())
    }

    async fn expire_idempotency(&self, before_ms: i64) -> Result<u64> {
        let r = sqlx::query("DELETE FROM idempotency WHERE created_at_ms < ?")
            .bind(before_ms)
            .execute(&self.pool)
            .await
            .map_err(|e| backend("expire request results", e))?;
        Ok(r.rows_affected())
    }

    async fn put_pack_locations(&self, pack: &[u8; 32], objects: &[PackedObject]) -> Result<()> {
        if objects.is_empty() {
            return Ok(());
        }
        let mut tx = self.pool.begin().await.map_err(|e| backend("begin", e))?;
        let pack_id: i32 = sqlx::query_scalar(
            "INSERT INTO packs (name) VALUES (?) ON CONFLICT (name) DO UPDATE SET name = excluded.name \
             RETURNING pack_id",
        )
        .bind(pack.to_vec())
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| backend("record pack", e))?;
        for chunk in objects.chunks(MAX_IDS_PER_QUERY) {
            let rows = chunk
                .iter()
                .map(|o| {
                    let base = o.base.map(|b| b.to_bytes().to_vec());
                    Ok((
                        o.id.to_bytes().to_vec(),
                        pack_range_to_db(o.offset)?,
                        pack_range_to_db(o.len)?,
                        base,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            let mut qb = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
                "INSERT INTO pack_locations (id, pack_id, pack_offset, len, base) ",
            );
            qb.push_values(rows, |mut b, (id, offset, len, base)| {
                b.push_bind(id)
                    .push_bind(pack_id)
                    .push_bind(offset)
                    .push_bind(len)
                    .push_bind(base);
            });
            qb.push(
                " ON CONFLICT (id) DO UPDATE SET pack_id = excluded.pack_id, pack_offset = excluded.pack_offset, \
                 len = excluded.len, base = excluded.base",
            );
            qb.build()
                .execute(&mut *tx)
                .await
                .map_err(|e| backend("record pack locations", e))?;
        }
        tx.commit().await.map_err(|e| backend("commit", e))?;
        Ok(())
    }

    async fn pack_locations(&self, ids: &[ObjectId]) -> Result<HashMap<ObjectId, PackLocation>> {
        let mut out = HashMap::new();
        for chunk in ids.chunks(MAX_IDS_PER_QUERY) {
            let mut qb = sqlx::QueryBuilder::<sqlx::Sqlite>::new(LOCATION_SELECT);
            let mut sep = qb.separated(", ");
            for id in chunk {
                sep.push_bind(id.to_bytes().to_vec());
            }
            qb.push(")");
            let rows: Vec<LocationRow> = qb
                .build_query_as()
                .fetch_all(&self.pool)
                .await
                .map_err(|e| backend("pack locations", e))?;
            for row in rows {
                let (id, loc) = location_from_db(row)?;
                out.insert(id, loc);
            }
        }
        Ok(out)
    }

    async fn delta_dependents(&self, base: &ObjectId) -> Result<Vec<ObjectId>> {
        // No index on base: only a redaction purge asks, and rarely
        let rows: Vec<Vec<u8>> = sqlx::query_scalar("SELECT id FROM pack_locations WHERE base = ? ORDER BY id")
            .bind(base.to_bytes().to_vec())
            .fetch_all(&self.pool)
            .await
            .map_err(|e| backend("delta dependents", e))?;
        rows.iter().map(|r| id_from_db(r)).collect()
    }

    async fn delete_packs(&self, packs: &[[u8; 32]]) -> Result<()> {
        for chunk in packs.chunks(MAX_IDS_PER_QUERY) {
            let mut qb = sqlx::QueryBuilder::<sqlx::Sqlite>::new("DELETE FROM packs WHERE name IN (");
            let mut sep = qb.separated(", ");
            for name in chunk {
                sep.push_bind(name.to_vec());
            }
            qb.push(")");
            qb.build()
                .execute(&self.pool)
                .await
                .map_err(|e| backend("forget packs", e))?;
        }
        Ok(())
    }

    async fn compact_after_repack(&self) -> Result<()> {
        // SQLite reuses freed pages for new rows; shrinking the file would need a full VACUUM
        Ok(())
    }

    async fn delete_pack_locations(&self, ids: &[ObjectId]) -> Result<()> {
        for chunk in ids.chunks(MAX_IDS_PER_QUERY) {
            let mut qb = sqlx::QueryBuilder::<sqlx::Sqlite>::new("DELETE FROM pack_locations WHERE id IN (");
            let mut sep = qb.separated(", ");
            for id in chunk {
                sep.push_bind(id.to_bytes().to_vec());
            }
            qb.push(")");
            qb.build()
                .execute(&self.pool)
                .await
                .map_err(|e| backend("forget pack locations", e))?;
        }
        Ok(())
    }

    async fn index_objects(&self, repo: &str, objects: &[IndexedObject]) -> Result<()> {
        let repo_id = self.repo_id(repo).await?;
        if objects.is_empty() {
            return Ok(());
        }
        let mut tx = self.pool.begin().await.map_err(|e| backend("begin", e))?;
        for chunk in objects.chunks(MAX_IDS_PER_QUERY) {
            let rows = chunk
                .iter()
                .map(|o| Ok((o.id.to_bytes().to_vec(), size_to_db(o.size)?)))
                .collect::<Result<Vec<_>>>()?;
            let mut qb = sqlx::QueryBuilder::<sqlx::Sqlite>::new("INSERT INTO objects_index (repo_id, id, size) ");
            qb.push_values(rows, |mut b, (id, size)| {
                b.push_bind(repo_id).push_bind(id).push_bind(size);
            });
            qb.push(" ON CONFLICT (repo_id, id) DO NOTHING");
            qb.build()
                .execute(&mut *tx)
                .await
                .map_err(|e| backend("index objects", e))?;
        }
        tx.commit().await.map_err(|e| backend("commit", e))?;
        Ok(())
    }

    async fn missing_objects(&self, repo: &str, ids: &[ObjectId]) -> Result<Vec<ObjectId>> {
        let repo_id = self.repo_id(repo).await?;
        let mut present = std::collections::HashSet::new();
        for chunk in ids.chunks(MAX_IDS_PER_QUERY) {
            let mut qb = sqlx::QueryBuilder::<sqlx::Sqlite>::new("SELECT id FROM objects_index WHERE repo_id = ");
            qb.push_bind(repo_id).push(" AND id IN (");
            let mut sep = qb.separated(", ");
            for id in chunk {
                sep.push_bind(id.to_bytes().to_vec());
            }
            qb.push(")");
            let found: Vec<Vec<u8>> = qb
                .build_query_scalar()
                .fetch_all(&self.pool)
                .await
                .map_err(|e| backend("find missing", e))?;
            present.extend(found);
        }
        Ok(subtract_present(ids, &present))
    }

    async fn object_sizes(&self, repo: &str, ids: &[ObjectId]) -> Result<std::collections::HashMap<ObjectId, u64>> {
        let repo_id = self.repo_id(repo).await?;
        let mut out = std::collections::HashMap::new();
        for chunk in ids.chunks(MAX_IDS_PER_QUERY) {
            let mut qb = sqlx::QueryBuilder::<sqlx::Sqlite>::new("SELECT id, size FROM objects_index WHERE repo_id = ");
            qb.push_bind(repo_id).push(" AND id IN (");
            let mut sep = qb.separated(", ");
            for id in chunk {
                sep.push_bind(id.to_bytes().to_vec());
            }
            qb.push(")");
            let rows: Vec<(Vec<u8>, i64)> = qb
                .build_query_as()
                .fetch_all(&self.pool)
                .await
                .map_err(|e| backend("object sizes", e))?;
            for (id, size) in rows {
                out.insert(id_from_db(&id)?, version_from_db(size)?);
            }
        }
        Ok(out)
    }

    async fn put_changeset(&self, repo: &str, rec: &ChangesetRecord) -> Result<()> {
        let repo_id = self.repo_id(repo).await?;
        sqlx::query(
            "INSERT INTO changesets (repo_id, cs_id, root_tree, first_parent, generation, parents, fp_depth, fp_jump) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (repo_id, cs_id) DO NOTHING",
        )
        .bind(repo_id)
        .bind(rec.id.to_bytes().to_vec())
        .bind(rec.root_tree.to_bytes().to_vec())
        .bind(rec.first_parent().map(|p| p.to_bytes().to_vec()))
        .bind(version_to_db(rec.generation)?)
        .bind(ids_to_db(&rec.parents))
        .bind(version_to_db(rec.fp_depth)?)
        .bind(rec.fp_jump.map(|j| j.to_bytes().to_vec()))
        .execute(&self.pool)
        .await
        .map_err(|e| backend("put changeset", e))?;
        Ok(())
    }

    async fn put_changesets(&self, repo: &str, recs: &[ChangesetRecord]) -> Result<()> {
        let repo_id = self.repo_id(repo).await?;
        if recs.is_empty() {
            return Ok(());
        }
        let mut tx = self.pool.begin().await.map_err(|e| backend("begin", e))?;
        // 每行 8 个参数
        for chunk in recs.chunks(MAX_IDS_PER_QUERY / 8) {
            let mut rows = Vec::with_capacity(chunk.len());
            for r in chunk {
                rows.push((
                    r.id.to_bytes().to_vec(),
                    r.root_tree.to_bytes().to_vec(),
                    r.first_parent().map(|p| p.to_bytes().to_vec()),
                    version_to_db(r.generation)?,
                    ids_to_db(&r.parents),
                    version_to_db(r.fp_depth)?,
                    r.fp_jump.map(|j| j.to_bytes().to_vec()),
                ));
            }
            let mut qb = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
                "INSERT INTO changesets (repo_id, cs_id, root_tree, first_parent, generation, parents, fp_depth, fp_jump) ",
            );
            qb.push_values(rows, |mut b, (id, root, first, generation, parents, depth, jump)| {
                b.push_bind(repo_id)
                    .push_bind(id)
                    .push_bind(root)
                    .push_bind(first)
                    .push_bind(generation)
                    .push_bind(parents)
                    .push_bind(depth)
                    .push_bind(jump);
            });
            qb.push(" ON CONFLICT (repo_id, cs_id) DO NOTHING");
            qb.build()
                .execute(&mut *tx)
                .await
                .map_err(|e| backend("put changesets", e))?;
        }
        tx.commit().await.map_err(|e| backend("commit", e))?;
        Ok(())
    }

    async fn get_changeset(&self, repo: &str, id: &ObjectId) -> Result<Option<ChangesetRecord>> {
        let repo_id = self.repo_id(repo).await?;
        let sql = format!("SELECT {CHANGESET_COLUMNS} FROM changesets WHERE repo_id = ? AND cs_id = ?");
        sqlx::query_as::<_, ChangesetRow>(&sql)
            .bind(repo_id)
            .bind(id.to_bytes().to_vec())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| backend("get changeset", e))?
            .map(record_from_row)
            .transpose()
    }

    async fn changesets_below(&self, repo: &str, max_generation: u64, limit: usize) -> Result<Vec<ChangesetRecord>> {
        let repo_id = self.repo_id(repo).await?;
        let sql = format!(
            "SELECT {CHANGESET_COLUMNS} FROM changesets WHERE repo_id = ? AND generation <= ? \
             ORDER BY generation DESC LIMIT ?"
        );
        sqlx::query_as::<_, ChangesetRow>(&sql)
            .bind(repo_id)
            .bind(version_to_db(max_generation.min(i64::MAX as u64))?)
            .bind(i64::try_from(limit).unwrap_or(i64::MAX))
            .fetch_all(&self.pool)
            .await
            .map_err(|e| backend("changesets below", e))?
            .into_iter()
            .map(record_from_row)
            .collect()
    }

    async fn repair_lineage(
        &self,
        repo: &str,
        id: &ObjectId,
        parents: &[ObjectId],
        fp_depth: u64,
        fp_jump: Option<ObjectId>,
    ) -> Result<()> {
        let repo_id = self.repo_id(repo).await?;
        sqlx::query(
            "UPDATE changesets SET parents = ?, first_parent = ?, fp_depth = ?, fp_jump = ? \
             WHERE repo_id = ? AND cs_id = ?",
        )
        .bind(ids_to_db(parents))
        .bind(parents.first().map(|p| p.to_bytes().to_vec()))
        .bind(version_to_db(fp_depth)?)
        .bind(fp_jump.map(|j| j.to_bytes().to_vec()))
        .bind(repo_id)
        .bind(id.to_bytes().to_vec())
        .execute(&self.pool)
        .await
        .map_err(|e| backend("repair lineage", e))?;
        Ok(())
    }

    async fn index_paths(&self, repo: &str, cs: ObjectId, generation: u64, changes: &[PathChange]) -> Result<()> {
        let repo_id = self.repo_id(repo).await?;
        if changes.is_empty() {
            return Ok(());
        }
        let generation = version_to_db(generation)?;
        let cs_bytes = cs.to_bytes().to_vec();
        let mut tx = self.pool.begin().await.map_err(|e| backend("begin", e))?;
        for chunk in changes.chunks(MAX_IDS_PER_QUERY) {
            let mut qb = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
                "INSERT INTO path_changes (repo_id, path, cs_id, generation, deleted) ",
            );
            qb.push_values(chunk, |mut b, c| {
                b.push_bind(repo_id)
                    .push_bind(c.path.clone())
                    .push_bind(cs_bytes.clone())
                    .push_bind(generation)
                    .push_bind(c.deleted);
            });
            qb.push(" ON CONFLICT (repo_id, path, generation, cs_id) DO NOTHING");
            qb.build()
                .execute(&mut *tx)
                .await
                .map_err(|e| backend("index paths", e))?;
        }
        tx.commit().await.map_err(|e| backend("commit", e))?;
        Ok(())
    }

    async fn index_paths_many(&self, repo: &str, entries: &[(ObjectId, u64, Vec<PathChange>)]) -> Result<()> {
        let repo_id = self.repo_id(repo).await?;
        let mut rows = Vec::new();
        for (cs, generation, changes) in entries {
            let generation = version_to_db(*generation)?;
            let cs = cs.to_bytes().to_vec();
            rows.extend(
                changes
                    .iter()
                    .map(|c| (c.path.clone(), cs.clone(), generation, c.deleted)),
            );
        }
        if rows.is_empty() {
            return Ok(());
        }
        let mut tx = self.pool.begin().await.map_err(|e| backend("begin", e))?;
        // 每行 5 个参数
        for chunk in rows.chunks(MAX_IDS_PER_QUERY / 5) {
            let mut qb = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
                "INSERT INTO path_changes (repo_id, path, cs_id, generation, deleted) ",
            );
            qb.push_values(chunk, |mut b, (path, cs, generation, deleted)| {
                b.push_bind(repo_id)
                    .push_bind(path.clone())
                    .push_bind(cs.clone())
                    .push_bind(*generation)
                    .push_bind(*deleted);
            });
            qb.push(" ON CONFLICT (repo_id, path, generation, cs_id) DO NOTHING");
            qb.build()
                .execute(&mut *tx)
                .await
                .map_err(|e| backend("index paths", e))?;
        }
        tx.commit().await.map_err(|e| backend("commit", e))?;
        Ok(())
    }

    async fn path_history(
        &self,
        repo: &str,
        path: &str,
        max_generation: u64,
        limit: usize,
    ) -> Result<Vec<PathHistoryEntry>> {
        let repo_id = self.repo_id(repo).await?;
        // 目录下的路径：区间 [path + "/", path + "0")，'0' 是 '/' 的下一个字符
        let rows = sqlx::query_as::<_, (Vec<u8>, i64, String, bool)>(
            "SELECT cs_id, generation, path, deleted FROM path_changes \
             WHERE repo_id = ? AND (path = ? OR (path >= ? AND path < ?)) AND generation <= ? \
             ORDER BY generation DESC, path LIMIT ?",
        )
        .bind(repo_id)
        .bind(path)
        .bind(format!("{path}/"))
        .bind(format!("{path}0"))
        .bind(version_to_db(max_generation.min(i64::MAX as u64))?)
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .fetch_all(&self.pool)
        .await
        .map_err(|e| backend("path history", e))?;
        rows.into_iter()
            .map(|(cs, generation, path, deleted)| {
                Ok(PathHistoryEntry {
                    cs: id_from_db(&cs)?,
                    generation: version_from_db(generation)?,
                    path,
                    deleted,
                })
            })
            .collect()
    }

    async fn get_derived(&self, repo: &str, kind: &str, version: u32, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let repo_id = self.repo_id(repo).await?;
        sqlx::query_scalar::<_, Vec<u8>>(
            "SELECT value FROM derived WHERE repo_id = ? AND kind = ? AND version = ? AND key = ?",
        )
        .bind(repo_id)
        .bind(kind)
        .bind(i64::from(version))
        .bind(key)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| backend("get derived", e))
    }

    async fn get_derived_many(
        &self,
        repo: &str,
        kind: &str,
        version: u32,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>> {
        let repo_id = self.repo_id(repo).await?;
        let mut found: HashMap<Vec<u8>, Vec<u8>> = HashMap::with_capacity(keys.len());
        for chunk in keys.chunks(MAX_IDS_PER_QUERY) {
            let mut qb = sqlx::QueryBuilder::<sqlx::Sqlite>::new("SELECT key, value FROM derived WHERE repo_id = ");
            qb.push_bind(repo_id)
                .push(" AND kind = ")
                .push_bind(kind)
                .push(" AND version = ")
                .push_bind(i64::from(version))
                .push(" AND key IN (");
            let mut sep = qb.separated(", ");
            for key in chunk {
                sep.push_bind(key.as_slice());
            }
            qb.push(")");
            let rows: Vec<(Vec<u8>, Vec<u8>)> = qb
                .build_query_as()
                .fetch_all(&self.pool)
                .await
                .map_err(|e| backend("get derived", e))?;
            found.extend(rows);
        }
        Ok(keys.iter().map(|k| found.get(k).cloned()).collect())
    }

    async fn put_derived(&self, repo: &str, kind: &str, version: u32, key: &[u8], value: &[u8]) -> Result<()> {
        let repo_id = self.repo_id(repo).await?;
        sqlx::query(
            "INSERT INTO derived (repo_id, kind, version, key, value) VALUES (?, ?, ?, ?, ?) \
             ON CONFLICT (repo_id, kind, version, key) DO NOTHING",
        )
        .bind(repo_id)
        .bind(kind)
        .bind(i64::from(version))
        .bind(key)
        .bind(value)
        .execute(&self.pool)
        .await
        .map_err(|e| backend("put derived", e))?;
        Ok(())
    }

    async fn list_refs(&self, repo: &str) -> Result<Vec<(String, RefState)>> {
        let repo_id = self.repo_id(repo).await?;
        let rows =
            sqlx::query_as::<_, (String, Vec<u8>, i64)>("SELECT name, target, version FROM refs WHERE repo_id = ?")
                .bind(repo_id)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| backend("list refs", e))?;
        let mut refs = rows
            .into_iter()
            .map(|(name, target, version)| {
                Ok((
                    name,
                    RefState {
                        target: id_from_db(&target)?,
                        version: version_from_db(version)?,
                    },
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        // 按字节序排序，不受数据库 collation 影响
        refs.sort_unstable_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
        Ok(refs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use scion_store::testsuite;
    use std::sync::Arc;

    async fn fresh() -> (tempfile::TempDir, SqliteMetaStore) {
        let dir = tempfile::tempdir().expect("tempdir");
        let url = format!("sqlite://{}", dir.path().join("meta.db").display());
        let store = SqliteMetaStore::connect(&url).await.expect("open");
        (dir, store)
    }

    #[tokio::test]
    async fn conformance_meta() {
        let (_dir, s) = fresh().await;
        testsuite::meta_store(&s).await;
    }

    #[tokio::test]
    async fn conformance_object_index() {
        let (_dir, s) = fresh().await;
        testsuite::object_index(&s).await;
    }

    #[tokio::test]
    async fn index_objects_large_batch_spans_chunks() {
        let (_dir, s) = fresh().await;
        s.create_repo("r").await.expect("create");
        let objs: Vec<_> = (0..1_234u32)
            .map(|i| IndexedObject {
                id: ObjectId::of_blob(&i.to_le_bytes()),
                size: u64::from(i),
            })
            .collect();
        s.index_objects("r", &objs).await.expect("index");
        let mut ids: Vec<_> = objs.iter().map(|o| o.id).collect();
        let extra = ObjectId::of_blob(b"extra");
        ids.push(extra);
        assert_eq!(s.missing_objects("r", &ids).await, Ok(vec![extra]));
    }

    #[tokio::test]
    async fn index_objects_size_overflow_rejected() {
        let (_dir, s) = fresh().await;
        s.create_repo("r").await.expect("create");
        let o = IndexedObject {
            id: ObjectId::of_blob(b"x"),
            size: u64::MAX,
        };
        assert!(matches!(s.index_objects("r", &[o]).await, Err(Error::Backend(_))));
    }

    #[tokio::test]
    async fn conformance_changesets_and_refs() {
        let (_dir, s) = fresh().await;
        testsuite::changesets_and_refs(&s).await;
    }

    #[tokio::test]
    async fn conformance_pack_locations() {
        let (_dir, s) = fresh().await;
        testsuite::pack_locations(&s).await;
    }

    #[tokio::test]
    async fn conformance_derived() {
        let (_dir, s) = fresh().await;
        testsuite::derived(&s).await;
    }

    #[tokio::test]
    async fn conformance_landing() {
        let (_dir, s) = fresh().await;
        testsuite::landing(&s).await;
    }

    #[tokio::test]
    async fn conformance_leases() {
        let (_dir, s) = fresh().await;
        testsuite::leases(&s).await;
        testsuite::tokens_and_settings(&s).await;
        testsuite::orgs_and_members(&s).await;
        testsuite::redactions(&s).await;
        testsuite::repo_admin(&s).await;
        testsuite::ref_events(&s).await;
        testsuite::ref_log(&s).await;
    }

    #[tokio::test]
    async fn conformance_batch_writes() {
        let (_dir, s) = fresh().await;
        testsuite::batch_writes(&s).await;
    }

    #[tokio::test]
    async fn conformance_history_index() {
        let (_dir, s) = fresh().await;
        testsuite::history_index(&s).await;
    }

    #[tokio::test]
    async fn conformance_fp_skip_chain() {
        let (_dir, s) = fresh().await;
        testsuite::fp_skip_chain(&s).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn conformance_concurrent_cas() {
        let (_dir, s) = fresh().await;
        testsuite::concurrent_cas(Arc::new(s)).await;
    }

    #[tokio::test]
    async fn data_survives_reopen() {
        let dir = tempfile::tempdir().expect("tempdir");
        let url = format!("sqlite://{}", dir.path().join("meta.db").display());
        let id = ObjectId::hash(scion_object::ObjectKind::Changeset, b"x");
        {
            let s = SqliteMetaStore::connect(&url).await.expect("open");
            s.create_repo("r").await.expect("create");
            s.cas_ref("r", "main", RefExpect::Absent, id).await.expect("cas");
        }
        let s = SqliteMetaStore::connect(&url)
            .await
            .expect("reopen runs migrations idempotently");
        assert_eq!(
            s.get_ref("r", "main").await,
            Ok(Some(RefState { target: id, version: 1 }))
        );
    }

    #[tokio::test]
    async fn cas_appends_ref_log() {
        let (_dir, s) = fresh().await;
        s.create_repo("r").await.expect("create");
        let a = ObjectId::hash(scion_object::ObjectKind::Changeset, b"a");
        let b = ObjectId::hash(scion_object::ObjectKind::Changeset, b"b");
        s.cas_ref("r", "main", RefExpect::Absent, a).await.expect("v1");
        s.cas_ref("r", "main", RefExpect::Version(1), b).await.expect("v2");
        let _ = s
            .cas_ref("r", "main", RefExpect::Version(1), a)
            .await
            .expect_err("stale");
        let versions: Vec<i64> = sqlx::query_scalar("SELECT version FROM ref_log ORDER BY version")
            .fetch_all(&s.pool)
            .await
            .expect("query");
        assert_eq!(versions, [1, 2], "failed CAS must not be logged");
    }

    #[tokio::test]
    async fn cas_version_overflow_rejected() {
        let (_dir, s) = fresh().await;
        s.create_repo("r").await.expect("create");
        let a = ObjectId::hash(scion_object::ObjectKind::Changeset, b"a");
        assert!(matches!(
            s.cas_ref("r", "main", RefExpect::Version(u64::MAX), a).await,
            Err(Error::Backend(_))
        ));
    }

    #[tokio::test]
    async fn corrupt_rows_reported_as_backend_errors() {
        let (_dir, s) = fresh().await;
        s.create_repo("r").await.expect("create");
        sqlx::query("INSERT INTO refs (repo_id, name, target, version) VALUES (1, 'bad', x'00', 1)")
            .execute(&s.pool)
            .await
            .expect("insert");
        sqlx::query("INSERT INTO refs (repo_id, name, target, version) VALUES (1, 'neg', ?, -1)")
            .bind(ObjectId::of_blob(b"").to_bytes().to_vec())
            .execute(&s.pool)
            .await
            .expect("insert");
        assert!(matches!(s.get_ref("r", "bad").await, Err(Error::Backend(_))));
        assert!(matches!(s.get_ref("r", "neg").await, Err(Error::Backend(_))));
    }

    #[tokio::test]
    async fn closed_pool_reports_backend_errors() {
        let (_dir, s) = fresh().await;
        s.create_repo("r").await.expect("create");
        s.pool.close().await;
        let id = ObjectId::hash(scion_object::ObjectKind::Changeset, b"a");
        assert!(matches!(s.create_repo("x").await, Err(Error::Backend(_))));
        assert!(matches!(s.repo_exists("r").await, Err(Error::Backend(_))));
        assert!(matches!(s.list_repos().await, Err(Error::Backend(_))));
        assert!(matches!(
            s.cas_ref("r", "m", RefExpect::Absent, id).await,
            Err(Error::Backend(_))
        ));
        let backend = |r: Result<()>| matches!(r, Err(Error::Backend(_)));
        assert!(backend(s.create_repo_with(&RepoInfo::new("y", 1)).await));
        assert!(matches!(s.get_repo("r").await, Err(Error::Backend(_))));
        assert!(matches!(
            s.list_repo_infos(&RepoFilter::default()).await,
            Err(Error::Backend(_))
        ));
        assert!(matches!(
            s.update_repo("r", &RepoPatch::default()).await,
            Err(Error::Backend(_))
        ));
        assert!(backend(s.delete_repo("r", 1).await));
        assert!(backend(s.restore_repo("r").await));
        assert!(matches!(s.purge_repo("r", 1).await, Err(Error::Backend(_))));
        assert!(backend(s.fork_repo("r", "f", 1).await));
        assert!(backend(s.delete_ref("r", "m", None).await));
        assert!(matches!(s.name_taken("r").await, Error::Backend(_)));
    }

    #[tokio::test]
    async fn object_index_backend_errors() {
        let (_dir, s) = fresh().await;
        s.create_repo("r").await.expect("create");
        // 让读写失败：删掉索引表
        sqlx::query("DROP TABLE objects_index")
            .execute(&s.pool)
            .await
            .expect("drop");
        let o = IndexedObject {
            id: ObjectId::of_blob(b"x"),
            size: 1,
        };
        assert!(matches!(s.index_objects("r", &[o]).await, Err(Error::Backend(_))));
        assert!(matches!(s.missing_objects("r", &[o.id]).await, Err(Error::Backend(_))));
        assert!(matches!(s.object_sizes("r", &[o.id]).await, Err(Error::Backend(_))));
        s.pool.close().await;
        assert!(matches!(s.index_objects("r", &[o]).await, Err(Error::Backend(_))));
    }

    /// 从 M1 升级：parents 为空、只有 first_parent 的旧行被修复，跳表被标记为未知；
    /// 没有旧行的仓库不受影响。
    #[tokio::test]
    async fn upgrade_restores_parents_of_m1_rows() {
        let dir = tempfile::tempdir().expect("tempdir");
        let url = format!("sqlite://{}", dir.path().join("meta.db").display());
        let before = sqlx::migrate::Migrator {
            migrations: MIGRATOR
                .migrations
                .iter()
                .filter(|m| m.version <= 7)
                .cloned()
                .collect::<Vec<_>>()
                .into(),
            ..sqlx::migrate::Migrator::DEFAULT
        };
        let id = |s: &[u8]| ObjectId::hash(scion_object::ObjectKind::Changeset, s);
        let (a, b, c, d) = (id(b"a"), id(b"b"), id(b"c"), id(b"d"));
        {
            let pool = SqlitePool::connect(&format!("{url}?mode=rwc")).await.expect("open");
            before.run(&pool).await.expect("migrate to 0007");
            sqlx::query("INSERT INTO repos (repo_id, name, created_at_ms) VALUES (1, 'old', 0), (2, 'new', 0)")
                .execute(&pool)
                .await
                .expect("repos");
            // old：M1 写入的行（父提交只在 first_parent），深度已被错误地算成 1
            // new：M2 写入的正确数据
            for (repo, cs, first, parents, depth) in [
                (1, a, None, vec![], 1),
                (1, b, Some(a), vec![], 1),
                (2, c, None, vec![], 1),
                (2, d, Some(c), c.to_bytes().to_vec(), 2),
            ] {
                sqlx::query(
                    "INSERT INTO changesets (repo_id, cs_id, root_tree, first_parent, generation, parents, fp_depth) \
                     VALUES (?, ?, ?, ?, 1, ?, ?)",
                )
                .bind(repo)
                .bind(cs.to_bytes().to_vec())
                .bind(cs.to_bytes().to_vec())
                .bind(first.map(|f: ObjectId| f.to_bytes().to_vec()))
                .bind(parents)
                .bind(depth)
                .execute(&pool)
                .await
                .expect("row");
            }
            pool.close().await;
        }
        let s = SqliteMetaStore::connect(&url).await.expect("upgrade");
        let get = |repo: &'static str, cs| {
            let s = &s;
            async move {
                let r = s.get_changeset(repo, &cs).await.expect("ok").expect("rec");
                (r.parents, r.fp_depth)
            }
        };
        assert_eq!(get("old", b).await, (vec![a], 0), "parent restored, skip unknown");
        assert_eq!(get("old", a).await, (vec![], 0), "whole repo marked for reindex");
        assert_eq!(get("new", d).await, (vec![c], 2), "untouched");
        assert_eq!(get("new", c).await, (vec![], 1), "untouched");
    }

    /// 0018 moves pack names into their own table and rebuilds path_changes: existing rows survive.
    #[tokio::test]
    async fn upgrade_keeps_pack_locations_and_path_changes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let url = format!("sqlite://{}", dir.path().join("meta.db").display());
        let before = sqlx::migrate::Migrator {
            migrations: MIGRATOR
                .migrations
                .iter()
                .filter(|m| m.version <= 17)
                .cloned()
                .collect::<Vec<_>>()
                .into(),
            ..sqlx::migrate::Migrator::DEFAULT
        };
        let (a, b) = (ObjectId::of_blob(b"a"), ObjectId::of_blob(b"b"));
        let cs = ObjectId::hash(scion_object::ObjectKind::Changeset, b"c");
        {
            let pool = SqlitePool::connect(&format!("{url}?mode=rwc")).await.expect("open");
            before.run(&pool).await.expect("migrate to 0017");
            sqlx::query("INSERT INTO repos (repo_id, name, created_at_ms) VALUES (1, 'r', 0)")
                .execute(&pool)
                .await
                .expect("repo");
            for (id, pack, off, base) in [(a, [1u8; 32], 5, None), (b, [2u8; 32], 9, Some(a))] {
                sqlx::query("INSERT INTO pack_locations (id, pack, pack_offset, len, base) VALUES (?, ?, ?, 3, ?)")
                    .bind(id.to_bytes().to_vec())
                    .bind(pack.to_vec())
                    .bind(off)
                    .bind(base.map(|x: ObjectId| x.to_bytes().to_vec()))
                    .execute(&pool)
                    .await
                    .expect("location");
            }
            sqlx::query(
                "INSERT INTO path_changes (repo_id, path, cs_id, generation, deleted) VALUES (1, 'p', ?, 4, 0)",
            )
            .bind(cs.to_bytes().to_vec())
            .execute(&pool)
            .await
            .expect("path");
            pool.close().await;
        }
        let s = SqliteMetaStore::connect(&url).await.expect("upgrade");
        let got = s.pack_locations(&[a, b]).await.expect("locations");
        assert_eq!(got[&a].pack, [1u8; 32]);
        assert_eq!((got[&b].pack, got[&b].offset, got[&b].len), ([2u8; 32], 9, 3));
        assert_eq!(s.delta_dependents(&a).await.expect("deps"), vec![b]);
        let h = s.path_history("r", "p", u64::MAX >> 1, 10).await.expect("history");
        assert_eq!((h.len(), h[0].cs, h[0].generation), (1, cs, 4));
        // The same change recorded again is still one row
        let change = PathChange {
            path: "p".into(),
            deleted: false,
        };
        s.index_paths("r", cs, 4, &[change]).await.expect("again");
        assert_eq!(
            s.path_history("r", "p", u64::MAX >> 1, 10)
                .await
                .expect("history")
                .len(),
            1
        );
        // Offsets beyond 4 bytes are refused
        let far = PackedObject {
            id: a,
            offset: 1 << 31,
            len: 1,
            base: None,
        };
        assert!(matches!(
            s.put_pack_locations(&[3; 32], &[far]).await,
            Err(Error::Backend(_))
        ));
    }

    #[tokio::test]
    async fn changeset_corrupt_rows_and_backend_errors() {
        let (_dir, s) = fresh().await;
        s.create_repo("r").await.expect("create");
        let id = ObjectId::hash(scion_object::ObjectKind::Changeset, b"c");
        sqlx::query("INSERT INTO changesets (repo_id, cs_id, root_tree, first_parent, generation) VALUES (1, ?, x'00', NULL, 1)")
            .bind(id.to_bytes().to_vec())
            .execute(&s.pool)
            .await
            .expect("insert");
        sqlx::query("INSERT INTO refs (repo_id, name, target, version) VALUES (1, 'bad', x'00', 1)")
            .execute(&s.pool)
            .await
            .expect("insert");
        assert!(matches!(s.get_changeset("r", &id).await, Err(Error::Backend(_))));
        assert!(matches!(s.list_refs("r").await, Err(Error::Backend(_))));
        // parents 列长度不是 34 的整数倍
        let id2 = ObjectId::hash(scion_object::ObjectKind::Changeset, b"c2");
        sqlx::query(
            "INSERT INTO changesets (repo_id, cs_id, root_tree, generation, parents) VALUES (1, ?, ?, 1, x'0102')",
        )
        .bind(id2.to_bytes().to_vec())
        .bind(id2.to_bytes().to_vec())
        .execute(&s.pool)
        .await
        .expect("insert");
        assert!(matches!(s.get_changeset("r", &id2).await, Err(Error::Backend(_))));
        let rec = ChangesetRecord {
            id,
            root_tree: id,
            parents: vec![],
            generation: u64::MAX,
            fp_depth: 0,
            fp_jump: None,
        };
        assert!(matches!(s.put_changeset("r", &rec).await, Err(Error::Backend(_))));
        sqlx::query("DROP TABLE changesets")
            .execute(&s.pool)
            .await
            .expect("drop");
        let rec = ChangesetRecord { generation: 1, ..rec };
        assert!(matches!(s.put_changeset("r", &rec).await, Err(Error::Backend(_))));
        assert!(matches!(s.get_changeset("r", &id).await, Err(Error::Backend(_))));
        s.pool.close().await;
        assert!(matches!(s.list_refs("r").await, Err(Error::Backend(_))));
    }

    #[tokio::test]
    async fn history_index_backend_errors() {
        let (_dir, s) = fresh().await;
        s.create_repo("r").await.expect("create");
        let id = ObjectId::hash(scion_object::ObjectKind::Changeset, b"c");
        let ch = [PathChange {
            path: "a".into(),
            deleted: false,
        }];
        assert!(matches!(
            s.index_paths("r", id, u64::MAX, &ch).await,
            Err(Error::Backend(_))
        ));
        sqlx::query(
            "INSERT INTO changesets (repo_id, cs_id, root_tree, generation, parents) VALUES (1, x'00', x'00', 1, x'')",
        )
        .execute(&s.pool)
        .await
        .expect("corrupt row");
        assert!(matches!(s.changesets_below("r", 10, 10).await, Err(Error::Backend(_))));
        sqlx::query(
            "INSERT INTO path_changes (repo_id, path, cs_id, generation, deleted) VALUES (1, 'p', x'00', 1, 0)",
        )
        .execute(&s.pool)
        .await
        .expect("corrupt row");
        assert!(matches!(s.path_history("r", "p", 10, 10).await, Err(Error::Backend(_))));
        sqlx::query("DROP TABLE path_changes")
            .execute(&s.pool)
            .await
            .expect("drop");
        assert!(matches!(s.index_paths("r", id, 1, &ch).await, Err(Error::Backend(_))));
        s.pool.close().await;
        assert!(matches!(s.changesets_below("r", 10, 10).await, Err(Error::Backend(_))));
    }

    #[tokio::test]
    async fn derived_backend_errors() {
        let (_dir, s) = fresh().await;
        s.create_repo("r").await.expect("create");
        sqlx::query("DROP TABLE derived").execute(&s.pool).await.expect("drop");
        assert!(matches!(s.get_derived("r", "k", 1, b"x").await, Err(Error::Backend(_))));
        assert!(matches!(
            s.put_derived("r", "k", 1, b"x", b"v").await,
            Err(Error::Backend(_))
        ));
    }

    #[tokio::test]
    async fn connect_bad_url_and_unwritable_path() {
        assert!(matches!(
            SqliteMetaStore::connect("sqlite://\0").await.map(|_| ()),
            Err(Error::Backend(_))
        ));
        let r = SqliteMetaStore::connect("sqlite:///nonexistent-dir-scion/x/meta.db").await;
        assert!(matches!(r.map(|_| ()), Err(Error::Backend(_))));
    }
}
