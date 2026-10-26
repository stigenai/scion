//! 内存后端：用于测试和 `scion server --meta mem`。进程退出即丢失全部数据。

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use async_trait::async_trait;
use bytes::Bytes;
use scion_object::ObjectId;

use crate::error::{Error, Result};
use crate::repo::validate_repo_name;
use crate::{
    BlobStore, ChangesetRecord, IdempotencyRecord, IndexedObject, LandedChange, Lease, MetaStore, OrgMember, OrgRecord,
    PackLocation, PackedObject, PathChange, PathHistoryEntry, Redaction, RedactionFilter, RefEvent, RefExpect,
    RefLogEntry, RefState, RepoFilter, RepoInfo, RepoPatch, TagAnnotation, TokenRecord, path_within,
    prefix_upper_bound,
};

#[derive(Clone)]
struct RepoData {
    info: RepoInfo,
    refs: HashMap<String, RefState>,
    objects: HashMap<ObjectId, u64>,
    changesets: HashMap<ObjectId, ChangesetRecord>,
    derived: HashMap<(String, u32, Vec<u8>), Vec<u8>>,
    /// PathHistory：(路径, 提交) → (代数, 是否删除)。
    paths: BTreeMap<(String, ObjectId), (u64, bool)>,
    /// (ref, change id) → 合入记录。
    landed: HashMap<(String, [u8; 16]), LandedChange>,
    /// request id → 请求结果。
    idempotency: HashMap<String, IdempotencyRecord>,
    /// ref → its moves, oldest first.
    ref_log: HashMap<String, Vec<RefLogEntry>>,
    /// tag ref → its annotation.
    tags: HashMap<String, TagAnnotation>,
}

impl RepoData {
    fn new(info: RepoInfo) -> Self {
        Self {
            info,
            refs: HashMap::new(),
            objects: HashMap::new(),
            changesets: HashMap::new(),
            derived: HashMap::new(),
            paths: BTreeMap::new(),
            landed: HashMap::new(),
            idempotency: HashMap::new(),
            ref_log: HashMap::new(),
            tags: HashMap::new(),
        }
    }

    fn live(&self) -> bool {
        self.info.deleted_at_ms.is_none()
    }
}

#[derive(Default)]
struct Inner {
    repos: BTreeMap<String, RepoData>,
    blobs: HashMap<ObjectId, Bytes>,
    leases: HashMap<String, Lease>,
    tokens: Vec<TokenRecord>,
    settings: HashMap<String, Vec<u8>>,
    orgs: BTreeMap<String, OrgRecord>,
    /// (organization, subject) to the membership.
    org_members: BTreeMap<(String, String), OrgMember>,
    redactions: BTreeMap<(String, ObjectId), Redaction>,
    redaction_seq: u64,
    ref_events: std::collections::VecDeque<RefEvent>,
    ref_event_seq: u64,
    pack_locations: HashMap<ObjectId, PackLocation>,
    delta_bases: HashMap<ObjectId, ObjectId>,
}

impl Inner {
    fn push_event(&mut self, repo: &str, name: &str, old: Option<ObjectId>, new: Option<ObjectId>, version: u64) {
        self.ref_event_seq += 1;
        self.ref_events.push_back(RefEvent {
            seq: self.ref_event_seq,
            repo: repo.to_owned(),
            name: name.to_owned(),
            old_target: old,
            new_target: new,
            version,
            created_at_ms: now_ms(),
        });
    }
}

fn now_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(i64::MAX)
}

/// 同时实现 [`MetaStore`] 与 [`BlobStore`] 的内存存储。
#[derive(Default)]
pub struct MemStore {
    inner: RwLock<Inner>,
}

impl MemStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn read(&self) -> Result<RwLockReadGuard<'_, Inner>> {
        self.inner
            .read()
            .map_err(|_| Error::Backend("memory store lock poisoned".into()))
    }

    fn write(&self) -> Result<RwLockWriteGuard<'_, Inner>> {
        self.inner
            .write()
            .map_err(|_| Error::Backend("memory store lock poisoned".into()))
    }
}

/// 正常（未删除）的仓库。
fn repo_mut<'a>(inner: &'a mut Inner, repo: &str) -> Result<&'a mut RepoData> {
    inner
        .repos
        .get_mut(repo)
        .filter(|d| d.live())
        .ok_or_else(|| Error::RepoNotFound(repo.to_owned()))
}

fn repo_ref<'a>(inner: &'a Inner, repo: &str) -> Result<&'a RepoData> {
    inner
        .repos
        .get(repo)
        .filter(|d| d.live())
        .ok_or_else(|| Error::RepoNotFound(repo.to_owned()))
}

/// 名字已被占用时的错误。
fn taken(inner: &Inner, repo: &str) -> Option<Error> {
    inner.repos.get(repo).map(|d| {
        if d.live() {
            Error::RepoAlreadyExists(repo.to_owned())
        } else {
            Error::RepoDeleted(repo.to_owned())
        }
    })
}

#[async_trait]
impl MetaStore for MemStore {
    async fn create_repo(&self, repo: &str) -> Result<()> {
        self.create_repo_with(&RepoInfo::new(repo, now_ms())).await
    }

    async fn repo_exists(&self, repo: &str) -> Result<bool> {
        Ok(self.read()?.repos.get(repo).is_some_and(RepoData::live))
    }

    async fn list_repos(&self) -> Result<Vec<String>> {
        Ok(self
            .read()?
            .repos
            .iter()
            .filter(|(_, d)| d.live())
            .map(|(n, _)| n.clone())
            .collect())
    }

    async fn create_repo_with(&self, info: &RepoInfo) -> Result<()> {
        validate_repo_name(&info.name)?;
        let mut inner = self.write()?;
        if let Some(e) = taken(&inner, &info.name) {
            return Err(e);
        }
        let info = RepoInfo {
            deleted_at_ms: None,
            ..info.clone()
        };
        inner.repos.insert(info.name.clone(), RepoData::new(info));
        Ok(())
    }

    async fn get_repo(&self, repo: &str) -> Result<Option<RepoInfo>> {
        Ok(self.read()?.repos.get(repo).map(|d| d.info.clone()))
    }

    async fn list_repo_infos(&self, filter: &RepoFilter) -> Result<Vec<RepoInfo>> {
        let inner = self.read()?;
        let upper = prefix_upper_bound(&filter.prefix);
        let lower = match &filter.after {
            Some(a) if a.as_str() >= filter.prefix.as_str() => std::ops::Bound::Excluded(a.clone()),
            _ => std::ops::Bound::Included(filter.prefix.clone()),
        };
        Ok(inner
            .repos
            .range((lower, std::ops::Bound::Excluded(upper)))
            .filter(|(_, d)| d.live() != filter.deleted)
            .take(filter.limit)
            .map(|(_, d)| d.info.clone())
            .collect())
    }

    async fn update_repo(&self, repo: &str, patch: &RepoPatch) -> Result<RepoInfo> {
        let mut inner = self.write()?;
        let data = repo_mut(&mut inner, repo)?;
        if let Some(b) = &patch.default_branch {
            data.info.default_branch = b.clone();
        }
        if let Some(d) = &patch.description {
            data.info.description = d.clone();
        }
        Ok(data.info.clone())
    }

    async fn delete_repo(&self, repo: &str, now_ms: i64) -> Result<()> {
        let mut inner = self.write()?;
        repo_mut(&mut inner, repo)?.info.deleted_at_ms = Some(now_ms);
        Ok(())
    }

    async fn restore_repo(&self, repo: &str) -> Result<()> {
        let mut inner = self.write()?;
        match inner.repos.get_mut(repo) {
            Some(d) if !d.live() => {
                d.info.deleted_at_ms = None;
                Ok(())
            }
            _ => Err(Error::RepoNotFound(repo.to_owned())),
        }
    }

    async fn purge_repo(&self, repo: &str, _batch: usize) -> Result<bool> {
        let mut inner = self.write()?;
        if !inner.repos.get(repo).is_some_and(|d| !d.live()) {
            return Err(Error::RepoNotFound(repo.to_owned()));
        }
        inner.repos.remove(repo);
        inner.redactions.retain(|(r, _), _| r != repo);
        Ok(true)
    }

    async fn fork_repo(&self, source: &str, target: &str, now_ms: i64) -> Result<()> {
        validate_repo_name(target)?;
        let mut inner = self.write()?;
        let src = repo_ref(&inner, source)?.clone();
        if let Some(e) = taken(&inner, target) {
            return Err(e);
        }
        let info = RepoInfo {
            name: target.to_owned(),
            created_at_ms: now_ms,
            deleted_at_ms: None,
            forked_from: Some(source.to_owned()),
            ..src.info.clone()
        };
        inner.repos.insert(
            target.to_owned(),
            RepoData {
                info,
                landed: HashMap::new(),
                idempotency: HashMap::new(),
                // As in SQL: a fork's ref log starts afresh
                ref_log: HashMap::new(),
                ..src
            },
        );
        let copied: Vec<Redaction> = inner
            .redactions
            .values()
            .filter(|r| r.repo == source && r.active)
            .cloned()
            .collect();
        for r in copied {
            inner.redaction_seq += 1;
            let rec = Redaction {
                repo: target.to_owned(),
                seq: inner.redaction_seq,
                ..r
            };
            inner.redactions.insert((target.to_owned(), rec.id), rec);
        }
        let mut refs: Vec<(String, RefState)> = repo_ref(&inner, target)?
            .refs
            .iter()
            .map(|(n, s)| (n.clone(), *s))
            .collect();
        refs.sort_by(|a, b| a.0.cmp(&b.0));
        for (name, s) in refs {
            inner.push_event(target, &name, None, Some(s.target), s.version);
        }
        Ok(())
    }

    async fn list_ref_events(&self, after_seq: u64, limit: usize, repo: Option<&str>) -> Result<Vec<RefEvent>> {
        Ok(self
            .read()?
            .ref_events
            .iter()
            .filter(|e| e.seq > after_seq && repo.is_none_or(|r| r == e.repo))
            .take(limit)
            .cloned()
            .collect())
    }

    async fn ref_event_bounds(&self) -> Result<(Option<u64>, u64)> {
        let inner = self.read()?;
        Ok((inner.ref_events.front().map(|e| e.seq), inner.ref_event_seq))
    }

    async fn prune_ref_events(&self, before_ms: i64) -> Result<u64> {
        let mut inner = self.write()?;
        let mut n = 0;
        while inner.ref_events.front().is_some_and(|e| e.created_at_ms < before_ms) {
            inner.ref_events.pop_front();
            n += 1;
        }
        Ok(n)
    }

    async fn delete_ref(&self, repo: &str, name: &str, expect: Option<u64>) -> Result<()> {
        let mut inner = self.write()?;
        let data = repo_mut(&mut inner, repo)?;
        let current = data.refs.get(name).map(|r| r.version);
        match (current, expect) {
            (Some(cur), Some(want)) if cur != want => Err(Error::RefVersionMismatch {
                name: name.to_owned(),
                current,
            }),
            (Some(_), _) => {
                data.ref_log.remove(name);
                data.tags.remove(name);
                if let Some(old) = data.refs.remove(name) {
                    inner.push_event(repo, name, Some(old.target), None, old.version);
                }
                Ok(())
            }
            (None, _) => Err(Error::RefVersionMismatch {
                name: name.to_owned(),
                current: None,
            }),
        }
    }

    async fn get_ref(&self, repo: &str, name: &str) -> Result<Option<RefState>> {
        let inner = self.read()?;
        Ok(repo_ref(&inner, repo)?.refs.get(name).copied())
    }

    async fn put_tag_annotation(&self, repo: &str, name: &str, annotation: &TagAnnotation) -> Result<()> {
        let mut inner = self.write()?;
        repo_mut(&mut inner, repo)?
            .tags
            .insert(name.to_owned(), annotation.clone());
        Ok(())
    }

    async fn tag_annotations(&self, repo: &str) -> Result<HashMap<String, TagAnnotation>> {
        let inner = self.read()?;
        Ok(repo_ref(&inner, repo)?.tags.clone())
    }

    async fn ref_log(&self, repo: &str, name: &str, before: Option<u64>, limit: usize) -> Result<Vec<RefLogEntry>> {
        let inner = self.read()?;
        let log = repo_ref(&inner, repo)?.ref_log.get(name);
        Ok(log
            .into_iter()
            .flatten()
            .rev()
            .filter(|e| before.is_none_or(|b| e.version < b))
            .take(limit)
            .copied()
            .collect())
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
        let mut inner = self.write()?;
        let data = repo_mut(&mut inner, repo)?;
        let current = data.refs.get(name).map(|r| r.version);
        let old_target = data.refs.get(name).map(|r| r.target);
        let next_version = match (expect, current) {
            (RefExpect::Absent, None) => 1,
            (RefExpect::Version(want), Some(cur)) if want == cur => cur
                .checked_add(1)
                .ok_or_else(|| Error::Backend("ref version overflow".into()))?,
            _ => {
                return Err(Error::RefVersionMismatch {
                    name: name.to_owned(),
                    current,
                });
            }
        };
        let state = RefState {
            target,
            version: next_version,
        };
        data.refs.insert(name.to_owned(), state);
        data.ref_log.entry(name.to_owned()).or_default().push(RefLogEntry {
            version: next_version,
            target,
            updated_at_ms: now_ms(),
        });
        for l in landed {
            data.landed.entry((name.to_owned(), l.change_id)).or_insert(*l);
        }
        for r in requests {
            data.idempotency
                .entry(r.request_id.clone())
                .or_insert_with(|| r.clone());
        }
        inner.push_event(repo, name, old_target, Some(target), next_version);
        Ok(state)
    }

    async fn landed_changes(&self, repo: &str, name: &str, change_ids: &[[u8; 16]]) -> Result<Vec<LandedChange>> {
        let inner = self.read()?;
        let data = repo_ref(&inner, repo)?;
        Ok(change_ids
            .iter()
            .filter_map(|c| data.landed.get(&(name.to_owned(), *c)).copied())
            .collect())
    }

    async fn get_idempotency(&self, repo: &str, request_id: &str) -> Result<Option<IdempotencyRecord>> {
        let inner = self.read()?;
        Ok(repo_ref(&inner, repo)?.idempotency.get(request_id).cloned())
    }

    async fn acquire_lease(&self, scope: &str, holder: &str, ttl_ms: i64, now_ms: i64) -> Result<Lease> {
        let mut inner = self.write()?;
        let expires_at_ms = now_ms.saturating_add(ttl_ms);
        let lease = match inner.leases.get(scope) {
            None => Lease {
                holder: holder.to_owned(),
                token: 1,
                expires_at_ms,
            },
            Some(l) if l.holder == holder => Lease {
                expires_at_ms,
                ..l.clone()
            },
            Some(l) if l.expires_at_ms <= now_ms => Lease {
                holder: holder.to_owned(),
                token: l.token + 1,
                expires_at_ms,
            },
            Some(l) => return Ok(l.clone()),
        };
        inner.leases.insert(scope.to_owned(), lease.clone());
        Ok(lease)
    }

    async fn put_token(&self, token: &TokenRecord) -> Result<()> {
        let mut inner = self.write()?;
        if inner.tokens.iter().any(|t| t.id == token.id || t.hash == token.hash) {
            return Err(Error::Backend(format!("token {} already exists", token.id)));
        }
        inner.tokens.push(token.clone());
        Ok(())
    }

    async fn get_token(&self, hash: &[u8; 32]) -> Result<Option<TokenRecord>> {
        Ok(self.read()?.tokens.iter().find(|t| &t.hash == hash).cloned())
    }

    async fn list_tokens(&self, subject: Option<&str>) -> Result<Vec<TokenRecord>> {
        let mut v: Vec<TokenRecord> = self
            .read()?
            .tokens
            .iter()
            .filter(|t| subject.is_none_or(|s| t.subject == s))
            .cloned()
            .collect();
        v.sort_by(|a, b| (a.created_at_ms, &a.id).cmp(&(b.created_at_ms, &b.id)));
        Ok(v)
    }

    async fn delete_token(&self, id: &str) -> Result<bool> {
        let mut inner = self.write()?;
        let before = inner.tokens.len();
        inner.tokens.retain(|t| t.id != id);
        Ok(inner.tokens.len() != before)
    }

    async fn create_org(&self, org: &OrgRecord) -> Result<()> {
        let mut inner = self.write()?;
        if inner.orgs.contains_key(&org.name) {
            return Err(Error::OrgAlreadyExists(org.name.clone()));
        }
        inner.orgs.insert(org.name.clone(), org.clone());
        Ok(())
    }

    async fn get_org(&self, name: &str) -> Result<Option<OrgRecord>> {
        Ok(self.read()?.orgs.get(name).cloned())
    }

    async fn list_orgs(&self) -> Result<Vec<OrgRecord>> {
        Ok(self.read()?.orgs.values().cloned().collect())
    }

    async fn set_org_visibility(&self, name: &str, visibility: &str) -> Result<bool> {
        Ok(match self.write()?.orgs.get_mut(name) {
            Some(o) => {
                visibility.clone_into(&mut o.visibility);
                true
            }
            None => false,
        })
    }

    async fn delete_org(&self, name: &str) -> Result<bool> {
        let mut inner = self.write()?;
        inner.org_members.retain(|(org, _), _| org != name);
        Ok(inner.orgs.remove(name).is_some())
    }

    async fn put_org_member(&self, m: &OrgMember) -> Result<()> {
        let mut inner = self.write()?;
        if !inner.orgs.contains_key(&m.org) {
            return Err(Error::OrgNotFound(m.org.clone()));
        }
        // A role change keeps when and by whom the member was added
        let key = (m.org.clone(), m.subject.clone());
        match inner.org_members.get_mut(&key) {
            Some(cur) => m.role.clone_into(&mut cur.role),
            None => {
                inner.org_members.insert(key, m.clone());
            }
        }
        Ok(())
    }

    async fn delete_org_member(&self, org: &str, subject: &str) -> Result<bool> {
        Ok(self
            .write()?
            .org_members
            .remove(&(org.to_owned(), subject.to_owned()))
            .is_some())
    }

    async fn list_org_members(&self, org: &str) -> Result<Vec<OrgMember>> {
        Ok(self
            .read()?
            .org_members
            .values()
            .filter(|m| m.org == org)
            .cloned()
            .collect())
    }

    async fn list_memberships(&self, subject: &str) -> Result<Vec<OrgMember>> {
        Ok(self
            .read()?
            .org_members
            .values()
            .filter(|m| m.subject == subject)
            .cloned()
            .collect())
    }

    async fn get_setting(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.read()?.settings.get(key).cloned())
    }

    async fn put_setting(&self, key: &str, value: &[u8]) -> Result<()> {
        self.write()?.settings.insert(key.to_owned(), value.to_vec());
        Ok(())
    }

    async fn put_redactions(&self, entries: &[Redaction]) -> Result<()> {
        let mut inner = self.write()?;
        for e in entries {
            repo_ref(&inner, &e.repo)?;
        }
        for e in entries {
            inner.redaction_seq += 1;
            let rec = Redaction {
                seq: inner.redaction_seq,
                ..e.clone()
            };
            inner.redactions.insert((e.repo.clone(), e.id), rec);
        }
        Ok(())
    }

    async fn get_redactions(&self, repo: &str, ids: &[ObjectId]) -> Result<Vec<Redaction>> {
        let inner = self.read()?;
        let mut seen = HashSet::new();
        Ok(ids
            .iter()
            .filter(|id| seen.insert(**id))
            .filter_map(|id| inner.redactions.get(&(repo.to_owned(), *id)).cloned())
            .collect())
    }

    async fn list_redactions(&self, filter: &RedactionFilter) -> Result<Vec<Redaction>> {
        let inner = self.read()?;
        let mut v: Vec<Redaction> = inner
            .redactions
            .values()
            .filter(|r| filter.matches(r))
            .cloned()
            .collect();
        v.sort_by_key(|r| r.seq);
        if filter.limit > 0 {
            v.truncate(filter.limit);
        }
        Ok(v)
    }

    async fn object_repos(&self, id: &ObjectId) -> Result<Vec<String>> {
        Ok(self
            .read()?
            .repos
            .iter()
            .filter(|(_, d)| d.live() && d.objects.contains_key(id))
            .map(|(name, _)| name.clone())
            .collect())
    }

    async fn list_leases(&self) -> Result<Vec<(String, Lease)>> {
        let mut v: Vec<(String, Lease)> = self
            .read()?
            .leases
            .iter()
            .map(|(k, l)| (k.clone(), l.clone()))
            .collect();
        v.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(v)
    }

    async fn release_lease(&self, scope: &str, holder: &str) -> Result<()> {
        let mut inner = self.write()?;
        if let Some(l) = inner.leases.get_mut(scope) {
            if l.holder == holder {
                l.expires_at_ms = 0;
            }
        }
        Ok(())
    }

    async fn expire_idempotency(&self, before_ms: i64) -> Result<u64> {
        let mut inner = self.write()?;
        let mut n = 0u64;
        for data in inner.repos.values_mut() {
            let before = data.idempotency.len();
            data.idempotency.retain(|_, r| r.created_at_ms >= before_ms);
            n += (before - data.idempotency.len()) as u64;
        }
        Ok(n)
    }

    async fn put_pack_locations(&self, pack: &[u8; 32], objects: &[PackedObject]) -> Result<()> {
        let mut inner = self.write()?;
        for o in objects {
            let loc = PackLocation {
                pack: *pack,
                offset: o.offset,
                len: o.len,
            };
            inner.pack_locations.insert(o.id, loc);
            match o.base {
                Some(b) => inner.delta_bases.insert(o.id, b),
                None => inner.delta_bases.remove(&o.id),
            };
        }
        Ok(())
    }

    async fn delta_dependents(&self, base: &ObjectId) -> Result<Vec<ObjectId>> {
        let inner = self.read()?;
        let mut out: Vec<ObjectId> = inner
            .delta_bases
            .iter()
            .filter(|(_, b)| *b == base)
            .map(|(id, _)| *id)
            .collect();
        out.sort();
        Ok(out)
    }

    async fn pack_locations(&self, ids: &[ObjectId]) -> Result<HashMap<ObjectId, PackLocation>> {
        let inner = self.read()?;
        Ok(ids
            .iter()
            .filter_map(|id| inner.pack_locations.get(id).map(|l| (*id, *l)))
            .collect())
    }

    async fn delete_pack_locations(&self, ids: &[ObjectId]) -> Result<()> {
        let mut inner = self.write()?;
        for id in ids {
            inner.pack_locations.remove(id);
            inner.delta_bases.remove(id);
        }
        Ok(())
    }

    async fn index_objects(&self, repo: &str, objects: &[IndexedObject]) -> Result<()> {
        let mut inner = self.write()?;
        let data = repo_mut(&mut inner, repo)?;
        for o in objects {
            data.objects.entry(o.id).or_insert(o.size);
        }
        Ok(())
    }

    async fn missing_objects(&self, repo: &str, ids: &[ObjectId]) -> Result<Vec<ObjectId>> {
        let inner = self.read()?;
        let data = repo_ref(&inner, repo)?;
        let mut seen = HashSet::new();
        Ok(ids
            .iter()
            .filter(|id| !data.objects.contains_key(id) && seen.insert(**id))
            .copied()
            .collect())
    }

    async fn object_sizes(&self, repo: &str, ids: &[ObjectId]) -> Result<HashMap<ObjectId, u64>> {
        let inner = self.read()?;
        let data = repo_ref(&inner, repo)?;
        Ok(ids
            .iter()
            .filter_map(|id| data.objects.get(id).map(|s| (*id, *s)))
            .collect())
    }

    async fn put_changeset(&self, repo: &str, rec: &ChangesetRecord) -> Result<()> {
        let mut inner = self.write()?;
        repo_mut(&mut inner, repo)?
            .changesets
            .entry(rec.id)
            .or_insert_with(|| rec.clone());
        Ok(())
    }

    async fn get_changeset(&self, repo: &str, id: &ObjectId) -> Result<Option<ChangesetRecord>> {
        let inner = self.read()?;
        Ok(repo_ref(&inner, repo)?.changesets.get(id).cloned())
    }

    async fn changesets_below(&self, repo: &str, max_generation: u64, limit: usize) -> Result<Vec<ChangesetRecord>> {
        let inner = self.read()?;
        let mut recs: Vec<_> = repo_ref(&inner, repo)?
            .changesets
            .values()
            .filter(|r| r.generation <= max_generation)
            .cloned()
            .collect();
        recs.sort_unstable_by_key(|r| std::cmp::Reverse(r.generation));
        recs.truncate(limit);
        Ok(recs)
    }

    async fn repair_lineage(
        &self,
        repo: &str,
        id: &ObjectId,
        parents: &[ObjectId],
        fp_depth: u64,
        fp_jump: Option<ObjectId>,
    ) -> Result<()> {
        let mut inner = self.write()?;
        if let Some(rec) = repo_mut(&mut inner, repo)?.changesets.get_mut(id) {
            rec.parents = parents.to_vec();
            rec.fp_depth = fp_depth;
            rec.fp_jump = fp_jump;
        }
        Ok(())
    }

    async fn index_paths(&self, repo: &str, cs: ObjectId, generation: u64, changes: &[PathChange]) -> Result<()> {
        let mut inner = self.write()?;
        let data = repo_mut(&mut inner, repo)?;
        for c in changes {
            data.paths
                .entry((c.path.clone(), cs))
                .or_insert((generation, c.deleted));
        }
        Ok(())
    }

    async fn path_history(
        &self,
        repo: &str,
        path: &str,
        max_generation: u64,
        limit: usize,
    ) -> Result<Vec<PathHistoryEntry>> {
        let inner = self.read()?;
        let data = repo_ref(&inner, repo)?;
        let mut out: Vec<_> = data
            .paths
            .range(
                (
                    path.to_owned(),
                    ObjectId::new(
                        scion_object::HashAlgo::Blake3_256,
                        scion_object::ObjectKind::Blob,
                        [0; 32],
                    ),
                )..,
            )
            .take_while(|((p, _), _)| p.as_str() == path || p.starts_with(path))
            .filter(|((p, _), (g, _))| path_within(p, path) && *g <= max_generation)
            .map(|((p, cs), (g, d))| PathHistoryEntry {
                cs: *cs,
                generation: *g,
                path: p.clone(),
                deleted: *d,
            })
            .collect();
        out.sort_unstable_by(|a, b| b.generation.cmp(&a.generation).then_with(|| a.path.cmp(&b.path)));
        out.truncate(limit);
        Ok(out)
    }

    async fn get_derived(&self, repo: &str, kind: &str, version: u32, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let inner = self.read()?;
        Ok(repo_ref(&inner, repo)?
            .derived
            .get(&(kind.to_owned(), version, key.to_vec()))
            .cloned())
    }

    async fn put_derived(&self, repo: &str, kind: &str, version: u32, key: &[u8], value: &[u8]) -> Result<()> {
        let mut inner = self.write()?;
        repo_mut(&mut inner, repo)?
            .derived
            .entry((kind.to_owned(), version, key.to_vec()))
            .or_insert_with(|| value.to_vec());
        Ok(())
    }

    async fn list_refs(&self, repo: &str) -> Result<Vec<(String, RefState)>> {
        let inner = self.read()?;
        let mut refs: Vec<_> = repo_ref(&inner, repo)?
            .refs
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        refs.sort_unstable_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
        Ok(refs)
    }
}

#[async_trait]
impl BlobStore for MemStore {
    async fn put(&self, id: ObjectId, data: Bytes) -> Result<()> {
        if !id.verify(&data) {
            return Err(Error::Backend(format!(
                "refusing to store bytes that do not match {id}"
            )));
        }
        self.write()?.blobs.entry(id).or_insert(data);
        Ok(())
    }

    async fn get(&self, id: &ObjectId) -> Result<Option<Bytes>> {
        Ok(self.read()?.blobs.get(id).cloned())
    }

    async fn exists(&self, id: &ObjectId) -> Result<bool> {
        Ok(self.read()?.blobs.contains_key(id))
    }

    async fn delete(&self, id: &ObjectId) -> Result<()> {
        self.write()?.blobs.remove(id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testsuite;

    #[tokio::test]
    async fn conformance_meta() {
        testsuite::meta_store(&MemStore::new()).await;
    }

    #[tokio::test]
    async fn conformance_object_index() {
        testsuite::object_index(&MemStore::new()).await;
    }

    #[tokio::test]
    async fn conformance_changesets_and_refs() {
        testsuite::changesets_and_refs(&MemStore::new()).await;
    }

    #[tokio::test]
    async fn conformance_derived() {
        testsuite::derived(&MemStore::new()).await;
    }

    #[tokio::test]
    async fn conformance_pack_locations() {
        testsuite::pack_locations(&MemStore::new()).await;
    }

    #[tokio::test]
    async fn conformance_landing() {
        testsuite::landing(&MemStore::new()).await;
    }

    #[tokio::test]
    async fn conformance_leases() {
        testsuite::leases(&MemStore::new()).await;
        testsuite::tokens_and_settings(&MemStore::new()).await;
        testsuite::orgs_and_members(&MemStore::new()).await;
        testsuite::redactions(&MemStore::new()).await;
        testsuite::repo_admin(&MemStore::new()).await;
        testsuite::ref_events(&MemStore::new()).await;
        testsuite::ref_log(&MemStore::new()).await;
        testsuite::tag_annotations(&MemStore::new()).await;
    }

    #[tokio::test]
    async fn conformance_batch_writes() {
        testsuite::batch_writes(&MemStore::new()).await;
    }

    #[tokio::test]
    async fn conformance_history_index() {
        testsuite::history_index(&MemStore::new()).await;
    }

    #[tokio::test]
    async fn conformance_fp_skip_chain() {
        testsuite::fp_skip_chain(&MemStore::new()).await;
    }

    #[tokio::test]
    async fn conformance_blob() {
        testsuite::blob_store(&MemStore::new()).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn conformance_concurrent_cas() {
        testsuite::concurrent_cas(std::sync::Arc::new(MemStore::new())).await;
    }

    #[tokio::test]
    async fn cas_version_overflow_rejected() {
        let s = MemStore::new();
        s.create_repo("r").await.expect("repo");
        let id = ObjectId::hash(scion_object::ObjectKind::Changeset, b"x");
        s.write().expect("lock").repos.get_mut("r").expect("r").refs.insert(
            "main".into(),
            RefState {
                target: id,
                version: u64::MAX,
            },
        );
        assert!(matches!(
            s.cas_ref("r", "main", RefExpect::Version(u64::MAX), id).await,
            Err(Error::Backend(_))
        ));
    }

    #[tokio::test]
    async fn poisoned_lock_reports_backend_error() {
        let s = std::sync::Arc::new(MemStore::new());
        let s2 = s.clone();
        let _ = std::thread::spawn(move || {
            let _g = s2.inner.write().expect("lock");
            panic!("poison the lock");
        })
        .join();
        assert!(matches!(s.repo_exists("a").await, Err(Error::Backend(_))));
        assert!(matches!(s.create_repo("a").await, Err(Error::Backend(_))));
    }
}
