//! 单个合入请求：读 ref → rebase 提交栈 → 条件更新 ref，条件不满足时重试。

use std::sync::Arc;
use std::time::Duration;

use scion_object::ObjectId;
use scion_store::{IdempotencyRecord, LandedChange, RefExpect, RefState};

use crate::stack::{Batch, StackOptions};
use crate::{ChangesetWriter, LandError, Landed, Result, record, validate_ref_name};

/// 合入结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LandOutcome {
    /// 合入后的 ref。
    pub state: RefState,
    /// 本次合入的提交（从旧到新）；为空表示要合入的提交早已在 ref 的历史中。
    pub landed: Vec<Landed>,
    /// 栈中早已合入过的 change（按 change id 去重），从旧到新。
    pub previously_landed: Vec<Landed>,
}

/// 一次合入请求。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LandRequest<'a> {
    pub repo: &'a str,
    pub reference: &'a str,
    pub tip: ObjectId,
    pub fast_forward_only: bool,
    /// 客户端生成的请求 id；为空时不做幂等记录。
    pub request_id: Option<&'a str>,
    /// Who asks (the authenticated subject), for the ref log.
    pub actor: &'a str,
}

/// 直接用条件更新合入（服务端设计文档 §6.2 中冷 ref 的路径）。
pub struct Lander {
    writer: Arc<ChangesetWriter>,
    opts: StackOptions,
    max_attempts: u32,
}

impl Lander {
    pub fn new(writer: Arc<ChangesetWriter>) -> Self {
        Self {
            writer,
            opts: StackOptions::default(),
            max_attempts: 16,
        }
    }

    /// 调整 rebase 参数（`fast_forward_only` 由每个请求决定，这里的值被忽略）。
    pub fn with_options(mut self, opts: StackOptions) -> Self {
        self.opts = opts;
        self
    }

    /// 条件更新失败后的最多尝试次数。
    pub fn with_max_attempts(mut self, n: u32) -> Self {
        self.max_attempts = n.max(1);
        self
    }

    pub fn writer(&self) -> &Arc<ChangesetWriter> {
        &self.writer
    }

    /// 把 `tip` 合入 `reference`。ref 不存在时创建；`tip` 不是 ref 当前提交的后继时在服务端 rebase
    /// （`fast_forward_only` 时报 [`LandError::NotFastForward`]）。
    ///
    /// 带 request id 时：同一 id 的重复请求直接返回第一次成功的结果，参数不同则报
    /// [`LandError::RequestIdReused`]。结果与 ref 更新在同一个事务中写入。
    pub async fn land(&self, req: LandRequest<'_>) -> Result<LandOutcome> {
        let LandRequest {
            repo,
            reference,
            tip,
            fast_forward_only,
            request_id,
            actor,
        } = req;
        validate_ref_name(reference)?;
        if crate::is_tag(reference) {
            return Err(LandError::TagImmutable(reference.to_owned()));
        }
        let meta = self.writer.meta().as_ref();
        if !meta.repo_exists(repo).await? {
            return Err(LandError::RepoNotFound(repo.to_owned()));
        }
        let opts = StackOptions {
            fast_forward_only,
            ..self.opts
        };
        let fingerprint = record::fingerprint(reference, &tip, fast_forward_only);
        for attempt in 0..self.max_attempts {
            // 每次尝试都先查：并发的同 id 请求可能刚刚成功
            if let Some(id) = request_id {
                if let Some(done) = self.replay(repo, id, &fingerprint).await? {
                    return Ok(done);
                }
            }
            let head = meta.get_ref(repo, reference).await?;
            let mut batch = Batch::new(&self.writer, repo, reference, head.map(|h| h.target)).await?;
            let prepared = batch.add(tip, opts).await?;
            let (expect, version) = match head {
                // 已经在历史中（或全部 change 早已合入）：直接返回当前状态
                Some(h) if prepared.landed.is_empty() => {
                    return Ok(LandOutcome {
                        state: h,
                        landed: Vec::new(),
                        previously_landed: prepared.previously_landed,
                    });
                }
                Some(h) => (RefExpect::Version(h.version), h.version.saturating_add(1)),
                None => (RefExpect::Absent, 1),
            };
            let outcome = LandOutcome {
                state: RefState {
                    target: prepared.tip,
                    version,
                },
                landed: prepared.landed,
                previously_landed: prepared.previously_landed,
            };
            let changes: Vec<LandedChange> = outcome
                .landed
                .iter()
                .map(|l| LandedChange {
                    change_id: l.change_id,
                    changeset: l.original,
                    landed: l.landed,
                    ref_version: version,
                })
                .collect();
            let requests: Vec<IdempotencyRecord> = request_id
                .map(|id| IdempotencyRecord {
                    request_id: id.to_owned(),
                    fingerprint: fingerprint.clone(),
                    response: record::encode(&outcome),
                    created_at_ms: now_ms(),
                })
                .into_iter()
                .collect();
            self.writer.persist(repo, batch.built()).await?;
            match meta
                .land_commit_as(repo, reference, expect, prepared.tip, &changes, &requests, actor)
                .await
            {
                Ok(state) => return Ok(LandOutcome { state, ..outcome }),
                Err(scion_store::Error::RefVersionMismatch { .. }) => {
                    tracing::debug!(repo, reference, attempt, "ref moved during land; retrying");
                    tokio::time::sleep(backoff(attempt)).await;
                }
                Err(e) => return Err(e.into()),
            }
        }
        Err(LandError::Contention)
    }

    /// 查询请求结果（GetLandStatus）：成功合入过返回结果，否则（未收到、进行中、失败或已过期）为 `None`。
    pub async fn status(&self, repo: &str, request_id: &str) -> Result<Option<LandOutcome>> {
        match self.writer.meta().get_idempotency(repo, request_id).await? {
            Some(r) => Ok(Some(record::decode(&r.response)?)),
            None => Ok(None),
        }
    }

    async fn replay(&self, repo: &str, request_id: &str, fingerprint: &[u8]) -> Result<Option<LandOutcome>> {
        match self.writer.meta().get_idempotency(repo, request_id).await? {
            Some(r) if r.fingerprint == fingerprint => Ok(Some(record::decode(&r.response)?)),
            Some(_) => Err(LandError::RequestIdReused(request_id.to_owned())),
            None => Ok(None),
        }
    }
}

pub(crate) fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// 抖动退避：上限随尝试次数翻倍（1、2、4… 毫秒，最多 64 毫秒），在 [上限/2, 上限] 中取值。
pub(crate) fn backoff(attempt: u32) -> Duration {
    let cap_us = 1000u64 << attempt.min(6);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::from(d.subsec_nanos()))
        .unwrap_or(0);
    Duration::from_micros(cap_us / 2 + nanos % (cap_us / 2 + 1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_and_is_bounded() {
        for attempt in 0..20 {
            let cap = Duration::from_micros(1000u64 << attempt.min(6));
            let d = backoff(attempt);
            assert!(d >= cap / 2 && d <= cap, "attempt {attempt}: {d:?}");
        }
    }
}
