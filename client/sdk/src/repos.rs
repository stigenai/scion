//! 仓库与分支管理（`RepoService`，以及 `RefService` 的写操作）。

use bytes::Bytes;
use scion_object::ObjectId;
use scion_proto::v1::{
    self, CreateRefRequest, CreateRepoRequest, DeleteRefRequest, DeleteRepoRequest, ForkRepoRequest, GetRepoRequest,
    ListRefEventsRequest, ListReposRequest, RestoreRepoRequest, UpdateRefRequest, UpdateRepoRequest,
    event_service_client::EventServiceClient, ref_service_client::RefServiceClient,
    repo_service_client::RepoServiceClient,
};

use crate::client::Client;
use crate::error::{Error, Result};
use crate::ops::{Signature, id_from};

/// A tag, and for an annotated one what it says.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagInfo {
    /// Without `tags/`.
    pub name: String,
    pub target: ObjectId,
    pub annotation: Option<TagNote>,
}

/// What an annotated tag says.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagNote {
    pub message: String,
    pub tagger: Signature,
}

/// 仓库属性。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoInfo {
    pub name: String,
    pub default_branch: String,
    pub description: String,
    pub created_at_ms: i64,
    /// 删除时间；`None` 为正常的仓库。
    pub deleted_at_ms: Option<i64>,
    /// 已删除的仓库在这个时间之后被清除。
    pub purge_after_ms: Option<i64>,
    pub forked_from: Option<String>,
}

impl From<v1::Repo> for RepoInfo {
    fn from(r: v1::Repo) -> Self {
        let nonzero = |v: i64| (v != 0).then_some(v);
        Self {
            deleted_at_ms: nonzero(r.deleted_at_ms),
            purge_after_ms: nonzero(r.purge_after_ms),
            forked_from: Some(r.forked_from).filter(|f| !f.is_empty()),
            name: r.name,
            default_branch: r.default_branch,
            description: r.description,
            created_at_ms: r.created_at_ms,
        }
    }
}

/// [`Client::list_repos`] 的一页。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoPage {
    pub repos: Vec<RepoInfo>,
    /// 下一页的游标；`None` 表示没有更多。
    pub next_page_token: Option<String>,
}

/// 一次 ref 变化（[`Client::list_ref_events`]）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefEventInfo {
    pub seq: u64,
    pub repo: String,
    pub name: String,
    /// `None` 表示 ref 是新建的。
    pub old: Option<ObjectId>,
    /// `None` 表示 ref 被删除。
    pub new: Option<ObjectId>,
    pub version: u64,
    pub created_at_ms: i64,
}

/// [`Client::list_ref_events`] 的一页。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefEventPage {
    pub events: Vec<RefEventInfo>,
    /// 下一次的 `after_seq`。
    pub next_seq: u64,
    /// 部分事件已被清理，需要重新同步全部 ref。
    pub truncated: bool,
}

fn opt_id(raw: &[u8]) -> Result<Option<ObjectId>> {
    if raw.is_empty() {
        Ok(None)
    } else {
        id_from(raw, "changeset id").map(Some)
    }
}

/// 新仓库的属性。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NewRepo {
    /// 为空时为 main。
    pub default_branch: String,
    pub description: String,
}

fn repo_of(r: Option<v1::Repo>) -> Result<RepoInfo> {
    r.map(RepoInfo::from)
        .ok_or_else(|| Error::Protocol("response without a repository".into()))
}

fn ref_of(r: Option<v1::RefInfo>) -> Result<(ObjectId, u64)> {
    let r = r.ok_or_else(|| Error::Protocol("response without a ref".into()))?;
    Ok((id_from(&r.changeset_id, "changeset id")?, r.version))
}

impl Client {
    fn repos(&self) -> RepoServiceClient<tonic::transport::Channel> {
        RepoServiceClient::new(self.channel.clone())
    }

    pub async fn create_repo(&self, name: &str, opts: NewRepo) -> Result<RepoInfo> {
        let req = CreateRepoRequest {
            name: name.to_owned(),
            default_branch: opts.default_branch,
            description: opts.description,
        };
        repo_of(self.repos().create_repo(self.request(req)?).await?.into_inner().repo)
    }

    /// 仓库属性；`include_deleted` 时也返回已删除、尚未清除的仓库。
    pub async fn get_repo(&self, name: &str, include_deleted: bool) -> Result<RepoInfo> {
        let req = GetRepoRequest {
            name: name.to_owned(),
            include_deleted,
        };
        repo_of(self.repos().get_repo(self.request(req)?).await?.into_inner().repo)
    }

    /// 列出调用者能看到的仓库（按名字排序）。`page_token` 为上一页的 `next_page_token`。
    pub async fn list_repos(
        &self,
        prefix: &str,
        page_token: Option<&str>,
        page_size: u32,
        deleted: bool,
    ) -> Result<RepoPage> {
        let req = ListReposRequest {
            prefix: prefix.to_owned(),
            page_token: page_token.unwrap_or_default().to_owned(),
            page_size,
            deleted,
        };
        let resp = self.repos().list_repos(self.request(req)?).await?.into_inner();
        Ok(RepoPage {
            repos: resp.repos.into_iter().map(RepoInfo::from).collect(),
            next_page_token: Some(resp.next_page_token).filter(|t| !t.is_empty()),
        })
    }

    /// 修改属性；`None` 的字段不变。
    pub async fn update_repo(
        &self,
        name: &str,
        default_branch: Option<&str>,
        description: Option<&str>,
    ) -> Result<RepoInfo> {
        let req = UpdateRepoRequest {
            name: name.to_owned(),
            default_branch: default_branch.map(str::to_owned),
            description: description.map(str::to_owned),
        };
        repo_of(self.repos().update_repo(self.request(req)?).await?.into_inner().repo)
    }

    /// 软删除；返回的属性中带清除时间。
    pub async fn delete_repo(&self, name: &str) -> Result<RepoInfo> {
        let req = DeleteRepoRequest { name: name.to_owned() };
        repo_of(self.repos().delete_repo(self.request(req)?).await?.into_inner().repo)
    }

    pub async fn restore_repo(&self, name: &str) -> Result<RepoInfo> {
        let req = RestoreRepoRequest { name: name.to_owned() };
        repo_of(self.repos().restore_repo(self.request(req)?).await?.into_inner().repo)
    }

    pub async fn fork_repo(&self, source: &str, target: &str) -> Result<RepoInfo> {
        let req = ForkRepoRequest {
            source: source.to_owned(),
            target: target.to_owned(),
        };
        repo_of(self.repos().fork_repo(self.request(req)?).await?.into_inner().repo)
    }

    /// 在当前仓库创建 ref（分支），返回 (提交, 版本)。
    pub async fn create_ref(&self, name: &str, target: &ObjectId) -> Result<(ObjectId, u64)> {
        self.create_ref_with(name, target, None).await
    }

    /// Creates the annotated tag `name` (without `tags/`) on `target`, with `message` and
    /// `tagger` (None: the server takes the caller and the time).
    pub async fn create_annotated_tag(
        &self,
        name: &str,
        target: &ObjectId,
        message: &str,
        tagger: Option<&Signature>,
    ) -> Result<(ObjectId, u64)> {
        let annotation = v1::TagAnnotation {
            message: message.to_owned(),
            tagger: tagger.map(Signature::to_proto),
            git_object: Bytes::new(),
        };
        self.create_ref_with(&format!("tags/{name}"), target, Some(annotation))
            .await
    }

    async fn create_ref_with(
        &self,
        name: &str,
        target: &ObjectId,
        annotation: Option<v1::TagAnnotation>,
    ) -> Result<(ObjectId, u64)> {
        let req = CreateRefRequest {
            repo: self.repo()?,
            r#ref: name.to_owned(),
            changeset_id: Bytes::copy_from_slice(&target.to_bytes()),
            annotation,
        };
        let resp = RefServiceClient::new(self.channel.clone())
            .create_ref(self.request(req)?)
            .await?;
        ref_of(resp.into_inner().r#ref)
    }

    /// The repository's tags, by name, with what the annotated ones say.
    pub async fn list_tags(&self) -> Result<Vec<TagInfo>> {
        let req = v1::ListRefsRequest { repo: self.repo()? };
        let resp = RefServiceClient::new(self.channel.clone())
            .list_refs(self.request(req)?)
            .await?
            .into_inner();
        resp.refs
            .into_iter()
            .filter_map(|r| {
                let name = r.name.strip_prefix("tags/")?.to_owned();
                Some((name, r))
            })
            .map(|(name, r)| {
                let annotation = r.annotation.map(|a| {
                    let t = a.tagger.unwrap_or_default();
                    TagNote {
                        message: a.message,
                        tagger: Signature {
                            name: t.name,
                            email: t.email,
                            time_ms: t.time_ms,
                            tz_offset_minutes: t.tz_offset_minutes,
                        },
                    }
                });
                Ok(TagInfo {
                    name,
                    target: id_from(&r.changeset_id, "changeset id")?,
                    annotation,
                })
            })
            .collect()
    }

    /// 把 ref 直接指向 `target`（可以不是快进）。`expected_version` 为 `None` 时不检查版本。
    pub async fn update_ref(
        &self,
        name: &str,
        target: &ObjectId,
        expected_version: Option<u64>,
    ) -> Result<(ObjectId, u64)> {
        let req = UpdateRefRequest {
            repo: self.repo()?,
            r#ref: name.to_owned(),
            changeset_id: Bytes::copy_from_slice(&target.to_bytes()),
            expected_version: expected_version.unwrap_or(0),
        };
        let resp = RefServiceClient::new(self.channel.clone())
            .update_ref(self.request(req)?)
            .await?;
        ref_of(resp.into_inner().r#ref)
    }

    /// 读取 `after_seq` 之后的 ref 变化；`repo` 为 `None` 时为全部仓库（需要系统管理员）。
    pub async fn list_ref_events(&self, after_seq: u64, limit: u32, repo: Option<&str>) -> Result<RefEventPage> {
        let req = ListRefEventsRequest {
            after_seq,
            limit,
            repo: repo.unwrap_or_default().to_owned(),
        };
        let resp = EventServiceClient::new(self.channel.clone())
            .list_ref_events(self.request(req)?)
            .await?
            .into_inner();
        let events = resp
            .events
            .into_iter()
            .map(|e| {
                Ok(RefEventInfo {
                    old: opt_id(&e.old_changeset_id)?,
                    new: opt_id(&e.new_changeset_id)?,
                    seq: e.seq,
                    repo: e.repo,
                    name: e.r#ref,
                    version: e.version,
                    created_at_ms: e.created_at_ms,
                })
            })
            .collect::<Result<_>>()?;
        Ok(RefEventPage {
            events,
            next_seq: resp.next_seq,
            truncated: resp.truncated,
        })
    }

    /// 删除 ref（不能删除默认分支）。
    pub async fn delete_ref(&self, name: &str, expected_version: Option<u64>) -> Result<()> {
        let req = DeleteRefRequest {
            repo: self.repo()?,
            r#ref: name.to_owned(),
            expected_version: expected_version.unwrap_or(0),
        };
        RefServiceClient::new(self.channel.clone())
            .delete_ref(self.request(req)?)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_repos_and_refs() {
        let r = RepoInfo::from(v1::Repo {
            name: "a".into(),
            default_branch: "main".into(),
            description: String::new(),
            created_at_ms: 5,
            deleted_at_ms: 0,
            purge_after_ms: 0,
            forked_from: String::new(),
        });
        assert_eq!((r.deleted_at_ms, r.purge_after_ms, r.forked_from), (None, None, None));
        let r = RepoInfo::from(v1::Repo {
            deleted_at_ms: 7,
            purge_after_ms: 9,
            forked_from: "src".into(),
            ..Default::default()
        });
        assert_eq!(
            (r.deleted_at_ms, r.purge_after_ms, r.forked_from.as_deref()),
            (Some(7), Some(9), Some("src"))
        );
        assert!(matches!(repo_of(None), Err(Error::Protocol(_))));
        assert!(matches!(ref_of(None), Err(Error::Protocol(_))));
        let bad = v1::RefInfo {
            name: "x".into(),
            changeset_id: Bytes::from_static(b"short"),
            version: 1,
            annotation: None,
        };
        assert!(ref_of(Some(bad)).is_err());
        assert_eq!(opt_id(&[]).expect("empty"), None);
        assert!(opt_id(b"bad").is_err());
    }
}
