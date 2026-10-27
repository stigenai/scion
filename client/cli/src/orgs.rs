//! `scn org`: organizations and their members (docs/guide/auth.md#organizations).

use scion_client::{MemberInfo, OrgInfo};
use serde::Serialize;

use crate::commands::{Ctx, connect};
use crate::history::format_time;
use crate::output::{CliError, Render};

#[derive(Debug, Serialize)]
pub struct OrgOut {
    pub schema: &'static str,
    pub name: String,
    pub visibility: String,
    pub created_at_ms: i64,
    pub created_by: String,
}

impl From<OrgInfo> for OrgOut {
    fn from(o: OrgInfo) -> Self {
        Self {
            schema: "scion.cli.org/v1",
            name: o.name,
            visibility: o.visibility,
            created_at_ms: o.created_at_ms,
            created_by: o.created_by,
        }
    }
}

impl Render for OrgOut {
    fn text(&self) -> String {
        format!(
            "{} ({})\ncreated by {} on {}",
            self.name,
            self.visibility,
            self.created_by,
            format_time(self.created_at_ms, 0)
        )
    }
}

#[derive(Debug, Serialize)]
pub struct OrgListOut {
    pub schema: &'static str,
    pub orgs: Vec<OrgOut>,
}

/// One per line: name and visibility.
impl Render for OrgListOut {
    fn text(&self) -> String {
        let width = self.orgs.iter().map(|o| o.name.len()).max().unwrap_or(0);
        self.orgs
            .iter()
            .map(|o| format!("{:<width$}  {}", o.name, o.visibility))
            .collect::<Vec<_>>()
            .join("\n")
    }
}

#[derive(Debug, Serialize)]
pub struct MemberOut {
    pub subject: String,
    pub role: String,
    pub added_at_ms: i64,
    pub added_by: String,
}

impl From<MemberInfo> for MemberOut {
    fn from(m: MemberInfo) -> Self {
        Self {
            subject: m.subject,
            role: m.role,
            added_at_ms: m.added_at_ms,
            added_by: m.added_by,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct MembersOut {
    pub schema: &'static str,
    pub org: String,
    pub members: Vec<MemberOut>,
}

/// One per line: subject and role.
impl Render for MembersOut {
    fn text(&self) -> String {
        let width = self.members.iter().map(|m| m.subject.len()).max().unwrap_or(0);
        self.members
            .iter()
            .map(|m| format!("{:<width$}  {}", m.subject, m.role))
            .collect::<Vec<_>>()
            .join("\n")
    }
}

/// The result of a change, with what to print.
#[derive(Debug, Serialize)]
pub struct OrgChangeOut {
    pub schema: &'static str,
    pub org: String,
    /// created | deleted | visibility | member-added | member-removed
    pub action: &'static str,
    pub subject: Option<String>,
    pub role: Option<String>,
    pub visibility: Option<String>,
}

impl Render for OrgChangeOut {
    fn text(&self) -> String {
        let (org, subject, role) = (
            &self.org,
            self.subject.as_deref().unwrap_or(""),
            self.role.as_deref().unwrap_or(""),
        );
        match self.action {
            "created" => format!(
                "created organization {org} ({}); you are its owner",
                self.visibility.as_deref().unwrap_or("")
            ),
            "deleted" => format!("deleted organization {org}"),
            "visibility" => format!("{org} is now {}", self.visibility.as_deref().unwrap_or("")),
            "member-added" => format!("{subject} is {role} of {org}"),
            _ => format!("removed {subject} from {org}"),
        }
    }

    const REPORTS: bool = true;
}

fn change(org: &str, action: &'static str) -> OrgChangeOut {
    OrgChangeOut {
        schema: "scion.cli.org-change/v1",
        org: org.to_owned(),
        action,
        subject: None,
        role: None,
        visibility: None,
    }
}

async fn client(ctx: &Ctx) -> Result<scion_client::Client, CliError> {
    connect(&ctx.endpoint_or_default(), None).await
}

pub async fn create(ctx: &Ctx, name: &str, visibility: &str) -> Result<OrgChangeOut, CliError> {
    let o = client(ctx).await?.create_org(name, visibility).await?;
    Ok(OrgChangeOut {
        visibility: Some(o.visibility),
        ..change(&o.name, "created")
    })
}

pub async fn list(ctx: &Ctx, mine: bool) -> Result<OrgListOut, CliError> {
    let orgs = client(ctx).await?.list_orgs(mine).await?;
    Ok(OrgListOut {
        schema: "scion.cli.orgs/v1",
        orgs: orgs.into_iter().map(OrgOut::from).collect(),
    })
}

pub async fn info(ctx: &Ctx, name: &str) -> Result<OrgOut, CliError> {
    Ok(client(ctx).await?.get_org(name).await?.into())
}

pub async fn set_visibility(ctx: &Ctx, name: &str, visibility: &str) -> Result<OrgChangeOut, CliError> {
    let o = client(ctx).await?.set_org_visibility(name, visibility).await?;
    Ok(OrgChangeOut {
        visibility: Some(o.visibility),
        ..change(name, "visibility")
    })
}

pub async fn delete(ctx: &Ctx, name: &str) -> Result<OrgChangeOut, CliError> {
    client(ctx).await?.delete_org(name).await?;
    Ok(change(name, "deleted"))
}

pub async fn add_member(ctx: &Ctx, org: &str, subject: &str, role: &str) -> Result<OrgChangeOut, CliError> {
    let m = client(ctx).await?.put_member(org, subject, role).await?;
    Ok(OrgChangeOut {
        subject: Some(m.subject),
        role: Some(m.role),
        ..change(org, "member-added")
    })
}

pub async fn remove_member(ctx: &Ctx, org: &str, subject: &str) -> Result<OrgChangeOut, CliError> {
    client(ctx).await?.remove_member(org, subject).await?;
    Ok(OrgChangeOut {
        subject: Some(subject.to_owned()),
        ..change(org, "member-removed")
    })
}

pub async fn members(ctx: &Ctx, org: &str) -> Result<MembersOut, CliError> {
    let members = client(ctx).await?.list_members(org).await?;
    Ok(MembersOut {
        schema: "scion.cli.org-members/v1",
        org: org.to_owned(),
        members: members.into_iter().map(MemberOut::from).collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn texts() {
        let org = OrgOut::from(OrgInfo {
            name: "acme".into(),
            visibility: "private".into(),
            created_at_ms: 0,
            created_by: "alice".into(),
        });
        assert_eq!(org.text(), "acme (private)\ncreated by alice on 1970-01-01 00:00 +0000");
        let list = OrgListOut {
            schema: "s",
            orgs: vec![
                org,
                OrgOut {
                    name: "open-source".into(),
                    visibility: "internal".into(),
                    created_at_ms: 0,
                    created_by: "bob".into(),
                    schema: "s",
                },
            ],
        };
        assert_eq!(list.text(), "acme         private\nopen-source  internal");
        let members = MembersOut {
            schema: "s",
            org: "acme".into(),
            members: vec![
                MemberOut::from(MemberInfo {
                    subject: "alice".into(),
                    role: "owner".into(),
                    added_at_ms: 0,
                    added_by: "alice".into(),
                }),
                MemberOut {
                    subject: "bo".into(),
                    role: "developer".into(),
                    added_at_ms: 0,
                    added_by: "alice".into(),
                },
            ],
        };
        assert_eq!(members.text(), "alice  owner\nbo     developer");
        let c = |action| change("acme", action);
        assert_eq!(
            OrgChangeOut {
                visibility: Some("internal".into()),
                ..c("created")
            }
            .text(),
            "created organization acme (internal); you are its owner"
        );
        assert_eq!(c("deleted").text(), "deleted organization acme");
        assert_eq!(
            OrgChangeOut {
                visibility: Some("private".into()),
                ..c("visibility")
            }
            .text(),
            "acme is now private"
        );
        let m = OrgChangeOut {
            subject: Some("bob".into()),
            role: Some("developer".into()),
            ..c("member-added")
        };
        assert_eq!(m.text(), "bob is developer of acme");
        assert_eq!(
            OrgChangeOut {
                action: "member-removed",
                ..m
            }
            .text(),
            "removed bob from acme"
        );
    }
}
