//! `scn`: the Scion command line client.
//!
//! Exit codes (client design §8.2): 0 success, 1 general error, 2 usage error, 3 conflict,
//! 4 retryable error (retries exhausted), 5 authentication failed.

mod commands;
mod config;
mod creds;
mod daemon;
mod history;
mod login;
mod orgs;
mod output;
mod progress;
mod remote;
mod repos;
mod watch;

use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{FromArgMatches, Parser, Subcommand};

use crate::commands::Ctx;
use crate::history::DiffOut;
use crate::output::{CliError, Render, error_out, render, version_out};

#[derive(Debug, Parser)]
#[command(name = "scn", version, about = "Scion command line client")]
struct Cli {
    /// Server address; defaults to the working copy's, then http://127.0.0.1:7080.
    #[arg(
        long,
        global = true,
        env = "SCION_ENDPOINT",
        value_name = "URL",
        help_heading = "Global options"
    )]
    endpoint: Option<String>,
    /// Repository, for commands run outside a working copy: a name, or its URL
    /// http(s)://host[:port]/REPOSITORY (whose server then replaces --endpoint).
    #[arg(long, global = true, env = "SCION_REPO", help_heading = "Global options")]
    repo: Option<String>,
    /// Print JSON (with a schema field) for agents and scripts.
    #[arg(long, global = true, help_heading = "Global options")]
    json: bool,
    /// No progress or notes on standard error (errors are still printed).
    #[arg(short, long, global = true, help_heading = "Global options")]
    quiet: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Check the connection to the server and what it supports.
    Doctor {
        /// The server, http(s)://host[:port]; instead of --endpoint.
        server: Option<String>,
    },
    /// Print the client and protocol versions.
    Version,
    /// Check out a repository into a local directory.
    #[command(after_help = "Examples:\n  scn clone http://scion.example.com:7080/org/app\n  \
        scn clone http://scion.example.com:7080/org/app app-dev --ref dev\n  \
        scn clone http://scion.example.com:7080/org/app --sparse src,docs\n  \
        scn --endpoint http://scion.example.com:7080 clone org/app")]
    Clone {
        /// A repository name, or its URL: http(s)://host[:port]/REPOSITORY.
        repo: String,
        /// The directory to create (default: the repository name's last part).
        dir: Option<PathBuf>,
        /// The branch to check out (git's `-b` / `--branch` work too).
        #[arg(
            long = "ref",
            short = 'b',
            visible_alias = "branch",
            value_name = "BRANCH",
            default_value = "main"
        )]
        reference: String,
        /// Accepted for git compatibility: a clone never downloads history, only the snapshot.
        #[arg(long, value_name = "N", hide_short_help = true)]
        depth: Option<u32>,
        /// Accepted for git compatibility: a clone checks out one branch.
        #[arg(long, hide_short_help = true)]
        single_branch: bool,
        /// Sparse checkout: only these directories (comma-separated) and the files at the root.
        /// Can be repeated: `--sparse src --sparse docs`.
        #[arg(long, value_name = "DIRS", value_delimiter = ',', num_args = 1)]
        sparse: Option<Vec<String>>,
    },
    /// The working copy's server and repository, git-style: `scn remote [-v]`, `scn remote get-url`,
    /// `scn remote set-url URL`. A working copy has one remote, named origin.
    #[command(after_help = "Examples:\n  scn remote -v\n  scn remote get-url\n  \
        scn remote set-url http://192.168.1.11:7080/org/app     # the server's new address")]
    Remote {
        /// Show the URL too.
        #[arg(short, long)]
        verbose: bool,
        #[command(subcommand)]
        cmd: Option<RemoteCmd>,
    },
    /// The directories of a sparse checkout.
    #[command(subcommand)]
    Sparse(SparseCmd),
    /// Repositories: create, list, change, delete, restore, fork.
    #[command(subcommand)]
    Repo(RepoCmd),
    /// Organizations and their members (on servers with organizations enabled).
    #[command(subcommand)]
    Org(OrgCmd),
    /// Branches: list, create, delete (of the working copy's repository, or --repo).
    /// git-style forms also work: `scn branch` (list), `scn branch NAME [REV]`, `scn branch -d NAME`.
    #[command(args_conflicts_with_subcommands = true)]
    #[command(after_help = "Examples:\n  scn branch\n  scn branch feature main~2\n  scn branch -d feature")]
    Branch {
        #[command(subcommand)]
        cmd: Option<BranchCmd>,
        /// Create this branch.
        name: Option<String>,
        /// Where the new branch starts (default: the current branch).
        #[arg(value_name = "REV")]
        revision: Option<String>,
        /// Delete this branch.
        #[arg(
            short = 'd',
            long = "delete",
            short_alias = 'D',
            value_name = "NAME",
            conflicts_with = "name"
        )]
        delete: Option<String>,
        /// List with each branch's commit.
        #[arg(short, long, conflicts_with_all = ["name", "delete"])]
        verbose: bool,
        /// Accepted for git compatibility: every branch is on the server, so the list has them all.
        #[arg(short = 'a', long = "all", short_alias = 'r', alias = "remotes")]
        all: bool,
        /// Print the working copy's branch.
        #[arg(long, conflicts_with_all = ["name", "delete", "verbose"])]
        show_current: bool,
    },
    /// Tags: fixed names for commits. `scn tag` lists, `scn tag NAME [REV]` creates (default
    /// HEAD), `scn tag -d NAME` deletes. A tag name works as a revision (`scn log v1.0`).
    #[command(after_help = "Examples:\n  scn tag\n  scn tag v1.0\n  scn tag v0.9 main~5\n  scn tag -d v0.9")]
    Tag {
        /// Create this tag.
        name: Option<String>,
        /// The commit to tag (default: HEAD in a working copy, else the default branch).
        #[arg(value_name = "REV")]
        revision: Option<String>,
        /// Delete this tag.
        #[arg(short = 'd', long = "delete", value_name = "NAME", conflicts_with = "name")]
        delete: Option<String>,
        /// List the tags (what `scn tag` with no name does).
        #[arg(short = 'l', long, conflicts_with_all = ["name", "delete"])]
        list: bool,
    },
    /// Show local changes, like `git status`.
    Status {
        /// git's short format: `XY PATH` per change (every change is staged in scn).
        #[arg(short, long)]
        short: bool,
        /// With the short format, start with `## BRANCH`.
        #[arg(short, long)]
        branch: bool,
        /// The short format, for scripts (as git's `--porcelain`).
        #[arg(long)]
        porcelain: bool,
        /// Do not ask the server how far its branch has moved past the working copy.
        #[arg(long)]
        no_ahead_behind: bool,
    },
    /// Commit the local changes (not landed yet; see `scn land`).
    #[command(after_help = "Examples:\n  scn commit -m \"Fix the parser\"\n  \
        scn commit -m \"Docs\" docs README.md # only the changes in these paths; the rest stay local\n  \
        scn commit --amend                 # fold new changes into the newest pending commit\n  \
        scn commit                         # finish a sync or merge that stopped on conflicts")]
    Commit {
        /// Required, except with --amend (keeps the old message) or when finishing a sync or
        /// merge that stopped on conflicts (keeps that commit's message). Several -m become
        /// separate paragraphs, as in git.
        #[arg(short, long)]
        message: Vec<String>,
        /// Replace the newest pending (not yet landed) commit, adding the current local changes.
        #[arg(long)]
        amend: bool,
        /// Accepted for git compatibility (`-am`); without paths scn commits every local change.
        #[arg(short = 'a', long = "all")]
        all: bool,
        /// Author as "Name <email>". Defaults to `scn config` `user.name` / `user.email`, then git's
        /// `user.name` / `user.email`.
        #[arg(long, env = "SCION_AUTHOR")]
        author: Option<String>,
        /// Commit only the changes at or under these paths (files or directories); the others stay
        /// local changes. Without paths, every local change.
        #[arg(value_name = "PATH")]
        paths: Vec<String>,
    },
    /// Move the working copy to another branch (-c creates it at the current snapshot), or with
    /// --detach to any commit, to look at it on no branch.
    #[command(after_help = "Examples:\n  scn switch feature\n  scn switch -c fix\n  \
        scn switch --detach v1.0       # the files of v1.0, on no branch\n  \
        scn switch --detach main~3")]
    Switch {
        /// The branch to switch to (with --detach: any revision).
        #[arg(value_name = "BRANCH")]
        reference: String,
        /// Create the branch at the current snapshot first.
        #[arg(short = 'c', long)]
        create: bool,
        /// Check out the revision on no branch: nothing can be committed there until you switch
        /// to a branch (`-c NAME` starts one).
        #[arg(long, conflicts_with = "create")]
        detach: bool,
    },
    /// Merge another branch into this one as a pending merge commit (`scn land` publishes it).
    /// On conflicts the files get conflict markers; see `scn resolve`.
    Merge {
        /// The branch to merge (not needed with --abort).
        #[arg(required_unless_present_any = ["abort", "cont"], value_name = "BRANCH")]
        reference: Option<String>,
        /// Abandon a merge that stopped on conflicts.
        // Not --author: SCION_AUTHOR sets it from the environment, and an abort ignores it
        #[arg(long, conflicts_with = "message")]
        abort: bool,
        /// Accepted for git compatibility: scn always makes a merge commit.
        #[arg(long = "no-ff")]
        no_ff: bool,
        /// git's `merge --continue`: transitional spelling of `scn commit`.
        #[arg(long = "continue", hide = true, conflicts_with = "abort")]
        cont: bool,
        /// Message for the merge commit (default: "Merge BRANCH into THIS").
        #[arg(short, long)]
        message: Option<String>,
        /// Author as "Name <email>"; defaults like `scn commit`.
        #[arg(long, env = "SCION_AUTHOR")]
        author: Option<String>,
    },
    /// Undo a commit's changes as a new pending commit (`scn land` publishes it). Later changes
    /// to the same lines are kept; where they overlap the files get conflict markers, as in
    /// `scn merge`.
    #[command(after_help = "Examples:\n  scn revert c1a2b3c4d5e6\n  scn revert main~2\n  \
        scn revert -m 1 c1a2b3c4d5e6      # a merge: undo what it brought in\n  \
        scn revert --abort                # give up a revert that stopped on conflicts")]
    Revert {
        /// The commit to undo.
        #[arg(required_unless_present_any = ["abort", "cont"], value_name = "REV")]
        revision: Option<String>,
        /// For a merge commit: the parent to keep (1 is the first, the branch it was merged into).
        #[arg(short = 'm', long, value_name = "PARENT")]
        mainline: Option<usize>,
        /// Abandon a revert that stopped on conflicts.
        #[arg(long, conflicts_with_all = ["revision", "mainline"])]
        abort: bool,
        /// git's `revert --continue`: transitional spelling of `scn commit`.
        #[arg(long = "continue", hide = true, conflicts_with = "abort")]
        cont: bool,
        /// Author as "Name <email>"; defaults like `scn commit`.
        #[arg(long, env = "SCION_AUTHOR")]
        author: Option<String>,
    },
    /// Apply another commit's changes here as a new pending commit (`scn land` publishes it),
    /// keeping its message and author. Where this branch changed the same lines differently the
    /// files get conflict markers, as in `scn merge`.
    #[command(
        after_help = "Examples:\n  scn cherry-pick c1a2b3c4d5e6        # e.g. a fix from main onto a release branch\n  \
        scn cherry-pick -x feature~1        # note the original commit in the message\n  \
        scn cherry-pick --abort             # give up a cherry-pick that stopped on conflicts"
    )]
    CherryPick {
        /// The commit to apply.
        #[arg(required_unless_present_any = ["abort", "cont"], value_name = "REV")]
        revision: Option<String>,
        /// For a merge commit: the parent to compare it with (1 is the first).
        #[arg(short = 'm', long, value_name = "PARENT")]
        mainline: Option<usize>,
        /// Add "(cherry picked from commit ID)" to the message.
        #[arg(short = 'x')]
        record_origin: bool,
        /// Abandon a cherry-pick that stopped on conflicts.
        #[arg(long, conflicts_with_all = ["revision", "mainline", "record_origin"])]
        abort: bool,
        /// git's `cherry-pick --continue`: transitional spelling of `scn commit`.
        #[arg(long = "continue", hide = true, conflicts_with = "abort")]
        cont: bool,
        /// Author as "Name <email>" (default: the original commit's author). Not read from
        /// SCION_AUTHOR, which names who makes new commits, not who wrote this one.
        #[arg(long)]
        author: Option<String>,
    },
    // ---- Transitional git spellings: run the scn command and print a hint (hidden from --help)
    #[command(hide = true)]
    Push {
        /// `[origin [BRANCH]]`: the only remote, and the working copy's branch.
        #[arg(num_args = 0..=2)]
        args: Vec<String>,
        /// Accepted: the branch is always the server's.
        #[arg(short = 'u', long = "set-upstream")]
        set_upstream: bool,
        /// Refused: landing never rewrites a branch.
        #[arg(short, long, alias = "force-with-lease")]
        force: bool,
    },
    #[command(hide = true)]
    Pull {
        /// `[origin [BRANCH]]`: the only remote, and the working copy's branch.
        #[arg(num_args = 0..=2)]
        args: Vec<String>,
        /// Accepted: a pending commit is always rebased.
        #[arg(long, alias = "no-rebase")]
        rebase: bool,
        /// Refuse to rebase a pending commit.
        #[arg(long = "ff-only")]
        ff_only: bool,
    },
    #[command(hide = true)]
    Checkout {
        /// `-b NAME`: create the branch and switch to it.
        #[arg(short = 'b')]
        create: bool,
        target: Option<String>,
        #[arg(last = true)]
        paths: Vec<String>,
    },
    #[command(hide = true)]
    Reset {
        #[arg(long)]
        soft: bool,
        #[arg(long, conflicts_with = "soft")]
        hard: bool,
        target: Option<String>,
    },
    #[command(hide = true)]
    Rebase {
        #[arg(long = "continue")]
        cont: bool,
        #[arg(long, conflicts_with = "cont")]
        abort: bool,
    },
    #[command(hide = true)]
    Add {
        #[arg(short = 'A', long = "all")]
        all: bool,
        paths: Vec<String>,
    },
    #[command(hide = true)]
    Rm {
        #[arg(short = 'r')]
        recursive: bool,
        #[arg(required = true)]
        paths: Vec<String>,
    },
    #[command(hide = true)]
    Mv {
        from: PathBuf,
        to: PathBuf,
        /// Accepted: the move replaces an existing file anyway.
        #[arg(short, long)]
        force: bool,
    },
    /// git commands scn does without: they say what to do instead.
    #[command(hide = true)]
    Init {
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
    #[command(hide = true)]
    Fetch {
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
    /// Mark conflicted files resolved (after editing out the markers), or take one side's
    /// version. Without PATHS, lists the conflicts. `.` means all.
    #[command(
        after_help = "Examples:\n  scn resolve                # list the conflicted files\n  \
        scn resolve src/lib.rs     # after editing out the markers\n  \
        scn resolve --theirs .     # take the other side for every file"
    )]
    Resolve {
        /// Conflicted files, relative to the current directory.
        #[arg(value_name = "PATH")]
        paths: Vec<String>,
        /// Take your version of each file.
        #[arg(long, conflicts_with = "theirs")]
        ours: bool,
        /// Take the other side's version (the branch's tip, or the merged branch).
        #[arg(long)]
        theirs: bool,
        /// Accept files that still contain conflict markers.
        #[arg(long)]
        force: bool,
    },
    /// Drop the newest pending (not yet landed) commit; its changes stay as uncommitted local changes.
    Uncommit,
    /// Discard local changes to tracked files under PATHS (`.` for everything). Added files are kept.
    Restore {
        /// Files or directories, relative to the current directory.
        #[arg(required = true, value_name = "PATH")]
        paths: Vec<String>,
        /// Take the files from this revision instead (they become local changes; files it lacks are
        /// deleted), as git's `restore --source`.
        #[arg(short, long, value_name = "REV")]
        source: Option<String>,
    },
    /// Put local changes aside and bring them back later (saved as a commit on the server, on no branch).
    #[command(
        args_conflicts_with_subcommands = true,
        after_help = "Examples:\n  scn stash                 # save every local change; the tree is the commit's again\n  \
            scn stash -m \"half done\"\n  scn stash list\n  scn stash pop             # apply the newest and drop it\n  \
            scn stash apply stash@{1}\n  scn stash show -p\n  scn stash drop"
    )]
    Stash {
        #[command(subcommand)]
        cmd: Option<StashCmd>,
        /// The entry's message (default: WIP on BRANCH: ID SUBJECT).
        #[arg(short, long)]
        message: Option<String>,
    },
    /// Remove untracked files (with -x ignored ones too); needs -f, or -n to only list them.
    #[command(after_help = "Examples:\n  scn clean -n          # list what would go\n  \
        scn clean -f          # remove untracked files\n  \
        scn clean -fx build   # also ignored ones, under build/ only")]
    Clean {
        /// Only list what would be removed.
        #[arg(short = 'n', long)]
        dry_run: bool,
        /// Remove them (required unless -n, as in git).
        #[arg(short, long)]
        force: bool,
        /// Also remove ignored files (.gitignore / .scionignore), such as build output.
        #[arg(short = 'x')]
        ignored: bool,
        /// Accepted for git compatibility: untracked directories are always removed.
        #[arg(short = 'd')]
        dirs: bool,
        /// Only under these files or directories, relative to the current directory.
        #[arg(value_name = "PATH")]
        paths: Vec<String>,
    },
    /// User settings: the default commit author (`user.name`, `user.email`).
    /// git-style forms also work: `scn config KEY [VALUE]`, `--list`, `--unset KEY`.
    #[command(
        args_conflicts_with_subcommands = true,
        after_help = "Examples:\n  scn config user.name \"Ada Lovelace\"\n  scn config user.email ada@example.com\n  \
            scn config --list\n  scn config --unset user.email"
    )]
    Config {
        #[command(subcommand)]
        cmd: Option<ConfigCmd>,
        /// user.name or user.email: print it, or set it to VALUE.
        key: Option<String>,
        /// The new value.
        value: Option<String>,
        /// Print every setting.
        #[arg(short = 'l', long, conflicts_with_all = ["key", "unset"])]
        list: bool,
        /// Remove this setting.
        #[arg(long, value_name = "KEY", conflicts_with = "key")]
        unset: Option<String>,
        /// Accepted for git compatibility: scn's settings are always per user.
        #[arg(long)]
        global: bool,
    },
    /// Land the pending commits on the branch.
    Land,
    /// Update to the branch tip, rebasing the pending commits onto it one by one.
    #[command(after_help = "Examples:\n  scn sync\n  \
        scn sync --abort    # give up a sync that stopped on conflicts: back to before it")]
    Sync {
        /// Abandon a sync that stopped on conflicts: back to the commit and pending commits from before it.
        #[arg(long)]
        abort: bool,
    },
    /// Print a file; no working copy needed.
    #[command(after_help = "Examples:\n  scn cat README.md\n  scn cat main~1:src/lib.rs\n  \
        scn cat v1.0:Cargo.toml --repo http://scion.example.com:7080/org/app")]
    Cat {
        /// The file, optionally at a revision (default: the current branch).
        #[arg(value_name = "[REV:]PATH")]
        spec: String,
    },
    /// List a directory; no working copy needed.
    #[command(after_help = "Examples:\n  scn ls\n  scn ls src\n  scn ls main~3:docs\n  \
        scn ls main            # a bare revision lists its root; `:DIR` forces a path")]
    Ls {
        /// The directory, optionally at a revision (default: the root of the current branch).
        #[arg(value_name = "[REV:][DIR]")]
        spec: Option<String>,
    },
    /// Download an archive of a revision or a directory; no working copy needed.
    #[command(after_help = "Examples:\n  scn archive -o mono.tar.gz\n  \
        scn archive v1.0:src --format zip --prefix src-v1.0/ -o src.zip\n  \
        scn archive main | tar -x            # to standard output")]
    Archive {
        /// What to archive (default: the whole current branch); a bare revision works too.
        #[arg(value_name = "[REV:][DIR]")]
        spec: Option<String>,
        /// tar.gz, zip or tar.
        #[arg(long, default_value = "tar.gz")]
        format: String,
        /// Prefix for every path in the archive, such as mono-main/.
        #[arg(long, default_value = "")]
        prefix: String,
        /// Output file; standard output when omitted.
        #[arg(short = 'o', long, value_name = "FILE")]
        output: Option<PathBuf>,
    },
    /// History from REV (default: the current branch), newest first.
    #[command(
        after_help = "Examples:\n  scn log\n  scn log --oneline -n 50\n  scn log v1.0 -- src\n  \
        scn log main --repo http://scion.example.com:7080/org/app"
    )]
    Log {
        /// Where to start (default: the current branch).
        #[arg(value_name = "REV")]
        revision: Option<String>,
        /// Only commits that changed this path. Same as `-- PATH`.
        #[arg(long)]
        path: Option<String>,
        /// At most this many commits.
        #[arg(short = 'n', long = "max-count", value_name = "N", default_value_t = 20)]
        max_count: usize,
        /// One line per commit: its short id and subject.
        #[arg(long)]
        oneline: bool,
        /// git's `--pretty=oneline` / `--format=oneline`; other formats are not supported.
        #[arg(long, alias = "format", value_name = "FORMAT", value_parser = ["oneline", "medium"])]
        pretty: Option<String>,
        /// `-- PATH`, as in git.
        #[arg(last = true, value_name = "PATH")]
        paths: Vec<String>,
    },
    /// Show how a branch or tag moved on the server, newest first (git's reflog, kept by the server).
    #[command(
        after_help = "Each line: ID BRANCH@{N}: TIME (UTC) HOW: SUBJECT, where HOW is created, fast-forward \
        (lands) or forced (moved anywhere else, such as back).\n\nExamples:\n  scn reflog\n  scn reflog release -n 50"
    )]
    Reflog {
        /// The branch or tag (default: the working copy's branch, else main).
        #[arg(value_name = "REF")]
        reference: Option<String>,
        /// At most this many moves.
        #[arg(short = 'n', long = "max-count", value_name = "N", default_value_t = 20)]
        max_count: usize,
    },
    /// Show a commit (default HEAD) and the changes it introduced, like `git show`.
    Show {
        /// The commit (default: HEAD in a working copy, else the default branch).
        #[arg(value_name = "REV")]
        revision: Option<String>,
        /// Only the changed files' names.
        #[arg(long)]
        name_only: bool,
        /// Only the changed files' names and status (A / M / D).
        #[arg(long, conflicts_with = "name_only")]
        name_status: bool,
        /// Per-file counts of changed lines, and totals, as git's `--stat`.
        #[arg(long, conflicts_with_all = ["name_only", "name_status"])]
        stat: bool,
        /// `-- PATH`: only changes under this path.
        #[arg(last = true, value_name = "PATH")]
        paths: Vec<String>,
    },
    /// Follow branches on the server and print each commit as it lands (all branches by default).
    #[command(
        after_help = "One line per commit: BRANCH  ID  AUTHOR  SUBJECT. --json prints one object per move of a \
        branch, with its commits.\n\nExamples:\n  scn watch\n  scn watch main --json\n  \
        scn watch main --repo http://scion.example.com:7080/org/app"
    )]
    Watch {
        /// The branches to follow; all of them when none are given.
        #[arg(value_name = "BRANCH")]
        refs: Vec<String>,
        /// Stop after this many moves of a branch.
        #[arg(long, value_name = "N")]
        count: Option<usize>,
    },
    /// The commit that last changed each line of a file.
    #[command(
        after_help = "Examples:\n  scn blame src/lib.rs\n  scn blame -L 10,20 src/lib.rs\n  \
        scn blame -L 40,+5 v1.0:src/lib.rs"
    )]
    Blame {
        /// The file, optionally at a revision (default: the current branch).
        #[arg(value_name = "[REV:]PATH")]
        spec: String,
        /// Only these lines: N,M, N,+K, N (to the end) or ,M; can be repeated, as in git.
        #[arg(short = 'L', value_name = "RANGE", value_parser = history::parse_line_range)]
        lines: Vec<(usize, usize)>,
    },
    /// Changes: without revisions, the uncommitted local changes; with one, what that commit introduced; with two, the difference between them.
    /// Revisions may be `A B`, `A..B` or `A...B` (from their common ancestor), as in git.
    #[command(
        after_help = "Examples:\n  scn diff                  # uncommitted local changes\n  \
        scn diff main             # what the tip of main changed\n  \
        scn diff v1.0 main        # or v1.0..main; v1.0...main diffs from their common ancestor\n  \
        scn diff HEAD~3 HEAD --name-status -- src"
    )]
    Diff {
        /// None, one or two revisions: `A`, `A B`, `A..B` or `A...B`.
        #[arg(value_name = "REV")]
        revisions: Vec<String>,
        /// Only this path. Same as `-- PATH`.
        #[arg(long)]
        path: Option<String>,
        /// Only the changed files' names.
        #[arg(long)]
        name_only: bool,
        /// Only the changed files' names and status (A / M / D).
        #[arg(long, conflicts_with = "name_only")]
        name_status: bool,
        /// Per-file counts of changed lines, and totals, as git's `--stat`.
        #[arg(long, conflicts_with_all = ["name_only", "name_status"])]
        stat: bool,
        /// Accepted for git compatibility: scn has no staging area, so every local change is
        /// staged and plain `scn diff` shows them all.
        #[arg(long, visible_alias = "staged")]
        cached: bool,
        /// `-- PATH`, as in git.
        #[arg(last = true, value_name = "PATH")]
        paths: Vec<String>,
    },
    /// Import history from elsewhere.
    #[command(subcommand)]
    Import(ImportCmd),
    /// Log in to the server: in a browser (OIDC device flow) by default, or with an existing token (PAT).
    #[command(
        override_usage = "scn login [SERVER] [TOKEN]\n       scn login [SERVER] --with-token < FILE",
        after_help = "Examples:\n  \
            scn login http://scion.example.com:7080             # in a browser\n  \
            scn login http://scion.example.com:7080 scn_pat_…   # with a token (either order)\n  \
            scn login http://scion.example.com:7080 --with-token < token.txt"
    )]
    Login {
        /// The server, http(s)://host[:port], and/or a token (scn_pat_…), in either order.
        /// Without a server: --endpoint, then the working copy's.
        #[arg(num_args = 0..=2, value_name = "SERVER | TOKEN")]
        args: Vec<String>,
        /// Log in with this token instead of a browser (same as giving it as an argument).
        #[arg(long, value_name = "TOKEN", conflicts_with = "with_token")]
        token: Option<String>,
        /// Read the token from standard input (keeps it out of the shell history).
        #[arg(long)]
        with_token: bool,
    },
    /// Delete the saved credentials.
    Logout {
        /// The server, http(s)://host[:port]; instead of --endpoint.
        server: Option<String>,
    },
    /// Show who you are logged in as.
    Whoami {
        /// The server, http(s)://host[:port]; instead of --endpoint.
        server: Option<String>,
    },
    /// The local daemon (L1 cache), started on demand by other commands; `SCION_NO_DAEMON=1` turns it off.
    #[command(subcommand)]
    Daemon(DaemonCmd),
}

#[derive(Debug, Subcommand)]
enum RemoteCmd {
    /// Same as `scn remote -v`.
    Show {
        /// origin (the only remote).
        name: Option<String>,
    },
    /// Not supported: a working copy has one remote (see set-url).
    #[command(hide = true, alias = "remove", alias = "rm", alias = "rename")]
    Add {
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
    /// Print the URL: http(s)://host[:port]/REPOSITORY.
    GetUrl {
        /// origin (the only remote).
        name: Option<String>,
    },
    /// Point the working copy at a new address for its server, or at another repository with the
    /// same commits (a fork, a migrated server). The new one is checked first.
    SetUrl {
        /// `[origin] URL`, as in git.
        #[arg(num_args = 1..=2, required = true, value_name = "URL")]
        args: Vec<String>,
    },
}

#[derive(Debug, Subcommand)]
enum OrgCmd {
    /// Create an organization; you become its owner. It owns the repositories named ORG/….
    #[command(
        after_help = "Examples:\n  scn org create acme\n  scn org create http://scion.example.com:7080/acme --internal"
    )]
    Create {
        /// The name, or its URL http(s)://host[:port]/ORG.
        #[arg(value_name = "ORG | URL")]
        name: String,
        /// Every signed-in user can read its repositories (default: only members).
        #[arg(long)]
        internal: bool,
    },
    /// List organizations.
    Ls {
        /// Only those you belong to.
        #[arg(long)]
        mine: bool,
    },
    /// Show an organization.
    Info {
        /// The name, or its URL.
        #[arg(value_name = "ORG | URL")]
        name: String,
    },
    /// Change who besides the members can read: private or internal.
    Visibility {
        /// The name, or its URL.
        #[arg(value_name = "ORG | URL")]
        name: String,
        /// private (only members) or internal (every signed-in user).
        visibility: String,
    },
    /// Delete an organization (it must have no repositories left).
    Rm {
        /// The name, or its URL.
        #[arg(value_name = "ORG | URL")]
        name: String,
    },
    /// Members: add (or change the role of), remove, list.
    #[command(subcommand)]
    Member(MemberCmd),
}

#[derive(Debug, Subcommand)]
enum MemberCmd {
    /// Add a member, or change a member's role.
    #[command(
        after_help = "Examples:\n  scn org member add acme bob --role developer\n  scn org member add acme bob --role owner"
    )]
    Add {
        /// The organization, or its URL.
        #[arg(value_name = "ORG | URL")]
        org: String,
        /// The member's subject (their user name).
        subject: String,
        /// owner (admin), maintainer (land), developer (write) or reporter (read).
        #[arg(long, default_value = "developer")]
        role: String,
    },
    /// Remove a member; you can remove yourself.
    Rm {
        /// The organization, or its URL.
        #[arg(value_name = "ORG | URL")]
        org: String,
        /// The member's subject.
        subject: String,
    },
    /// List the members.
    Ls {
        /// The organization, or its URL.
        #[arg(value_name = "ORG | URL")]
        org: String,
    },
}

#[derive(Debug, Subcommand)]
enum RepoCmd {
    /// Create a repository.
    Create {
        /// The name (such as org/app), or the repository's URL.
        #[arg(value_name = "NAME | URL")]
        name: String,
        /// Default branch (main if omitted).
        #[arg(long, value_name = "BRANCH")]
        default_branch: Option<String>,
        /// A one-line description.
        #[arg(long, value_name = "TEXT", default_value = "")]
        description: String,
    },
    /// List the repositories you can see.
    Ls {
        /// Name prefix, such as opencsg/.
        #[arg(default_value = "")]
        prefix: String,
        /// List deleted repositories that are not purged yet.
        #[arg(long)]
        deleted: bool,
        /// At most this many.
        #[arg(long, value_name = "N", default_value_t = repos::DEFAULT_LIST_LIMIT)]
        limit: usize,
    },
    /// Show a repository's settings (also for deleted repositories).
    Info {
        /// The name, or the repository's URL.
        #[arg(value_name = "NAME | URL")]
        name: String,
    },
    /// Change the default branch or the description.
    Set {
        /// The name, or the repository's URL.
        #[arg(value_name = "NAME | URL")]
        name: String,
        /// The new default branch (it must exist).
        #[arg(long, value_name = "BRANCH")]
        default_branch: Option<String>,
        /// The new description.
        #[arg(long, value_name = "TEXT")]
        description: Option<String>,
    },
    /// Delete a repository (restorable during the grace period).
    Rm {
        /// The name, or the repository's URL.
        #[arg(value_name = "NAME | URL")]
        name: String,
    },
    /// Restore a deleted repository.
    Restore {
        /// The name, or the repository's URL.
        #[arg(value_name = "NAME | URL")]
        name: String,
    },
    /// Fork: copy all history and branches into a new repository.
    #[command(after_help = "Example:\n  scn repo fork org/app me/app\n  \
        scn repo fork http://scion.example.com:7080/org/app http://scion.example.com:7080/me/app")]
    Fork {
        /// The repository to copy (name or URL).
        source: String,
        /// The new repository (name or URL, on the same server).
        target: String,
    },
}

#[derive(Debug, Subcommand)]
enum ConfigCmd {
    /// Print every setting.
    List,
    /// Print one setting.
    Get {
        /// user.name or user.email.
        key: String,
    },
    /// Change one setting.
    #[command(
        after_help = "Examples:\n  scn config set user.name \"Ada Lovelace\"\n  scn config set user.email ada@example.com"
    )]
    Set {
        /// user.name or user.email.
        key: String,
        /// The new value.
        value: String,
    },
    /// Remove one setting.
    Unset {
        /// user.name or user.email.
        key: String,
    },
}

#[derive(Debug, Subcommand)]
enum StashCmd {
    /// Save every local change and put the tree back to the commit (what `scn stash` does).
    Push {
        /// The entry's message (default: WIP on BRANCH: ID SUBJECT).
        #[arg(short, long)]
        message: Option<String>,
    },
    /// List the entries, newest first.
    List,
    /// Show an entry's files and changed lines (-p: the diff).
    Show {
        /// stash@{N} or N (default: the newest).
        stash: Option<String>,
        /// The diff instead of the per-file counts.
        #[arg(short = 'p', long)]
        patch: bool,
    },
    /// Apply an entry as local changes and drop it (kept when it conflicts).
    Pop {
        /// stash@{N} or N (default: the newest).
        stash: Option<String>,
    },
    /// Apply an entry as local changes and keep it.
    Apply {
        /// stash@{N} or N (default: the newest).
        stash: Option<String>,
    },
    /// Forget an entry.
    Drop {
        /// stash@{N} or N (default: the newest).
        stash: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
enum BranchCmd {
    /// List branches (* marks the current branch; outside a working copy, the default branch).
    Ls,
    /// Create a branch.
    Create {
        /// The new branch.
        name: String,
        /// Where it starts (default: the current branch).
        #[arg(value_name = "REV")]
        revision: Option<String>,
    },
    /// Delete a branch (not the default branch).
    Rm {
        /// The branch to delete.
        name: String,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum SparseCmd {
    /// List the directories in the checkout.
    Ls,
    /// Set the directories (turns a full checkout into a sparse one).
    Set {
        /// Directories, relative to the repository root.
        #[arg(required = true, value_name = "DIR")]
        dirs: Vec<String>,
    },
    /// Add directories and check them out.
    Add {
        /// Directories, relative to the repository root.
        #[arg(required = true, value_name = "DIR")]
        dirs: Vec<String>,
    },
    /// Remove directories and their files (needs no uncommitted changes).
    Rm {
        /// Directories, relative to the repository root.
        #[arg(required = true, value_name = "DIR")]
        dirs: Vec<String>,
    },
    /// Go back to a full checkout.
    Disable,
}

#[derive(Debug, Subcommand)]
enum DaemonCmd {
    /// Run the daemon in the foreground.
    Run {
        /// Disk cache limit in bytes.
        #[arg(long, value_name = "BYTES")]
        max_disk: Option<u64>,
        /// Exit after this many idle seconds; 0 runs forever.
        #[arg(long, value_name = "SECS")]
        idle_timeout: Option<u64>,
    },
    /// Show whether the daemon is running.
    Status,
    /// Stop the daemon.
    Stop,
}

#[derive(Debug, Subcommand)]
enum ImportCmd {
    /// Import the history of a local git repository into the --repo repository.
    #[command(after_help = "Examples:\n  scn import git ~/src/app --repo org/app\n  \
        scn import git ~/src/app --repo http://scion.example.com:7080/org/app --git-ref dev --ref dev\n  \
        scn import git ~/src/app --repo org/app --git-ref v1.0 --ref tags/v1.0")]
    Git {
        /// The git repository directory.
        path: PathBuf,
        /// The git ref to import.
        #[arg(long, value_name = "REF", default_value = "main")]
        git_ref: String,
        /// The Scion ref to update after importing.
        #[arg(long = "ref", value_name = "REF", default_value = "main")]
        reference: String,
    },
}

/// A git spelling accepted while people move over: says which scn command to use instead.
fn transitional(git: &str, scn: &str) {
    eprintln!("hint: `scn {git}` is a transitional git-style spelling; use `scn {scn}` instead");
}

/// `git push origin main` / `git pull origin main`: the remote must be origin and the branch the
/// working copy's (land and sync work on that one).
fn remote_and_branch(ctx: &Ctx, args: &[String]) -> Result<(), CliError> {
    remote::check_name(args.first().map(String::as_str))?;
    if let Some(branch) = args.get(1) {
        let current = scion_wc::WorkingCopy::open(&ctx.cwd)?.state().reference.clone();
        if *branch != current {
            return Err(CliError::Usage(format!(
                "the working copy is on {current}; `scn switch {branch}` first"
            )));
        }
    }
    Ok(())
}

/// Several `-m` make one message of paragraphs, as in git.
fn paragraphs(messages: &[String]) -> Option<String> {
    (!messages.is_empty()).then(|| messages.join("\n\n"))
}

/// git's `log -5` is `log -n 5`.
fn git_count_args(args: impl IntoIterator<Item = std::ffi::OsString>) -> Vec<std::ffi::OsString> {
    let args: Vec<std::ffi::OsString> = args.into_iter().collect();
    // The subcommand: the first word that is not an option or a global option's value
    let mut i = 1;
    while let Some(a) = args.get(i).and_then(|a| a.to_str()) {
        match a {
            "--endpoint" | "--repo" => i += 2,
            a if a.starts_with('-') => i += 1,
            _ => break,
        }
    }
    if args.get(i).and_then(|a| a.to_str()) != Some("log") {
        return args;
    }
    let mut out: Vec<std::ffi::OsString> = Vec::with_capacity(args.len() + 1);
    let mut rest = args.into_iter();
    out.extend(rest.by_ref().take(i + 1));
    for a in rest.by_ref() {
        match a.to_str() {
            Some("--") => {
                out.push(a);
                break;
            }
            Some(s) if s.len() > 1 && s.starts_with('-') && s[1..].bytes().all(|b| b.is_ascii_digit()) => {
                out.push("-n".into());
                out.push(s[1..].into());
            }
            _ => out.push(a),
        }
    }
    out.extend(rest);
    out
}

/// `scn login`'s arguments, in either order: the one that is an http(s) URL is the server, the
/// other the token.
fn login_args(args: &[String]) -> Result<(Option<&str>, Option<&str>), CliError> {
    let is_url = |a: &str| a.starts_with("http://") || a.starts_with("https://");
    let (urls, tokens): (Vec<&str>, Vec<&str>) = args.iter().map(String::as_str).partition(|a| is_url(a));
    match (urls.as_slice(), tokens.as_slice()) {
        ([_, _], _) => Err(CliError::Usage("give one server URL".into())),
        (_, [_, _]) => Err(CliError::Usage(
            "expected a server URL, http(s)://host[:port], and a token".into(),
        )),
        (u, t) => Ok((u.first().copied(), t.first().copied())),
    }
}

/// `--path P` or git's `-- P`; history queries filter by one path.
fn one_path(flag: Option<String>, rest: Vec<String>) -> Result<Option<String>, CliError> {
    match (flag, rest.as_slice()) {
        (p, []) => Ok(p),
        (None, [p]) => Ok(Some(p.trim_end_matches('/').to_owned())),
        _ => Err(CliError::Usage("give one path, with --path or after --".into())),
    }
}

fn emit<T: Render>(v: Result<T, CliError>, json: bool) -> ExitCode {
    match v {
        Ok(v) => {
            // Like git, commands with nothing to say print nothing
            let text = render(&v, json);
            if !text.is_empty() {
                println!("{text}");
            }
            ExitCode::SUCCESS
        }
        Err(e) => fail(&e, json),
    }
}

fn fail(e: &CliError, json: bool) -> ExitCode {
    let out = error_out(e);
    if json {
        println!("{}", serde_json::to_string(&out).unwrap_or_default());
    } else {
        // Like git, the conflicts come first
        for c in &out.conflicts {
            eprintln!(
                "CONFLICT ({}): Merge conflict in {}",
                output::git_conflict_kind(&c.kind),
                c.path
            );
        }
        eprintln!("scn: {}", out.error);
        if let Some(h) = out.hint {
            eprintln!("hint: {h}");
        }
    }
    ExitCode::from(out.exit_code)
}

async fn run(cli: Cli) -> ExitCode {
    let cwd = match std::env::current_dir() {
        Ok(d) => d,
        Err(e) => {
            return fail(
                &CliError::Usage(format!("cannot read current directory: {e}")),
                cli.json,
            );
        }
    };
    let json = cli.json;
    let mut ctx = Ctx {
        endpoint: cli.endpoint.clone(),
        repo: None,
        cwd,
        chatty: !(json || cli.quiet),
    };
    // A repository or server given as a URL replaces --endpoint for this command.
    macro_rules! arg {
        ($e:expr) => {
            match $e {
                Ok(v) => v,
                Err(e) => return fail(&e, json),
            }
        };
    }
    if let Some(r) = &cli.repo {
        ctx.repo = Some(arg!(ctx.named_repo_arg(r)));
    }
    match cli.command {
        Command::Version => emit(Ok(version_out()), json),
        Command::Doctor { server } => {
            if let Some(s) = server {
                arg!(ctx.server_arg(&s));
            }
            emit(commands::doctor(&ctx).await, json)
        }
        Command::Clone {
            repo,
            dir,
            reference,
            sparse,
            ..
        } => {
            let repo = arg!(ctx.named_repo_arg(&repo));
            emit(
                commands::clone(&ctx, &repo, dir.as_deref(), &reference, sparse.as_deref()).await,
                json,
            )
        }
        Command::Sparse(cmd) => emit(commands::sparse(&ctx, cmd).await, json),
        Command::Remote { verbose, cmd } => match cmd {
            None => emit(
                remote::show(
                    &ctx,
                    if verbose {
                        remote::Show::Verbose
                    } else {
                        remote::Show::Name
                    },
                ),
                json,
            ),
            Some(RemoteCmd::Show { name }) => {
                arg!(remote::check_name(name.as_deref()));
                emit(remote::show(&ctx, remote::Show::Verbose), json)
            }
            Some(RemoteCmd::Add { .. }) => fail(
                &CliError::Usage(
                    "a working copy has one remote, origin; point it elsewhere with `scn remote set-url URL`, \
                     or clone the other repository"
                        .into(),
                ),
                json,
            ),
            Some(RemoteCmd::GetUrl { name }) => {
                arg!(remote::check_name(name.as_deref()));
                emit(remote::show(&ctx, remote::Show::Url), json)
            }
            Some(RemoteCmd::SetUrl { args }) => {
                let (name, url) = match args.as_slice() {
                    [name, url] => (Some(name.as_str()), url.as_str()),
                    _ => (None, args[0].as_str()),
                };
                arg!(remote::check_name(name));
                emit(remote::set_url(&ctx, url).await, json)
            }
        },
        Command::Org(cmd) => match cmd {
            OrgCmd::Create { name, internal } => {
                let name = arg!(ctx.named_repo_arg(&name));
                emit(
                    orgs::create(&ctx, &name, if internal { "internal" } else { "private" }).await,
                    json,
                )
            }
            OrgCmd::Ls { mine } => emit(orgs::list(&ctx, mine).await, json),
            OrgCmd::Info { name } => {
                let name = arg!(ctx.named_repo_arg(&name));
                emit(orgs::info(&ctx, &name).await, json)
            }
            OrgCmd::Visibility { name, visibility } => {
                let name = arg!(ctx.named_repo_arg(&name));
                emit(orgs::set_visibility(&ctx, &name, &visibility).await, json)
            }
            OrgCmd::Rm { name } => {
                let name = arg!(ctx.named_repo_arg(&name));
                emit(orgs::delete(&ctx, &name).await, json)
            }
            OrgCmd::Member(MemberCmd::Add { org, subject, role }) => {
                let org = arg!(ctx.named_repo_arg(&org));
                emit(orgs::add_member(&ctx, &org, &subject, &role).await, json)
            }
            OrgCmd::Member(MemberCmd::Rm { org, subject }) => {
                let org = arg!(ctx.named_repo_arg(&org));
                emit(orgs::remove_member(&ctx, &org, &subject).await, json)
            }
            OrgCmd::Member(MemberCmd::Ls { org }) => {
                let org = arg!(ctx.named_repo_arg(&org));
                emit(orgs::members(&ctx, &org).await, json)
            }
        },
        Command::Repo(cmd) => match cmd {
            RepoCmd::Create {
                name,
                default_branch,
                description,
            } => {
                let name = arg!(ctx.named_repo_arg(&name));
                emit(
                    repos::create(&ctx, &name, default_branch.as_deref(), &description).await,
                    json,
                )
            }
            RepoCmd::Ls { prefix, deleted, limit } => {
                let prefix = arg!(ctx.repo_arg(&prefix));
                emit(repos::list(&ctx, &prefix, deleted, limit).await, json)
            }
            RepoCmd::Info { name } => {
                let name = arg!(ctx.named_repo_arg(&name));
                emit(repos::info(&ctx, &name).await, json)
            }
            RepoCmd::Set {
                name,
                default_branch,
                description,
            } => {
                let name = arg!(ctx.named_repo_arg(&name));
                emit(
                    repos::update(&ctx, &name, default_branch.as_deref(), description.as_deref()).await,
                    json,
                )
            }
            RepoCmd::Rm { name } => {
                let name = arg!(ctx.named_repo_arg(&name));
                emit(repos::delete(&ctx, &name).await, json)
            }
            RepoCmd::Restore { name } => {
                let name = arg!(ctx.named_repo_arg(&name));
                emit(repos::restore(&ctx, &name).await, json)
            }
            RepoCmd::Fork { source, target } => {
                let (source, target) = arg!(ctx.fork_args(&source, &target));
                emit(repos::fork(&ctx, &source, &target).await, json)
            }
        },
        Command::Branch { show_current: true, .. } => emit(repos::current_branch(&ctx), json),
        Command::Branch {
            cmd,
            name,
            revision,
            delete,
            verbose,
            ..
        } => match (cmd, name, delete) {
            (Some(BranchCmd::Ls), _, _) | (None, None, None) => emit(
                repos::branches(&ctx)
                    .await
                    .map(|b| repos::BranchListOut { verbose, ..b }),
                json,
            ),
            (Some(BranchCmd::Create { name, revision }), _, _) => {
                emit(repos::create_branch(&ctx, &name, revision.as_deref()).await, json)
            }
            (None, Some(name), _) => emit(repos::create_branch(&ctx, &name, revision.as_deref()).await, json),
            (Some(BranchCmd::Rm { name }), _, _) | (None, None, Some(name)) => {
                emit(repos::delete_branch(&ctx, &name).await, json)
            }
        },
        Command::Tag {
            name, revision, delete, ..
        } => match (name, delete) {
            (Some(name), _) => emit(repos::create_tag(&ctx, &name, revision.as_deref()).await, json),
            (None, Some(name)) => emit(repos::delete_tag(&ctx, &name).await, json),
            (None, None) => emit(repos::tags(&ctx).await, json),
        },
        Command::Status {
            short,
            branch,
            porcelain,
            no_ahead_behind,
        } => emit(
            // --porcelain paths are relative to the root, as in git. The short format shows the
            // server's branch only with -b, so only then is it asked
            commands::status(
                &ctx,
                short || porcelain,
                !no_ahead_behind && (branch || !(short || porcelain)),
            )
            .await
            .map(|s| commands::StatusOut {
                branch,
                cwd: if porcelain { String::new() } else { s.cwd.clone() },
                ..s
            }),
            json,
        ),
        Command::Commit {
            ref paths, all, amend, ..
        } if !paths.is_empty() && (all || amend) => fail(
            &CliError::Usage(format!(
                "paths with {} do not make sense: {}",
                if all { "-a" } else { "--amend" },
                if all {
                    "-a commits every change, paths only the changes in them"
                } else {
                    "--amend takes every local change; commit the paths on their own first"
                }
            )),
            json,
        ),
        Command::Commit {
            message,
            amend: true,
            author,
            ..
        } => emit(
            commands::amend(&ctx, paragraphs(&message).as_deref(), author.as_deref()).await,
            json,
        ),
        Command::Commit {
            message, author, paths, ..
        } => emit(
            commands::commit_paths(&ctx, paragraphs(&message).as_deref(), author.as_deref(), &paths).await,
            json,
        ),
        Command::Uncommit => emit(commands::uncommit(&ctx).await, json),
        Command::Switch {
            reference,
            detach: true,
            ..
        } => emit(commands::switch_detach(&ctx, &reference).await, json),
        Command::Switch { reference, create, .. } => emit(commands::switch(&ctx, &reference, create).await, json),
        Command::Merge { abort: true, .. } => emit(commands::abort_merge(&ctx).await, json),
        Command::Revert { abort: true, .. } => emit(commands::abort_pick(&ctx, scion_wc::PickKind::Revert).await, json),
        Command::Revert { cont: true, .. } => {
            transitional("revert --continue", "commit");
            emit(commands::commit(&ctx, None, None).await, json)
        }
        Command::Revert {
            revision,
            mainline,
            author,
            ..
        } => emit(
            commands::pick(
                &ctx,
                scion_wc::PickKind::Revert,
                revision.as_deref().unwrap_or_default(),
                mainline,
                author.as_deref(),
                false,
            )
            .await,
            json,
        ),
        Command::CherryPick { abort: true, .. } => {
            emit(commands::abort_pick(&ctx, scion_wc::PickKind::CherryPick).await, json)
        }
        Command::CherryPick { cont: true, .. } => {
            transitional("cherry-pick --continue", "commit");
            emit(commands::commit(&ctx, None, None).await, json)
        }
        Command::CherryPick {
            revision,
            mainline,
            record_origin,
            author,
            ..
        } => emit(
            commands::pick(
                &ctx,
                scion_wc::PickKind::CherryPick,
                revision.as_deref().unwrap_or_default(),
                mainline,
                author.as_deref(),
                record_origin,
            )
            .await,
            json,
        ),
        Command::Merge { cont: true, .. } => {
            transitional("merge --continue", "commit");
            emit(commands::commit(&ctx, None, None).await, json)
        }
        Command::Push { force: true, .. } => fail(
            &CliError::Usage(
                "scn land never rewrites a branch, so there is no force push; \
                 `scn uncommit` or `scn commit --amend` change a commit before it lands"
                    .into(),
            ),
            json,
        ),
        Command::Push { args, .. } => {
            arg!(remote_and_branch(&ctx, &args));
            transitional("push", "land");
            emit(commands::land(&ctx).await, json)
        }
        Command::Pull { args, ff_only, .. } => {
            arg!(remote_and_branch(&ctx, &args));
            if ff_only && scion_wc::WorkingCopy::open(&ctx.cwd).is_ok_and(|w| w.state().head.is_some()) {
                return fail(
                    &CliError::Usage(
                        "not possible to fast-forward: you have a pending commit, which `scn sync` would rebase".into(),
                    ),
                    json,
                );
            }
            transitional("pull", "sync");
            emit(commands::sync(&ctx).await, json)
        }
        Command::Checkout { create, target, paths } => match (create, target, paths.is_empty()) {
            // git checkout REV -- PATH: the files from REV
            (false, Some(rev), false) => {
                transitional("checkout REV -- PATH", "restore --source REV PATH");
                emit(commands::restore(&ctx, &paths, Some(&rev)).await, json)
            }
            (false, None, false) => {
                transitional("checkout -- PATH", "restore PATH");
                emit(commands::restore(&ctx, &paths, None).await, json)
            }
            (true, Some(name), true) => {
                transitional("checkout -b NAME", "switch -c NAME");
                emit(commands::switch(&ctx, &name, true).await, json)
            }
            // A path in the current directory restores it, anything else is a branch
            (false, Some(t), true) if ctx.cwd.join(&t).exists() => {
                transitional("checkout PATH", "restore PATH");
                emit(commands::restore(&ctx, &[t], None).await, json)
            }
            // A branch, else any revision on no branch
            (false, Some(name), true) => match commands::switch(&ctx, &name, false).await {
                Err(CliError::Wc(scion_wc::Error::RefNotFound(_))) => {
                    transitional("checkout REV", "switch --detach REV");
                    emit(commands::switch_detach(&ctx, &name).await, json)
                }
                r => {
                    transitional("checkout BRANCH", "switch BRANCH");
                    emit(r, json)
                }
            },
            _ => fail(
                &CliError::Usage(
                    "scn supports `checkout BRANCH`, `checkout -b NAME` and `checkout -- PATH`; \
                     use `scn switch` / `scn restore`"
                        .into(),
                ),
                json,
            ),
        },
        Command::Reset { soft, hard, target } => {
            let head = matches!(target.as_deref(), None | Some("HEAD" | "@"));
            let parent = matches!(
                target.as_deref(),
                None | Some("HEAD~1" | "HEAD~" | "HEAD^" | "@~1" | "@^")
            );
            if soft && parent {
                transitional("reset --soft HEAD~1", "uncommit");
                emit(commands::uncommit(&ctx).await, json)
            } else if hard && head {
                transitional("reset --hard", "restore .");
                emit(commands::restore(&ctx, &[".".to_owned()], None).await, json)
            } else {
                fail(
                    &CliError::Usage(
                        "scn maps only `reset --soft HEAD~1` (scn uncommit) and `reset --hard` (scn restore .)".into(),
                    ),
                    json,
                )
            }
        }
        Command::Rebase { cont, abort } => {
            if cont {
                transitional("rebase --continue", "commit");
                emit(commands::commit(&ctx, None, None).await, json)
            } else if abort {
                transitional("rebase --abort", "sync --abort");
                emit(commands::abort_sync(&ctx).await, json)
            } else {
                fail(
                    &CliError::Usage("scn rebases with `scn sync` (and on the server during `scn land`)".into()),
                    json,
                )
            }
        }
        Command::Add { .. } => {
            transitional("add", "commit");
            eprintln!(
                "note: there is no staging area; `scn commit` takes every local change, `scn commit PATH...` \
                 only the changes in those paths"
            );
            ExitCode::SUCCESS
        }
        Command::Init { .. } => fail(
            &CliError::Usage("repositories live on the server: `scn repo create NAME`, then `scn clone URL`".into()),
            json,
        ),
        Command::Fetch { .. } => fail(
            &CliError::Usage(
                "there is no local history to fetch into: commands read the server directly, so \
                 `scn log main` (or `origin/main`) and `scn diff HEAD main` show what others landed; \
                 `scn sync` updates the working copy"
                    .into(),
            ),
            json,
        ),
        Command::Rm { paths, recursive } => {
            transitional("rm", "commit (after deleting the files yourself)");
            let mut first_error = None;
            for p in &paths {
                let path = ctx.cwd.join(p);
                let r = if recursive && path.is_dir() {
                    std::fs::remove_dir_all(&path)
                } else {
                    std::fs::remove_file(&path)
                };
                if let Err(e) = r {
                    first_error.get_or_insert_with(|| CliError::Usage(format!("rm {p}: {e}")));
                }
            }
            match first_error {
                Some(e) => fail(&e, json),
                None => ExitCode::SUCCESS,
            }
        }
        Command::Mv { from, to, .. } => {
            transitional("mv", "commit (after moving the files yourself)");
            let (from, to) = (ctx.cwd.join(from), ctx.cwd.join(to));
            let to = if to.is_dir() {
                match from.file_name() {
                    Some(name) => to.join(name),
                    None => to,
                }
            } else {
                to
            };
            match std::fs::rename(&from, &to) {
                Ok(()) => ExitCode::SUCCESS,
                Err(e) => fail(&CliError::Usage(format!("mv {}: {e}", from.display())), json),
            }
        }
        Command::Merge {
            reference,
            message,
            author,
            ..
        } => emit(
            commands::merge(
                &ctx,
                reference.as_deref().unwrap_or_default(),
                message.as_deref(),
                author.as_deref(),
            )
            .await,
            json,
        ),
        Command::Resolve {
            paths,
            ours,
            theirs,
            force,
        } => {
            let side = match (ours, theirs) {
                (true, _) => Some(scion_wc::Side::Ours),
                (_, true) => Some(scion_wc::Side::Theirs),
                _ => None,
            };
            emit(commands::resolve(&ctx, &paths, side, force).await, json)
        }
        Command::Restore { paths, source } => emit(commands::restore(&ctx, &paths, source.as_deref()).await, json),
        Command::Stash { cmd, message } => match cmd {
            None => emit(commands::stash_push(&ctx, message.as_deref()).await, json),
            Some(StashCmd::Push { message }) => emit(commands::stash_push(&ctx, message.as_deref()).await, json),
            Some(StashCmd::List) => emit(commands::stash_list(&ctx), json),
            Some(StashCmd::Pop { stash }) => emit(commands::stash_apply(&ctx, stash.as_deref(), true).await, json),
            Some(StashCmd::Apply { stash }) => emit(commands::stash_apply(&ctx, stash.as_deref(), false).await, json),
            Some(StashCmd::Drop { stash }) => emit(commands::stash_drop(&ctx, stash.as_deref()), json),
            Some(StashCmd::Show { stash, patch }) => match commands::stash_commit(&ctx, stash.as_deref()) {
                Ok(commit) => emit(
                    history::show(&ctx, Some(&commit), None, false).await.map(|mut s| {
                        s.diff.stat = !patch;
                        s
                    }),
                    json,
                ),
                Err(e) => fail(&e, json),
            },
        },
        Command::Clean {
            dry_run: false,
            force: false,
            ..
        } => fail(
            &CliError::Usage("scn clean deletes files: pass -f to remove them, or -n to list them first".into()),
            json,
        ),
        Command::Clean {
            dry_run,
            ignored,
            paths,
            ..
        } => emit(commands::clean(&ctx, &paths, ignored, dry_run).await, json),
        Command::Config {
            cmd, key, value, unset, ..
        } => {
            let path = config::path(|k| std::env::var(k).ok());
            let cmd = match (cmd, key, value, unset) {
                (Some(cmd), ..) => cmd,
                (None, _, _, Some(key)) => ConfigCmd::Unset { key },
                (None, Some(key), Some(value), None) => ConfigCmd::Set { key, value },
                (None, Some(key), None, None) => ConfigCmd::Get { key },
                // `--list`, or nothing at all
                _ => ConfigCmd::List,
            };
            emit(
                match cmd {
                    ConfigCmd::List => config::list(&path),
                    ConfigCmd::Get { key } => config::get(&path, &key),
                    ConfigCmd::Set { key, value } => config::set(&path, &key, &value),
                    ConfigCmd::Unset { key } => config::unset(&path, &key),
                },
                json,
            )
        }
        Command::Land => emit(commands::land(&ctx).await, json),
        Command::Sync { abort: false } => emit(commands::sync(&ctx).await, json),
        Command::Sync { abort: true } => emit(commands::abort_sync(&ctx).await, json),
        Command::Ls { spec } => emit(commands::ls(&ctx, spec.as_deref()).await, json),
        Command::Archive {
            spec,
            format,
            prefix,
            output,
        } => {
            let mut stdout = std::io::stdout();
            let r = commands::archive(&ctx, spec.as_deref(), &format, &prefix, output.as_deref(), &mut stdout).await;
            match r {
                // 写到标准输出时不再输出摘要（会混进归档内容）
                Ok(_) if output.is_none() => match std::io::Write::flush(&mut stdout) {
                    Ok(()) => ExitCode::SUCCESS,
                    Err(e) => fail(&CliError::Usage(format!("write stdout: {e}")), json),
                },
                other => emit(other, json),
            }
        }
        Command::Log {
            revision,
            path,
            max_count,
            oneline,
            pretty,
            paths,
        } => match one_path(path, paths) {
            Ok(path) => emit(
                history::log(
                    &ctx,
                    revision.as_deref(),
                    path.as_deref(),
                    max_count,
                    oneline || pretty.as_deref() == Some("oneline"),
                )
                .await,
                json,
            ),
            Err(e) => fail(&e, json),
        },
        Command::Show {
            revision,
            name_only,
            name_status,
            stat,
            paths,
        } => match one_path(None, paths) {
            Ok(path) => emit(
                history::show(&ctx, revision.as_deref(), path.as_deref(), name_only || name_status)
                    .await
                    .map(|mut s| {
                        s.diff.name_status = name_status;
                        s.diff.stat = stat;
                        s
                    }),
                json,
            ),
            Err(e) => fail(&e, json),
        },
        Command::Blame { spec, lines } => emit(history::blame(&ctx, &spec, &lines).await, json),
        Command::Reflog { reference, max_count } => {
            emit(history::reflog(&ctx, reference.as_deref(), max_count).await, json)
        }
        Command::Watch { refs, count } => {
            let quiet = cli.quiet;
            let mut start = |tips: &[watch::Tip]| {
                if !(json || quiet) {
                    let at: Vec<String> = tips
                        .iter()
                        .map(|t| format!("{} at {}", t.name, &t.id[..t.id.len().min(12)]))
                        .collect();
                    eprintln!(
                        "Watching {}; Ctrl-C stops",
                        if at.is_empty() {
                            "for new branches".to_owned()
                        } else {
                            at.join(", ")
                        }
                    );
                }
            };
            let mut print = |u: &watch::RefUpdate| {
                let text = render(u, json);
                if !text.is_empty() {
                    println!("{text}");
                }
            };
            match watch::watch(&ctx, &refs, count, &mut start, &mut print).await {
                Ok(()) => ExitCode::SUCCESS,
                Err(e) => fail(&e, json),
            }
        }
        Command::Diff {
            revisions,
            path,
            name_only,
            name_status,
            stat,
            paths,
            ..
        } => match one_path(path, paths) {
            Ok(path) => emit(
                history::diff(&ctx, &revisions, path.as_deref(), name_only || name_status)
                    .await
                    .map(|d| DiffOut { name_status, stat, ..d }),
                json,
            ),
            Err(e) => fail(&e, json),
        },
        Command::Login {
            args,
            token,
            with_token,
        } => {
            let (server, positional) = arg!(login_args(&args));
            if let Some(s) = server {
                arg!(ctx.server_arg(s));
            }
            let token = match (token, positional) {
                (Some(_), Some(_)) => {
                    return fail(
                        &CliError::Usage("give the token once: as an argument or with --token".into()),
                        json,
                    );
                }
                (_, Some(_)) if with_token => {
                    return fail(
                        &CliError::Usage("give the token as an argument or with --with-token, not both".into()),
                        json,
                    );
                }
                (t, p) => t.or_else(|| p.map(str::to_owned)),
            };
            let token = if with_token {
                let mut s = String::new();
                match std::io::Read::read_to_string(&mut std::io::stdin(), &mut s) {
                    Ok(_) => Some(s),
                    Err(e) => return fail(&CliError::Usage(format!("read stdin: {e}")), json),
                }
            } else {
                token
            };
            match token {
                Some(t) => emit(login::login_with_token(&ctx, &t).await, json),
                None => emit(login::login_device(&ctx, |m| eprintln!("{m}")).await, json),
            }
        }
        Command::Logout { server } => {
            if let Some(s) = server {
                arg!(ctx.server_arg(&s));
            }
            emit(login::logout(&ctx), json)
        }
        Command::Whoami { server } => {
            if let Some(s) = server {
                arg!(ctx.server_arg(&s));
            }
            emit(login::whoami(&ctx).await, json)
        }
        Command::Daemon(cmd) => {
            let mut cfg = daemon::config(|k| std::env::var(k).ok());
            match cmd {
                DaemonCmd::Run { max_disk, idle_timeout } => {
                    if let Some(b) = max_disk {
                        cfg.disk_bytes = b;
                    }
                    if let Some(t) = idle_timeout {
                        cfg.idle_timeout = (t > 0).then(|| std::time::Duration::from_secs(t));
                    }
                    match daemon::run(cfg).await {
                        Ok(()) => ExitCode::SUCCESS,
                        Err(e) => fail(&e, json),
                    }
                }
                DaemonCmd::Status => emit(Ok(daemon::status(&cfg).await), json),
                DaemonCmd::Stop => emit(daemon::stop(&cfg).await, json),
            }
        }
        Command::Import(ImportCmd::Git {
            path,
            git_ref,
            reference,
        }) => emit(commands::import_git(&ctx, &path, &git_ref, &reference).await, json),
        // 文本模式下原样输出字节（文件可能不是文本）
        Command::Cat { spec } => match commands::cat(&ctx, &spec).await {
            Ok(out) if !json => {
                let mut stdout = std::io::stdout().lock();
                match stdout.write_all(&out.data).and_then(|()| stdout.flush()) {
                    Ok(()) => ExitCode::SUCCESS,
                    Err(e) => fail(&CliError::Usage(format!("write stdout: {e}")), false),
                }
            }
            other => emit(other, json),
        },
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    // clap 对用法错误以退出码 2 结束，符合约定
    let matches = command().get_matches_from(git_count_args(std::env::args_os()));
    run(Cli::from_arg_matches(&matches).unwrap_or_else(|e| e.exit())).await
}

/// `scn --help` lists the commands by what they are for, like `git --help`.
const COMMANDS: &str = "\
Start a working copy:
  clone      Check out a repository into a new directory
  sparse     Choose the directories a sparse checkout has

Work on a change:
  status     Show local changes
  diff       Show changes: local, of a commit, or between two revisions
  commit     Commit the local changes (published by land)
  uncommit   Drop the newest pending commit, keeping its changes
  restore    Discard local changes to files
  clean      Remove untracked files
  stash      Put local changes aside and bring them back
  resolve    Mark conflicts resolved, or take one side

Share and update:
  land       Land the pending commits on the branch (git push)
  sync       Update to the branch tip, rebasing pending commits (git pull)
  remote     Show or change the working copy's server and repository

Branches and history:
  branch     List, create or delete branches
  tag        List, create or delete tags
  switch     Move the working copy to another branch
  merge      Merge another branch into this one
  revert     Undo a commit's changes as a new commit
  cherry-pick Apply another commit's changes here
  log        Show the history
  show       Show a commit and its changes
  blame      Show the commit that last changed each line
  reflog     Show how a branch moved on the server
  watch      Follow branches on the server as commits land

Read without a working copy:
  cat        Print a file at a revision
  ls         List a directory at a revision
  archive    Download a revision or directory as tar.gz, zip or tar

Repositories, accounts and setup:
  repo       Create, list, change, delete, restore or fork repositories
  org        Organizations and their members
  import     Import the history of a git repository
  login      Log in, in a browser or with a token
  logout     Delete the saved credentials
  whoami     Show who you are logged in as
  config     Set the default commit author
  doctor     Check the connection to a server
  daemon     The local cache daemon
  version    Print the client and protocol versions

git spellings (add, rm, mv, push, pull, checkout, reset, rebase) work too and say what to use instead.
`scn help COMMAND` or `scn COMMAND --help` shows a command's options and examples.";

fn command() -> clap::Command {
    use clap::CommandFactory;
    Cli::command().help_template(format!(
        "{{about-with-newline}}\n{{usage-heading}} {{usage}}\n\n{COMMANDS}\n\nOptions:\n{{options}}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn cli_definition_is_valid() {
        Cli::command().debug_assert();
    }

    #[test]
    fn git_spellings_of_options() {
        let os = |a: &[&str]| a.iter().map(std::ffi::OsString::from).collect::<Vec<_>>();
        assert_eq!(
            git_count_args(os(&["scn", "log", "-5", "main"])),
            os(&["scn", "log", "-n", "5", "main"])
        );
        assert_eq!(
            git_count_args(os(&["scn", "--repo", "log", "log", "-3", "--", "-4"])),
            os(&["scn", "--repo", "log", "log", "-n", "3", "--", "-4"])
        );
        assert_eq!(git_count_args(os(&["scn", "diff", "-5"])), os(&["scn", "diff", "-5"]));
        assert_eq!(git_count_args(os(&["scn", "log", "-"])), os(&["scn", "log", "-"]));
        assert_eq!(git_count_args(os(&["scn", "-q"])), os(&["scn", "-q"]));
        assert_eq!(paragraphs(&[]), None);
        assert_eq!(paragraphs(&["a".into(), "b".into()]).as_deref(), Some("a\n\nb"));
        let cli = Cli::try_parse_from(["scn", "clone", "r", "-b", "dev"]).expect("parse");
        assert!(matches!(cli.command, Command::Clone { ref reference, .. } if reference == "dev"));
        let cli = Cli::try_parse_from(["scn", "clone", "r", "--branch", "dev"]).expect("parse");
        assert!(matches!(cli.command, Command::Clone { ref reference, .. } if reference == "dev"));
        for args in [
            &["scn", "diff", "--cached"][..],
            &["scn", "diff", "--staged"],
            &["scn", "merge", "--no-ff", "x"],
            &["scn", "clone", "r", "--depth", "1", "--single-branch"],
            &["scn", "mv", "-f", "a", "b"],
            &["scn", "log", "--pretty=oneline"],
            &["scn", "log", "--format", "medium"],
            // What SCION_AUTHOR in the environment amounts to
            &["scn", "merge", "--abort", "--author", "A <a@x>"],
            &["scn", "revert", "--abort", "--author", "A <a@x>"],
            &["scn", "cherry-pick", "--abort", "--author", "A <a@x>"],
        ] {
            assert!(Cli::try_parse_from(args).is_ok(), "{args:?}");
        }
        assert!(
            Cli::try_parse_from(["scn", "log", "--pretty=%H"]).is_err(),
            "only oneline / medium"
        );
    }

    #[test]
    fn help_lists_every_command_once() {
        let mut listed: Vec<&str> = COMMANDS
            .lines()
            .filter_map(|l| l.strip_prefix("  ")?.split_whitespace().next())
            .collect();
        let mut visible: Vec<String> = Cli::command()
            .get_subcommands()
            .filter(|c| !c.is_hide_set() && c.get_name() != "help")
            .map(|c| c.get_name().to_owned())
            .collect();
        listed.sort_unstable();
        visible.sort_unstable();
        assert_eq!(listed, visible);
        let help = command().render_help().to_string();
        assert!(
            help.contains("Share and update:") && help.contains("--endpoint <URL>"),
            "{help}"
        );
    }

    #[test]
    fn every_argument_is_described() {
        fn walk(cmd: &clap::Command, path: &str, missing: &mut Vec<String>) {
            for a in cmd.get_arguments() {
                if !a.is_hide_set() && a.get_help().is_none() && !["help", "version"].contains(&a.get_id().as_str()) {
                    missing.push(format!("{path} {}", a.get_id()));
                }
            }
            for sub in cmd
                .get_subcommands()
                .filter(|c| !c.is_hide_set() && c.get_name() != "help")
            {
                walk(sub, &format!("{path} {}", sub.get_name()), missing);
            }
        }
        let mut missing = Vec::new();
        walk(&Cli::command(), "scn", &mut missing);
        assert!(missing.is_empty(), "arguments without help: {missing:?}");
    }

    #[test]
    fn parse_global_flags_after_subcommand() {
        let cli = Cli::try_parse_from(["scn", "doctor", "--json", "--endpoint", "http://h:1"]).expect("parse");
        assert!(cli.json);
        assert_eq!(cli.endpoint.as_deref(), Some("http://h:1"));
    }

    #[test]
    fn parse_commands() {
        let cli = Cli::try_parse_from(["scn", "clone", "org/repo", "dir", "--ref", "dev"]).expect("parse");
        assert!(
            matches!(cli.command, Command::Clone { ref repo, ref reference, .. } if repo == "org/repo" && reference == "dev")
        );
        let cli = Cli::try_parse_from(["scn", "clone", "r", "--sparse", "a/b,c"]).expect("parse");
        assert!(
            matches!(cli.command, Command::Clone { sparse: Some(ref d), .. } if d == &["a/b".to_string(), "c".into()])
        );
        let cli = Cli::try_parse_from(["scn", "sparse", "add", "x", "y"]).expect("parse");
        assert!(matches!(cli.command, Command::Sparse(SparseCmd::Add { ref dirs }) if dirs.len() == 2));
        assert!(Cli::try_parse_from(["scn", "sparse", "set"]).is_err(), "dirs required");
        let cli = Cli::try_parse_from(["scn", "repo", "ls", "org/", "--deleted", "--limit", "5"]).expect("parse");
        assert!(matches!(
            cli.command,
            Command::Repo(RepoCmd::Ls {
                deleted: true,
                limit: 5,
                ..
            })
        ));
        let cli = Cli::try_parse_from(["scn", "branch", "create", "dev", "main"]).expect("parse");
        assert!(matches!(
            cli.command,
            Command::Branch {
                cmd: Some(BranchCmd::Create { revision: Some(_), .. }),
                ..
            }
        ));
        let cli = Cli::try_parse_from(["scn", "sparse", "disable"]).expect("parse");
        assert!(matches!(cli.command, Command::Sparse(SparseCmd::Disable)));
        let cli = Cli::try_parse_from(["scn", "commit", "-m", "msg", "--author", "A <a@b>"]).expect("parse");
        assert!(matches!(cli.command, Command::Commit { ref message, amend: false, .. } if message == &["msg"]));
        assert!(Cli::try_parse_from(["scn", "commit"]).is_ok(), "checked at run time");
        let cli = Cli::try_parse_from(["scn", "commit", "--amend"]).expect("parse");
        assert!(matches!(
            cli.command,
            Command::Commit {
                ref message,
                amend: true,
                ..
            } if message.is_empty()
        ));
        assert!(Cli::try_parse_from(["scn", "restore"]).is_err(), "paths required");
        let cli = Cli::try_parse_from(["scn", "restore", "src", "a.txt"]).expect("parse");
        assert!(matches!(cli.command, Command::Restore { ref paths, source: None } if paths.len() == 2));
        assert!(matches!(
            Cli::try_parse_from(["scn", "uncommit"]).expect("parse").command,
            Command::Uncommit
        ));
        let cli =
            Cli::try_parse_from(["scn", "import", "git", "../repo", "--repo", "r", "--git-ref", "dev"]).expect("parse");
        assert!(matches!(cli.command, Command::Import(ImportCmd::Git { ref git_ref, .. }) if git_ref == "dev"));
        let cli = Cli::try_parse_from(["scn", "log", "main", "--path", "src", "-n", "5"]).expect("parse");
        assert!(matches!(cli.command, Command::Log { max_count: 5, .. }));
        let cli = Cli::try_parse_from(["scn", "log", "HEAD~1", "--oneline", "--", "src"]).expect("parse");
        assert!(matches!(cli.command, Command::Log { ref paths, oneline: true, .. } if paths == &["src"]));
        let cli = Cli::try_parse_from(["scn", "show", "main^2", "--", "a"]).expect("parse");
        assert!(matches!(cli.command, Command::Show { revision: Some(_), ref paths, .. } if paths.len() == 1));
        let cli = Cli::try_parse_from(["scn", "commit", "-am", "msg"]).expect("parse");
        assert!(matches!(cli.command, Command::Commit { all: true, ref message, .. } if message == &["msg"]));
        let cli = Cli::try_parse_from(["scn", "clone", "r", "--sparse", "a,b", "--sparse", "c"]).expect("parse");
        assert!(matches!(cli.command, Command::Clone { sparse: Some(ref d), .. } if d.len() == 3));
        for (args, want) in [
            (&["scn", "branch"][..], "ls"),
            (&["scn", "branch", "ls"][..], "ls"),
            (&["scn", "branch", "feat"][..], "create"),
            (&["scn", "branch", "feat", "main~1"][..], "create"),
            (&["scn", "branch", "create", "feat"][..], "create"),
            (&["scn", "branch", "-d", "feat"][..], "rm"),
            (&["scn", "branch", "-D", "feat"][..], "rm"),
            (&["scn", "branch", "rm", "feat"][..], "rm"),
        ] {
            let Command::Branch { cmd, name, delete, .. } = Cli::try_parse_from(args).expect("parse").command else {
                panic!("{args:?}");
            };
            let got = match (cmd, name, delete) {
                (Some(BranchCmd::Ls), ..) | (None, None, None) => "ls",
                (Some(BranchCmd::Create { .. }), ..) | (None, Some(_), _) => "create",
                _ => "rm",
            };
            assert_eq!(got, want, "{args:?}");
        }
        assert!(Cli::try_parse_from(["scn", "branch", "x", "-d", "y"]).is_err());
        // Transitional git spellings
        for args in [
            &["scn", "push"][..],
            &["scn", "pull", "--rebase"],
            &["scn", "checkout", "-b", "x"],
            &["scn", "checkout", "--", "a", "b"],
            &["scn", "reset", "--soft", "HEAD~1"],
            &["scn", "reset", "--hard"],
            &["scn", "rebase", "--continue"],
            &["scn", "rebase", "--abort"],
            &["scn", "sync", "--abort"],
            &["scn", "merge", "--continue"],
            &["scn", "add", "-A"],
            &["scn", "rm", "-r", "dir"],
            &["scn", "mv", "a", "b"],
        ] {
            assert!(Cli::try_parse_from(args).is_ok(), "{args:?}");
        }
        assert!(Cli::try_parse_from(["scn", "reset", "--soft", "--hard"]).is_err());
        assert!(Cli::try_parse_from(["scn", "merge", "--continue", "--abort"]).is_err());
        assert!(Cli::try_parse_from(["scn", "rm"]).is_err());
        assert_eq!(one_path(Some("a".into()), vec![]).ok(), Some(Some("a".into())));
        assert_eq!(one_path(None, vec!["src/".into()]).ok(), Some(Some("src".into())));
        assert!(one_path(Some("a".into()), vec!["b".into()]).is_err());
        assert!(one_path(None, vec!["a".into(), "b".into()]).is_err());
        let cli = Cli::try_parse_from(["scn", "diff", "a", "b", "--name-only"]).expect("parse");
        assert!(matches!(cli.command, Command::Diff { ref revisions, name_only: true, .. } if revisions.len() == 2));
        let cli = Cli::try_parse_from(["scn", "ls"]).expect("parse");
        assert!(matches!(cli.command, Command::Ls { spec: None }));
        let cli = Cli::try_parse_from(["scn", "daemon", "run", "--idle-timeout", "0"]).expect("parse");
        assert!(matches!(
            cli.command,
            Command::Daemon(DaemonCmd::Run {
                idle_timeout: Some(0),
                ..
            })
        ));
        let cli = Cli::try_parse_from(["scn", "login", "--token", "t"]).expect("parse");
        assert!(matches!(
            cli.command,
            Command::Login {
                ref args,
                token: Some(_),
                with_token: false
            } if args.is_empty()
        ));
        let cli = Cli::try_parse_from(["scn", "login", "https://h", "--token", "t"]).expect("parse");
        assert!(matches!(cli.command, Command::Login { ref args, .. } if args == &["https://h"]));
        let cli = Cli::try_parse_from(["scn", "login", "scn_pat_x", "https://h"]).expect("parse");
        assert!(matches!(cli.command, Command::Login { ref args, .. } if args.len() == 2));
        assert!(Cli::try_parse_from(["scn", "login", "a", "b", "c"]).is_err());
        let v = |a: &[&str]| a.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert_eq!(
            login_args(&v(&["scn_pat_x", "https://h"])).ok(),
            Some((Some("https://h"), Some("scn_pat_x")))
        );
        assert_eq!(login_args(&v(&["http://h"])).ok(), Some((Some("http://h"), None)));
        assert_eq!(login_args(&v(&[])).ok(), Some((None, None)));
        assert!(login_args(&v(&["http://a", "http://b"])).is_err());
        assert!(login_args(&v(&["t1", "t2"])).is_err());
        for cmd in ["doctor", "logout", "whoami"] {
            assert!(Cli::try_parse_from(["scn", cmd, "http://h:1"]).is_ok(), "{cmd}");
        }
        assert!(Cli::try_parse_from(["scn", "login", "--token", "t", "--with-token"]).is_err());
        let cli = Cli::try_parse_from(["scn", "daemon", "stop"]).expect("parse");
        assert!(matches!(cli.command, Command::Daemon(DaemonCmd::Stop)));
    }

    #[test]
    fn fail_returns_exit_code() {
        assert_eq!(fail(&CliError::Usage("x".into()), true), ExitCode::from(2));
        assert_eq!(fail(&CliError::Wc(scion_wc::Error::Dirty), false), ExitCode::from(1));
    }
}
