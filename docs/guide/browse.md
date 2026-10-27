# Directory listings: last commits and statistics

English | [简体中文](browse.zh-CN.md)

A web page that shows a directory usually needs two things:
- the commit that last changed each file or subdirectory (the "last commit" column);
- the number of files and the size of the repository or directory.

The server answers both queries and caches what it computes, so viewing the same revision again costs nothing more.

## The last commit of each entry

```
QueryService.ListLastCommits(repo, revision, path, offset, limit)
  → { changeset_id, entries: [{name, changeset_id}], commits: [LogEntry], total }
```

- **Scope:** `path` is a directory, empty for the root. Entries are sorted by name and paged with `offset` and `limit`; `limit` defaults to 1000, which is also its maximum, and `total` is the number of entries.
- **`commits`:** the commits `entries` refer to, without duplicates, newest generation first, with author, time and message, so they need no further queries. For 40-digit git SHAs, pass these ids to `GetGitShas` (see [git-compatible commit SHAs](git-sha.md)).
- **Semantics:** along the first-parent chain:
  - an entry is changed by a commit when it is among the commit's changes against its first parent;
  - a subdirectory is changed when any file below it is;
  - a merge that took the entry as one of the merged branches had it did not change it: the last change is that branch's, as `git log -1 -- PATH` shows; a merge that made the entry differ from every parent changed it.
- **Users who can read only some paths:** only visible entries are listed. An entry whose contents they cannot all see (`src` for someone who can read only `src/ui`) comes back with its name and no last commit.
- **SDK:** `Client::last_commits(revision, path, offset, limit)`.

### Computing and caching

1. Results are cached per commit and directory, and only when one request covered all the directory's entries.
2. When the parent commit's result is cached, a new commit's is an incremental update from its own changes: the entries it changes go to it, the others keep the parent's. Viewing a branch's head after a land takes this path.
3. Entries without a cached result use the PathHistory index to find the latest change on the first-parent chain.
4. Without the index (old data written before it existed), commits along the first-parent chain are checked one by one, at most 2,000. Entries still unresolved at the limit have an empty `changeset_id`, and such results are not cached.

## Directory statistics

```
QueryService.GetTreeStats(repo, revision, path)
  → { changeset_id, tree_id, files, directories, bytes }
```

- **`files`:** the number of files, executables and symbolic links included.
- **`directories`:** the number of subdirectories, not counting itself.
- **`bytes`:** the total size of the files. In stage S1, LFS files count at the size of their pointer files.
- **Permissions:** statistics cover a whole subtree, so they need read access to all of it:
  - with an empty `path`, read access to the whole repository;
  - with a `path`, read access to that path.
- **Caching:** the result depends only on the tree's contents and is cached by tree id. Unchanged subdirectories hit the cache, and forks copy these results too.
- **SDK:** `Client::tree_stats(revision, path)`.

The server declares the features `query.last_commits` and `query.tree_stats` in `GetServerInfo`.
