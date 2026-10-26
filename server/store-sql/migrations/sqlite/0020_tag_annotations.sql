-- Annotated tags: what a tag (the ref tags/NAME) says about the commit it names. A row counts only
-- while its target is the ref's; deleting the ref deletes it.
CREATE TABLE tag_annotations (
    repo_id       INTEGER NOT NULL,
    name          TEXT    NOT NULL,
    target        BLOB    NOT NULL,
    tagger_name   TEXT    NOT NULL,
    tagger_email  TEXT    NOT NULL,
    time_ms       INTEGER NOT NULL,
    tz_offset_min INTEGER NOT NULL,
    message       TEXT    NOT NULL,
    git_object    BLOB,
    PRIMARY KEY (repo_id, name)
) WITHOUT ROWID;
