use super::*;
use std::os::unix::fs::PermissionsExt;

#[test]
fn lock_path_preserves_existing_entries() {
    let dir = tempfile::tempdir().expect("tempdir");
    let backend = Backend::File(dir.path().join("credentials.json"));
    let lock = dir.path().join(".scion-credentials.lock");
    let account = br#"{"http://b":"bob"}"#;
    std::fs::write(&lock, account).expect("existing profile");
    assert!(store(&backend, "http://a", "alice").is_err());
    assert_eq!(std::fs::read(&lock).expect("read existing profile"), account);
    std::fs::remove_file(&lock).expect("remove fixture");

    let target = tempfile::tempdir().expect("symlink target");
    std::os::unix::fs::symlink(target.path(), &lock).expect("symlink fixture");
    assert!(store(&backend, "http://a", "alice").is_err());
    assert_eq!(std::fs::read_link(&lock).expect("read existing symlink"), target.path());
    assert!(!endpoint_path(&backend).exists());
    assert!(!credential_path(&backend).exists());
}

#[test]
fn store_and_remove_without_directory_read_access() {
    let dir = tempfile::tempdir().expect("tempdir");
    let backend = Backend::File(dir.path().join("credentials.json"));
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o300)).expect("restrict directory");
    let stored = store(&backend, "http://a", "alice");
    let loaded = load(&backend, "http://a");
    let selected = saved_endpoint(&backend);
    let removed = remove(&backend, "http://a");
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).expect("restore directory");

    stored.expect("store without directory read access");
    assert_eq!(loaded.as_deref(), Some("alice"));
    assert_eq!(selected.as_deref(), Some("http://a"));
    assert!(removed.expect("remove without directory read access"));
    assert_eq!(load(&backend, "http://a"), None);
    assert_eq!(saved_endpoint(&backend), None);
}
