use super::*;
use std::os::unix::fs::PermissionsExt;

#[test]
fn disjoint_profiles_use_distinct_locks() {
    let dir = tempfile::tempdir().expect("tempdir");
    let first = dir.path().join("a");
    let second = dir.path().join("b");
    let first_lock = lock_credentials(&first).expect("first profile lock");
    let (done, result) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        scope.spawn(|| {
            let backend = Backend::File(second);
            done.send(store(&backend, "http://b", "bob")).expect("store result");
        });
        let stored = result.recv_timeout(std::time::Duration::from_secs(1));
        drop(first_lock);
        stored.expect("the second profile must not wait").expect("store");
    });
}

#[test]
#[ignore = "requires a privileged launcher with two distinct non-root UIDs"]
fn private_profiles_in_shared_directory() {
    use std::os::unix::fs::MetadataExt;
    use std::os::unix::process::CommandExt;
    use std::process::Command;

    const CHILD: &str = "SCION_SHARED_PROFILE_TEST";
    if let Some(path) = std::env::var_os(CHILD) {
        let backend = Backend::File(path.into());
        let action = std::env::var("SCION_SHARED_PROFILE_ACTION").expect("action");
        let uid = Command::new("id").arg("-u").output().expect("effective UID");
        assert!(uid.status.success());
        let uid: u32 = String::from_utf8(uid.stdout).unwrap().trim().parse().unwrap();
        assert_ne!(uid, 0);
        println!("UID {uid}: {action}");
        if action == "store" {
            store(&backend, "http://a", "test-token").expect("store private profile");
        }
        assert_eq!(load(&backend, "http://a").as_deref(), Some("test-token"));
        let metadata = std::fs::metadata(credential_path(&backend)).expect("profile metadata");
        assert_eq!(metadata.uid(), uid);
        assert_eq!(metadata.mode() & 0o777, 0o600);
        if action == "remove" {
            assert!(remove(&backend, "http://a").expect("remove private credential"));
            assert_eq!(load(&backend, "http://a"), None);
        }
        return;
    }

    for order in [[61001, 61002], [61002, 61001]] {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o1777)).expect("shared directory");
        let run = |uid, action| {
            let output = Command::new("sh")
                .args(["-c", "umask 077; exec \"$@\"", "sh"])
                .arg(std::env::current_exe().expect("test binary"))
                .args([
                    "--ignored",
                    "--exact",
                    "creds::permissions_tests::private_profiles_in_shared_directory",
                    "--nocapture",
                ])
                .env(CHILD, dir.path().join(format!("profile-{uid}")))
                .env("SCION_SHARED_PROFILE_ACTION", action)
                .gid(uid)
                .uid(uid)
                .output()
                .expect("launch test under a separate UID");
            println!(
                "UID {uid}: {action}: {}\n{}",
                output.status,
                String::from_utf8_lossy(&output.stdout)
            );
            assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
        };
        run(order[0], "store");
        run(order[1], "store");
        run(order[0], "remove");
        run(order[1], "remove");
    }
}

#[test]
fn lock_path_preserves_existing_entries() {
    let dir = tempfile::tempdir().expect("tempdir");
    let backend = Backend::File(dir.path().join("credentials.json"));
    let account = br#"{"http://b":"bob"}"#;
    let target = tempfile::tempdir().expect("symlink target");
    for name in ["credentials.json.scion-lock", "credentials.json.endpoint.scion-lock"] {
        let lock = dir.path().join(name);
        std::fs::write(&lock, account).expect("existing profile");
        assert!(store(&backend, "http://a", "alice").is_err());
        assert!(remove(&backend, "http://a").is_err());
        assert_eq!(std::fs::read(&lock).expect("read existing profile"), account);
        std::fs::remove_file(&lock).expect("remove fixture");
        assert!(!endpoint_path(&backend).exists());
        assert!(!credential_path(&backend).exists());

        std::os::unix::fs::symlink(target.path(), &lock).expect("symlink fixture");
        assert!(store(&backend, "http://a", "alice").is_err());
        assert!(remove(&backend, "http://a").is_err());
        assert_eq!(std::fs::read_link(&lock).expect("read existing symlink"), target.path());
        assert_eq!(std::fs::read_dir(target.path()).expect("symlink target").count(), 0);
        assert!(!endpoint_path(&backend).exists());
        assert!(!credential_path(&backend).exists());
        std::fs::remove_file(&lock).expect("remove symlink");
    }
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
