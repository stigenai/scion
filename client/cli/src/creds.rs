//! 登录凭据的保存（客户端设计文档 §3）：按服务端地址保存长期凭据（刷新令牌或 PAT）。
//!
//! Signed macOS builds (the released ones) keep them in the system keychain; elsewhere, and in
//! builds that are not signed, they go to the file `$XDG_CONFIG_HOME/scion/credentials.json`
//! (default `~/.config/scion/credentials.json`, mode 0600). macOS asks again whether a program
//! may read a keychain item each time it is a different unsigned binary, so every local build
//! would ask, and without a screen to answer on it would wait for ever. `SCION_CREDENTIALS=file`
//! or `keychain` chooses; `SCION_CREDENTIALS_FILE` names the file (and always uses it). A
//! keychain that does not answer within [`KEYCHAIN_WAIT`] counts as unavailable.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use tonic::codegen::http::uri::Authority;

/// 钥匙串条目的服务名。
#[cfg(target_os = "macos")]
const KEYRING_SERVICE: &str = "scion";

/// Whether this build is signed (set by the builds that sign it).
const SIGNED: bool = match option_env!("SCION_SIGNED") {
    Some(v) => !v.is_empty(),
    None => false,
};

/// How long to wait for the keychain, which may be waiting on a dialog nobody can see.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
pub const KEYCHAIN_WAIT: std::time::Duration = std::time::Duration::from_secs(3);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Backend {
    /// 系统钥匙串（macOS），文件作为后备。
    Keyring(PathBuf),
    File(PathBuf),
}

/// 凭据存储的位置。
pub fn backend(env: impl Fn(&str) -> Option<String>) -> Backend {
    if let Some(f) = env("SCION_CREDENTIALS_FILE").filter(|f| !f.is_empty()) {
        return Backend::File(PathBuf::from(f));
    }
    let config = env("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env("HOME").unwrap_or_else(|| ".".into())).join(".config"));
    let file = config.join("scion").join("credentials.json");
    let keychain = match env("SCION_CREDENTIALS").as_deref() {
        Some("file") => false,
        Some("keychain") => true,
        _ => SIGNED,
    };
    if cfg!(target_os = "macos") && keychain {
        Backend::Keyring(file)
    } else {
        Backend::File(file)
    }
}

/// `op` on another thread, given up after `wait`: the keychain blocks while it asks the user.
/// Says so once on standard error when it gives up.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
fn within<T: Send + 'static>(wait: std::time::Duration, op: impl FnOnce() -> Option<T> + Send + 'static) -> Option<T> {
    static WARNED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(op());
    });
    match rx.recv_timeout(wait) {
        Ok(v) => v,
        Err(_) => {
            if !WARNED.swap(true, std::sync::atomic::Ordering::Relaxed) {
                eprintln!(
                    "scn: the system keychain did not answer (it may be asking on a screen); \
                     set SCION_TOKEN, or SCION_CREDENTIALS=file to keep credentials in a file"
                );
            }
            None
        }
    }
}

fn key(endpoint: &str) -> String {
    endpoint.trim_end_matches('/').to_owned()
}

fn origin(endpoint: &str) -> Option<&str> {
    let endpoint = endpoint.strip_suffix('/').unwrap_or(endpoint);
    let authority = endpoint
        .strip_prefix("http://")
        .or_else(|| endpoint.strip_prefix("https://"))?;
    if authority.contains(['/', '@', '?', '#']) {
        return None;
    }
    let authority: Authority = authority.parse().ok()?;
    let host = authority.host();
    let port = &authority.as_str()[host.len()..];
    if host.is_empty() || (!port.is_empty() && (!port.starts_with(':') || authority.port_u16().is_none())) {
        return None;
    }
    if let Some(ip) = host.strip_prefix('[') {
        ip.strip_suffix(']')?.parse::<std::net::Ipv6Addr>().ok()?;
    } else if host.contains(['[', ']']) {
        return None;
    }
    Some(endpoint)
}

fn credential_path(b: &Backend) -> &Path {
    match b {
        Backend::File(path) | Backend::Keyring(path) => path,
    }
}

fn sibling_path(path: &Path, suffix: &str) -> PathBuf {
    let mut path = path.as_os_str().to_owned();
    path.push(suffix);
    path.into()
}

fn endpoint_path(b: &Backend) -> PathBuf {
    sibling_path(credential_path(b), ".endpoint")
}

pub fn saved_endpoint(b: &Backend) -> Option<String> {
    match std::fs::read_to_string(endpoint_path(b)) {
        Ok(value) => origin(&value).map(str::to_owned),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            let map = read_file(credential_path(b)).ok()?;
            if map.len() != 1 {
                return None;
            }
            origin(map.first_key_value()?.0).map(str::to_owned)
        }
        Err(_) => None,
    }
}

fn parent_dir(path: &Path) -> &Path {
    path.parent()
        .filter(|dir| !dir.as_os_str().is_empty())
        .unwrap_or(Path::new("."))
}

fn lock_credentials(path: &Path) -> io::Result<File> {
    let path = sibling_path(path, ".lock");
    std::fs::create_dir_all(parent_dir(&path))?;
    let file = File::options().write(true).create(true).truncate(false).open(path)?;
    file.lock()?;
    Ok(file)
}

fn lock_profile(b: &Backend) -> io::Result<(File, File)> {
    let credentials = lock_credentials(credential_path(b))?;
    let path = endpoint_path(b);
    // This lock also protects the file if another profile uses it for credentials.
    let preference = lock_credentials(&path)?;
    match std::fs::read_to_string(&path) {
        Ok(value) if value.is_empty() || origin(&value).is_some() => (),
        Err(e) if e.kind() == io::ErrorKind::NotFound => (),
        Err(e) => return Err(e),
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("{} contains data that is not an endpoint preference", path.display()),
            ));
        }
    }
    Ok((credentials, preference))
}

fn write_atomic(path: &Path, body: &[u8]) -> io::Result<()> {
    let mut temp = tempfile::NamedTempFile::new_in(parent_dir(path))?;
    temp.write_all(body)?;
    temp.persist(path)?;
    Ok(())
}

fn read_file(path: &Path) -> std::io::Result<BTreeMap<String, String>> {
    match std::fs::read(path) {
        Ok(b) => serde_json::from_slice(&b).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(BTreeMap::new()),
        Err(e) => Err(e),
    }
}

fn write_file(path: &Path, map: &BTreeMap<String, String>) -> std::io::Result<()> {
    let body = serde_json::to_vec_pretty(map).map_err(std::io::Error::other)?;
    write_atomic(path, &body)?;
    #[cfg(test)]
    if tests::INTERRUPT_CREDENTIAL_UPDATE.take() {
        return Err(io::ErrorKind::StorageFull.into());
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn keyring_entry(endpoint: &str) -> Option<keyring::Entry> {
    keyring::Entry::new(KEYRING_SERVICE, &key(endpoint)).ok()
}

/// 读取某个服务端的凭据。
pub fn load(b: &Backend, endpoint: &str) -> Option<String> {
    match b {
        #[cfg(target_os = "macos")]
        Backend::Keyring(file) => {
            let e = endpoint.to_owned();
            within(KEYCHAIN_WAIT, move || keyring_entry(&e)?.get_password().ok())
                .or_else(|| read_file(file).ok()?.remove(&key(endpoint)))
        }
        #[cfg(not(target_os = "macos"))]
        Backend::Keyring(file) => read_file(file).ok()?.remove(&key(endpoint)),
        Backend::File(file) => read_file(file).ok()?.remove(&key(endpoint)),
    }
}

/// Save the credentials and select this server for future commands.
pub fn store(b: &Backend, endpoint: &str, secret: &str) -> std::io::Result<String> {
    let endpoint = origin(endpoint)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "expected an HTTP(S) server origin"))?;
    let _locks = lock_profile(b)?;
    let location = store_secret(b, endpoint, secret)?;
    write_atomic(&endpoint_path(b), endpoint.as_bytes())?;
    Ok(location)
}

fn store_secret(b: &Backend, endpoint: &str, secret: &str) -> io::Result<String> {
    let file = match b {
        Backend::Keyring(file) => {
            #[cfg(target_os = "macos")]
            {
                let (e, s) = (endpoint.to_owned(), secret.to_owned());
                if within(KEYCHAIN_WAIT, move || keyring_entry(&e)?.set_password(&s).ok()).is_some() {
                    return Ok("the system keychain".into());
                }
            }
            file
        }
        Backend::File(file) => file,
    };
    let mut map = read_file(file)?;
    map.insert(key(endpoint), secret.to_owned());
    write_file(file, &map)?;
    Ok(file.display().to_string())
}

/// Delete the credentials. Clear the default if it selects this server.
pub fn remove(b: &Backend, endpoint: &str) -> std::io::Result<bool> {
    let _locks = lock_profile(b)?;
    let active_endpoint = saved_endpoint(b);
    let (file, mut found) = match b {
        Backend::Keyring(file) => {
            #[cfg(target_os = "macos")]
            let found = {
                let e = endpoint.to_owned();
                within(KEYCHAIN_WAIT, move || keyring_entry(&e)?.delete_credential().ok()).is_some()
            };
            #[cfg(not(target_os = "macos"))]
            let found = false;
            (file, found)
        }
        Backend::File(file) => (file, false),
    };
    let mut map = read_file(file)?;
    if map.remove(&key(endpoint)).is_some() {
        write_file(file, &map)?;
        found = true;
    }
    if active_endpoint.map_or(found, |saved| saved == endpoint.trim_end_matches('/')) {
        // An empty preference prevents a fallback to an older account.
        write_atomic(&endpoint_path(b), b"")?;
    }
    Ok(found)
}

#[cfg(test)]
mod tests {
    use super::*;

    thread_local! {
        pub(super) static INTERRUPT_CREDENTIAL_UPDATE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    }

    #[test]
    fn backend_selection() {
        let env = |vars: &'static [(&'static str, &'static str)]| {
            move |k: &str| vars.iter().find(|(n, _)| *n == k).map(|(_, v)| (*v).to_string())
        };
        assert_eq!(
            backend(env(&[("SCION_CREDENTIALS_FILE", "/tmp/c.json")])),
            Backend::File("/tmp/c.json".into())
        );
        let default = backend(env(&[("HOME", "/home/a")]));
        let file = match &default {
            Backend::Keyring(f) | Backend::File(f) => f.clone(),
        };
        assert_eq!(file, PathBuf::from("/home/a/.config/scion/credentials.json"));
        // The keychain only for signed builds on macOS, unless asked
        assert_eq!(
            cfg!(target_os = "macos") && SIGNED,
            matches!(default, Backend::Keyring(_))
        );
        let asked = backend(env(&[("HOME", "/h"), ("SCION_CREDENTIALS", "keychain")]));
        assert_eq!(cfg!(target_os = "macos"), matches!(asked, Backend::Keyring(_)));
        assert!(matches!(
            backend(env(&[("HOME", "/h"), ("SCION_CREDENTIALS", "file")])),
            Backend::File(_)
        ));
        let xdg = backend(env(&[("XDG_CONFIG_HOME", "/x"), ("SCION_CREDENTIALS_FILE", "")]));
        assert!(
            matches!(xdg, Backend::Keyring(f) | Backend::File(f) if f == std::path::Path::new("/x/scion/credentials.json"))
        );
    }

    /// The keychain paths, against keyring's in-memory mock (whose entries keep nothing between
    /// them, so a read after a write finds nothing and falls back to the file).
    #[cfg(target_os = "macos")]
    #[test]
    fn keychain_paths_with_a_mock_keychain() {
        keyring::set_default_credential_builder(keyring::mock::default_credential_builder());
        let dir = tempfile::tempdir().expect("tempdir");
        let b = Backend::Keyring(dir.path().join("credentials.json"));
        assert_eq!(store(&b, "http://k", "s").expect("store"), "the system keychain");
        assert_eq!(
            std::fs::read_to_string(endpoint_path(&b)).expect("preference"),
            "http://k"
        );
        assert!(!credential_path(&b).exists());
        assert_eq!(load(&b, "http://k"), None);
        assert!(!remove(&b, "http://k").expect("remove"));
        assert_eq!(saved_endpoint(&b), None);
    }

    #[test]
    fn legacy_and_malformed_preferences() {
        let dir = tempfile::tempdir().expect("tempdir");
        let file = dir.path().join("credentials.json");
        let b = Backend::File(file.clone());
        assert_eq!(saved_endpoint(&b), None);
        std::fs::write(&file, br#"{"https://[::1]:7080":"older"}"#).expect("legacy account");
        assert_eq!(saved_endpoint(&b).as_deref(), Some("https://[::1]:7080"));
        for invalid in [
            "",
            "http://a\nhttp://b",
            "ftp://a",
            "http://",
            "http://:80",
            "http://a b",
            "http://user@a",
            "http://u:pw@a:80",
            "http://a/path",
            "http://a//",
            "http://a?",
            "http://a#",
            "http://a:65536",
            "http://a:",
            "http://[invalid]",
            "http://[::1]x:80",
            "http://a[x]:80",
        ] {
            std::fs::write(endpoint_path(&b), invalid).expect("preference");
            assert_eq!(saved_endpoint(&b), None, "{invalid:?}");
        }
        std::fs::remove_file(endpoint_path(&b)).expect("remove preference");
        std::fs::write(&file, br#"{"http://a":"older","http://b":"old"}"#).expect("legacy accounts");
        assert_eq!(saved_endpoint(&b), None);
        std::fs::write(&file, br#"{"http://a/path":"older"}"#).expect("invalid legacy endpoint");
        assert_eq!(saved_endpoint(&b), None);
    }

    #[test]
    fn logout_only_clears_the_selected_server() {
        let dir = tempfile::tempdir().expect("tempdir");
        let b = Backend::File(dir.path().join("creds.json"));
        store(&b, "http://b", "bob").expect("login");
        store(&b, "https://a/", "alice").expect("login");
        assert_eq!(
            std::fs::read_to_string(endpoint_path(&b)).expect("preference"),
            "https://a"
        );
        let other = Backend::File(dir.path().join("creds-other.json"));
        assert_eq!(saved_endpoint(&other), None);
        assert!(remove(&b, "http://b").expect("logout"));
        assert_eq!(saved_endpoint(&b).as_deref(), Some("https://a"));

        store(&b, "http://b", "bob").expect("login");
        assert!(remove(&b, "http://b/").expect("logout"));
        assert_eq!(saved_endpoint(&b), None);
        assert_eq!(load(&b, "https://a").as_deref(), Some("alice"));
    }

    #[test]
    fn login_and_logout_preserve_a_colliding_credential_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let first = Backend::File(dir.path().join("c"));
        let second = Backend::File(endpoint_path(&first));
        std::fs::write(credential_path(&first), br#"{"http://a":"alice"}"#).expect("legacy account");
        store(&second, "http://b", "bob").expect("login to the second profile");

        assert!(store(&first, "http://a", "new").is_err());
        assert_eq!(load(&second, "http://b").as_deref(), Some("bob"));
        assert!(remove(&first, "http://a").is_err());
        assert_eq!(load(&first, "http://a").as_deref(), Some("alice"));
        assert_eq!(load(&second, "http://b").as_deref(), Some("bob"));
        assert!(remove(&second, "http://b").expect("logout from the second profile"));
        assert_eq!(load(&first, "http://a").as_deref(), Some("alice"));
    }

    #[test]
    fn logout_does_not_select_a_remaining_legacy_account() {
        let dir = tempfile::tempdir().expect("tempdir");
        let file = dir.path().join("creds.json");
        let b = Backend::File(file.clone());
        std::fs::write(&file, br#"{"http://a":"alice","http://b":"bob"}"#).expect("legacy accounts");
        assert!(remove(&b, "http://b").expect("logout"));
        assert_eq!(saved_endpoint(&b), None);
        assert_eq!(load(&b, "http://a").as_deref(), Some("alice"));
    }

    #[test]
    fn interrupted_login_preserves_the_default_without_a_preference() {
        for legacy_endpoint in [None, Some("http://a")] {
            let dir = tempfile::tempdir().expect("tempdir");
            let b = Backend::File(dir.path().join("credentials.json"));
            if legacy_endpoint.is_some() {
                std::fs::write(credential_path(&b), br#"{"http://a":"alice"}"#).expect("legacy account");
            }
            assert!(!endpoint_path(&b).exists());
            assert_eq!(saved_endpoint(&b).as_deref(), legacy_endpoint);

            INTERRUPT_CREDENTIAL_UPDATE.set(true);
            let error = store(&b, "http://b", "bob").expect_err("interrupted login");
            assert_eq!(error.kind(), io::ErrorKind::StorageFull);
            assert_eq!(load(&b, "http://b").as_deref(), Some("bob"));
            assert_eq!(saved_endpoint(&b).as_deref(), legacy_endpoint);
        }
    }

    #[test]
    fn interrupted_logout_preserves_the_default_without_a_preference() {
        let dir = tempfile::tempdir().expect("tempdir");
        let b = Backend::File(dir.path().join("credentials.json"));
        std::fs::write(credential_path(&b), br#"{"http://a":"alice","http://b":"bob"}"#).expect("legacy accounts");
        assert!(!endpoint_path(&b).exists());
        assert_eq!(saved_endpoint(&b), None);

        INTERRUPT_CREDENTIAL_UPDATE.set(true);
        let error = remove(&b, "http://b").expect_err("interrupted logout");
        assert_eq!(error.kind(), io::ErrorKind::StorageFull);
        assert_eq!(load(&b, "http://b"), None);
        assert_eq!(saved_endpoint(&b), None);
    }

    #[test]
    fn login_and_logout_use_the_same_lock() {
        use std::sync::mpsc;
        use std::time::Duration;

        let dir = tempfile::tempdir().expect("tempdir");
        let b = Backend::File(dir.path().join("creds.json"));
        store(&b, "http://a", "alice").expect("login");
        let lock = lock_credentials(credential_path(&b)).expect("lock");
        let (started, attempts) = mpsc::channel();
        let (finished, results) = mpsc::channel();
        std::thread::scope(|scope| {
            let login = scope.spawn(|| {
                started.send(()).expect("start");
                store(&b, "http://b", "bob").expect("login");
                finished.send(()).expect("finish");
            });
            let logout = scope.spawn(|| {
                started.send(()).expect("start");
                assert!(remove(&b, "http://a").expect("logout"));
                finished.send(()).expect("finish");
            });
            attempts.recv().expect("login or logout start");
            attempts.recv().expect("login and logout start");
            let blocked = results.recv_timeout(Duration::from_millis(100));
            drop(lock);
            login.join().expect("login");
            logout.join().expect("logout");
            assert_eq!(blocked, Err(mpsc::RecvTimeoutError::Timeout));
        });
        assert_eq!(saved_endpoint(&b).as_deref(), Some("http://b"));
        assert_eq!(load(&b, "http://b").as_deref(), Some("bob"));
    }

    #[test]
    fn failed_store_keeps_the_previous_server() {
        let dir = tempfile::tempdir().expect("tempdir");
        let b = Backend::File(dir.path().join("creds.json"));
        store(&b, "http://a", "alice").expect("login");
        assert!(store(&b, "http://b/path", "bob").is_err());
        assert_eq!(load(&b, "http://b/path"), None);
        std::fs::write(credential_path(&b), "invalid json").expect("corrupt credentials");
        assert!(store(&b, "http://b", "bob").is_err());
        assert_eq!(saved_endpoint(&b).as_deref(), Some("http://a"));
    }

    #[test]
    fn preference_write_error_reaches_the_caller() {
        let dir = tempfile::tempdir().expect("tempdir");
        let b = Backend::File(dir.path().join("creds.json"));
        std::fs::create_dir(endpoint_path(&b)).expect("block preference write");
        assert!(store(&b, "http://a", "alice").is_err());
        assert_eq!(saved_endpoint(&b), None);
    }

    #[test]
    fn a_keychain_that_does_not_answer_is_given_up() {
        let quick = std::time::Duration::from_millis(50);
        assert_eq!(within(quick, || Some(1)), Some(1));
        let slow = within(quick, || {
            std::thread::sleep(std::time::Duration::from_millis(500));
            Some(2)
        });
        assert_eq!(slow, None);
    }

    #[test]
    fn file_store_round_trip_with_private_permissions() {
        let dir = tempfile::tempdir().expect("tempdir");
        let b = Backend::File(dir.path().join("sub/credentials.json"));
        assert_eq!(load(&b, "http://a"), None);
        assert!(!remove(&b, "http://a").expect("remove"));
        store(&b, "http://a/", "secret-a").expect("store");
        store(&b, "http://b", "secret-b").expect("store");
        assert_eq!(
            load(&b, "http://a").as_deref(),
            Some("secret-a"),
            "trailing slash ignored"
        );
        assert!(remove(&b, "http://a").expect("remove"));
        assert_eq!(load(&b, "http://a"), None);
        assert_eq!(load(&b, "http://b").as_deref(), Some("secret-b"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let Backend::File(f) = &b else { unreachable!() };
            let mode = std::fs::metadata(f).expect("meta").permissions().mode() & 0o777;
            assert_eq!(mode, 0o600);
        }
        // 文件损坏：读不到，写入报错
        let Backend::File(f) = &b else { unreachable!() };
        std::fs::write(f, b"not json").expect("corrupt");
        assert_eq!(load(&b, "http://b"), None);
        assert!(store(&b, "http://c", "x").is_err());
    }
}
