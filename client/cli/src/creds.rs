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
use std::path::PathBuf;

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

fn read_file(path: &PathBuf) -> std::io::Result<BTreeMap<String, String>> {
    match std::fs::read(path) {
        Ok(b) => serde_json::from_slice(&b).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(BTreeMap::new()),
        Err(e) => Err(e),
    }
}

fn write_file(path: &PathBuf, map: &BTreeMap<String, String>) -> std::io::Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let tmp = path.with_extension("tmp");
    let body = serde_json::to_vec_pretty(map).map_err(std::io::Error::other)?;
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    {
        use std::io::Write;
        let mut f = opts.open(&tmp)?;
        f.write_all(&body)?;
    }
    std::fs::rename(&tmp, path)
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

/// 保存凭据，返回保存的位置（用于提示）。
pub fn store(b: &Backend, endpoint: &str, secret: &str) -> std::io::Result<String> {
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

/// 删除凭据，返回是否存在。
pub fn remove(b: &Backend, endpoint: &str) -> std::io::Result<bool> {
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
    Ok(found)
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(load(&b, "http://k"), None);
        assert!(!remove(&b, "http://k").expect("remove"));
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
