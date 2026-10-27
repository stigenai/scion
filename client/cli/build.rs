//! macOS：把 Info.plist 嵌入 scn 的 `__TEXT,__info_plist` 段。
//!
//! 签了 Developer ID 的命令行工具按自己的身份接受本地网络隐私检查；没有
//! `NSLocalNetworkUsageDescription` 时系统不弹授权框，直接拒绝连接局域网
//! （`No route to host`）。CFBundleIdentifier 与 scripts/sign-macos.sh 的 identifier 一致。

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=build.rs");
    // Builds that are signed say so (scripts/sign-macos.sh signs them): credentials go to the keychain
    println!("cargo:rerun-if-env-changed=SCION_SIGNED");
    if std::env::var("CARGO_CFG_TARGET_OS")? != "macos" {
        return Ok(());
    }
    let plist = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
	<key>CFBundleIdentifier</key>
	<string>com.opencsg.scn</string>
	<key>CFBundleName</key>
	<string>scn</string>
	<key>CFBundleInfoDictionaryVersion</key>
	<string>6.0</string>
	<key>CFBundleShortVersionString</key>
	<string>{version}</string>
	<key>NSLocalNetworkUsageDescription</key>
	<string>scn connects to Scion servers and caches on your local network.</string>
</dict>
</plist>
"#,
        version = std::env::var("CARGO_PKG_VERSION")?
    );
    let path = std::path::PathBuf::from(std::env::var("OUT_DIR")?).join("Info.plist");
    std::fs::write(&path, plist)?;
    println!(
        "cargo:rustc-link-arg-bins=-Wl,-sectcreate,__TEXT,__info_plist,{}",
        path.display()
    );
    Ok(())
}
