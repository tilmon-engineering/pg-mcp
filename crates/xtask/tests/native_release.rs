use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
};

static NEXT: AtomicU64 = AtomicU64::new(0);
fn dir() -> PathBuf {
    let p = std::env::temp_dir().join(format!(
        "postgres-assets-{}",
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir_all(&p).unwrap();
    p
}
fn archive_names() -> [&'static str; 4] {
    [
        "postgres-mcp-x86_64-unknown-linux-gnu.tar.gz",
        "postgres-mcp-aarch64-unknown-linux-gnu.tar.gz",
        "postgres-mcp-aarch64-apple-darwin.tar.gz",
        "postgres-mcp-x86_64-apple-darwin.tar.gz",
    ]
}
fn sums(p: &Path) {
    let mut s = String::new();
    for n in archive_names() {
        fs::write(p.join(n), n).unwrap();
        let o = Command::new("sha256sum").arg(p.join(n)).output().unwrap();
        let h = String::from_utf8(o.stdout)
            .unwrap()
            .split_whitespace()
            .next()
            .unwrap()
            .to_owned();
        s.push_str(&format!("{h}  {n}\n"));
    }
    fs::write(p.join("SHA256SUMS"), s).unwrap();
}
fn run(p: &Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_xtask"))
        .args([
            "verify-downloads",
            "v0.1.0",
            p.to_str().unwrap(),
            "/missing",
        ])
        .output()
        .unwrap()
}
#[test]
fn package_has_exact_five_file_manifest() {
    let p = dir();
    sums(&p);
    let o = run(&p);
    assert!(!o.status.success());
    assert!(!String::from_utf8_lossy(&o.stderr).contains("asset directory"));
}
#[test]
fn package_rejects_tampered_sha256sums() {
    let p = dir();
    sums(&p);
    let s = fs::read_to_string(p.join("SHA256SUMS")).unwrap();
    let first = s.lines().next().unwrap();
    let tampered = format!(
        "{}{}\n{}",
        "0".repeat(64),
        &first[64..],
        s.lines().skip(1).collect::<Vec<_>>().join("\n")
    );
    fs::write(p.join("SHA256SUMS"), tampered).unwrap();
    let o = run(&p);
    assert!(!o.status.success());
    assert!(!String::from_utf8_lossy(&o.stderr).is_empty());
}
