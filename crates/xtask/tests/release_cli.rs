use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
};
static NEXT: AtomicU64 = AtomicU64::new(0);
fn temp() -> PathBuf {
    let p = std::env::temp_dir().join(format!(
        "postgres-xtask-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir_all(&p).unwrap();
    p
}
fn write(root: &Path, rel: &str, body: &str) {
    let p = root.join(rel);
    fs::create_dir_all(p.parent().unwrap()).unwrap();
    fs::write(p, body).unwrap();
}
fn fixture(version: &str) -> PathBuf {
    let r = temp();
    write(
        &r,
        "Cargo.toml",
        &format!(
            "[workspace]\nmembers=[\"crates/postgres-mcp\",\"crates/postgres-mcp-core\"]\nresolver=\"3\"\n[workspace.package]\nversion=\"{version}\"\nedition=\"2024\"\nrust-version=\"1.96\"\n"
        ),
    );
    for n in ["postgres-mcp", "postgres-mcp-core"] {
        write(
            &r,
            &format!("crates/{n}/Cargo.toml"),
            &format!("[package]\nname=\"{n}\"\nversion.workspace=true\nedition.workspace=true\n"),
        );
        write(&r, &format!("crates/{n}/src/lib.rs"), "");
    }
    write(
        &r,
        "CHANGELOG.md",
        &format!(
            "# Changelog\n\n## [{version}] - 2026-09-17\n### Added\n- release fixture\n\n## [Unreleased]\n- next\n"
        ),
    );
    assert!(
        Command::new("cargo")
            .args(["generate-lockfile"])
            .current_dir(&r)
            .status()
            .unwrap()
            .success()
    );
    r
}
fn run(root: &Path, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_xtask"))
        .args(args)
        .current_dir(root)
        .output()
        .unwrap()
}
#[test]
fn release_check_accepts_postgres_workspace() {
    let r = fixture("0.1.0");
    let o = run(&r, &["release-check", "v0.1.0"]);
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
}
#[test]
fn release_notes_extracts_exact_section() {
    let r = fixture("0.1.0");
    let out = r.join("notes.md");
    let o = run(&r, &["release-notes", "v0.1.0", out.to_str().unwrap()]);
    assert!(o.status.success());
    assert_eq!(
        fs::read_to_string(out).unwrap(),
        "### Added\n- release fixture\n"
    );
}
#[test]
fn release_check_rejects_tag_and_inheritance_mismatch() {
    let r = fixture("0.1.0");
    let o = run(&r, &["release-check", "v0.2.0"]);
    assert!(!o.status.success());
    let p = r.join("crates/postgres-mcp/Cargo.toml");
    fs::write(&p, "[package]\nname=\"postgres-mcp\"\nversion=\"0.1.0\"\n").unwrap();
    let o = run(&r, &["release-check", "v0.1.0"]);
    assert!(!o.status.success());
}
