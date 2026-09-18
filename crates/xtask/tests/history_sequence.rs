use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
};
static NEXT: AtomicU64 = AtomicU64::new(0);
fn temp() -> PathBuf {
    let p = std::env::temp_dir().join(format!(
        "postgres-history-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir_all(&p).unwrap();
    p
}
fn git(root: &PathBuf, args: &[&str]) -> std::process::Output {
    let o = Command::new("git")
        .args(args)
        .current_dir(root)
        .output()
        .unwrap();
    assert!(
        o.status.success(),
        "git {:?}: {}",
        args,
        String::from_utf8_lossy(&o.stderr)
    );
    o
}
fn write(root: &Path, rel: &str, body: &str) {
    let p = root.join(rel);
    fs::create_dir_all(p.parent().unwrap()).unwrap();
    fs::write(p, body).unwrap();
}
#[test]
fn first_release_history_uses_final_notes_commit_and_annotated_tag() {
    let r = temp();
    write(
        &r,
        "Cargo.toml",
        "[workspace]\nmembers=[\"crates/postgres-mcp\",\"crates/postgres-mcp-core\"]\nresolver=\"3\"\n[workspace.package]\nversion=\"0.1.0\"\nedition=\"2024\"\nrust-version=\"1.96\"\n",
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
        "# Changelog\n\n## [0.1.0] - 2026-09-17\n### Added\n- final notes\n\n## [Unreleased]\n- next\n",
    );
    assert!(
        Command::new("cargo")
            .args(["generate-lockfile"])
            .current_dir(&r)
            .status()
            .unwrap()
            .success()
    );
    git(&r, ["init", "-q"].as_ref());
    git(
        &r,
        ["config", "user.email", "test@example.invalid"].as_ref(),
    );
    git(&r, ["config", "user.name", "Release Test"].as_ref());
    git(&r, ["add", "."].as_ref());
    git(
        &r,
        ["commit", "-qm", "implementation and provisional notes"].as_ref(),
    );
    let implementation = String::from_utf8(git(&r, ["rev-parse", "HEAD"].as_ref()).stdout).unwrap();
    write(
        &r,
        "CHANGELOG.md",
        "# Changelog\n\n## [0.1.0] - 2026-09-17\n### Added\n- final user-visible notes\n\n## [Unreleased]\n- next\n",
    );
    git(&r, ["add", "CHANGELOG.md"].as_ref());
    git(&r, ["commit", "-qm", "final release notes"].as_ref());
    let final_sha = String::from_utf8(git(&r, ["rev-parse", "HEAD"].as_ref()).stdout).unwrap();
    assert_ne!(implementation.trim(), final_sha.trim());
    git(&r, ["tag", "-a", "v0.1.0", "-m", "Release v0.1.0"].as_ref());
    assert_eq!(
        String::from_utf8(git(&r, ["cat-file", "-t", "v0.1.0"].as_ref()).stdout)
            .unwrap()
            .trim(),
        "tag"
    );
    assert_eq!(
        String::from_utf8(git(&r, ["rev-list", "-n", "1", "v0.1.0"].as_ref()).stdout)
            .unwrap()
            .trim(),
        final_sha.trim()
    );
}
