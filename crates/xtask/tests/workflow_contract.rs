use std::{fs, path::PathBuf};
fn workflow() -> String {
    fs::read_to_string(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.github/workflows/release.yml"),
    )
    .unwrap()
}
#[test]
fn workflow_release_jobs_and_pins() {
    let text = workflow();
    for marker in [
        "validate:",
        "native:",
        "publish:",
        "verify-published:",
        "verify-release-metadata:",
        "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1",
        "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a",
        "actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c",
    ] {
        assert!(text.contains(marker), "missing {marker}");
    }
}
#[test]
fn workflow_native_matrix_contract() {
    let text = workflow();
    for marker in [
        "ubuntu-24.04",
        "ubuntu-24.04-arm",
        "macos-15",
        "macos-15-intel",
        "x86_64-unknown-linux-gnu",
        "aarch64-unknown-linux-gnu",
        "aarch64-apple-darwin",
        "x86_64-apple-darwin",
    ] {
        assert!(text.contains(marker), "missing {marker}");
    }
}
#[test]
fn workflow_libpq_and_dynamic_link_contract() {
    let text = workflow();
    for marker in [
        "pkg-config --atleast-version=17 libpq",
        "libpq@17",
        "LD_LIBRARY_PATH",
        "DYLD_LIBRARY_PATH",
        "readelf -d",
        "otool -L",
        "SHA256SUMS",
        "--draft",
        "--draft=false",
        "git rev-parse HEAD",
        "cat-file",
        "verify-tag",
    ] {
        assert!(text.contains(marker), "missing {marker}");
    }
    assert!(!text.contains("PQ_LIB_STATIC=") && !text.contains("LIBPQ_STATIC="));
}
