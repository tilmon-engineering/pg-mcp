use serde::{Deserialize, Serialize};
use std::{
    env, fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{SystemTime, UNIX_EPOCH},
};

const REPO: &str = "tilmon-engineering/pg-mcp";
const PRODUCT: &str = "postgres-mcp";
const TARGETS: [&str; 4] = [
    "x86_64-unknown-linux-gnu",
    "aarch64-unknown-linux-gnu",
    "aarch64-apple-darwin",
    "x86_64-apple-darwin",
];

pub fn archive_names() -> [&'static str; 4] {
    [
        "postgres-mcp-x86_64-unknown-linux-gnu.tar.gz",
        "postgres-mcp-aarch64-unknown-linux-gnu.tar.gz",
        "postgres-mcp-aarch64-apple-darwin.tar.gz",
        "postgres-mcp-x86_64-apple-darwin.tar.gz",
    ]
}
pub fn expected_tag(version: &str) -> String {
    format!("v{version}")
}
fn run(program: &str, args: &[&str], dir: Option<&Path>) -> Result<std::process::Output, String> {
    let mut c = Command::new(program);
    c.args(args).stdout(Stdio::piped()).stderr(Stdio::piped());
    if let Some(d) = dir {
        c.current_dir(d);
    }
    c.output().map_err(|e| format!("run {program}: {e}"))
}
fn ok(o: &std::process::Output, what: &str) -> Result<(), String> {
    if o.status.success() {
        Ok(())
    } else {
        Err(format!(
            "{what} failed: {}",
            String::from_utf8_lossy(&o.stderr).trim()
        ))
    }
}
fn value(path: &Path) -> Result<toml::Value, String> {
    toml::from_str(&fs::read_to_string(path).map_err(|e| e.to_string())?).map_err(|e| e.to_string())
}
fn package_version(path: &Path) -> Result<(String, bool), String> {
    let document = value(path)?;
    let p = document
        .get("package")
        .and_then(toml::Value::as_table)
        .ok_or("missing package")?;
    let v = p.get("version").ok_or("missing version")?;
    Ok((
        v.as_str().unwrap_or_default().to_string(),
        v.get("workspace")
            .and_then(toml::Value::as_bool)
            .unwrap_or(false),
    ))
}
pub fn validate_tag(tag: &str, version: &str) -> Result<(), String> {
    if tag == expected_tag(version)
        && version.split('.').count() == 3
        && version.as_bytes().first().is_some_and(u8::is_ascii_digit)
        && version
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || ".-+".contains(c))
    {
        Ok(())
    } else {
        Err(format!("invalid release tag {tag:?}; expected v{version}"))
    }
}

pub fn validate_repository(root: &Path, tag: &str) -> Result<String, String> {
    let doc = value(&root.join("Cargo.toml"))?;
    let ws = doc
        .get("workspace")
        .and_then(toml::Value::as_table)
        .ok_or("missing workspace")?;
    let version = ws
        .get("package")
        .and_then(|x| x.get("version"))
        .and_then(toml::Value::as_str)
        .ok_or("missing workspace version")?;
    validate_tag(tag, version)?;
    let members = ws
        .get("members")
        .and_then(toml::Value::as_array)
        .ok_or("missing members")?;
    for member in ["crates/postgres-mcp", "crates/postgres-mcp-core"] {
        if !members.iter().any(|m| m.as_str() == Some(member)) {
            return Err(format!("workspace missing {member}"));
        }
        let (literal, inherited) = package_version(&root.join(member).join("Cargo.toml"))?;
        if !inherited || !literal.is_empty() {
            return Err(format!("{member} must use version.workspace = true"));
        }
    }
    let lock = fs::read_to_string(root.join("Cargo.lock")).map_err(|e| e.to_string())?;
    for package in ["postgres-mcp", "postgres-mcp-core"] {
        let needle = format!("name = \"{package}\"\nversion = \"{version}\"");
        if lock.matches(&needle).count() != 1 {
            return Err(format!(
                "Cargo.lock entry for {package} does not match {version}"
            ));
        }
    }
    let metadata = run(
        "cargo",
        &["metadata", "--locked", "--no-deps", "--format-version", "1"],
        Some(root),
    )?;
    ok(&metadata, "cargo metadata")?;
    Ok(version.to_owned())
}
fn date_valid(s: &str) -> bool {
    let p: Vec<_> = s.split('-').collect();
    p.len() == 3
        && p[0].len() == 4
        && p[1].len() == 2
        && p[2].len() == 2
        && p.iter().all(|x| x.chars().all(|c| c.is_ascii_digit()))
        && (1..=12).contains(&p[1].parse::<u32>().unwrap_or(0))
        && (1..=31).contains(&p[2].parse::<u32>().unwrap_or(0))
}
pub fn extract_notes(text: &str, version: &str) -> Result<String, String> {
    let lines: Vec<_> = text.lines().collect();
    if lines.first() != Some(&"# Changelog") {
        return Err("changelog must begin with # Changelog".into());
    }
    let mut starts = Vec::new();
    let mut fence: Option<(u8, usize)> = None;
    for (i, line) in lines.iter().enumerate() {
        let trimmed = line.trim_start();
        if let Some((ch, n)) = fence {
            let close = trimmed.bytes().take_while(|b| *b == ch).count();
            if close >= n && trimmed[close..].trim().is_empty() {
                fence = None;
            }
        } else if trimmed.starts_with("```") || trimmed.starts_with("~~~") {
            fence = Some((
                trimmed.as_bytes()[0],
                trimmed
                    .bytes()
                    .take_while(|b| *b == trimmed.as_bytes()[0])
                    .count(),
            ));
        } else if line.starts_with("## ") {
            starts.push((i, *line));
        } else if line.starts_with("##") && !line.starts_with("###") {
            return Err("malformed level-two heading".into());
        }
    }
    if fence.is_some() {
        return Err("unterminated fenced block".into());
    }
    let mut found = Vec::new();
    for (i, h) in &starts {
        if *h == "## [Unreleased]" {
            continue;
        }
        let rest = h.strip_prefix("## [").ok_or("malformed release heading")?;
        let at = rest.find("] - ").ok_or("malformed release heading")?;
        let v = &rest[..at];
        if !date_valid(&rest[at + 4..]) {
            return Err("release heading date must be YYYY-MM-DD".into());
        }
        if v == version {
            found.push(*i);
        }
    }
    if found.len() != 1 {
        return Err(format!(
            "expected exactly one changelog section for {version}"
        ));
    }
    let end = starts
        .iter()
        .find(|(i, _)| *i > found[0])
        .map(|x| x.0)
        .unwrap_or(lines.len());
    let body = lines[found[0] + 1..end].join("\n");
    if body.trim().is_empty() {
        return Err("release notes are empty".into());
    }
    Ok(format!("{}\n", body.trim()))
}
fn checksum(path: &Path) -> Result<String, String> {
    let p = path.to_str().ok_or("non-utf8 path")?;
    let o = run("sha256sum", &[p], None)?;
    ok(&o, "sha256sum")?;
    Ok(String::from_utf8_lossy(&o.stdout)
        .split_whitespace()
        .next()
        .ok_or("missing checksum")?
        .into())
}
pub fn verify_checksums(text: &str, expected: &[&str]) -> Result<(), String> {
    let mut names = Vec::new();
    for l in text.lines() {
        let p: Vec<_> = l.split_whitespace().collect();
        if p.len() != 2 || p[0].len() != 64 || !p[0].chars().all(|c| c.is_ascii_hexdigit()) {
            return Err("invalid checksum line".into());
        }
        names.push(p[1].trim_start_matches('*'));
    }
    if names.len() != expected.len()
        || expected
            .iter()
            .any(|x| names.iter().filter(|n| *n == x).count() != 1)
    {
        return Err("checksums must contain exactly expected archives".into());
    }
    Ok(())
}
pub fn verify_asset_dir(dir: &Path) -> Result<Vec<PathBuf>, String> {
    let names = archive_names();
    let mut got: Vec<_> = fs::read_dir(dir)
        .map_err(|e| e.to_string())?
        .map(|e| e.map(|x| x.file_name()).map_err(|e| e.to_string()))
        .collect::<Result<_, _>>()?;
    got.sort();
    let mut expected: Vec<_> = names.iter().map(std::ffi::OsString::from).collect();
    expected.push("SHA256SUMS".into());
    expected.sort();
    if got != expected {
        return Err("asset directory must contain exactly four archives and SHA256SUMS".into());
    }
    let sums = fs::read_to_string(dir.join("SHA256SUMS")).map_err(|e| e.to_string())?;
    verify_checksums(&sums, &names)?;
    let mut out = Vec::new();
    for n in names {
        let p = dir.join(n);
        if checksum(&p)?
            != sums
                .lines()
                .find(|l| l.ends_with(n))
                .and_then(|l| l.split_whitespace().next())
                .unwrap_or("")
        {
            return Err(format!("checksum mismatch for {n}"));
        }
        out.push(p);
    }
    Ok(out)
}
fn verify_binary(binary: &Path, version: &str) -> Result<(), String> {
    if !binary.is_file() {
        return Err("binary missing".into());
    }
    let o = Command::new(binary)
        .arg("--version")
        .output()
        .map_err(|e| e.to_string())?;
    if !o.status.success()
        || !o.stderr.is_empty()
        || o.stdout != format!("{PRODUCT} {version}\n").as_bytes()
    {
        return Err("--version contract failed".into());
    }
    Ok(())
}
fn fetch_public_assets(tag: &str, dir: &Path) -> Result<(), String> {
    if !tag.starts_with('v') {
        return Err("invalid release tag".into());
    }
    fs::create_dir_all(dir).map_err(|e| e.to_string())?;
    for name in archive_names().iter().chain(["SHA256SUMS"].iter()) {
        let url = format!("https://github.com/{REPO}/releases/download/{tag}/{name}");
        let destination = dir.join(name);
        let out = run(
            "curl",
            &[
                "--disable",
                "--proto",
                "=https",
                "--proto-redir",
                "=https",
                "--fail",
                "--location",
                "--silent",
                "--show-error",
                "--output",
                destination.to_str().ok_or("bad asset path")?,
                &url,
            ],
            None,
        )?;
        ok(&out, "anonymous asset download")?;
    }
    verify_asset_dir(dir).map(|_| ())
}
fn verify_archive_members(dir: &Path) -> Result<(), String> {
    for name in archive_names() {
        let archive = dir.join(name);
        let output = run(
            "tar",
            &["-tzf", archive.to_str().ok_or("bad archive path")?],
            None,
        )?;
        ok(&output, "archive listing")?;
        let listing = String::from_utf8_lossy(&output.stdout);
        let members: Vec<_> = listing.lines().collect();
        if members != [PRODUCT] {
            return Err(format!("archive {name} must contain only {PRODUCT}"));
        }
    }
    Ok(())
}
fn verify_archive(dir: &Path) -> Result<(), String> {
    verify_asset_dir(dir)?;
    verify_archive_members(dir)
}
fn verify_downloads(root: &Path, tag: &str, dir: &Path, binary: &Path) -> Result<(), String> {
    let version = validate_repository(root, tag)?;
    verify_asset_dir(dir)?;
    verify_archive_members(dir)?;
    verify_binary(binary, &version)
}
#[derive(Debug, Deserialize, Serialize)]
struct ReleaseAsset {
    name: String,
    size: u64,
    browser_download_url: Option<String>,
}
#[derive(Debug, Deserialize, Serialize)]
struct ReleaseJson {
    draft: bool,
    prerelease: bool,
    tag_name: String,
    target_commitish: String,
    body: String,
    published_at: Option<String>,
    assets: Vec<ReleaseAsset>,
}
fn decode_release(output: &std::process::Output) -> Result<ReleaseJson, String> {
    serde_json::from_slice(&output.stdout).map_err(|e| format!("invalid GitHub release JSON: {e}"))
}
fn compare_release_assets(root: &Path, tag: &str, dir: &Path) -> Result<(), String> {
    let download_dir = root
        .join("target")
        .join(format!(".xtask-release-download-{}", std::process::id()));
    if download_dir.exists() {
        return Err("release download directory already exists; manual recovery required".into());
    }
    fs::create_dir_all(&download_dir).map_err(|e| e.to_string())?;
    let result = (|| {
        let output = run(
            "gh",
            &[
                "release",
                "download",
                tag,
                "--dir",
                download_dir.to_str().ok_or("bad download path")?,
            ],
            Some(root),
        )?;
        ok(&output, "release asset download")?;
        verify_archive(&download_dir)?;
        for name in archive_names().iter().chain([&"SHA256SUMS"]) {
            let local = dir.join(name);
            let fetched = download_dir.join(name);
            if checksum(&local)? != checksum(&fetched)? {
                return Err(format!("release asset digest mismatch for {name}"));
            }
            if fs::metadata(&local).map_err(|e| e.to_string())?.len()
                != fs::metadata(&fetched).map_err(|e| e.to_string())?.len()
            {
                return Err(format!("release asset size mismatch for {name}"));
            }
        }
        Ok(())
    })();
    let _ = fs::remove_dir_all(&download_dir);
    result
}
fn confirm_published(root: &Path, tag: &str) -> Result<(), String> {
    let output = run(
        "gh",
        &["api", &format!("repos/{REPO}/releases/tags/{tag}")],
        Some(root),
    )?;
    let release = decode_release(&output)?;
    if release.draft || release.published_at.is_none() {
        return Err("release publish transition was not confirmed".into());
    }
    Ok(())
}
fn publish(root: &Path, tag: &str, expected_sha: &str, dir: &Path) -> Result<(), String> {
    let version = validate_repository(root, tag)?;
    let head = run("git", &["rev-parse", "HEAD"], Some(root))?;
    ok(&head, "git HEAD")?;
    if String::from_utf8_lossy(&head.stdout).trim() != expected_sha {
        return Err("checked-out HEAD does not match expected SHA".into());
    }
    // Read-only origin preflight: do not rewrite SSH or non-canonical remotes.
    let remote = run("git", &["remote", "get-url", "origin"], Some(root))?;
    ok(&remote, "origin preflight")?;
    if String::from_utf8_lossy(&remote.stdout)
        .trim()
        .trim_end_matches('/')
        != format!("https://github.com/{REPO}.git")
    {
        return Err("origin must be canonical HTTPS GitHub URL".into());
    }
    let tag_type = run("git", &["cat-file", "-t", tag], Some(root))?;
    ok(&tag_type, "annotated tag check")?;
    if String::from_utf8_lossy(&tag_type.stdout).trim() != "tag" {
        return Err("release tag must be annotated".into());
    }
    let peeled = run("git", &["rev-parse", &format!("{tag}^{{}}")], Some(root))?;
    ok(&peeled, "tag target check")?;
    if String::from_utf8_lossy(&peeled.stdout).trim() != expected_sha {
        return Err("annotated tag does not resolve to expected SHA".into());
    }
    let notes = root.join("target/release-notes.md");
    fs::create_dir_all(notes.parent().unwrap()).map_err(|e| e.to_string())?;
    fs::write(
        &notes,
        extract_notes(
            &fs::read_to_string(root.join("CHANGELOG.md")).map_err(|e| e.to_string())?,
            &version,
        )?,
    )
    .map_err(|e| e.to_string())?;
    verify_asset_dir(dir)?;
    let api = format!("repos/{REPO}/releases/tags/{tag}");
    let existing = run("gh", &["api", &api], Some(root))?;
    let release_output = if existing.status.success() {
        existing
    } else {
        let list = run(
            "gh",
            &["api", &format!("repos/{REPO}/releases"), "--paginate"],
            Some(root),
        )?;
        ok(&list, "release list")?;
        let releases: Vec<ReleaseJson> = serde_json::from_slice(&list.stdout)
            .map_err(|e| format!("invalid release list JSON: {e}"))?;
        let matches: Vec<_> = releases
            .into_iter()
            .filter(|release| release.tag_name == tag)
            .collect();
        if matches.len() > 1 {
            return Err("multiple releases share this tag; manual recovery required".into());
        }
        if let Some(release) = matches.into_iter().next() {
            let mut synthetic = run("true", &[], None)?;
            synthetic.stdout = serde_json::to_vec(&release).map_err(|e| e.to_string())?;
            synthetic
        } else {
            run("false", &[], None).map_err(|e| format!("release lookup fallback: {e}"))?
        }
    };
    if release_output.status.success() {
        let release = decode_release(&release_output)?;
        if !release.draft {
            return Err("release is already published; refusing overwrite".into());
        }
        if release.tag_name != tag
            || release.target_commitish != expected_sha
            || release.prerelease != version.contains('-')
        {
            return Err(
                "existing draft release metadata mismatch; manual recovery required".into(),
            );
        }
        let expected: Vec<_> = archive_names()
            .iter()
            .chain([&"SHA256SUMS"])
            .map(|x| (*x).to_owned())
            .collect();
        let actual: Vec<_> = release.assets.iter().map(|a| a.name.clone()).collect();
        if release.body.trim_end()
            != fs::read_to_string(&notes)
                .map_err(|e| e.to_string())?
                .trim_end()
        {
            return Err("existing draft notes mismatch; manual recovery required".into());
        }
        if actual != expected {
            return Err("existing draft assets mismatch; manual recovery required".into());
        }
        compare_release_assets(root, tag, dir)?;
        let edit = run("gh", &["release", "edit", tag, "--draft=false"], Some(root))?;
        ok(&edit, "GitHub release publish")?;
        confirm_published(root, tag)?;
        return Ok(());
    }
    let mut args = vec![
        "release",
        "create",
        tag,
        "--verify-tag",
        "--target",
        expected_sha,
        "--draft",
        "--notes-file",
        notes.to_str().ok_or("bad notes path")?,
    ];
    let asset_paths: Vec<PathBuf> = archive_names()
        .iter()
        .map(|name| dir.join(name))
        .chain([dir.join("SHA256SUMS")])
        .collect();
    let asset_strings: Vec<String> = asset_paths
        .iter()
        .map(|path| path.to_str().map(str::to_owned).ok_or("bad asset path"))
        .collect::<Result<_, _>>()?;
    args.extend(asset_strings.iter().map(String::as_str));
    let out = run("gh", &args, Some(root))?;
    ok(&out, "GitHub draft release")?;
    let created = run(
        "gh",
        &["api", &format!("repos/{REPO}/releases"), "--paginate"],
        Some(root),
    )?;
    ok(&created, "created release lookup")?;
    let created_list: Vec<ReleaseJson> = serde_json::from_slice(&created.stdout)
        .map_err(|e| format!("invalid created release list JSON: {e}"))?;
    let created_release = created_list
        .into_iter()
        .find(|release| release.tag_name == tag)
        .ok_or("created draft disappeared")?;
    if !created_release.draft
        || created_release.tag_name != tag
        || created_release.target_commitish != expected_sha
        || created_release.body.trim_end()
            != fs::read_to_string(&notes)
                .map_err(|e| e.to_string())?
                .trim_end()
    {
        return Err("created draft metadata mismatch; manual recovery required".into());
    }
    let edit = run("gh", &["release", "edit", tag, "--draft=false"], Some(root))?;
    ok(&edit, "GitHub release publish")?;
    confirm_published(root, tag)
}
fn verify_public_release(
    _root: &Path,
    tag: &str,
    expected_sha: &str,
    evidence: &Path,
) -> Result<(), String> {
    let url = format!("https://api.github.com/repos/{REPO}/releases/tags/{tag}");
    let out = run(
        "curl",
        &[
            "--disable",
            "--proto",
            "=https",
            "--proto-redir",
            "=https",
            "--fail",
            "--location",
            "--silent",
            "--show-error",
            &url,
        ],
        None,
    )?;
    ok(&out, "public release metadata")?;
    let release: ReleaseJson = serde_json::from_slice(&out.stdout)
        .map_err(|e| format!("invalid public release JSON: {e}"))?;
    let expected_names: Vec<_> = archive_names()
        .iter()
        .chain([&"SHA256SUMS"])
        .map(|x| (*x).to_owned())
        .collect();
    let actual_names: Vec<_> = release.assets.iter().map(|a| a.name.clone()).collect();
    if release.tag_name != tag
        || release.draft
        || release.prerelease
        || release.published_at.is_none()
        || release.target_commitish != expected_sha
        || actual_names != expected_names
    {
        return Err("public release metadata mismatch".into());
    }
    if release.assets.iter().any(|a| {
        a.size == 0
            || a.browser_download_url.as_deref()
                != Some(&format!(
                    "https://github.com/{REPO}/releases/download/{tag}/{}",
                    a.name
                ))
    }) {
        return Err("public release asset metadata mismatch".into());
    }
    let reference_url = format!("https://api.github.com/repos/{REPO}/git/ref/tags/{tag}");
    let reference = run(
        "curl",
        &[
            "--disable",
            "--proto",
            "=https",
            "--proto-redir",
            "=https",
            "--fail",
            "--location",
            "--silent",
            "--show-error",
            &reference_url,
        ],
        None,
    )?;
    ok(&reference, "public tag lookup")?;
    let reference_json: serde_json::Value = serde_json::from_slice(&reference.stdout)
        .map_err(|e| format!("invalid public tag JSON: {e}"))?;
    if reference_json
        .get("object")
        .and_then(|o| o.get("type"))
        .and_then(|v| v.as_str())
        != Some("tag")
    {
        return Err("public tag is not annotated".into());
    }
    let tag_sha = reference_json
        .get("object")
        .and_then(|o| o.get("sha"))
        .and_then(|v| v.as_str())
        .ok_or("public tag object SHA missing")?;
    let annotated_url = format!("https://api.github.com/repos/{REPO}/git/tags/{tag_sha}");
    let annotated = run(
        "curl",
        &[
            "--disable",
            "--proto",
            "=https",
            "--proto-redir",
            "=https",
            "--fail",
            "--location",
            "--silent",
            "--show-error",
            &annotated_url,
        ],
        None,
    )?;
    ok(&annotated, "public annotated tag lookup")?;
    let annotated_json: serde_json::Value = serde_json::from_slice(&annotated.stdout)
        .map_err(|e| format!("invalid public annotated tag JSON: {e}"))?;
    if annotated_json
        .get("object")
        .and_then(|o| o.get("type"))
        .and_then(|v| v.as_str())
        != Some("commit")
        || annotated_json
            .get("object")
            .and_then(|o| o.get("sha"))
            .and_then(|v| v.as_str())
            != Some(expected_sha)
    {
        return Err("public annotated tag target mismatch".into());
    }
    fs::write(evidence, serde_json::to_vec_pretty(&serde_json::json!({"tag": tag, "expected_sha": expected_sha, "body": release.body, "assets": release.assets.iter().map(|a| &a.name).collect::<Vec<_>>()})).map_err(|e| e.to_string())?).map_err(|e| e.to_string())
}
fn workflow_check(root: &Path) -> Result<(), String> {
    let text = fs::read_to_string(root.join(".github/workflows/release.yml"))
        .map_err(|e| e.to_string())?;
    for marker in [
        "validate:",
        "native:",
        "publish:",
        "verify-published:",
        "verify-release-metadata:",
        "x86_64-unknown-linux-gnu",
        "aarch64-unknown-linux-gnu",
        "aarch64-apple-darwin",
        "x86_64-apple-darwin",
        "libpq@17",
        "pkg-config",
        "readelf -d",
        "otool -L",
        "SHA256SUMS",
    ] {
        if !text.contains(marker) {
            return Err(format!("workflow missing {marker}"));
        }
    }
    let publish_pos = text
        .find("  publish:")
        .ok_or("workflow missing publish job")?;
    let native_pos = text
        .find("  native:")
        .ok_or("workflow missing native job")?;
    let verify_pos = text
        .find("  verify-published:")
        .ok_or("workflow missing public verification job")?;
    if native_pos > publish_pos
        || !text[publish_pos..].contains("needs: native")
        || !text[verify_pos..].contains("needs: publish")
    {
        return Err("workflow job dependency contract failed".into());
    }
    let archive_pos = text
        .find("verify-archive")
        .ok_or("workflow missing pre-extraction archive verification")?;
    let extract_pos = text
        .find("tar -xzf \"$RUNNER_TEMP/assets")
        .ok_or("workflow missing controlled extraction")?;
    if archive_pos > extract_pos {
        return Err("archive verification must precede extraction".into());
    }
    Ok(())
}
fn main() {
    if let Err(e) = real_main() {
        eprintln!("xtask: {e}");
        std::process::exit(1);
    }
}
fn real_main() -> Result<(), String> {
    let mut a = env::args().skip(1);
    let action = a.next().ok_or("usage: xtask <command>")?;
    let root = env::current_dir().map_err(|e| e.to_string())?;
    match action.as_str() {
        "expected-tag" => {
            let document = value(&root.join("Cargo.toml"))?;
            let v = document
                .get("workspace")
                .and_then(|x| x.get("package"))
                .and_then(|x| x.get("version"))
                .and_then(toml::Value::as_str)
                .ok_or("missing version")?;
            println!("{}", expected_tag(v));
        }
        "release-check" => {
            let t = a.next().ok_or("TAG required")?;
            validate_repository(&root, &t)?;
            extract_notes(
                &fs::read_to_string(root.join("CHANGELOG.md")).map_err(|e| e.to_string())?,
                &t[1..],
            )?;
            println!("release metadata valid: {t}");
        }
        "release-notes" => {
            let t = a.next().ok_or("TAG required")?;
            let out = PathBuf::from(a.next().ok_or("OUTPUT required")?);
            let v = validate_repository(&root, &t)?;
            fs::write(
                out,
                extract_notes(
                    &fs::read_to_string(root.join("CHANGELOG.md")).map_err(|e| e.to_string())?,
                    &v,
                )?,
            )
            .map_err(|e| e.to_string())?;
        }
        "workflow-check" => workflow_check(&root)?,
        "release-build" => {
            let tag = a.next().ok_or("TAG required")?;
            let target = a.next().ok_or("TARGET required")?;
            if !TARGETS.iter().any(|candidate| *candidate == target) {
                return Err("unsupported target".into());
            }
            let binary = PathBuf::from(a.next().ok_or("BINARY required")?);
            validate_repository(&root, &tag)?;
            let build = run(
                "cargo",
                &[
                    "build",
                    "--locked",
                    "--release",
                    "-p",
                    PRODUCT,
                    "--bin",
                    PRODUCT,
                    "--target",
                    &target,
                ],
                Some(&root),
            )?;
            ok(&build, "release build")?;
            verify_binary(&binary, &tag[1..])?;
            println!("release build verified: {}", binary.display());
        }
        "fetch-public-assets" => {
            let tag = a.next().ok_or("TAG required")?;
            let dir = PathBuf::from(a.next().ok_or("ASSET_DIR required")?);
            fetch_public_assets(&tag, &dir)?;
        }
        "verify-archive" => {
            let dir = PathBuf::from(a.next().ok_or("ASSET_DIR required")?);
            verify_archive(&dir)?;
        }
        "verify-downloads" => {
            let tag = a.next().ok_or("TAG required")?;
            let dir = PathBuf::from(a.next().ok_or("ASSET_DIR required")?);
            let binary = PathBuf::from(a.next().ok_or("BINARY required")?);
            verify_downloads(&root, &tag, &dir, &binary)?;
        }
        "publish" => {
            let tag = a.next().ok_or("TAG required")?;
            let sha = a.next().ok_or("EXPECTED_SHA required")?;
            let dir = PathBuf::from(a.next().ok_or("ASSET_DIR required")?);
            publish(&root, &tag, &sha, &dir)?;
        }
        "verify-public-release" => {
            let tag = a.next().ok_or("TAG required")?;
            let sha = a.next().ok_or("EXPECTED_SHA required")?;
            let evidence = PathBuf::from(a.next().ok_or("EVIDENCE_PATH required")?);
            verify_public_release(&root, &tag, &sha, &evidence)?;
        }
        "native-smoke" => {
            let t = a.next().ok_or("TAG required")?;
            let b = PathBuf::from(a.next().ok_or("BINARY required")?);
            let v = validate_repository(&root, &t)?;
            verify_binary(&b, &v)?;
        }
        "package" => {
            let t = a.next().ok_or("TAG required")?;
            let target = a.next().ok_or("TARGET required")?;
            if !TARGETS.iter().any(|candidate| *candidate == target) {
                return Err("unsupported target".into());
            }
            let b = PathBuf::from(a.next().ok_or("BINARY required")?);
            let out = PathBuf::from(a.next().ok_or("OUTPUT required")?);
            let v = validate_repository(&root, &t)?;
            verify_binary(&b, &v)?;
            fs::create_dir_all(&out).map_err(|e| e.to_string())?;
            let archive = out.join(format!("{PRODUCT}-{target}.tar.gz"));
            if archive.exists() {
                return Err("refusing to overwrite archive".into());
            }
            let stage = out.join(format!(
                ".stage-{}",
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            fs::create_dir(&stage).map_err(|e| e.to_string())?;
            fs::copy(&b, stage.join(PRODUCT)).map_err(|e| e.to_string())?;
            let ar = archive.to_str().ok_or("bad archive path")?;
            let st = stage.to_str().ok_or("bad stage path")?;
            let o = run("tar", &["-czf", ar, "-C", st, PRODUCT], Some(&root))?;
            ok(&o, "tar")?;
            fs::remove_dir_all(stage).ok();
            println!("{}", archive.display());
        }
        _ => return Err(format!("unknown command {action}")),
    }
    Ok(())
}
