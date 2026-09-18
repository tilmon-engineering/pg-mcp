use std::env;

const MINIMUM_LIBPQ: (u32, u32, u32) = (17, 0, 0);

fn main() {
    println!("cargo:rerun-if-env-changed=PKG_CONFIG_PATH");
    println!("cargo:rerun-if-env-changed=PKG_CONFIG_LIBDIR");
    println!("cargo:rerun-if-env-changed=PKG_CONFIG_SYSROOT_DIR");
    println!("cargo:rerun-if-env-changed=PKG_CONFIG_ALLOW_CROSS");
    println!("cargo:rerun-if-env-changed=TARGET");

    reject_unsupported_environment();

    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    let target = env::var("TARGET").unwrap_or_default();
    if !matches!(
        target.as_str(),
        "x86_64-unknown-linux-gnu"
            | "aarch64-unknown-linux-gnu"
            | "aarch64-apple-darwin"
            | "x86_64-apple-darwin"
    ) || !matches!(target_os.as_str(), "linux" | "macos")
    {
        panic!(
            "postgres-mcp supports only the selected Linux GNU and macOS Darwin targets with dynamic libpq"
        );
    }

    // Keep this probe deliberately independent from pq-sys's optional pg_config,
    // vcpkg, and bundled paths.  `statik(false)` is important: accepting a
    // static pkg-config answer would violate the distribution contract even if
    // the version itself is new enough.
    let library = pkg_config::Config::new()
        .statik(false)
        .atleast_version("17")
        .probe("libpq")
        .unwrap_or_else(|error| {
            panic!("libpq >= 17 is required through dynamic pkg-config discovery; {error}")
        });

    let version = library.version.trim();
    if !version_at_least(version, MINIMUM_LIBPQ) {
        panic!("pkg-config selected libpq {version}, but libpq >= 17.0.0 is required");
    }
    if !library.link_files.is_empty()
        || !library.frameworks.is_empty()
        || library
            .ld_args
            .iter()
            .flatten()
            .any(|argument| argument.contains("-Bstatic") || argument.contains("--whole-archive"))
    {
        panic!("pkg-config returned static or unsupported libpq link metadata");
    }
    if !library.libs.iter().any(|name| name == "pq") {
        panic!("pkg-config did not provide the dynamic libpq library");
    }

    for path in &library.include_paths {
        println!(
            "cargo:warning=postgres-mcp libpq include path: {}",
            path.display()
        );
    }
    for path in &library.link_paths {
        println!(
            "cargo:warning=postgres-mcp libpq dynamic link path: {}",
            path.display()
        );
    }
    println!("cargo:warning=postgres-mcp libpq pkg-config version: {version}");
}

fn reject_unsupported_environment() {
    let names = [
        "PQ_LIB_DIR",
        "PQ_LIB_STATIC",
        "LIBPQ_STATIC",
        "PKG_CONFIG_ALL_STATIC",
        "PG_CONFIG",
        "PQ_LIB_DIR_x86_64_unknown_linux_gnu",
        "PQ_LIB_STATIC_x86_64_unknown_linux_gnu",
        "LIBPQ_STATIC_x86_64_unknown_linux_gnu",
        "PG_CONFIG_x86_64_unknown_linux_gnu",
    ];
    for name in names {
        if env::var_os(name).is_some() {
            panic!("{name} is unsupported: use dynamic libpq discovered by pkg-config");
        }
    }
    for (name, _) in env::vars_os() {
        let upper = name.to_string_lossy().to_ascii_uppercase();
        if upper.starts_with("PQ_LIB_DIR_")
            || upper.starts_with("PQ_LIB_STATIC_")
            || upper.starts_with("LIBPQ_STATIC_")
            || upper.starts_with("PG_CONFIG_")
        {
            panic!(
                "{} is unsupported: use dynamic libpq discovered by pkg-config",
                name.to_string_lossy()
            );
        }
    }

    // pq-sys can also be asked to build bundled variants through Cargo feature
    // environment variables.  Keep the check here so a future workspace edit
    // cannot silently change the native distribution mode.
    for (name, value) in env::vars() {
        if name.starts_with("CARGO_FEATURE_")
            && (name.ends_with("BUNDLED") || name.ends_with("BUNDLED_WITHOUT_OPENSSL"))
        {
            panic!("{name}={value} is unsupported: postgres-mcp requires dynamic system libpq");
        }
    }
}

fn version_at_least(value: &str, minimum: (u32, u32, u32)) -> bool {
    let mut components = value
        .split(|character: char| !character.is_ascii_digit())
        .filter(|component| !component.is_empty())
        .map(|component| component.parse::<u32>().unwrap_or(0));
    let actual = (
        components.next().unwrap_or(0),
        components.next().unwrap_or(0),
        components.next().unwrap_or(0),
    );
    actual >= minimum
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_major_minor_patch_versions() {
        assert!(version_at_least("17.0", MINIMUM_LIBPQ));
        assert!(version_at_least("18.6", MINIMUM_LIBPQ));
        assert!(!version_at_least("16.9.9", MINIMUM_LIBPQ));
        assert!(!version_at_least("", MINIMUM_LIBPQ));
    }

    #[test]
    fn version_comparison_does_not_treat_text_as_ordered() {
        assert!(version_at_least("17.10.0", MINIMUM_LIBPQ));
        assert!(!version_at_least("9.99.99", MINIMUM_LIBPQ));
    }
}
