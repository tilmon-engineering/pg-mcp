use postgres_mcp_core::config::ConfigFile;

#[test]
fn documentation_contract() {
    let example = include_str!("../../../config.example.toml");
    assert!(
        ConfigFile::from_toml(example)
            .unwrap()
            .profiles
            .contains_key("local")
    );
    let readme = include_str!("../../../README.md");
    let design = include_str!("../../../DESIGN.md");
    for tool in [
        "open_database",
        "list_handles",
        "get_schema",
        "open_read",
        "open_write",
        "query",
        "commit",
        "rollback",
        "close_database",
    ] {
        assert!(readme.contains(tool), "README lacks {tool}");
        assert!(design.contains(tool), "DESIGN lacks {tool}");
    }
    for key in [
        "type_oid",
        "value",
        "columns",
        "rows",
        "command_tag",
        "affected_rows",
        "handle_state",
        "next_moves",
        "transaction_status",
        "transaction_open",
    ] {
        assert!(readme.contains(key), "README lacks {key}");
        assert!(design.contains(key), "DESIGN lacks {key}");
    }
    assert!(readme.contains("PG_MCP_LIVE=1 cargo test --workspace --locked -- --ignored"));
    assert!(readme.contains("not a general side-effect sandbox"));
    assert!(readme.contains("libpq >=17"));
    assert!(readme.contains("incoming-frame allocation"));
    assert!(readme.contains("libpq's internal row/message allocation"));
    assert!(readme.contains("never automatically replay a commit"));
    assert!(readme.contains("verified row-only commits and rollbacks"));
    assert!(design.contains("SELECT-tagged `CREATE TABLE AS`/`SELECT INTO`"));
}
