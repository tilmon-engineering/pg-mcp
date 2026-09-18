#!/usr/bin/env bash
set -euo pipefail

repo_root=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
export PKG_CONFIG_PATH="${PKG_CONFIG_PATH:-/home/linuxbrew/.linuxbrew/opt/libpq/lib/pkgconfig}"
export LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-/home/linuxbrew/.linuxbrew/opt/libpq/lib}"

exec mise exec -- cargo run --locked --manifest-path "$repo_root/Cargo.toml" \
  --bin postgres-mcp -- --config "$repo_root/config.local.toml"
