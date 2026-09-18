# Changelog

All notable changes to `postgres-mcp` are documented here.

## [Unreleased]

- Further improvements are planned.

## [0.1.0] - 2026-09-17

### Added

- Initial PostgreSQL MCP server with bounded, profile-aware libpq workers.
- Native release archives for four explicitly supported targets: Linux GNU x86_64 and aarch64, and macOS x86_64 and arm64.
- Checksummed GitHub release assets and dynamic `libpq >=17` discovery through pkg-config.

### Platform notes

These archives are dynamically linked and are not standalone or statically portable. Linux artifacts use the Ubuntu 24.04 GNU/glibc baseline. macOS requires a compatible Homebrew `libpq@17` runtime, and artifacts are unsigned and unnotarized.
