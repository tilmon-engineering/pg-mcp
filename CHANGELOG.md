# Changelog

All notable changes to `postgres-mcp` are documented here.

## [Unreleased]

- Further improvements are planned.

## [0.1.1] - 2026-09-23

### Changed

Notes were finalized from inspected history spanning published `v0.1.0` through implementation commit `9aa0550` (2 commits); this subsequent notes-only commit is not part of that inspected history.

### Changed

- The release publisher now uses the canonical HTTPS repository origin when publishing GitHub releases, avoiding dependence on the checkout's configured remote URL.

### Platform notes

Release targets and runtime requirements are unchanged from 0.1.0: Linux GNU/glibc on Ubuntu 24.04 and macOS with compatible Homebrew `libpq@17`; macOS artifacts remain unsigned and unnotarized.

## [0.1.0] - 2026-09-17

### Added

- Initial PostgreSQL MCP server with bounded, profile-aware libpq workers.
- Native release archives for four explicitly supported targets: Linux GNU x86_64 and aarch64, and macOS x86_64 and arm64.
- Checksummed GitHub release assets and dynamic `libpq >=17` discovery through pkg-config.

### Platform notes

These archives are dynamically linked and are not standalone or statically portable. Linux artifacts use the Ubuntu 24.04 GNU/glibc baseline. macOS requires a compatible Homebrew `libpq@17` runtime, and artifacts are unsigned and unnotarized.
