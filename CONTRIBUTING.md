# Contributing to Kora

Thanks for your interest in contributing to Kora! This document covers the basics.

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/<your-username>/Kora.git`
3. Create a feature branch: `git checkout -b feat/your-feature`
4. Make your changes
5. Submit a pull request

## Development Setup

```bash
# Build the workspace
cargo build

# Run all tests
cargo test --workspace

# Run lints
cargo clippy --workspace --all-targets

# Format code
cargo fmt --all
```

Requires Rust 1.75+ (edition 2021).

## Code Standards

- All code must pass `cargo clippy` with zero warnings
- All code must be formatted with `cargo fmt`
- All tests must pass before submitting a PR
- Public items should have doc comments (`///`)
- Use `thiserror` for library error types
- Never use `unsafe` without a `// SAFETY:` comment
- Avoid allocations in hot paths

## Architecture Rules

- `kora-core` has zero internal workspace dependencies — keep it that way
- Each shard worker owns its data and I/O — no locks on the data path
- Use message passing via channels for cross-shard communication
- Follow the existing crate dependency graph (strictly acyclic)

## Testing

- Unit tests go in `#[cfg(test)] mod tests {}` blocks
- Integration tests go in `tests/` directories within each crate
- Benchmarks use Criterion in `benches/` directories
- Run the full suite before submitting: `cargo fmt --all && cargo clippy --workspace --all-targets && cargo test --workspace`

## Commit Messages

Use the format: `type: description`

Types: `feat`, `fix`, `refactor`, `docs`, `test`, `perf`, `chore`

Keep commits focused — one logical change per commit.

## Pull Requests

- Keep PRs focused on a single change
- Include tests for new functionality
- Update documentation if behavior changes
- Reference any related issues

## Reporting Bugs

Open an issue with:
- Steps to reproduce
- Expected vs actual behavior
- Kora version and platform

## Security Issues

Do **not** open public issues for security vulnerabilities. See [SECURITY.md](SECURITY.md) for responsible disclosure instructions.
