# Changelog

## v0.1.0 — 2026-10-28

- Tencent Cloud AS target for the unmodified Nomad Autoscaler Agent.
- Absolute desired capacity, exact instance deletion and SDK node drain/post processing.
- Persistent recovery, partial-failure backoff, registration grace and optional scale-in protection.
- Instance-role and explicit API-key authentication, Nomad ACL/TLS and per-group state isolation.
- Linux amd64/arm64 plugin and recovery CLI; build provenance, checksums and dependency notices.

Tested baseline: Agent v0.6.0, Nomad 2.0.7, Autoscaler SDK v0.5.0; Go 1.26.6 builds.
Set `restart_cooldown` to the policy cooldown and set `policy_dir` to the Agent's policy directory.
Use a controlled Agent restart for policy changes; see [operations](docs/operations.md).
Release evidence and packaging instructions are in [release preparation](docs/release.md).
