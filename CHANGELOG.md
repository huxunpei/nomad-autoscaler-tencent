# Changelog

## v0.1.1 — 2026-10-30

Bug fixes and documentation. The plugin binaries change.

- Return startup authentication errors without a nil-pointer panic or credential-source fallback, with regression coverage through the production constructor.
- Add the sanitized cloud `cause_code` to group-initialization retry warnings while preserving retry behavior; an empty group result remains ambiguous between absence and permission filtering.
- Clarify CAM resource regions and root-account UINs, and the evidence needed to distinguish unchanged capacity from no submitted write request.

- README: step-by-step install from a release (download, verify, unpack, install), terms, state-directory requirements, and what a healthy start looks like; the Chinese README now has the same steps.
- Permissions: the read-only CVM statement used in the real-cloud checks, and a note that STS `GetCallerIdentity` needed no statement.
- Operations: a troubleshooting table of startup and runtime messages, and a note on the recovery tool's subcommands.
- Configuration: state-directory mode and ownership, how to set `cloud_instance_id` at boot, the metadata service host, and definitions of the terms used.
- Corrected the install-verification wording and two Tencent documentation links.

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
