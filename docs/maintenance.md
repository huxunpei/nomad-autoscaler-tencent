# Maintenance plan

## Project and maintenance commitments

Project details and maintenance commitments:

- Repository: `huxunpei/nomad-autoscaler-tencent`.
- Public Go module path: `github.com/huxunpei/nomad-autoscaler-tencent`.
- License: MPL-2.0.
- Maintainer: xunpeihu (GitHub: `huxunpei`).
- Fix deadline: one month.

The repository and module identify the publication target. See [release preparation](release.md) for the current release status.

## Supported-version records

For each release, publish the source revision, Go version, module checksums,
official Autoscaler and Nomad versions, and hashes of Linux amd64/arm64 binaries.
Distinguish the Autoscaler SDK dependency from the Agent executable: successful
compilation against one SDK is not Agent compatibility evidence. Only list
versions that actually passed the required checks; never claim an untested
future version or require a privately patched Agent.

## Review triggers and ownership

The designated maintainer reviews upstream Agent/SDK releases and Tencent SDK/API
changes at least monthly, and before each plugin release. Security advisories,
credential refresh changes, drain/purge behavior changes and cloud failure
contract changes trigger an additional review. These are planned duties, not a
configured scheduler or a record of completed reviews.

For each change, record the upstream version/source, affected behavior, decision
to update or defer, responsible maintainer and next review date. Dependency
updates need an explanation of observable behavior, not just a passing build.
Keep supported-version records and known differences beside the release notes.

## Required change checks

1. Compare configuration defaults, plugin RPC, SDK selection/drain/post behavior,
   credential acquisition/refresh, API response fields and failure semantics.
   Preserve the absolute-target and exact-node deletion contracts.
2. Add a regression for any changed behavior. Run the [development checks](checks.md)
   on an exact source export, and affected official Agent/real Nomad scenarios.
   Report skipped cases separately.
3. Rebuild both architectures from the same source and verify actual plugin
   loading as described in [architecture checks](architecture-smoke.md). For
   changes affecting cloud writes, authentication or recovery, rerun the relevant
   dedicated-cloud scenarios, including capacity, exact deletion and next action.
4. Exercise restart with representative saved journals and interrupted audit
   transactions. Review schema compatibility before upgrade; an older binary
   may reject new fields. Stop evaluations and retain evidence before rollback.
   Never remove journal/audit fields to force an incompatible downgrade.
5. Review docs, required permissions, dependency/license inventory and exported
   artifacts. Publish the tested scope, remaining failures and recovery guidance.
   Passing a model test does not close a real-cloud acceptance item.

## Bug and security handling

Use the agreed repository/support channel for reports. Request versions,
sanitized configuration, operation/request/activity IDs and the relevant time
window; never request credentials or an unredacted state archive. Triage
incorrect deletion, unauthorized scope, credential exposure and stuck recovery
first. Record impact, reproduction, fix/regression evidence and affected
versions; a maintainer approves release and communication.

Record the reporting channel, supported-version lifetime and responsibility
for test resources with the release. Any additional
response targets or backup-maintainer arrangements require a separate agreement;
the commitments confirmed above remain the current baseline.
