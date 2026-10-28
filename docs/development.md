# Development and verification

Build and test in a dedicated Linux workspace/CI. Keep cloud credentials out of normal tests; the user/developer workstation does not need a Nomad container cluster. Go requirements and exact dependencies are in `go.mod`/`go.sum`. Record `go version`, the source digest and actual commands for each run.

```sh
make test
make race
go vet -mod=readonly ./...
make cross-build
```

`make build` also builds development probes. `make cross-build` builds the plugin and recovery tool for Linux amd64/arm64. These commands do not approve or publish a release. The project license is in LICENSE; candidate preparation and CI configuration are described separately. For an exact review-source export, the [artifact build guide](building.md) describes source/hash guards, build provenance, dependency-license candidates and reproduction checks; its output is still unapproved.

## Code boundaries

| Package | Responsibility |
| --- | --- |
| `plugin` | Base/Target RPCs, Agent/policy routing, authenticated account isolation and request telemetry. |
| `internal/targetconfig` | Agent/policy validation and SDK option mapping; no cloud calls. |
| `internal/tencentcloud` | Selected credential source, caller identity, bounded API transport and exact AS/CVM observations/mutations. |
| `internal/nomadclient` / `internal/nomadsdk` | Shared connection/identity, task visibility and official scaleutils integration. |
| `internal/live` | Single-writer workflow, durable fixed intent, completion, recovery and audit. |
| `internal/telemetry` | Loopback counters, independent of cloud/Nomad reads. |

Use official `ClusterNodeIDLookupFunc`, pre/post helpers and selection options through the adapter. A mock ranking method is not proof of SDK selection behavior. Never remove operation provenance or turn an accepted/unknown action into completed merely to satisfy a test.

## Evidence levels

Normal Go tests use synthetic cloud/Nomad services and real local files. Some tests use actual SDK/gRPC, process exits, locks or storage faults. Tests requiring an official Agent or Nomad runtime explicitly skip without their pinned binaries; an all-package pass does not turn these skips into runtime acceptance.

Example loading can be checked with an approved official Agent binary. Export the two non-secret runtime variables first:

```sh
: "${NOMAD_AUTOSCALER_TEST_BIN:?export the official binary path}"
: "${NOMAD_AUTOSCALER_TEST_SHA256:?export its verified SHA256}"
go test -race -mod=readonly ./plugin \
  -run '^TestStandardAgentDocumentationExamples$' -count=1 -json
```

That test loads the default target through an external **test-only** executable. Only the cloud authentication/API factory is replaced. It copies the example files, replaces local addresses/paths, loads their disabled policies, then enables an isolated copy against a synthetic no-op target/metric. It does not demonstrate live credential validation, real Prometheus data, cloud permissions, or a scaling operation. Never ship a `go test -c` helper as the product plugin.

Separate checks cover official Agent evaluation/cooldown, real Nomad tasks/ACL/TLS, real AS/CVM effects, recovery after faults, workload `1→5→1`, two-architecture loading, source/artifact identity and installation verification. Changing an exported source/module or dependencies requires the affected verification again.

## Reproducibility and publication

Preserve the original failed run and add a new result after repair. A polling timeout does not mean the worker stopped; inspect/re-poll its exact handle. Keep source, commands, artifact SHA256 and model/real boundaries together.

Export only the files selected by `scripts/prepare_source.py`, then review the exact snapshot and package metadata. Module rewriting requires an external privacy policy; same-module exports use the built-in checks and may supply additional rules. Rules and local evidence are not exported. See [building](building.md) for the policy schema and [release preparation](release.md) for publication approval. A passing scan does not prove that all content may be published.
