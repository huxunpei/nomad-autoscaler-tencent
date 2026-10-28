# Plugin loading on each architecture

This smoke test runs an **actual product plugin binary** under an unmodified official Nomad Autoscaler Agent. It explicitly selects the local `simulation` backend, with synthetic members and journal files. No Tencent credentials are supplied; the Nomad policy source is disabled. The three scenarios are dry-run at count 1, synthetic 1→5, and synthetic 5→1 after restarting the Agent. They verify loading/RPC/state persistence, not the default authenticated target, official SDK drain/post, real Nomad jobs or cloud scaling.

Use the exact plugin from an inspected build manifest and `SHA256SUMS`; never substitute a `go test -c` executable. Verify the official Agent archive against its release checksum file, then record the extracted binary hash. CLI hash arguments below refer to **binary files**, not ZIP/DEB archive hashes. Keep the archive provenance separately.

## Native execution

On a dedicated Linux machine matching the Agent/plugin architecture:

```sh
: "${SMOKE_AGENT:?set the official Agent binary path}"
: "${SMOKE_PLUGIN:?set the product plugin binary path}"
: "${SMOKE_AGENT_SHA256:?set the verified extracted binary hash}"
: "${SMOKE_PLUGIN_SHA256:?set the build artifact hash}"
python3 -B scripts/test_agent_smoke.py
python3 -B scripts/agent_smoke.py \
  --agent "$SMOKE_AGENT" --agent-sha256 "$SMOKE_AGENT_SHA256" \
  --plugin "$SMOKE_PLUGIN" --plugin-sha256 "$SMOKE_PLUGIN_SHA256" \
  --report /tmp/new-native-smoke.json
```

The report path must be new. The script retains its generated configuration, logs and synthetic state in its reported temporary directory. It strips cloud/Nomad/proxy credential variables, preserves the existing HOME, and disables checkpoint checks. Each Agent has its own process session; timed-out shutdown kills that owned process group. Check recorded Agent/plugin PIDs and listeners independently after the test. Keep raw reports private until paths and local topology have been reviewed for publication.

## AArch64 user-mode emulation

When a native arm64 runner is unavailable, an explicit Linux AArch64 user emulator can execute the arm64 Agent and plugin ELF files on the test host. Both payloads must have ELF machine 183. The helper generates native shell launchers which delegate to the supplied emulator, including a stable launcher argv[0] for re-exec; the actual Agent/plugin files remain unchanged and are hashed before/after. It does not install or register binfmt handlers.

```sh
: "${SMOKE_EMULATOR:?set the inspected AArch64 user emulator path}"
: "${SMOKE_EMULATOR_SHA256:?set its verified extracted binary hash}"
python3 -B scripts/agent_smoke.py \
  --agent "$SMOKE_AGENT" --agent-sha256 "$SMOKE_AGENT_SHA256" \
  --plugin "$SMOKE_PLUGIN" --plugin-sha256 "$SMOKE_PLUGIN_SHA256" \
  --emulator "$SMOKE_EMULATOR" --emulator-sha256 "$SMOKE_EMULATOR_SHA256" \
  --report /tmp/new-emulated-smoke.json
```

The report says `aarch64-user-emulation`, host architecture, ELF machines and emulator version/hash. Keep this separate from native arm64 results. It provides evidence of ARM64 execution and Agent/plugin interaction under the tested emulator; native behavior/performance and the supported final platform matrix remain separate. Native-hardware support requires a separate result for the exact release candidate.

One preparation run used an extracted [Debian bookworm qemu-user-static package](https://packages.debian.org/bookworm/amd64/qemu-user-static/download), checked against its published SHA256, without running package installation scripts. This is test tooling, not a plugin dependency to bundle with the product. Pin and inspect the chosen emulator separately. No privileged container, kernel binfmt registration, new cloud instance or local workstation cluster is required by this helper.

## Evidence and remaining gates

Record the official Agent version/archive and binary hashes, exact product build/source manifest, plugin hash, mode/host/ELF architecture, three scenario outcomes and exit codes, logs and process cleanup. The report labels zero cloud/Nomad API calls according to the explicit local simulation route; it is not a packet capture or proof of cloud credentials/permissions. A recovery tool `--help` check validates startup/CLI only.

This does not test cooldown after restart, policy reload/full boundary semantics, default instance identity, SDK lifecycle, failures or real workload `1→5→1`. Keep known official Agent failures open. Release validation must identify the tested source/runtime/platform combination and the separate runtime, cloud and installation results. Rebuild/retest affected inputs after changes; helper-only changes do not turn an old product build into a newly frozen release.
