# Configuration reference

This reference describes the current default target. Do not set `tencent_backend`: `simulation` and `live-experimental` are separate historical development modes. Values inside the Agent target `config` and policy target block are strings.

## Agent target configuration

Use the [Agent template](../examples/autoscaler.hcl). One authenticated target can discover multiple policy groups; persistent routing is isolated by validated account, region and ASG. Use one active writer per group and preserve its state. Cross-host coordination is not provided by a local file lock.

| Key | Default | Meaning |
| --- | --- | --- |
| `tencent_region` | required | Region of the target groups; a different region needs a separately configured target instance. |
| `tencent_auth_method` | `instance_identity` | Use the current CVM's existing instance role; `api_key` is explicit. No silent fallback or IAM creation. |
| `tencent_credentials_file` | empty | With `api_key`, an explicit absolute JSON file path. Without it, use the selected process environment. |
| `tencent_state_dir` | `/var/lib/nomad-autoscaler/tencent-as` | Persistent absolute private directory, not `/`; account/region/group routes and locks live below it. |
| `policy_dir` | empty | Absolute path matching the Agent's file-policy directory. Enables advisory policy min/max checks; no path produces an Info message and skips this diagnostic. |
| `tencent_registration_grace_period` | `30m` | Positive initial-registration duration measured from AS AddTime. Waiting protects the VM and gates readiness; expiry reports failure and permits otherwise safe evaluation, never automatic deletion. See [registration grace](registration-grace.md). |
| `retry_attempts` | `15` | Positive stabilization poll budget per acknowledged cloud write; separate from bounded API throttling retries and drain/task deadlines. The normal poll interval is 5 seconds. |
| `scale_in_protection` | `false` | Enable filtering of AS scale-in protection; policy may override it. This never removes protection or bypasses CVM deletion protection. |
| `tencent_metrics_listen_address` | empty | Disable the optional plugin metrics listener, or select a numeric loopback endpoint such as `127.0.0.1:9273`. Other interfaces/hostnames are rejected. |

## Policy target configuration

Group membership and cloud limits come from AS. The policy target contains the group ID; do not repeat it in Agent config. Top-level policy `min`/`max`, cooldown, checks and evaluation interval belong to the Agent. `restart_cooldown` is the one target setting that must repeat the policy cooldown; see below. See the [official policy structure](https://developer.hashicorp.com/nomad/tools/autoscaling/policy).

| Key | Default | Meaning |
| --- | --- | --- |
| `tencent_group_id` | required | Exact ASG ID. |
| `dry-run` | `false` | Standard Agent execution control: exactly `"true"` or `"false"`. True skips new scaling requests; status observation remains active. |
| `node_class` | empty | SDK node-class filter. |
| `datacenter` | empty | SDK datacenter filter. |
| `node_pool` | empty | SDK node-pool filter. Filters can be combined. |
| `node_selector_strategy` | `least_busy` | SDK strategy: `least_busy`, `newest_create_index`, `oldest_create_index`, `empty`, or `empty_ignore_system`. Candidates still require verified cloud/node identity. |
| `node_drain_deadline` | `15m` | Go duration passed to the Nomad drain contract. Positive values are not capped by the old prototype's 24-hour limit; `0` disables Nomad's drain deadline, negative requests force-drain semantics. Required task/poststop observation and bounded controller recovery still apply. |
| `node_drain_ignore_system_jobs` | `false` | Ignore system jobs in drain/task completion when explicitly true. Does not ignore required poststop tasks of other jobs. |
| `node_purge` | `false` | Run SDK purge of the exact Nomad record after cloud removal is confirmed. False retains the node record. |
| `instance_id_meta_key` | `cloud_instance_id` | Exact VM-ID metadata key used for unique mapping. No implicit private-IP fallback. |
| `scale_in_protection` | Agent value | Optional override of the Agent AS-protection filter. |
| `restart_cooldown` | unset | Positive Go duration. After this plugin process starts, a successful completion from the previous process stays `Ready=false` until `last_event + restart_cooldown`. Omit it and an official Agent restart can scale again immediately. |

Durations whose derived controller wait cannot be represented are rejected; removing the old 24-hour cap does not permit arithmetic overflow. Unknown or misplaced settings are rejected. Empty filters are valid; they do not allow a candidate from another ASG. A protection/filter shortage is an error, not successful scale-in to a different target.

## Restart cooldown

Set `restart_cooldown` on every policy target. Use the same duration as that policy's `cooldown`. If `cooldown_on_scale_up` is longer, use the longer of the two: one value has to cover both directions, and the shorter one would leave the other direction unprotected after a restart.

Official Autoscaler v0.5.0 and v0.6.0, and current `main`, read `nomad_autoscaler.last_event` after a restart but do not store the deadline `Run` checks. The first evaluation therefore treats the remaining cooldown as already over. While this setting still covers that completion, Status keeps the real count and completion time and returns `Ready=false` with `tencent.restart_cooldown=active`. The official Agent skips evaluation while the target is not ready, so it does not scale. When the window ends, readiness returns to the normal result. Completions made by the already-running process are not held here; the Agent's own cooldown still applies to those.

The key is optional only in parsing. Leaving it unset preserves the restart bug. It is not stored in the group route or SDK policy, so changing it does not require the offline policy update. Put it in the policy target block, not in the Agent target config. The [fixed](../examples/cluster-fixed.hcl) and [Prometheus](../examples/cluster-prometheus.hcl) examples both set `2m` next to `cooldown = "2m"`.

This does not change the official Agent. The deadline fix is [hashicorp/nomad-autoscaler#1362](https://github.com/hashicorp/nomad-autoscaler/pull/1362). Until a release contains it, a deployment that omits `restart_cooldown` can scale again immediately after a restart.

`dry-run` does not execute node selection, drain, or cloud mutations for a new Scale request, and does not simulate changed capacity. It is not a global pause: recovery of operations already recorded in persistent state may continue. The flag is excluded from the persistent group policy and SDK options, so changing it alone does not require offline policy adoption. For the tested Agent versions, stop and restart the Agent to ensure an updated file policy is loaded; this flag does not resolve Agent reload behavior.

The [minimal example](../examples/cluster-fixed.hcl) explicitly uses `10m`, as a deployment example rather than the `15m` default. Choose only one policy per ASG. Both supplied policies are disabled. Enabling a policy and changing its fixed target can cause real billed instance creation/deletion.

## Policy boundary diagnostics and growth failures

Set the target plugin's `policy_dir` to the same directory as the Agent's `policy.dir`. The Target RPC does not carry this path or full policy min/max. The plugin reads enabled cluster policies for `tencent-as`, using the same SDK structures and HCL/JSON decoder as the Agent. It reads at startup and refreshes at most once per minute during Status. Each matching group's policies are compared with current AS limits; warnings include file/policy names and both ranges and are deduplicated until a boundary changes. For example, policy `1..20` and group `2..10` warn even when the requested target is `5`.

Parsing or read failures only produce diagnostics. File contents can temporarily differ from the Agent's loaded policy; these checks never choose targets or authorize writes. Execution always uses the current cloud group limits. Keep diagnostic files regular and at most 1 MiB each; symlinks are skipped.

New growth uses `ModifyDesiredCapacity` with the absolute target and `AssertOriginDesiredCapacity` from the durable pre-write observation. A lost response triggers a fresh Desired read: an installed target is followed without replay; an unchanged baseline may retry the same target and assertion. A different Desired or an explicit assertion rejection stops automatic replay and reports `DesiredCapacityConflict`. The assertion compares capacity values, not a monotonic version; it cannot detect a capacity that changed and later returned to the original value. Use one active capacity writer and investigate conflicts without deleting state.

If the latest post-write growth activity has failed/partially succeeded, the group is inactive and actual capacity is below Desired, the operation fails without a success timestamp. Once the observed members are stable, Status can return `Ready=true` with actual Count while Desired remains unchanged. Repeated failures for the same target delay the next cloud write by 2, 4, 8, 16, then at most 30 minutes. This backoff survives process restart; Scale fails during the window and does not write. A changed target or successful operation resets it. The next normal evaluation resends the same absolute target to resume AS retries. This is separate from successful-action cooldown and API throttling retries. Existing in-flight `ScaleOutInstances` records retain their original recovery path.

Disabled groups return `Ready=false` with a warning; owned pre-delete drains are restored and no new deletion proceeds. Scaling policies and scheduled actions cause warnings because they can compete for capacity. Unsupported allocation modes (including `SPOT_MIXED`) and non-hourly launch configurations are rejected in both directions. Wake-up-stopped mode is diagnosed and remains outside the initial supported deployment scope.

## Updating an existing policy

Use the [offline preview/apply procedure](safe-policy-updates.md) for a stopped, idle group when changing filters, strategy, drain/purge or protection settings. Keep complete state and audit history. A mapping-key change remains blocked until separate identity verification is available; merely restarting with different target options does not adopt them.

## Node identity and bootstrap

Merge identity into your existing Nomad client configuration at VM bootstrap, using the trusted instance ID from the launch environment:

```hcl
client {
  meta {
    cloud_instance_id = "ins-example"
  }
}
```

This is a fragment, not a standalone client configuration. Each client needs its own real ID and connectivity/authentication to the intended Nomad servers. If a custom key is configured, write that key consistently. The current default target obtains group/region membership from cloud reads; extra `tencent_group_id`/`tencent_region` node metadata are not required. Other providers' nodes must not collide with the selected mapping.

Starting/unregistered VMs count toward capacity and are never selected for deletion. The [registration grace contract](registration-grace.md) defaults to 30 minutes from AS AddTime. After timeout, report the failed registration and permit other safe operations when the cloud is stable; do not automatically destroy or replace the absent VM. A late registration clears the timeout. Query errors and ambiguous mappings remain blocked.

## Credentials

Default instance identity requires an already available CVM instance role and metadata access. Startup performs a bounded `GetCallerIdentity` validation; failure does not try static credentials. It does not prove all group permissions.

For `api_key` without a file, supply `TENCENTCLOUD_SECRET_ID`, `TENCENTCLOUD_SECRET_KEY`, and optional `TENCENTCLOUD_TOKEN` through the runtime's existing secret injection. The environment is captured at startup, so replacement requires restart. Historical `TENCENT_AS_*` variables are not a fallback for this entrypoint.

With `tencent_credentials_file`, the JSON has `secret_id`, `secret_key`, optional `token`, and optional Unix-seconds `expired_time`. An expiry requires a token and at least one minute remaining. Use a private regular file (0600 or 0400), at most 64 KiB, an explicit absolute path and atomic updates. No credential values belong in this repository, policy, logs or journal. The selected file is re-read at cloud-operation entry and re-authenticated when changed; an unreadable file does not fall back to cached/environment credentials. Rotation cannot switch the original account.

## Nomad connection

Prefer the Agent's `nomad` block plus runtime `NOMAD_TOKEN`; the Agent passes connection settings to the plugin. Remote API addresses require HTTPS. Plain HTTP is accepted only on loopback. Configure CA and client certificate/key for the actual deployment rather than disabling certificate verification.

Advanced inherited/plugin keys are `nomad_address`, `nomad_region`, `nomad_namespace`, `nomad_token`, `nomad_http-auth`, `nomad_ca-cert`, `nomad_ca-path`, `nomad_client-cert`, `nomad_client-key`, `nomad_tls-server-name`, `nomad_skip-verify`, `nomad_config_inherit`, and `nomad_block-query-wait-time`. Standard environment fallback uses the corresponding `NOMAD_*` variables; an explicitly supplied value, even an empty token, wins. Missing address defaults to `http://127.0.0.1:4646`. Do not put secret values directly into target config to demonstrate these keys.

Node write/read and complete required-job visibility are needed. The current conservative ACL proof reads native identity/policy metadata and may reject complex/limited-namespace combinations; confirm the exact Nomad version and deployment ACL before acceptance. TLS success alone does not prove authorization or complete allocation visibility.

## Metrics-driven policy

The [Prometheus example](../examples/cluster-prometheus.hcl) adapts the allocated-CPU query from HashiCorp's [cluster scaling tutorial](https://developer.hashicorp.com/nomad/tutorials/autoscaler/horizontal-cluster-scaling). Provide a single matching client-metric series per intended node, matching `node_class="workers"`, and verify a finite percentage before enabling. Empty or duplicated series and mismatched labels are deployment errors; this template does not configure a scraper or promise zero-capacity bootstrap from an empty pool.

The [Prometheus APM](https://developer.hashicorp.com/nomad/tools/autoscaling/plugins/apm/prometheus) queries the configured server; it is independent of the target's optional operational counters. This batch validates configuration/disabled-policy loading, not an actual scraped signal or workload `1→5→1`.
