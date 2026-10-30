# Operations and current limits

This guide describes deployment requirements, recovery behavior and known limits. Use the tested versions and verification scope in [release preparation](release.md), and validate the intended deployment before enabling scaling.

## Deployment preconditions

Before enabling scaling, configure the region/ASG, AS-created hourly-billed membership, controller ownership, startup/bootstrap, identity and Nomad endpoint; set resource and cost limits for deployment validation. The implementation checks reported AS scaling policies, scheduled actions, group activity and selected launch/member fields. These reads do not establish exclusive ownership: health replacement, instance refresh and independent callers can also change the group. Confirm that the deployment fits the supported scope below. Do not add a second scaling policy/writer to compensate for a blocked operation.

Use a persistent private state directory on a stable filesystem, one active controller per group and a deployment that preserves its route/operation evidence. Local locks do not coordinate separate hosts. Review lifecycle hooks, health replacement and other group writers; observed capacity equality alone does not prove stability.

The current default target has these limits:

| Group or member condition | Actual behavior and remaining adaptation |
| --- | --- |
| Enabled ASG, normal hourly launch configuration, AS-created hourly members | Current implementation and real-cloud development coverage. New growth also requires `LAUNCH_CONFIGURATION`; other group options require separate verification. |
| Manually attached member | Counted, but prevents ordinary readiness and cannot be selected for removal. AS `RemoveInstances` retains manually attached VMs; satisfying termination would need a separately reconciled VM-deletion step and permissions. |
| Spot or prepaid member/configuration | Not supported by the current launch/member checks. Spot interruption/replacement and prepaid termination semantics need separate contracts and verification. |
| `SPOT_MIXED` allocation | Rejected by the group guard for both growth and shrink; multi-activity and replacement semantics remain outside the initial supported scope. |
| Disabled ASG | Returns truthful Count with Ready=false and a warning, without a Status error. Existing owned pre-delete drains are restored; no new deletion proceeds while disabled. |
| Wake stopped instances before creating new ones | The adapter reads `ServiceSettings.ScalingMode` and warns. Stopped members and reused IDs still need separate capacity/completion handling before this mode can be supported. |
| AS alarm policy or another capacity writer | Alarm policies and scheduled actions cause warnings, not rejection. A local lock cannot exclude external callers. Single-writer ownership is a deployment requirement, not an API-proven guarantee. |

The [RemoveInstances contract](https://cloud.tencent.com/document/product/377/20431) distinguishes AS-created and attached VMs. The [ScaleOutInstances contract](https://cloud.tencent.com/document/api/377/56369) describes disabled-group support and mixed-mode activities. Review [group service settings](https://cloud.tencent.com/document/product/377/20453) when choosing a deployment; no group option is changed automatically by the plugin.

## Permission inventory

The implementation uses these APIs. The [scoped permission examples](permissions.md) describe AS resources and the separate CVM member-scope requirement. Neither the inventory nor the examples are a certified effective CAM policy; verify cross-group denial and future-member reads for the intended deployment identity. Reuse an approved existing identity; no IAM role/policy creation is part of the examples.

| Purpose | APIs |
| --- | --- |
| Credential authentication | STS `GetCallerIdentity`. |
| Group/config/member/activity reads | AS `DescribeAutoScalingGroups`, `DescribeAutoScalingInstances`, `DescribeLaunchConfigurations`, `DescribeScalingPolicies`, `DescribeAutoScalingActivities`, `DescribeScheduledActions`. |
| VM state/protection reads | CVM `DescribeInstances`. |
| New capacity actions and exact removal | AS `ModifyDesiredCapacity` with `AssertOriginDesiredCapacity`, `RemoveInstances`. |

New growth through the default target persists an absolute target and original Desired, then calls `ModifyDesiredCapacity` with `AssertOriginDesiredCapacity`. Completion still requires stable running cloud members. Partial failure leaves Desired at the target and records failure without a successful cooldown; inactive, stable groups may return actual Count as ready. The next normal evaluation can resend the same target after persistent exponential backoff (2, 4, 8, 16, then 30 minutes). Target changes and successful completion reset backoff. See [configuration](configuration.md#policy-boundary-diagnostics-and-growth-failures).

A lost response is reconciled from fresh Desired: follow an already installed target; retry an unchanged original assertion; stop on a conflicting value or explicit assertion rejection. This is a value comparison, not a versioned fencing token: a value that changes and returns to the original cannot be distinguished. Keep one capacity writer and preserve diagnostics for unresolved conflicts. Existing `ScaleOutInstances` records retain their activity-based recovery and are never converted to new writes. Review the changed AS actions in the deployment identity before enabling; the plugin never modifies IAM.

Nomad permissions need exact node reads/writes, required allocation/job visibility across namespaces and access to the current principal's native policy metadata. A reviewed non-management policy used in isolated development was:

```hcl
node { policy = "write" }
namespace "*" { capabilities = ["read-job"] }
```

This is a permission review example, not an instruction to create a token or proof for every ACL/version. Existing role/JWT/namespace combinations and policy-cache propagation need deployment confirmation. Avoid changing ACLs while scaling is in flight; permission loss must not make a filtered allocation list look like an empty node.

## Completion and recovery

A Scale success means the effective target is stable and required post work is finished and durably recorded. VM registration is a later bootstrap concern for new cloud capacity, while reliable registration/identity is necessary for deletion eligibility. NoOp and failures do not advance the successful completion timestamp.

On a known deletion failure, an owned running survivor can regain eligibility. AS can still delete it on retry, so restored eligibility is not an assurance of future VM survival. The original action remains failed; background reconciliation retains the original activity/request and later exact evidence. It must not label a later retry as a successful original action or replay the deletion.

If credential acquisition/refresh fails before an AS mutation client is called, the adapter supplies explicit local non-submission evidence. Once durable, the action ends as failed; any owned drain is restored only after running-VM and exact-node checks. Successful-completion time is unchanged and later actions can proceed when credentials and normal conditions recover. `Attempted=false` or error text alone is insufficient. Failure to persist the evidence keeps the conservative unknown/recovery boundary.

Local argument/cancellation gates and failures before obtaining an HTTP connection use the same durable recovery path. The plugin's owned native transport must prove that every round trip failed before obtaining a usable connection and returned no response. This covers traced DNS, connection and TLS handshake failures. The failed request is not immediately replayed; later normal evaluation can proceed after recovery. Any obtained/reused connection, partial write, lost response, contradictory identifier or opaque transport remains outside this proof. Journal codes distinguish credential, client and transport gates; an older reader may reject new codes, so do not strip evidence to downgrade.

Unknown cloud/Nomad outcomes keep their exact pending obligations and block conflicting writes. Supported observed effects can be reconciled automatically with audit; ambiguous/no-acceptance evidence and some partial-growth outcomes remain unresolved. There is no safe generic “unlock by deleting live.json” procedure. Manual tools require a separate bounded, reviewed process and cannot substitute for ordinary automatic recovery verification.

Record and preserve journal, immutable backups, audit records/receipts, exact node/VM mapping and API request/activity IDs. Do not store credentials in them. A recovery preview does not itself apply changes. A backup is evidence, not authorization to roll back after external effects.

## Upgrade/reconfiguration

Stop new evaluations and let known work reconcile before changing strategy/mapping or versions. Preserve state and verify that the intended reader understands every recorded origin/audit. Same-config SetConfig repetition is supported; changed live target configuration requires a restart and may require a migration. Old binaries can reject new optional journal fields. Do not downgrade by stripping fields, replacing backups or copying a different group's state.

Stopped, idle v3 groups can change filters, selection/drain/purge and protection using [audited policy preview/apply](safe-policy-updates.md). The update preserves historical operations, cooldown and recovery evidence, and rolls forward interrupted local commits. In-flight/recovery states and mapping-key changes are refused.

Supported legacy migrations have explicit source/identity proof; that is not blanket support for arbitrary old plugins, unknown human audit records, or a different already-bound policy. Failed migration or identity ambiguity is a reason to investigate the exact source and current mapping, not to rerun a destructive action.

## Logs and metrics

Logs associate policy/group/operation, requested/effective targets and observed actual capacity. A target value is not an observed new count. Background settlement and the synchronous Scale result are separate facts; use persisted audit/API evidence when log delivery is interrupted.

Confirmed growth failures retain a bounded, sorted `provider_failure_codes`
list from activity details, alongside the activity status/ID. Known capacity/quota codes and the exact launch-hook rejection code
`LifecycleHookActionAbandon` are retained; other recognized codes are reduced to their family,
and unrecognized values become `Unclassified`. Raw provider messages are never
copied. Missing/legacy diagnostics explicitly report availability as false.
Intermediate errors on an overall successful activity do not make it a failure.
These diagnostics do not authorize replay or settle an unknown cloud outcome.
Existing `Unclassified` records remain unchanged; no cause is reconstructed from
an old generic value. Older binaries may reject newly recorded diagnostic codes.
Preserve the complete state/audit and use a matching reader; do not remove or
rewrite diagnostics to downgrade.

The optional numeric-loopback listener exposes `/metrics`:

| Counter | Labels |
| --- | --- |
| `tencent_as_scale_requests_total` | `result`: success/error/no_op/dry_run. |
| `tencent_as_api_attempts_total` | bounded `action` values. |
| `tencent_as_api_errors_total` | bounded `action` values; includes throttling failures. |
| `tencent_as_api_throttles_total` | bounded `action` values. |

`tencent_as_process_start_time_seconds` identifies counter resets. Counters aggregate groups in one plugin process and are separate from Agent metrics. Scraping only reads memory, not credentials, journal or cloud APIs. Use an existing local monitoring agent for remote collection; do not widen the listener to a public interface.

## Troubleshooting

A healthy start logs `successfully launched and dispensed plugin` with `"plugin_name":"tencent-as"` in the Agent log. Errors from the plugin are short codes or messages:

Starting with v0.1.1, a startup authentication failure returns its error without
switching credential sources. v0.1.0 instead could panic during failure cleanup,
leaving the Agent with an EOF; the authentication rows below describe the fixed
behavior. Initialization warnings retain their generic `code` and add
`cause_code` when a structured cloud error is available. Raw error messages are
not logged, and retry timing is unchanged.

| Message or code | Meaning | What to do |
| --- | --- | --- |
| `invalid <key>` (for example `invalid tencent_region`, `invalid tencent_group_id`, `invalid restart_cooldown`) | The value does not have the documented format | Check the key in the [configuration reference](configuration.md). A region looks like `ap-guangzhou`; a group ID starts with `asg-`; durations are Go durations such as `2m`. |
| `unknown or misplaced Agent option` / `unknown or misplaced policy option` | A key is not valid at that layer | Region, state directory and authentication keys go in the Agent `target` block. `tencent_group_id`, node filters and `restart_cooldown` go in the policy `target` block. |
| `UnknownNomadOption` | A `nomad_*` key is not one of the supported connection keys | See "Nomad connection" in the configuration reference. |
| `StateDirectoryNotPrivate`, `RouteDirectoryNotPrivate` | The state directory is accessible to group or other users | `chmod 0700` the directory; it must belong to the Agent's user. |
| `UnsafeStateDirectory`, `UnsafeRouteDirectory`, `StateDirectoryFailed` | A path component is a symbolic link, or the directory cannot be created | Use a real directory path and check permissions of its parents. |
| `InstanceIdentityUnavailable`, `InvalidInstanceRole`, `InvalidMetadataResponse` | The instance role or its credentials could not be read | Check that a CAM role is bound to the CVM and that `metadata.tencentyun.com` is reachable. There is no fallback to static keys. |
| `InvalidCredentialMaterial` | The explicitly selected API-key source has missing or invalid credential values | Supply both `TENCENTCLOUD_SECRET_ID` and `TENCENTCLOUD_SECRET_KEY`, or a valid explicit credentials file. Empty API-key configuration does not fall back to the instance role. |
| `CredentialFileUnavailable`, `InvalidCredentialFile`, `UnsafeCredentialFile`, `CredentialFileUnreadable`, `CredentialExpiredOrExpiring` | Problem with the `api_key` credentials file | It must be an absolute path, a regular file with mode `0600` or `0400`, at most 64 KiB, with `secret_id` and `secret_key`; an `expired_time` needs a `token` and at least one minute remaining. |
| `GroupInitializationObservationFailed` with `cause_code=GroupMissingOrIncomplete` | The group read did not yield exactly one complete group; missing permission can also produce an empty result | Check the group ID, account, API region and [CAM resource scope](permissions.md). Initialization retries and the Agent may report a Status timeout; this code alone cannot distinguish a missing group from missing permission. |
| `UnauthorizedOperation` | The identity lacks a permission | See [permissions](permissions.md). A denied read is an error, never "the VM is gone". |
| `LaunchConfigurationChanged` | The group's launch configuration changed after it was bound | Accept it deliberately with `lc-preview` / `lc-apply` ([procedure](safe-policy-updates.md#accepting-a-changed-launch-configuration)). |
| `DesiredCapacityConflict` | The group's desired size was changed by someone else | Keep one capacity writer; preserve the diagnostics and investigate before any retry. |
| `prototype config reload requires plugin restart` | The Agent re-sent a changed target configuration (for example on SIGHUP) | Restart the Agent. |

`tencent-recover --help` prints the usage. The documented subcommands are `policy-preview` / `policy-apply` and `lc-preview` / `lc-apply` ([procedures](safe-policy-updates.md)). The older `preview`, `apply` and `repair` subcommands are for completing interrupted recovery; they are not part of routine operation, and normal failures recover automatically. Do not use them to get past an unresolved state: keep the whole state directory and open an issue first.

## Known limitations

Use an unmodified official Agent. On the tested v0.6.0 baseline, an Agent restart skips any remaining cooldown unless the policy target sets `restart_cooldown` to that policy's cooldown (or the longer of `cooldown` and `cooldown_on_scale_up` when they differ). The plugin then reports the target not ready until that window ends. Omitting the setting allows another scale immediately after restart; waiting out the old window by hand is only a fallback when the setting was forgotten. With `policy_dir` set to the Agent policy directory, the plugin warns when loaded policy bounds exceed current cloud limits, even if the requested target is within those limits. These warnings are advisory; the plugin enforces fresh cloud boundaries. Align policy and group limits during deployment/change review. Valid SIGHUP file-policy updates also have a known Agent limitation; apply changes through a controlled restart. A patched Agent is not a deployment dependency. The upstream cooldown deadline fix still has to reach the official release the deployment runs.

Core cloud validation covers workload scaling `1→5→1`, partial-growth and failed-deletion recovery, instance identity, scoped permissions and scale-in protection. Architecture loading and targeted runtime checks have separate results. The complete default 30-minute expiry-to-scale-out path was not successfully completed and will not be rerun for the first release; the separately configured 3-minute path passed. The v0.1.1 linux-amd64 binary passed a read-only startup check on real AS; scaling was not rerun with the v0.1.1 binaries. See [release preparation](release.md) for source bindings and the exact verification scope.
