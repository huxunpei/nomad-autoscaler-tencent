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

## Known limitations

Use an unmodified official Agent. On the tested v0.6.0 baseline, an Agent restart skips any remaining cooldown unless the policy target sets `restart_cooldown` to that policy's cooldown (or the longer of `cooldown` and `cooldown_on_scale_up` when they differ). The plugin then reports the target not ready until that window ends. Omitting the setting allows another scale immediately after restart; waiting out the old window by hand is only a fallback when the setting was forgotten. With `policy_dir` set to the Agent policy directory, the plugin warns when loaded policy bounds exceed current cloud limits, even if the requested target is within those limits. These warnings are advisory; the plugin enforces fresh cloud boundaries. Align policy and group limits during deployment/change review. Valid SIGHUP file-policy updates also have a known Agent limitation; apply changes through a controlled restart. A patched Agent is not a deployment dependency. The upstream cooldown deadline fix still has to reach the official release the deployment runs.

Core cloud validation covers workload scaling `1→5→1`, partial-growth and failed-deletion recovery, instance identity, scoped permissions and scale-in protection. Architecture loading and targeted runtime checks have separate results. The complete default 30-minute expiry-to-scale-out path was not successfully completed and will not be rerun for the first release; the separately configured 3-minute path passed. Public-module binaries have not run against real AS. See [release preparation](release.md) for source bindings and the exact verification scope.
