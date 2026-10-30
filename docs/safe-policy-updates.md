# Safe offline policy updates

The default target can update a stopped, idle group (one whose state was created by this target, state version 3) without discarding its history. Use the matching `tencent-recover` binary. This is local configuration maintenance: neither preview nor apply constructs cloud/Nomad clients or calls provider APIs.

Supported changes are node filters, selector strategy, drain deadline (including zero/negative modes), system-job handling, purge and AS-protection filtering. The account, region, ASG and `instance_id_meta_key` must remain unchanged. A mapping-key change requires separate identity verification and is rejected by this tool. Agent-level configuration and outer policy min/max/cooldown are outside this update.

## Procedure

1. Pause new evaluations, allow the group's work and restoration to finish, and stop the Agent. Preserve the complete state directory. An active controller, pending operation/batch, manual-review gate, unfinished legacy migration or recovery transaction prevents the update.
2. Prepare a private JSON object containing the **complete intended target options**. Omitted target settings take their normal defaults; omitted `scale_in_protection` retains the effective old value. Include an explicit value to change protection. This file contains no credentials. Its permissions must be 0600 or 0400.
3. Preview against the exact group directory containing `route.json` and `live.json`:

```sh
tencent-recover policy-preview \
  --state-dir /var/lib/nomad-autoscaler/tencent-as/accounts/ACCOUNT/REGION/asg-example \
  --policy /private/path/target-options.json \
  --operator operator-name --reference CHANGE-123 \
  --plan /private/path/policy-plan.json
```

Example target options:

```json
{
  "tencent_group_id": "asg-example",
  "node_class": "workers",
  "node_selector_strategy": "least_busy",
  "node_drain_deadline": "10m",
  "node_drain_ignore_system_jobs": "false",
  "node_purge": "false",
  "instance_id_meta_key": "cloud_instance_id",
  "scale_in_protection": "true"
}
```

4. Review the printed before/after policy and digest. Keep the Agent stopped. Apply within five minutes using that exact digest:

```sh
tencent-recover policy-apply \
  --plan /private/path/policy-plan.json --confirm PREVIEW_DIGEST
```

5. Change the corresponding Agent policy target block to the reviewed values, then restart the Agent. No HCL file is edited by this tool. A different requested policy is still rejected. On official Agent versions with the documented restart-cooldown limitation, wait out the remaining cooldown before resuming evaluations.

## State and crash behavior

Both group locks are acquired. Apply binds the exact before versions of both files, writes an immutable audit and a durable transaction marker, and updates the journal and route by atomic replacement. Startup completes a prepared transaction before opening group controllers. A crash after the marker is durable rolls forward to the reviewed version, even if the original preview has since expired. A conflicting file or damaged/missing audit blocks startup; it is never overwritten to force progress. Before the marker is durable, files retain the old configuration and the same unexpired plan can be retried.

Completion time, membership, sequence, previous operation/batch, recovery audit references and original historical drain bounds are preserved. Later actions use the new policy. `policy-audit/` and `policy-update-pending.json`, if present, are part of the durable state; keep them with `live.json` and `route.json`. Reapplying a successfully committed plan returns its recorded completion and does not undo later policy changes.

The new optional journal fields can be rejected by older binaries. Do not remove audit fields, reset journals, or restore old snapshots over later external effects to downgrade. This feature does not supply distributed locks, Agent hot reload, node-identity migration or installation verification.


## Accepting a changed launch configuration

An already bound group still rejects an unreviewed cloud launch configuration change with `LaunchConfigurationChanged`. To adopt an intentional image, instance type, security group or LC-version change, use `lc-preview` and `lc-apply` from the matching recovery binary. These commands read AS/CVM/Nomad and validate the caller with STS; they write only local acceptance/audit files. They do not change the cloud LC, capacity, instances or Nomad eligibility, and do not roll existing instances onto the new image.

1. Stop evaluations and let all operations/restoration settle, then stop the Agent. Keep the entire state directory. Use the normal cloud change process to update the group's LC. Keep other capacity writers stopped throughout maintenance.
2. Use the same account and region. Authentication defaults to the instance's existing identity; explicit `--auth-method api_key` uses `TENCENTCLOUD_SECRET_ID`, `TENCENTCLOUD_SECRET_KEY` and optional `TENCENTCLOUD_TOKEN` in the scoped process environment. No credential values belong in arguments, plans or shell history. Nomad address, ACL and TLS use the existing environment settings.
3. Preview the **current cloud configuration**, rather than supplying a desired configuration JSON:

```sh
tencent-recover lc-preview \
  --state-dir /var/lib/nomad-autoscaler/tencent-as/accounts/ACCOUNT/REGION/asg-example \
  --region REGION --operator operator-name --reference CHANGE-123 \
  --plan /private/path/lc-plan.json
```

Review the before/after LC, digest and expiry. Within five minutes, with the Agent still stopped:

```sh
tencent-recover lc-apply \
  --state-dir /var/lib/nomad-autoscaler/tencent-as/accounts/ACCOUNT/REGION/asg-example \
  --region REGION --plan /private/path/lc-plan.json --confirm PREVIEW_DIGEST
```

Both phases acquire the route and journal locks, verify the account/region/group/policy binding, and require two stable observations. Apply repeats all provider reads and checks the exact plan and journal bytes. Only idle v3 groups with normal on-demand LC allocation, Count=Desired=membership, unchanged exact members, and ready/eligible mapped nodes are accepted. Empty stable groups are supported. Pending operations, unknown/manual review, migrations, retries, active cloud operations, drain, missing registration or observation drift require resolution and a fresh preview; there is no force flag.

4. Restart the Agent and verify Status and the next intended scaling action. Historical batches, completion/cooldown, policy and registration records remain unchanged. If the cloud changes again, the normal guard rejects it again. Reverting the cloud LC requires a new preview/acceptance; do not reset the journal.

The immutable record is a manual version-4 transaction in `recovery-audit/`. If interrupted with `recovery-pending.json`, use the `repair` subcommand (`tencent-recover repair --help` lists its flags; it is used only to finish an interrupted `lc-apply`) with `--group-contract`, the same group/region, the route's drain deadline/protection values and this plan's digest. Repair only completes audit bookkeeping: before journal replacement it records an abort; after replacement it records a commit. Inspect the result and create a fresh preview after an abort. Old binaries can reject the new audit version; preserve state and use the matching binary rather than deleting audit fields to downgrade. Official Agent restart-cooldown limits still apply.
