# Nomad registration grace period

The default target accepts the optional Agent target setting
`tencent_registration_grace_period = "30m"`. It must be a positive Go duration
(for example `15m`, `30m`, or `2h`). The default is 30 minutes. It applies to
the groups managed by that configured target; it is not a policy target option.
Change it through a controlled plugin restart. Existing SDK policy identities
and recorded scaling actions are not rewritten.

## Time and state

The deadline is AS `AddTime` plus the configured duration, not the time the
plugin first observes the VM. Restarting the plugin therefore does not restart
the clock. A new controller may immediately report an already old VM as timed
out. Changing the duration recomputes the deadline from the same AS timestamp.
A future or invalid timestamp cannot release the readiness gate.

| State | Meaning and behavior |
| --- | --- |
| `waiting` | A complete successful Nomad node read found no unique mapping and the deadline has not passed. The VM counts toward capacity, cannot be selected for removal, and the group continues waiting. |
| `timed_out` | The deadline has passed and the VM has not been observed registered by this controller. Warn and retain the VM. This absence alone stops blocking group readiness once the cloud is stable. |
| `registered` | A unique node mapping exists. Its actual Nomad health, eligibility, drain and cloud state still determine readiness and removal eligibility. |
| `unknown` | Cloud/Nomad reads failed, mapping is ambiguous, time is invalid, or a previously registered member disappeared. Do not infer initial-join failure or permit writes based on this observation. |

All actual ASG members continue to count, including waiting and timed-out VMs.
Timeout never changes DesiredCapacity, marks an AS/CVM instance unhealthy,
removes it, or starts replacement. Cloud activity, capacity consistency, group
limits, instance protection, in-progress operations and existing recovery
obligations remain effective. When a timed-out VM later registers, the next
successful observation clears its timeout automatically. A down, draining or
ineligible registered node is still not ready.

A successful complete read means all node-list pages and their required node
details were obtained, with no duplicate mapping. It is not proof that the
bootstrap process failed: a wrong metadata key or wrong cluster configuration
can also produce no mapping. Missing data must never authorize VM deletion.
Keep the state directory to preserve the controller's previously joined
evidence; a fresh installation cannot reconstruct unobserved registration
history from Nomad after a record has been purged.

## Continuing scaling

After timeout, a stable group can grow through the ordinary policy path.
Scale-in still selects only registered, eligible nodes and runs the original
SDK pre-scale/drain and exact cloud deletion path. The full requested selection
must be available before starting: insufficient candidates returns an error
without silently choosing an absent VM or reporting a different target as
successful. A retained timed-out VM can therefore prevent reaching a smaller
target and continues to incur cost.

Cloud completion of an already accepted scale-out remains separate from Nomad
registration. The ordinary cooldown and polling budgets remain independent of
this initial-registration deadline. Expiry does not reset successful scaling
timestamps or remove existing cooldown behavior.

## Recovery of confirmed cloud growth

For the current group contract, automatic and manual recovery use the same
cloud completion boundary as an acknowledged scale-out. Exact successful
activity evidence, the original target and baseline membership, and fresh,
consistent running-VM observations must prove completion. New clients do not
have to register before that cloud action can finish. This also applies to
current-contract absolute-target operations already in flight; historical
migration contracts and deletion recovery retain their existing mapping checks.

Recovery records cloud evidence and an empty Nomad-node evidence list. It does
not claim that registration was observed, enable nodes, or replay the cloud
write. The journal retains whether each member was previously seen registered,
but discards stale node readiness. Status stays unready until a fresh normal
observation establishes readiness. Missing initial registrations then use the
original AS AddTime deadline; restart does not reset the deadline or the saved
successful completion time. A previously registered node disappearing remains
unknown, even after that deadline. A failed or ambiguous Nomad read cannot be
converted to an initial-registration timeout.

## Diagnostics

The journal member records expose `registration_state`,
`registration_deadline` and `ever_joined`. These are observation fields, not
deletion authorization. Existing journals without them can be read; old strict
readers may reject new fields, so do not downgrade by deleting journal data.
Historical active migration contracts keep their original strict behavior
until they finish.

`Status.Meta` exposes `tencent.registration_waiting`,
`tencent.registration_timed_out` and `tencent.registration_unknown` counts.
The optional metrics endpoint exposes:

- `tencent_as_registration_members{state="registered|waiting|timed_out|unknown"}`:
  current counts aggregated over the target's controllers; only one state per
  series, with no per-instance or per-group labels.
- `tencent_as_registration_observation_failed_groups`: controllers whose latest
  observation failed. On failure, known members become `unknown`, not stale
  `timed_out` counts. Missing telemetry or startup failure is not a healthy zero.

Use the existing metrics listener described in [configuration](configuration.md).
An alert may match `tencent_as_registration_members{state="timed_out"} > 0`;
also monitor observation failures and scrape availability. This feature does
not create a monitoring service or send notifications itself.

`Nomad registration grace period expired` is a WARN transition with the group,
instance ID, elapsed wait, configured grace and deadline; it is not repeated on
every poll. `Nomad registration recovered` is an INFO transition when the node
appears. Node IDs and instance IDs appear in diagnostic logs, not metric labels.

## Acceptance scope

The implementation detects expiry and resumes safe evaluation; it does
not promise automatic recycling of never-registered VMs. If a deployment needs
such recycling, agree separately on the no-drain exception and prevention of
late registration before adding it. Synthetic regression remains development evidence.
The real-cloud validation baseline verified the default 30-minute timing, deadline
persistence across restart, VM retention and late registration. A separately
configured 3-minute case verified the complete expiry-to-scale-out path.
The complete path at the default 30-minute duration was not executed successfully;
no additional run is planned for the first release. This is not a 30-minute
end-to-end PASS. The public-module binary hashes have not run against real AS.
See [release preparation](release.md) for the complete validation scope.
