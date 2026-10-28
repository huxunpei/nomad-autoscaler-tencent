# Tencent Cloud AS target for Nomad Autoscaler

[中文说明](README.zh-CN.md) · [Configuration](docs/configuration.md) · [Operations](docs/operations.md) · [Development](docs/development.md)

`tencent-as` is a standalone Target plugin that connects Nomad Autoscaler to Tencent Cloud Auto Scaling (AS). The Agent runs the plugin from `plugin_dir`; no Nomad server fork or Kubernetes cluster is required. Metrics and strategy evaluation remain in the Autoscaler Agent, while this target manages AS capacity and the Nomad node lifecycle. See the [official plugin model](https://developer.hashicorp.com/nomad/tools/autoscaling/concepts/plugins).

**Release candidate:** v0.1.0 is being prepared under [MPL-2.0](LICENSE). The cloud-tested baseline covers workload scaling 1→5→1 and failure recovery. See [release preparation](docs/release.md) for the tested versions, verification limits and publication status.

## Implemented workflow

- Count actual ASG members, including VMs not yet registered in Nomad. Clamp absolute targets to the group's current Min/Max.
- Use the official SDK for candidate selection, pre-scale drain and post-scale work. Delete only the exact selected VM after required tasks, including poststop, finish.
- On confirmed deletion failure, restore the owned surviving node's scheduling eligibility, retain the failed result and reconcile cloud state. AS may still retry deletion; restoring eligibility does not cancel that retry.
- Keep durable intents, fixed selections, account/region/group routing, operation evidence and recovery audits. Unknown outcomes do not authorize blind write retries or deletion of the journal.
- Share Nomad connection/ACL/TLS settings with the Agent; support an existing instance identity or explicitly selected API-key credentials. Optional metrics listen only on loopback.

Implemented branches have unit, SDK/HTTP model and selected real Agent/Nomad development evidence. Validate installation and workload behavior in the intended deployment before enabling scaling. Normal production binaries do not include the test-only cloud factories or fault injectors.

## Build

Use a Linux development host or CI. The minimum Go version declared by `go.mod` is 1.25.10; candidate builds and CI use Go 1.26.6. Record the actual `go version`. The plugin SDK dependency is pinned to Nomad Autoscaler **v0.5.0**. The Agent executable is a separate product: the current tested baseline is unmodified Agent v0.6.0 with Nomad 2.0.7. Apply the restart-cooldown and policy-directory settings documented below.

```sh
make race
make cross-build
```

The four build outputs are `dist/linux-amd64/tencent-as`, `dist/linux-arm64/tencent-as`, and `tencent-recover` in each directory. Compile success is separate from loading each architecture in the Agent. A release must add the approved source/version, checksums, dependency/license inventory and matching validation evidence.

## Configure

Start with [examples/autoscaler.hcl](examples/autoscaler.hcl) and choose **one** of [fixed capacity](examples/cluster-fixed.hcl) or [Prometheus CPU](examples/cluster-prometheus.hcl) for an ASG. Set each policy target's `restart_cooldown` to that policy's `cooldown`. Official Agent restarts otherwise skip the remaining cooldown and can scale again immediately. See [Restart cooldown](docs/configuration.md#restart-cooldown). Both policies are disabled and contain a synthetic ASG ID. Keep them disabled while adapting the approved region, resources, identity, networking and Nomad configuration.

Install the executable as `<plugin_dir>/tencent-as`, grant its process exclusive access to a persistent state directory, and place the chosen policy in the configured policy directory. The ASG belongs in the policy; it is not duplicated in Agent target config.

```sh
nomad-autoscaler agent -config=/etc/nomad-autoscaler/autoscaler.hcl
```

Starting the default target authenticates with Tencent Cloud even if a policy is disabled. A disabled policy prevents evaluation; it is not a credential or network sandbox. Syntax tests use a dedicated external test plugin and synthetic services instead of production authentication.

The [configuration reference](docs/configuration.md) covers defaults, scopes, credentials, filters and node identity. The [operations guide](docs/operations.md) covers prerequisites, current API use, permission review, recovery, metrics and upgrade limits.

[Permission examples](docs/permissions.md) separate ASG authorization from dynamic CVM member access. The [maintenance plan](docs/maintenance.md) defines dependency review and upgrade checks; xunpeihu is the maintainer, with a one-month fix deadline.

## Deployment scope and operations

The initial scope is pay-as-you-go ASG-created members using a launch configuration. Use one capacity controller for each group and keep persistent state across restarts.

Configure `restart_cooldown` to match each policy's `cooldown`, and set `policy_dir` to the same directory used by the Agent. The target applies current cloud group bounds and warns when loaded policy bounds differ. Apply policy changes through a controlled Agent restart; valid same-ID file updates through SIGHUP have an upstream limitation.

Core real-cloud checks used the cloud-tested product source. ACL/TLS and the full five-minute restart cooldown have separate official Agent/real Nomad or synthetic-service evidence. Linux arm64 loading uses user-mode emulation. The complete expiry-to-scale-out path at the default 30-minute duration was not successfully completed; no additional run is planned for the first release. Default-duration timing, persistence and retention, and a separate 3-minute complete path, have their own results. See [release preparation](docs/release.md).

## License and maintenance

Project source is under [MPL-2.0](LICENSE). Dependency copyrights and terms are retained in [third-party notices](THIRD_PARTY_NOTICES.md). Maintenance is described in [maintenance](docs/maintenance.md).
