# Permissions and resource scope

Use an already approved identity and review its **effective** permissions,
including inherited policies. These examples do not create or attach IAM
policies. They are deployment inputs; real allow/deny checks remain required.

## AS group and launch configuration

The [AS authorization table](https://intl.cloud.tencent.com/zh/document/product/598/57063)
documents group-level resources for the group/member/policy/activity reads
and the two writes below. Launch-configuration reads use a separate resource.
Replace the placeholders and enumerate only the managed groups/configurations:

```json
{
  "version": "2.0",
  "statement": [
    {
      "effect": "allow",
      "action": [
        "name/as:DescribeAutoScalingGroups",
        "name/as:DescribeAutoScalingInstances",
        "name/as:DescribeScalingPolicies",
        "name/as:DescribeAutoScalingActivities",
        "name/as:DescribeScheduledActions",
        "name/as:ModifyDesiredCapacity",
        "name/as:RemoveInstances"
      ],
      "resource": [
        "qcs::as:<REGION>:uin/<ACCOUNT_UIN>:auto-scaling-group/<GROUP_ID>"
      ]
    },
    {
      "effect": "allow",
      "action": ["name/as:DescribeLaunchConfigurations"],
      "resource": [
        "qcs::as:<REGION>:uin/<ACCOUNT_UIN>:launch-configuration/<LAUNCH_CONFIG_ID>"
      ]
    }
  ]
}
```

New growth through the default target requires `as:ModifyDesiredCapacity`, including the
`AssertOriginDesiredCapacity` request parameter. `as:DescribeScheduledActions`
is used for advisory diagnostics; a failed diagnostic read logs a warning and
does not reject an otherwise valid observation. Verify its effective resource
scope with the deployment identity. Existing in-flight `ScaleOutInstances`
records are recovered by reading their original activity; no new incremental
write is sent. This fragment excludes group/template changes, IAM,
security-group changes and general CVM writes. It is not a complete identity
policy, and writing this example does not authorize changing a role.

## CVM reads and identity validation

[CVM authorization](https://intl.cloud.tencent.com/zh/document/product/598/57095)
lists `DescribeInstances` with resource
`qcs::cvm:<REGION>:uin/<ACCOUNT_UIN>:instance/<INSTANCE_ID>`. The adapter requests
exact group members and tracked removals, but request filtering is **not** an
IAM boundary. Permission for an ASG does not by itself grant or constrain CVM
reads. A static list of current instances will not automatically cover future
scale-out members.

Before deployment, the identity owner must supply a reviewed way to authorize
current and future managed members, and verify absent/deleted-instance queries.
An existing enforced resource/tag policy may be considered only after its
behavior is verified. Do not silently use account-wide `instance/*` or add IAM
writes to the plugin to bypass this requirement. Dynamic member scope remains
unverified until this is demonstrated for the deployment.

`SetConfig` and refresh validation also call
[STS GetCallerIdentity](https://cloud.tencent.com/document/api/1312/66098).
Verify this read using the intended principal; its success establishes identity,
not permission to resize a group. Instance-identity mode requires an existing
approved instance role and working metadata credentials. API-key mode is an
explicit alternative and does not prove the default identity path works.

## Permission verification

Record the exact policy revision and principal type without credentials. Check
authorized group/config/member reads; normal expansion and exact removal in the
dedicated test group; newly created members; temporary credential renewal; and
lookup of a genuinely removed VM. Then verify that a separate group and its
members cannot be accessed or modified. A denied read must be reported as an
error, never interpreted as an absent VM.

Prepare mutation denial checks with disposable, explicitly authorized resources:
an accidentally permissive identity can make such a check perform the mutation.
No negative write probe against an unrelated production group is implied here.
The effective policy must exclude permissions unrelated to these operations.

Nomad ACL requirements and their namespace visibility constraints are described
in [operations](operations.md#permission-inventory). Validate both permission
sets; cloud authentication alone does not authorize node draining or purging.
