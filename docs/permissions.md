# Permissions and resource scope

Use an already approved identity and review its **effective** permissions,
including inherited policies. These examples do not create or attach IAM
policies. They are deployment inputs; real allow/deny checks remain required.

## AS group and launch configuration

The [AS authorization table](https://intl.cloud.tencent.com/document/product/598/57063)
documents group-level resources for the group/member/policy/activity reads
and the two writes below. Launch-configuration reads use a separate resource.
Replace the placeholders and enumerate only the managed groups/configurations:

- `<CAM_REGION>` is the region in the CAM resource string. The Guangzhou checks
  used `gz` for AS, launch configurations and CVM resources. The plugin's
  `tencent_region` and API request region remain `ap-guangzhou`. For another
  region, verify its CAM resource form rather than copying the API region name.
- `<OWNER_ACCOUNT_UIN>` is the resource owner's root-account UIN, not the calling
  sub-account's UIN or a role ID. For resources in the deployment identity's own
  account, use `AccountId` from
  [STS GetCallerIdentity](https://cloud.tencent.com/document/api/1312/66098),
  which identifies that root account. Do not substitute `UserId`, `PrincipalId`
  or the UIN at the end of a sub-account ARN.

For example, a Guangzhou group resource is
`qcs::as:gz:uin/<OWNER_ACCOUNT_UIN>:auto-scaling-group/<GROUP_ID>`.

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
        "qcs::as:<CAM_REGION>:uin/<OWNER_ACCOUNT_UIN>:auto-scaling-group/<GROUP_ID>"
      ]
    },
    {
      "effect": "allow",
      "action": ["name/as:DescribeLaunchConfigurations"],
      "resource": [
        "qcs::as:<CAM_REGION>:uin/<OWNER_ACCOUNT_UIN>:launch-configuration/<LAUNCH_CONFIG_ID>"
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

[CVM authorization](https://intl.cloud.tencent.com/document/product/598/57095)
lists `DescribeInstances` with resource
`qcs::cvm:<CAM_REGION>:uin/<OWNER_ACCOUNT_UIN>:instance/<INSTANCE_ID>`. The adapter requests
exact group members and tracked removals, but request filtering is **not** an
IAM boundary. Permission for an ASG does not by itself grant or constrain CVM
reads. A static list of current instances will not automatically cover future
scale-out members.

CAM cannot name instances that do not exist yet, so a per-instance list does not
cover future members. The real-cloud checks of the default identity used one
read-only statement for the managed region, in addition to the AS statements
above:

```json
{
  "effect": "allow",
  "action": ["name/cvm:DescribeInstances"],
  "resource": ["qcs::cvm:<CAM_REGION>:uin/<OWNER_ACCOUNT_UIN>:instance/*"]
}
```

It lets the identity read every CVM instance in that region of the account. The
test used the short region code `gz` for Guangzhou; use the form your CAM console
shows for your region. No other CVM action and no IAM action was granted. A
narrower scope, for example an enforced resource or tag policy, was not
verified: accept the region-wide read knowingly, or verify the narrower scope
with the checks under "Permission verification" before relying on it, including
queries for a VM that was really removed. Never add CVM write actions or IAM
writes to bypass this requirement.

`SetConfig` and refresh validation also call
[STS GetCallerIdentity](https://cloud.tencent.com/document/api/1312/66098).
In the real-cloud check it succeeded for an instance role with no policy
attached, so no CAM statement was needed for it. Verify this read using the
intended principal; its success establishes identity, not permission to resize a
group. Instance-identity mode requires an existing
approved instance role and working metadata credentials. API-key mode is an
explicit alternative and does not prove the default identity path works.

## Permission verification

Record the exact policy revision and principal type without credentials. Check
authorized group/config/member reads; normal expansion and exact removal in the
dedicated test group; newly created members; temporary credential renewal; and
lookup of a genuinely removed VM. Then verify that a separate group and its
members cannot be accessed or modified. A denied VM read must be reported as an
error, never interpreted as an absent VM.

Group reads have a different ambiguity: `DescribeAutoScalingGroups` can return
success with `TotalCount=0` for a group outside the identity's permissions.
The adapter reports `GroupMissingOrIncomplete`; it cannot distinguish a missing
group from a group hidden by authorization. During initialization, v0.1.1 logs
`code=GroupInitializationObservationFailed` and
`cause_code=GroupMissingOrIncomplete`, retries, and may exhaust the Agent's
Status RPC wait budget. Check the group ID, account, region and effective policy;
do not assume that the group does not exist or that permission was definitely
denied. v0.1.0 logged only the generic initialization code.

For a negative group check, collect cloud-side evidence using a separately
authorized observer. Unchanged activity lists, desired capacity and member
count establish only that no change was observed. Proving that no write request
was submitted requires audit or API-call records covering the identity, group,
write actions and complete test interval, including rejected calls. If that
coverage is unavailable, record the submission claim as unverified; plugin logs
and unchanged capacity alone do not prove it.

Prepare mutation denial checks with disposable, explicitly authorized resources:
an accidentally permissive identity can make such a check perform the mutation.
No negative write probe against an unrelated production group is implied here.
The effective policy must exclude permissions unrelated to these operations.

Nomad ACL requirements and their namespace visibility constraints are described
in [operations](operations.md#permission-inventory). Validate both permission
sets; cloud authentication alone does not authorize node draining or purging.
