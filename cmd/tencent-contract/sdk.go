package main

import (
	"context"
	"errors"
	"regexp"
	"time"

	as "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/as/v20180419"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
	sdkerrors "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/errors"
	cvm "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/cvm/v20170312"
)

type safeFailure struct {
	Code      string `json:"code"`
	Action    string `json:"action,omitempty"`
	RequestID string `json:"request_id,omitempty"`
}

func (e *safeFailure) Error() string { return e.Code }

var safeID = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,128}$`)

func sanitize(action string, err error) *safeFailure {
	var own *safeFailure
	if errors.As(err, &own) {
		return own
	}
	o := &safeFailure{Code: "RequestFailed", Action: action}
	var sdk *sdkerrors.TencentCloudSDKError
	if errors.As(err, &sdk) {
		if safeID.MatchString(sdk.GetCode()) {
			o.Code = sdk.GetCode()
		}
		if safeID.MatchString(sdk.GetRequestId()) {
			o.RequestID = sdk.GetRequestId()
		}
	} else if errors.Is(err, context.DeadlineExceeded) {
		o.Code = "RequestTimeout"
	} else if errors.Is(err, context.Canceled) {
		o.Code = "RequestCanceled"
	}
	return o
}
func sanitizeOptional(action string, err error) *safeFailure {
	if err == nil {
		return nil
	}
	return sanitize(action, err)
}

type asAPI interface {
	DescribeAutoScalingGroupsWithContext(context.Context, *as.DescribeAutoScalingGroupsRequest) (*as.DescribeAutoScalingGroupsResponse, error)
	DescribeAutoScalingInstancesWithContext(context.Context, *as.DescribeAutoScalingInstancesRequest) (*as.DescribeAutoScalingInstancesResponse, error)
	DescribeLaunchConfigurationsWithContext(context.Context, *as.DescribeLaunchConfigurationsRequest) (*as.DescribeLaunchConfigurationsResponse, error)
	DescribeScalingPoliciesWithContext(context.Context, *as.DescribeScalingPoliciesRequest) (*as.DescribeScalingPoliciesResponse, error)
	ModifyDesiredCapacityWithContext(context.Context, *as.ModifyDesiredCapacityRequest) (*as.ModifyDesiredCapacityResponse, error)
	RemoveInstancesWithContext(context.Context, *as.RemoveInstancesRequest) (*as.RemoveInstancesResponse, error)
}
type cvmAPI interface {
	DescribeInstancesWithContext(context.Context, *cvm.DescribeInstancesRequest) (*cvm.DescribeInstancesResponse, error)
}
type sdkCloud struct {
	as      asAPI
	cvm     cvmAPI
	timeout time.Duration
}

func invoke[T any](ctx context.Context, timeout time.Duration, action string, fn func(context.Context) (T, error)) (T, error) {
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	value, err := fn(reqCtx)
	if err != nil {
		return value, sanitize(action, err)
	}
	return value, nil
}
func stringValue(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}
func boolValue(p *bool) bool { return p != nil && *p }
func stringValues(ps []*string) []string {
	out := make([]string, 0, len(ps))
	for _, p := range ps {
		if p != nil {
			out = append(out, *p)
		}
	}
	return out
}

func (c *sdkCloud) Observe(ctx context.Context, groupID, trackedID string) (observation, error) {
	o := observation{At: time.Now().UTC(), Members: make([]member, 0), Requests: make([]requestResult, 0)}
	greq := as.NewDescribeAutoScalingGroupsRequest()
	greq.AutoScalingGroupIds = common.StringPtrs([]string{groupID})
	gresp, err := invoke(ctx, c.timeout, "DescribeAutoScalingGroups", func(ctx context.Context) (*as.DescribeAutoScalingGroupsResponse, error) {
		return c.as.DescribeAutoScalingGroupsWithContext(ctx, greq)
	})
	if err != nil {
		return o, err
	}
	if gresp == nil || gresp.Response == nil || len(gresp.Response.AutoScalingGroupSet) != 1 || gresp.Response.AutoScalingGroupSet[0] == nil {
		return o, fail("GroupMissing")
	}
	g := gresp.Response.AutoScalingGroupSet[0]
	if stringValue(g.AutoScalingGroupId) != groupID || g.MinSize == nil || g.MaxSize == nil || g.DesiredCapacity == nil || g.InstanceCount == nil {
		return o, fail("IncompleteGroupResponse")
	}
	o.Group = groupState{ID: groupID, Min: *g.MinSize, Max: *g.MaxSize, Desired: *g.DesiredCapacity, Count: *g.InstanceCount, Enabled: stringValue(g.EnabledStatus), Activity: stringValue(g.InActivityStatus), LaunchConfiguration: stringValue(g.LaunchConfigurationId)}
	o.Requests = append(o.Requests, requestResult{Action: "DescribeAutoScalingGroups", RequestID: stringValue(gresp.Response.RequestId)})
	var expected *uint64
	seen := map[string]bool{}
	for offset := int64(0); ; {
		req := as.NewDescribeAutoScalingInstancesRequest()
		req.Filters = []*as.Filter{{Name: common.StringPtr("auto-scaling-group-id"), Values: common.StringPtrs([]string{groupID})}}
		req.Offset, req.Limit = common.Int64Ptr(offset), common.Int64Ptr(100)
		resp, e := invoke(ctx, c.timeout, "DescribeAutoScalingInstances", func(ctx context.Context) (*as.DescribeAutoScalingInstancesResponse, error) {
			return c.as.DescribeAutoScalingInstancesWithContext(ctx, req)
		})
		if e != nil {
			return o, e
		}
		if resp == nil || resp.Response == nil || resp.Response.TotalCount == nil {
			return o, fail("IncompleteMembersResponse")
		}
		r := resp.Response
		o.Requests = append(o.Requests, requestResult{Action: "DescribeAutoScalingInstances", RequestID: stringValue(r.RequestId)})
		if expected != nil && *expected != *r.TotalCount {
			return o, fail("MembersChangedDuringPagination")
		}
		expected = r.TotalCount
		for _, m := range r.AutoScalingInstanceSet {
			if m == nil || stringValue(m.InstanceId) == "" || stringValue(m.AutoScalingGroupId) != groupID || seen[stringValue(m.InstanceId)] {
				return o, fail("InvalidMemberResponse")
			}
			seen[*m.InstanceId] = true
			o.Members = append(o.Members, member{ID: *m.InstanceId, CreationType: stringValue(m.CreationType), Lifecycle: stringValue(m.LifeCycleState), AddedAt: stringValue(m.AddTime), Protected: boolValue(m.ProtectedFromScaleIn)})
		}
		offset += int64(len(r.AutoScalingInstanceSet))
		if uint64(offset) == *expected {
			break
		}
		if len(r.AutoScalingInstanceSet) == 0 || uint64(offset) > *expected {
			return o, fail("IncompleteMembersPagination")
		}
	}
	if err = c.launchConfiguration(ctx, &o); err != nil {
		return o, err
	}
	if err = c.policies(ctx, &o); err != nil {
		return o, err
	}
	if trackedID == "" && len(o.Members) == 1 {
		trackedID = o.Members[0].ID
	}
	if trackedID != "" {
		req := cvm.NewDescribeInstancesRequest()
		req.InstanceIds = common.StringPtrs([]string{trackedID})
		req.Limit = common.Int64Ptr(1)
		resp, e := invoke(ctx, c.timeout, "DescribeInstances", func(ctx context.Context) (*cvm.DescribeInstancesResponse, error) {
			return c.cvm.DescribeInstancesWithContext(ctx, req)
		})
		if e != nil {
			return o, e
		}
		if resp == nil || resp.Response == nil || resp.Response.TotalCount == nil || *resp.Response.TotalCount != int64(len(resp.Response.InstanceSet)) || len(resp.Response.InstanceSet) > 1 {
			return o, fail("IncompleteCVMResponse")
		}
		o.VMID = trackedID
		o.Requests = append(o.Requests, requestResult{Action: "DescribeInstances", RequestID: stringValue(resp.Response.RequestId)})
		if len(resp.Response.InstanceSet) == 1 {
			vm := resp.Response.InstanceSet[0]
			if vm == nil || stringValue(vm.InstanceId) != trackedID {
				return o, fail("UnexpectedCVMInstance")
			}
			o.VMReturned, o.VMState, o.VMProtected = true, stringValue(vm.InstanceState), boolValue(vm.DisableApiTermination)
		}
	}
	return o, nil
}

func (c *sdkCloud) launchConfiguration(ctx context.Context, o *observation) error {
	if o.Group.LaunchConfiguration == "" {
		return fail("LaunchConfigurationMissing")
	}
	req := as.NewDescribeLaunchConfigurationsRequest()
	req.LaunchConfigurationIds = common.StringPtrs([]string{o.Group.LaunchConfiguration})
	resp, err := invoke(ctx, c.timeout, "DescribeLaunchConfigurations", func(ctx context.Context) (*as.DescribeLaunchConfigurationsResponse, error) {
		return c.as.DescribeLaunchConfigurationsWithContext(ctx, req)
	})
	if err != nil {
		return err
	}
	if resp == nil || resp.Response == nil || len(resp.Response.LaunchConfigurationSet) != 1 || resp.Response.LaunchConfigurationSet[0] == nil {
		return fail("LaunchConfigurationMissing")
	}
	lc := resp.Response.LaunchConfigurationSet[0]
	if stringValue(lc.LaunchConfigurationId) != o.Group.LaunchConfiguration {
		return fail("UnexpectedLaunchConfiguration")
	}
	// Whitelist only: never serialize UserData or LoginSettings from the SDK object.
	o.LaunchConfiguration = launchConfiguration{ID: *lc.LaunchConfigurationId, Status: stringValue(lc.LaunchConfigurationStatus), InstanceType: stringValue(lc.InstanceType), InstanceTypes: stringValues(lc.InstanceTypes), ChargeType: stringValue(lc.InstanceChargeType), ImageID: stringValue(lc.ImageId), SecurityGroupIDs: stringValues(lc.SecurityGroupIds)}
	o.LaunchConfiguration.VersionNumber = lc.VersionNumber
	o.Requests = append(o.Requests, requestResult{Action: "DescribeLaunchConfigurations", RequestID: stringValue(resp.Response.RequestId)})
	return nil
}

func (c *sdkCloud) policies(ctx context.Context, o *observation) error {
	o.Policies = make([]scalingPolicy, 0)
	var expected *uint64
	seen := map[string]bool{}
	for offset := uint64(0); ; {
		req := as.NewDescribeScalingPoliciesRequest()
		req.Filters = []*as.Filter{{Name: common.StringPtr("auto-scaling-group-id"), Values: common.StringPtrs([]string{o.Group.ID})}}
		req.Offset, req.Limit = common.Uint64Ptr(offset), common.Uint64Ptr(100)
		resp, err := invoke(ctx, c.timeout, "DescribeScalingPolicies", func(ctx context.Context) (*as.DescribeScalingPoliciesResponse, error) {
			return c.as.DescribeScalingPoliciesWithContext(ctx, req)
		})
		if err != nil {
			return err
		}
		if resp == nil || resp.Response == nil || resp.Response.TotalCount == nil {
			return fail("IncompletePoliciesResponse")
		}
		r := resp.Response
		o.Requests = append(o.Requests, requestResult{Action: "DescribeScalingPolicies", RequestID: stringValue(r.RequestId)})
		if expected != nil && *expected != *r.TotalCount {
			return fail("PoliciesChangedDuringPagination")
		}
		expected = r.TotalCount
		for _, p := range r.ScalingPolicySet {
			if p == nil || stringValue(p.AutoScalingGroupId) != o.Group.ID || stringValue(p.AutoScalingPolicyId) == "" || seen[stringValue(p.AutoScalingPolicyId)] {
				return fail("InvalidPolicyResponse")
			}
			seen[*p.AutoScalingPolicyId] = true
			o.Policies = append(o.Policies, scalingPolicy{ID: *p.AutoScalingPolicyId, Type: stringValue(p.ScalingPolicyType)})
		}
		offset += uint64(len(r.ScalingPolicySet))
		if offset == *expected {
			return nil
		}
		if len(r.ScalingPolicySet) == 0 || offset > *expected {
			return fail("IncompletePoliciesPagination")
		}
	}
}

func (c *sdkCloud) ExpandOne(ctx context.Context, groupID string) (requestResult, error) {
	req := as.NewModifyDesiredCapacityRequest()
	req.AutoScalingGroupId, req.DesiredCapacity = common.StringPtr(groupID), common.Uint64Ptr(1)
	resp, err := invoke(ctx, c.timeout, "ModifyDesiredCapacity", func(ctx context.Context) (*as.ModifyDesiredCapacityResponse, error) {
		return c.as.ModifyDesiredCapacityWithContext(ctx, req)
	})
	out := requestResult{Action: "ModifyDesiredCapacity"}
	if err != nil {
		return out, err
	}
	if resp == nil || resp.Response == nil {
		return out, fail("ModifyDesiredResponseUnknown")
	}
	out.RequestID = stringValue(resp.Response.RequestId)
	if out.RequestID == "" {
		return out, fail("ModifyDesiredResponseUnknown")
	}
	return out, nil
}
func (c *sdkCloud) RemoveExact(ctx context.Context, groupID, instanceID string) (requestResult, error) {
	req := as.NewRemoveInstancesRequest()
	req.AutoScalingGroupId, req.InstanceIds = common.StringPtr(groupID), common.StringPtrs([]string{instanceID})
	resp, err := invoke(ctx, c.timeout, "RemoveInstances", func(ctx context.Context) (*as.RemoveInstancesResponse, error) {
		return c.as.RemoveInstancesWithContext(ctx, req)
	})
	out := requestResult{Action: "RemoveInstances"}
	if err != nil {
		return out, err
	}
	if resp == nil || resp.Response == nil {
		return out, fail("RemoveResponseUnknown")
	}
	out.RequestID, out.ActivityID = stringValue(resp.Response.RequestId), stringValue(resp.Response.ActivityId)
	if out.RequestID == "" || out.ActivityID == "" {
		return out, fail("RemoveResponseUnknown")
	}
	return out, nil
}
