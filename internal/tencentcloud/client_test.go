package tencentcloud

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	as "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/as/v20180419"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
	sdkerrors "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/errors"
	cvm "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/cvm/v20170312"
)

const requestID = "11111111-2222-3333-4444-555555555555"
const secretMarker = "SHOULD-NOT-APPEAR-credential"

type fakeAS struct {
	groups                               func(context.Context, *as.DescribeAutoScalingGroupsRequest) (*as.DescribeAutoScalingGroupsResponse, error)
	members                              func(context.Context, *as.DescribeAutoScalingInstancesRequest) (*as.DescribeAutoScalingInstancesResponse, error)
	lc                                   func(context.Context, *as.DescribeLaunchConfigurationsRequest) (*as.DescribeLaunchConfigurationsResponse, error)
	policies                             func(context.Context, *as.DescribeScalingPoliciesRequest) (*as.DescribeScalingPoliciesResponse, error)
	activity                             func(context.Context, *as.DescribeAutoScalingActivitiesRequest) (*as.DescribeAutoScalingActivitiesResponse, error)
	desired                              func(context.Context, *as.ModifyDesiredCapacityRequest) (*as.ModifyDesiredCapacityResponse, error)
	remove                               func(context.Context, *as.RemoveInstancesRequest) (*as.RemoveInstancesResponse, error)
	desiredCalls, removeCalls, readCalls int
}

func (f *fakeAS) DescribeAutoScalingGroupsWithContext(c context.Context, r *as.DescribeAutoScalingGroupsRequest) (*as.DescribeAutoScalingGroupsResponse, error) {
	f.readCalls++
	return f.groups(c, r)
}
func (f *fakeAS) DescribeAutoScalingInstancesWithContext(c context.Context, r *as.DescribeAutoScalingInstancesRequest) (*as.DescribeAutoScalingInstancesResponse, error) {
	f.readCalls++
	return f.members(c, r)
}
func (f *fakeAS) DescribeLaunchConfigurationsWithContext(c context.Context, r *as.DescribeLaunchConfigurationsRequest) (*as.DescribeLaunchConfigurationsResponse, error) {
	f.readCalls++
	return f.lc(c, r)
}
func (f *fakeAS) DescribeScalingPoliciesWithContext(c context.Context, r *as.DescribeScalingPoliciesRequest) (*as.DescribeScalingPoliciesResponse, error) {
	f.readCalls++
	return f.policies(c, r)
}
func (f *fakeAS) DescribeAutoScalingActivitiesWithContext(c context.Context, r *as.DescribeAutoScalingActivitiesRequest) (*as.DescribeAutoScalingActivitiesResponse, error) {
	f.readCalls++
	return f.activity(c, r)
}
func (f *fakeAS) ModifyDesiredCapacityWithContext(c context.Context, r *as.ModifyDesiredCapacityRequest) (*as.ModifyDesiredCapacityResponse, error) {
	f.desiredCalls++
	return f.desired(c, r)
}
func (f *fakeAS) RemoveInstancesWithContext(c context.Context, r *as.RemoveInstancesRequest) (*as.RemoveInstancesResponse, error) {
	f.removeCalls++
	return f.remove(c, r)
}

type fakeCVM struct {
	fn    func(context.Context, *cvm.DescribeInstancesRequest) (*cvm.DescribeInstancesResponse, error)
	calls int
}

func (f *fakeCVM) DescribeInstancesWithContext(c context.Context, r *cvm.DescribeInstancesRequest) (*cvm.DescribeInstancesResponse, error) {
	f.calls++
	return f.fn(c, r)
}
func member(id string) *as.Instance {
	return &as.Instance{InstanceId: common.StringPtr(id), AutoScalingGroupId: common.StringPtr("asg-test"), CreationType: common.StringPtr("AUTO_CREATION"), LifeCycleState: common.StringPtr("IN_SERVICE"), ProtectedFromScaleIn: common.BoolPtr(false), AddTime: common.StringPtr("2026-10-15T01:00:00Z")}
}
func vm(id string) *cvm.Instance {
	return &cvm.Instance{InstanceId: common.StringPtr(id), InstanceState: common.StringPtr("RUNNING"), InstanceChargeType: common.StringPtr("POSTPAID_BY_HOUR"), DisableApiTermination: common.BoolPtr(false)}
}
func membersResponse(ms ...*as.Instance) *as.DescribeAutoScalingInstancesResponse {
	return &as.DescribeAutoScalingInstancesResponse{Response: &as.DescribeAutoScalingInstancesResponseParams{TotalCount: common.Uint64Ptr(uint64(len(ms))), AutoScalingInstanceSet: ms, RequestId: common.StringPtr(requestID)}}
}
func cvmResponse(vs ...*cvm.Instance) *cvm.DescribeInstancesResponse {
	return &cvm.DescribeInstancesResponse{Response: &cvm.DescribeInstancesResponseParams{TotalCount: common.Int64Ptr(int64(len(vs))), InstanceSet: vs, RequestId: common.StringPtr(requestID)}}
}
func fixture(t *testing.T) (*Client, *fakeAS, *fakeCVM) {
	t.Helper()
	a := &fakeAS{}
	v := &fakeCVM{}
	a.groups = func(ctx context.Context, r *as.DescribeAutoScalingGroupsRequest) (*as.DescribeAutoScalingGroupsResponse, error) {
		if len(r.AutoScalingGroupIds) != 1 || str(r.AutoScalingGroupIds[0]) != "asg-test" {
			t.Fatal("group query was not exact")
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("request has no timeout")
		}
		return &as.DescribeAutoScalingGroupsResponse{Response: &as.DescribeAutoScalingGroupsResponseParams{TotalCount: common.Uint64Ptr(1), RequestId: common.StringPtr(requestID), AutoScalingGroupSet: []*as.AutoScalingGroup{{AutoScalingGroupId: common.StringPtr("asg-test"), MinSize: common.Int64Ptr(0), MaxSize: common.Int64Ptr(1), DesiredCapacity: common.Int64Ptr(1), InstanceCount: common.Int64Ptr(1), EnabledStatus: common.StringPtr("ENABLED"), InActivityStatus: common.StringPtr("NOT_IN_ACTIVITY"), AutoScalingGroupStatus: common.StringPtr("NORMAL"), LaunchConfigurationId: common.StringPtr("asc-test"), RetryPolicy: common.StringPtr("NO_RETRY")}}}}, nil
	}
	a.members = func(_ context.Context, r *as.DescribeAutoScalingInstancesRequest) (*as.DescribeAutoScalingInstancesResponse, error) {
		if len(r.Filters) != 1 || str(r.Filters[0].Name) != "auto-scaling-group-id" || str(r.Filters[0].Values[0]) != "asg-test" {
			t.Fatal("unscoped member query")
		}
		return membersResponse(member("ins-one")), nil
	}
	a.lc = func(_ context.Context, r *as.DescribeLaunchConfigurationsRequest) (*as.DescribeLaunchConfigurationsResponse, error) {
		if len(r.LaunchConfigurationIds) != 1 || str(r.LaunchConfigurationIds[0]) != "asc-test" {
			t.Fatal("unscoped LC query")
		}
		return &as.DescribeLaunchConfigurationsResponse{Response: &as.DescribeLaunchConfigurationsResponseParams{TotalCount: common.Uint64Ptr(1), RequestId: common.StringPtr(requestID), LaunchConfigurationSet: []*as.LaunchConfiguration{{LaunchConfigurationId: common.StringPtr("asc-test"), LaunchConfigurationStatus: common.StringPtr("NORMAL"), InstanceType: common.StringPtr("S5.SMALL2"), InstanceChargeType: common.StringPtr("POSTPAID_BY_HOUR"), ImageId: common.StringPtr("img-test"), SecurityGroupIds: common.StringPtrs([]string{"sg-test"}), VersionNumber: common.Int64Ptr(1), UserData: common.StringPtr(secretMarker)}}}}, nil
	}
	a.policies = func(_ context.Context, r *as.DescribeScalingPoliciesRequest) (*as.DescribeScalingPoliciesResponse, error) {
		return &as.DescribeScalingPoliciesResponse{Response: &as.DescribeScalingPoliciesResponseParams{TotalCount: common.Uint64Ptr(0), RequestId: common.StringPtr(requestID)}}, nil
	}
	v.fn = func(_ context.Context, r *cvm.DescribeInstancesRequest) (*cvm.DescribeInstancesResponse, error) {
		out := []*cvm.Instance{}
		for _, id := range r.InstanceIds {
			if str(id) != "ins-gone" {
				out = append(out, vm(*id))
			}
		}
		return cvmResponse(out...), nil
	}
	return &Client{as: a, cvm: v, region: "ap-chengdu", timeout: time.Second}, a, v
}
func errorCode(t *testing.T, err error, want string) {
	t.Helper()
	e, ok := err.(*Error)
	if !ok || e.Code != want {
		t.Fatalf("got %v; want %s", err, want)
	}
}

func TestObserveWhitelistsAllMembersAndTrackedAbsent(t *testing.T) {
	c, _, v := fixture(t)
	got, err := c.Observe(context.Background(), "asg-test", "ins-gone")
	if err != nil {
		t.Fatal(err)
	}
	if got.Group.Desired != 1 || got.Group.Count != 1 || len(got.Members) != 1 || len(got.VMs) != 2 || !got.VMs["ins-one"].Returned || got.VMs["ins-gone"].Returned || got.VMs["ins-gone"].ID != "ins-gone" || got.PolicyCount != 0 || len(got.RequestIDs) != 5 || v.calls != 1 {
		t.Fatalf("incomplete whitelist snapshot: %+v", got)
	}
	b, _ := json.Marshal(got)
	if strings.Contains(string(b), secretMarker) || strings.Contains(string(b), "UserData") {
		t.Fatal("raw LC data leaked")
	}
}

func TestMembersPaginationAndCVMExactBatching(t *testing.T) {
	c, a, v := fixture(t)
	memberCalls := 0
	a.members = func(_ context.Context, r *as.DescribeAutoScalingInstancesRequest) (*as.DescribeAutoScalingInstancesResponse, error) {
		memberCalls++
		start := int(*r.Offset)
		end := start + 100
		if end > 101 {
			end = 101
		}
		ms := []*as.Instance{}
		for i := start; i < end; i++ {
			ms = append(ms, member(fmt.Sprintf("ins-n%03d", i)))
		}
		resp := membersResponse(ms...)
		resp.Response.TotalCount = common.Uint64Ptr(101)
		return resp, nil
	}
	got, err := c.Observe(context.Background(), "asg-test", "ins-gone")
	if err != nil {
		t.Fatal(err)
	}
	if memberCalls != 2 || v.calls != 2 || len(got.Members) != 101 || len(got.VMs) != 102 {
		t.Fatalf("pagination/batching failed: memberCalls=%d vmCalls=%d", memberCalls, v.calls)
	}
	// This is a non-atomic observation; preserve provider Count rather than
	// manufacturing agreement with the separately collected member list.
	if got.Group.Count != 1 {
		t.Fatal("Count was overwritten")
	}
}

func TestMembersRejectDriftDuplicateAndWrongGroup(t *testing.T) {
	for _, name := range []string{"drift", "duplicate", "wrong-group", "missing-protection", "missing-time"} {
		t.Run(name, func(t *testing.T) {
			c, a, _ := fixture(t)
			calls := 0
			a.members = func(_ context.Context, r *as.DescribeAutoScalingInstancesRequest) (*as.DescribeAutoScalingInstancesResponse, error) {
				calls++
				m := member("ins-one")
				resp := membersResponse(m)
				switch name {
				case "drift":
					resp.Response.TotalCount = common.Uint64Ptr(uint64(3 - calls))
				case "duplicate":
					resp = membersResponse(m, m)
				case "wrong-group":
					m.AutoScalingGroupId = common.StringPtr("asg-other")
				case "missing-protection":
					m.ProtectedFromScaleIn = nil
				case "missing-time":
					m.AddTime = nil
				}
				return resp, nil
			}
			_, err := c.Observe(context.Background(), "asg-test", "")
			if err == nil {
				t.Fatal("unsafe member response accepted")
			}
		})
	}
}

func TestCVMRejectsExtraDuplicateIncompleteAndUnknownProtection(t *testing.T) {
	for _, name := range []string{"extra", "duplicate", "incomplete", "missing-protection"} {
		t.Run(name, func(t *testing.T) {
			c, _, v := fixture(t)
			v.fn = func(_ context.Context, _ *cvm.DescribeInstancesRequest) (*cvm.DescribeInstancesResponse, error) {
				m := vm("ins-one")
				r := cvmResponse(m)
				switch name {
				case "extra":
					r = cvmResponse(vm("ins-other"))
				case "duplicate":
					r = cvmResponse(m, m)
				case "incomplete":
					r.Response.TotalCount = common.Int64Ptr(2)
				case "missing-protection":
					m.DisableApiTermination = nil
				}
				return r, nil
			}
			_, err := c.Observe(context.Background(), "asg-test", "")
			if err == nil {
				t.Fatal("unproven CVM response accepted")
			}
		})
	}
}

func TestPoliciesFullPaginationAndScope(t *testing.T) {
	c, a, _ := fixture(t)
	calls := 0
	a.policies = func(_ context.Context, r *as.DescribeScalingPoliciesRequest) (*as.DescribeScalingPoliciesResponse, error) {
		calls++
		if len(r.Filters) != 1 || str(r.Filters[0].Values[0]) != "asg-test" {
			t.Fatal("unscoped policy query")
		}
		id := "asp-one"
		if *r.Offset == 1 {
			id = "asp-two"
		}
		return &as.DescribeScalingPoliciesResponse{Response: &as.DescribeScalingPoliciesResponseParams{TotalCount: common.Uint64Ptr(2), RequestId: common.StringPtr(requestID), ScalingPolicySet: []*as.ScalingPolicy{{AutoScalingGroupId: common.StringPtr("asg-test"), AutoScalingPolicyId: common.StringPtr(id), ScalingPolicyType: common.StringPtr("SIMPLE")}}}}, nil
	}
	got, err := c.Observe(context.Background(), "asg-test", "")
	if err != nil || got.PolicyCount != 2 || calls != 2 {
		t.Fatalf("policies pagination failed: %+v %v", got, err)
	}
	a.policies = func(_ context.Context, _ *as.DescribeScalingPoliciesRequest) (*as.DescribeScalingPoliciesResponse, error) {
		return &as.DescribeScalingPoliciesResponse{Response: &as.DescribeScalingPoliciesResponseParams{TotalCount: common.Uint64Ptr(1), RequestId: common.StringPtr(requestID), ScalingPolicySet: []*as.ScalingPolicy{{AutoScalingGroupId: common.StringPtr("asg-other"), AutoScalingPolicyId: common.StringPtr("asp-one"), ScalingPolicyType: common.StringPtr("SIMPLE")}}}}, nil
	}
	_, err = c.Observe(context.Background(), "asg-test", "")
	errorCode(t, err, "InvalidPolicyResponse")
}

func TestActivityExactIdentityAndPerInstanceResult(t *testing.T) {
	for _, name := range []string{"valid", "wrong-group", "wrong-activity", "duplicate-related", "missing"} {
		t.Run(name, func(t *testing.T) {
			c, a, _ := fixture(t)
			a.activity = func(_ context.Context, r *as.DescribeAutoScalingActivitiesRequest) (*as.DescribeAutoScalingActivitiesResponse, error) {
				if len(r.ActivityIds) != 1 || str(r.ActivityIds[0]) != "asa-test" || len(r.Filters) != 0 {
					t.Fatal("activity lookup not exact or incompatible filters added")
				}
				item := &as.Activity{ActivityId: common.StringPtr("asa-test"), AutoScalingGroupId: common.StringPtr("asg-test"), StatusCode: common.StringPtr("FAILED"), StatusMessage: common.StringPtr(secretMarker), RelatedInstanceSet: []*as.RelatedInstance{{InstanceId: common.StringPtr("ins-one"), InstanceStatus: common.StringPtr("FAILED")}}}
				resp := &as.DescribeAutoScalingActivitiesResponse{Response: &as.DescribeAutoScalingActivitiesResponseParams{TotalCount: common.Uint64Ptr(1), RequestId: common.StringPtr(requestID), ActivitySet: []*as.Activity{item}}}
				switch name {
				case "wrong-group":
					item.AutoScalingGroupId = common.StringPtr("asg-other")
				case "wrong-activity":
					item.ActivityId = common.StringPtr("asa-other")
				case "duplicate-related":
					item.RelatedInstanceSet = append(item.RelatedInstanceSet, item.RelatedInstanceSet[0])
				case "missing":
					resp.Response.ActivitySet = nil
					resp.Response.TotalCount = common.Uint64Ptr(0)
				}
				return resp, nil
			}
			got, err := c.Activity(context.Background(), "asg-test", "asa-test")
			if name != "valid" {
				if err == nil {
					t.Fatal("unrelated/invalid activity accepted")
				}
				return
			}
			if err != nil || got.Status != "FAILED" || len(got.RelatedInstances) != 1 || got.RelatedInstances[0].ID != "ins-one" {
				t.Fatalf("activity result incomplete: %+v %v", got, err)
			}
			b, _ := json.Marshal(got)
			if strings.Contains(string(b), secretMarker) {
				t.Fatal("activity details leaked")
			}
		})
	}
}

func TestWritesAreSingleAttemptAndDoNotChangeLimits(t *testing.T) {
	for _, count := range []int64{0, 1, 2, 100, 150, 2000} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			c, a, _ := fixture(t)
			a.desired = func(ctx context.Context, r *as.ModifyDesiredCapacityRequest) (*as.ModifyDesiredCapacityResponse, error) {
				if str(r.AutoScalingGroupId) != "asg-test" || r.DesiredCapacity == nil || *r.DesiredCapacity != uint64(count) || r.MinSize != nil || r.MaxSize != nil {
					t.Fatal("mutation changed scope or bounds")
				}
				if _, ok := ctx.Deadline(); !ok {
					t.Fatal("write timeout missing")
				}
				return &as.ModifyDesiredCapacityResponse{Response: &as.ModifyDesiredCapacityResponseParams{RequestId: common.StringPtr(requestID)}}, nil
			}
			got, err := c.SetDesired(context.Background(), "asg-test", count)
			if err != nil || !got.Attempted || got.Unknown || a.desiredCalls != 1 {
				t.Fatalf("write not proven %+v %v", got, err)
			}
		})
	}
	c, a, _ := fixture(t)
	a.remove = func(_ context.Context, r *as.RemoveInstancesRequest) (*as.RemoveInstancesResponse, error) {
		if str(r.AutoScalingGroupId) != "asg-test" || len(r.InstanceIds) != 1 || str(r.InstanceIds[0]) != "ins-one" {
			t.Fatal("non-exact deletion")
		}
		return &as.RemoveInstancesResponse{Response: &as.RemoveInstancesResponseParams{RequestId: common.StringPtr(requestID), ActivityId: common.StringPtr("asa-test")}}, nil
	}
	got, err := c.RemoveExact(context.Background(), "asg-test", "ins-one")
	if err != nil || got.ActivityID != "asa-test" || !got.Attempted || got.Unknown || a.removeCalls != 1 {
		t.Fatalf("delete not proven %+v %v", got, err)
	}
}

func TestUnknownWriteResponsesNeverReplay(t *testing.T) {
	for _, name := range []string{"nil", "missing-request", "invalid-request", "sdk-error", "timeout"} {
		t.Run(name, func(t *testing.T) {
			c, a, _ := fixture(t)
			a.desired = func(context.Context, *as.ModifyDesiredCapacityRequest) (*as.ModifyDesiredCapacityResponse, error) {
				switch name {
				case "nil":
					return nil, nil
				case "sdk-error":
					return nil, sdkerrors.NewTencentCloudSDKError("InternalError", secretMarker, requestID)
				case "timeout":
					return nil, context.DeadlineExceeded
				case "invalid-request":
					return &as.ModifyDesiredCapacityResponse{Response: &as.ModifyDesiredCapacityResponseParams{RequestId: common.StringPtr(secretMarker)}}, nil
				}
				return &as.ModifyDesiredCapacityResponse{Response: &as.ModifyDesiredCapacityResponseParams{}}, nil
			}
			got, err := c.SetDesired(context.Background(), "asg-test", 1)
			e, ok := err.(*Error)
			if !ok || !e.Unknown || !got.Unknown || !got.Attempted || a.desiredCalls != 1 {
				t.Fatalf("write uncertainty not preserved %+v %v", got, err)
			}
			b, _ := json.Marshal(struct {
				Result WriteResult
				Error  *Error
			}{got, e})
			if strings.Contains(string(b), secretMarker) {
				t.Fatal("SDK details leaked")
			}
		})
	}
	c, a, _ := fixture(t)
	a.remove = func(context.Context, *as.RemoveInstancesRequest) (*as.RemoveInstancesResponse, error) {
		return &as.RemoveInstancesResponse{Response: &as.RemoveInstancesResponseParams{RequestId: common.StringPtr(requestID)}}, nil
	}
	got, err := c.RemoveExact(context.Background(), "asg-test", "ins-one")
	if err == nil || !got.Unknown || a.removeCalls != 1 {
		t.Fatal("missing activity ID accepted")
	}
}

func TestInvalidInputsAndCanceledContextDoNotWrite(t *testing.T) {
	c, a, _ := fixture(t)
	for _, count := range []int64{-1} {
		got, err := c.SetDesired(context.Background(), "asg-test", count)
		if err == nil || got.Attempted {
			t.Fatal("invalid count sent")
		}
	}
	if got, err := c.RemoveExact(context.Background(), "asg-test", "ins-../secret"); err == nil || got.Attempted {
		t.Fatal("invalid instance sent")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got, err := c.SetDesired(ctx, "asg-test", 1)
	if err == nil || got.Attempted || got.Unknown {
		t.Fatal("canceled request attempted")
	}
	if a.desiredCalls != 0 || a.removeCalls != 0 {
		t.Fatal("invalid/canceled write reached SDK")
	}
	_, err = c.Observe(context.Background(), "asg-test", "ins-../secret")
	if err == nil || a.readCalls != 0 {
		t.Fatal("invalid observation identity reached SDK")
	}
}

func TestRemoveUnknownPreservesEachValidResponseIdentifier(t *testing.T) {
	for _, tc := range []struct {
		name, request, activity, wantRequest, wantActivity string
		withError                                          bool
	}{
		{name: "missing-request", activity: "asa-test", wantActivity: "asa-test"},
		{name: "invalid-request", request: secretMarker, activity: "asa-test", wantActivity: "asa-test"},
		{name: "missing-activity", request: requestID, wantRequest: requestID},
		{name: "invalid-activity", request: requestID, activity: secretMarker, wantRequest: requestID},
		{name: "sdk-error-with-response", request: requestID, activity: "asa-test", wantRequest: requestID, wantActivity: "asa-test", withError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, a, _ := fixture(t)
			a.remove = func(context.Context, *as.RemoveInstancesRequest) (*as.RemoveInstancesResponse, error) {
				resp := &as.RemoveInstancesResponse{Response: &as.RemoveInstancesResponseParams{RequestId: common.StringPtr(tc.request), ActivityId: common.StringPtr(tc.activity)}}
				if tc.withError {
					return resp, sdkerrors.NewTencentCloudSDKError("InternalError", secretMarker, requestID)
				}
				return resp, nil
			}
			got, err := c.RemoveExact(context.Background(), "asg-test", "ins-one")
			e, ok := err.(*Error)
			if !ok || !e.Unknown || !got.Unknown || !got.Attempted || a.removeCalls != 1 || got.RequestID != tc.wantRequest || got.ActivityID != tc.wantActivity {
				t.Fatalf("partial response evidence lost or replayed: %+v %v", got, err)
			}
			b, _ := json.Marshal(struct {
				Result WriteResult
				Error  *Error
			}{got, e})
			if strings.Contains(string(b), secretMarker) {
				t.Fatal("invalid response identifier leaked")
			}
		})
	}
}

func TestConfigurationDisablesSDKRetriesAndDebug(t *testing.T) {
	p := clientProfile(1500 * time.Millisecond)
	if p.Debug || !p.DisableRegionBreaker || p.NetworkFailureMaxRetries != 0 || p.RateLimitExceededMaxRetries != 0 || p.UnsafeRetryOnConnectionFailure || p.HttpProfile.ReqTimeout != 2 {
		t.Fatal("unsafe SDK profile")
	}
	for _, cfg := range []Config{{Region: "ap-chengdu"}, {Region: "../unsafe", SecretID: "test-id", SecretKey: "test-key"}, {Region: "ap-chengdu", SecretID: "test-id", SecretKey: "test-key", Timeout: time.Hour}} {
		if _, err := New(cfg); err == nil {
			t.Fatal("invalid config accepted")
		}
	}
}

func TestRawErrorsNeverEscape(t *testing.T) {
	c, a, _ := fixture(t)
	a.groups = func(context.Context, *as.DescribeAutoScalingGroupsRequest) (*as.DescribeAutoScalingGroupsResponse, error) {
		return nil, errors.New("url=https://" + secretMarker + "@example.invalid request-body=" + secretMarker)
	}
	_, err := c.Observe(context.Background(), "asg-test", "")
	errorCode(t, err, "RequestFailed")
	b, _ := json.Marshal(err)
	if strings.Contains(string(b), secretMarker) {
		t.Fatal("raw transport error leaked")
	}
	e := sanitize("Describe", sdkerrors.NewTencentCloudSDKError("AKIDSecretLookingValue", "hidden-message", "not-a-request-id"), false)
	if e.Code != "RequestFailed" || e.RequestID != "" {
		t.Fatal("unrecognized SDK fields leaked")
	}
}

func TestRejectChangedGroupLaunchConfigurationAndRequestIdentity(t *testing.T) {
	for _, name := range []string{"group", "group-request", "lc", "lc-version"} {
		t.Run(name, func(t *testing.T) {
			c, a, _ := fixture(t)
			if strings.HasPrefix(name, "group") {
				orig := a.groups
				a.groups = func(ctx context.Context, r *as.DescribeAutoScalingGroupsRequest) (*as.DescribeAutoScalingGroupsResponse, error) {
					resp, err := orig(ctx, r)
					if name == "group" {
						resp.Response.AutoScalingGroupSet[0].AutoScalingGroupId = common.StringPtr("asg-other")
					} else {
						resp.Response.RequestId = common.StringPtr(secretMarker)
					}
					return resp, err
				}
			} else {
				orig := a.lc
				a.lc = func(ctx context.Context, r *as.DescribeLaunchConfigurationsRequest) (*as.DescribeLaunchConfigurationsResponse, error) {
					resp, err := orig(ctx, r)
					if name == "lc" {
						resp.Response.LaunchConfigurationSet[0].LaunchConfigurationId = common.StringPtr("asc-other")
					} else {
						resp.Response.LaunchConfigurationSet[0].VersionNumber = nil
					}
					return resp, err
				}
			}
			_, err := c.Observe(context.Background(), "asg-test", "")
			if err == nil {
				t.Fatal("mismatched or incomplete identity accepted")
			}
		})
	}
}

func TestSetDesiredAbovePrototypeLimitDoesNotModifyGroupBounds(t *testing.T) {
	c, a, _ := fixture(t)
	a.desired = func(_ context.Context, r *as.ModifyDesiredCapacityRequest) (*as.ModifyDesiredCapacityResponse, error) {
		var fields map[string]any
		if json.Unmarshal([]byte(r.ToJsonString()), &fields) != nil || fields["DesiredCapacity"] != float64(150) || len(fields) != 2 {
			t.Fatal("unexpected capacity write fields")
		}
		return &as.ModifyDesiredCapacityResponse{Response: &as.ModifyDesiredCapacityResponseParams{RequestId: common.StringPtr(requestID)}}, nil
	}
	if result, err := c.SetDesired(context.Background(), "asg-test", 150); err != nil || !result.Attempted || a.desiredCalls != 1 {
		t.Fatal(result, err)
	}
}
