package tencentcloud

import (
	"context"
	"errors"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/telemetry"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"

	as "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/as/v20180419"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
	sdkerrors "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/errors"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/profile"
	cvm "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/cvm/v20170312"
)

const pageSize = 100
const maxPages = 1000

var regionPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{1,63}$`)
var resourcePattern = regexp.MustCompile(`^[a-z]+-[A-Za-z0-9]{1,64}$`)
var requestPattern = regexp.MustCompile(`^[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}$`)
var codePattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_.]{0,127}$`)
var statePattern = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,127}$`)
var typePattern = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,128}$`)

func resource(id, prefix string) bool {
	return strings.HasPrefix(id, prefix+"-") && resourcePattern.MatchString(id)
}
func str(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}
func bad(action, code string) error { return &Error{Action: action, Code: code} }
func safeCode(code string) bool {
	if !codePattern.MatchString(code) {
		return false
	}
	root := strings.SplitN(code, ".", 2)[0]
	return oneOf(root, "AuthFailure", "ClientError", "ClientNetworkError", "FailedOperation", "InternalError", "InvalidAction", "InvalidParameter", "InvalidParameterValue", "LimitExceeded", "MissingParameter", "OperationDenied", "RequestLimitExceeded", "ResourceInUse", "ResourceInsufficient", "ResourceNotFound", "ResourceUnavailable", "ResourcesSoldOut", "UnauthorizedOperation", "UnknownParameter", "UnsupportedOperation")
}
func sanitize(action string, err error, unknown bool) *Error {
	e := &Error{Code: "RequestFailed", Action: action, Unknown: unknown}
	var sdk *sdkerrors.TencentCloudSDKError
	if errors.As(err, &sdk) {
		if safeCode(sdk.GetCode()) {
			e.Code = sdk.GetCode()
		}
		if requestPattern.MatchString(sdk.GetRequestId()) {
			e.RequestID = sdk.GetRequestId()
		}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		e.Code = "RequestTimeout"
	} else if errors.Is(err, context.Canceled) {
		e.Code = "RequestCanceled"
	}
	return e
}

type asAPI interface {
	DescribeAutoScalingGroupsWithContext(context.Context, *as.DescribeAutoScalingGroupsRequest) (*as.DescribeAutoScalingGroupsResponse, error)
	DescribeAutoScalingInstancesWithContext(context.Context, *as.DescribeAutoScalingInstancesRequest) (*as.DescribeAutoScalingInstancesResponse, error)
	DescribeLaunchConfigurationsWithContext(context.Context, *as.DescribeLaunchConfigurationsRequest) (*as.DescribeLaunchConfigurationsResponse, error)
	DescribeScalingPoliciesWithContext(context.Context, *as.DescribeScalingPoliciesRequest) (*as.DescribeScalingPoliciesResponse, error)
	DescribeAutoScalingActivitiesWithContext(context.Context, *as.DescribeAutoScalingActivitiesRequest) (*as.DescribeAutoScalingActivitiesResponse, error)
	ModifyDesiredCapacityWithContext(context.Context, *as.ModifyDesiredCapacityRequest) (*as.ModifyDesiredCapacityResponse, error)
	RemoveInstancesWithContext(context.Context, *as.RemoveInstancesRequest) (*as.RemoveInstancesResponse, error)
}
type cvmAPI interface {
	DescribeInstancesWithContext(context.Context, *cvm.DescribeInstancesRequest) (*cvm.DescribeInstancesResponse, error)
}
type Client struct {
	metrics *telemetry.Collector
	as      asAPI
	cvm     cvmAPI
	region  string
	timeout time.Duration
}

func clientProfile(timeout time.Duration) *profile.ClientProfile {
	p := profile.NewClientProfile()
	p.Debug = false
	p.DisableRegionBreaker = true
	p.NetworkFailureMaxRetries = 0
	p.RateLimitExceededMaxRetries = 0
	p.UnsafeRetryOnConnectionFailure = false
	p.HttpProfile.ReqTimeout = int((timeout + time.Second - 1) / time.Second)
	return p
}
func New(cfg Config) (*Client, error) {
	return newClient(cfg, nil)
}

func newClient(cfg Config, transport http.RoundTripper) (*Client, error) {
	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Second
	}
	if !regionPattern.MatchString(cfg.Region) || cfg.SecretID == "" || cfg.SecretKey == "" || cfg.Timeout < time.Millisecond || cfg.Timeout > 30*time.Second {
		return nil, bad("Configure", "InvalidConfig")
	}
	cred := common.NewTokenCredential(cfg.SecretID, cfg.SecretKey, cfg.Token)
	a, err := as.NewClient(cred, cfg.Region, clientProfile(cfg.Timeout))
	if err != nil {
		return nil, sanitize("ConfigureAS", err, false)
	}
	v, err := cvm.NewClient(cred, cfg.Region, clientProfile(cfg.Timeout))
	if err != nil {
		return nil, sanitize("ConfigureCVM", err, false)
	}
	mutationTransport := transport
	if mutationTransport == nil {
		mutationTransport = newSubmissionTransport()
	}
	a.WithHttpTransport(&mutationResponseTransport{base: mutationTransport})
	if transport != nil {
		v.WithHttpTransport(transport)
	}
	return &Client{metrics: cfg.Metrics, as: a, cvm: v, region: cfg.Region, timeout: cfg.Timeout}, nil
}
func call[T any](ctx context.Context, c *Client, action string, fn func(context.Context) (T, error)) (T, error) {
	value, err := retryRateLimited(ctx, c.timeout, func(attempt context.Context) (T, error) {
		value, err := fn(attempt)
		c.metrics.API(action, err != nil, readRateLimited(err))
		return value, err
	}, func(_ T, err error) bool { return readRateLimited(err) })
	if err != nil {
		return value, sanitize(action, err, false)
	}
	return value, nil
}
func trace(action string, id *string) (RequestTrace, error) {
	if !requestPattern.MatchString(str(id)) {
		return RequestTrace{}, bad(action, "InvalidRequestID")
	}
	return RequestTrace{Action: action, RequestID: *id}, nil
}

func (c *Client) Observe(ctx context.Context, groupID, trackedID string) (out Snapshot, err error) {
	out = Snapshot{At: time.Now().UTC(), Region: c.region, Members: []Member{}, VMs: map[string]VM{}, Policies: []Policy{}, RequestIDs: []RequestTrace{}}
	if !resource(groupID, "asg") || (trackedID != "" && !resource(trackedID, "ins")) {
		return out, bad("Observe", "InvalidIdentity")
	}
	const action = "DescribeAutoScalingGroups"
	req := as.NewDescribeAutoScalingGroupsRequest()
	req.AutoScalingGroupIds = common.StringPtrs([]string{groupID})
	resp, err := call(ctx, c, action, func(ctx context.Context) (*as.DescribeAutoScalingGroupsResponse, error) {
		return c.as.DescribeAutoScalingGroupsWithContext(ctx, req)
	})
	if err != nil {
		return out, err
	}
	if resp == nil || resp.Response == nil || resp.Response.TotalCount == nil || *resp.Response.TotalCount != 1 || len(resp.Response.AutoScalingGroupSet) != 1 || resp.Response.AutoScalingGroupSet[0] == nil {
		return out, bad(action, "GroupMissingOrIncomplete")
	}
	r := resp.Response
	g := r.AutoScalingGroupSet[0]
	if str(g.AutoScalingGroupId) != groupID || g.MinSize == nil || g.MaxSize == nil || g.DesiredCapacity == nil || g.InstanceCount == nil || *g.MinSize < 0 || *g.MaxSize < *g.MinSize || *g.DesiredCapacity < 0 || *g.InstanceCount < 0 || !resource(str(g.LaunchConfigurationId), "asc") || !statePattern.MatchString(str(g.AutoScalingGroupStatus)) || !statePattern.MatchString(str(g.RetryPolicy)) || (str(g.EnabledStatus) != "ENABLED" && str(g.EnabledStatus) != "DISABLED") || (str(g.InActivityStatus) != "IN_ACTIVITY" && str(g.InActivityStatus) != "NOT_IN_ACTIVITY") {
		return out, bad(action, "InvalidGroupResponse")
	}
	tr, err := trace(action, r.RequestId)
	if err != nil {
		return out, err
	}
	out.RequestIDs = append(out.RequestIDs, tr)
	out.Group = Group{ID: groupID, Min: *g.MinSize, Max: *g.MaxSize, Desired: *g.DesiredCapacity, Count: *g.InstanceCount, Enabled: *g.EnabledStatus, Activity: *g.InActivityStatus, Status: *g.AutoScalingGroupStatus, LaunchConfiguration: *g.LaunchConfigurationId, RetryPolicy: *g.RetryPolicy}
	switch policy := str(g.InstanceAllocationPolicy); policy {
	case "", "LAUNCH_CONFIGURATION", "SPOT_MIXED":
		out.Group.AllocationPolicy = policy
	default:
		out.Group.AllocationPolicy = "UNSUPPORTED"
	}
	if g.ServiceSettings != nil {
		mode := str(g.ServiceSettings.ScalingMode)
		if mode != "" && !statePattern.MatchString(mode) {
			return out, bad(action, "InvalidScalingMode")
		}
		out.Group.ScalingMode = mode
	}
	if err = c.members(ctx, &out); err != nil {
		return out, err
	}
	if err = c.launchConfiguration(ctx, &out); err != nil {
		return out, err
	}
	if err = c.policies(ctx, &out); err != nil {
		return out, err
	}
	// Diagnostics are advisory: a missing read permission is warned about by
	// the controller and must not prevent otherwise safe capacity operations.
	if err = c.scheduledActions(ctx, &out); err != nil {
		out.ScheduledActionsUnavailable = true
	}
	if err = c.vms(ctx, &out, trackedID); err != nil {
		return out, err
	}
	return out, nil
}

func (c *Client) members(ctx context.Context, out *Snapshot) error {
	const action = "DescribeAutoScalingInstances"
	seen := map[string]bool{}
	var expected *uint64
	offset := int64(0)
	for page := 0; page < maxPages; page++ {
		req := as.NewDescribeAutoScalingInstancesRequest()
		req.Filters = []*as.Filter{{Name: common.StringPtr("auto-scaling-group-id"), Values: common.StringPtrs([]string{out.Group.ID})}}
		req.Offset, req.Limit = common.Int64Ptr(offset), common.Int64Ptr(pageSize)
		resp, err := call(ctx, c, action, func(ctx context.Context) (*as.DescribeAutoScalingInstancesResponse, error) {
			return c.as.DescribeAutoScalingInstancesWithContext(ctx, req)
		})
		if err != nil {
			return err
		}
		if resp == nil || resp.Response == nil || resp.Response.TotalCount == nil {
			return bad(action, "IncompleteMembersResponse")
		}
		r := resp.Response
		tr, err := trace(action, r.RequestId)
		if err != nil {
			return err
		}
		out.RequestIDs = append(out.RequestIDs, tr)
		if expected != nil && *expected != *r.TotalCount {
			return bad(action, "MembersChangedDuringPagination")
		}
		total := *r.TotalCount
		expected = &total
		for _, m := range r.AutoScalingInstanceSet {
			if m == nil || !resource(str(m.InstanceId), "ins") || str(m.AutoScalingGroupId) != out.Group.ID || seen[str(m.InstanceId)] || m.ProtectedFromScaleIn == nil || !statePattern.MatchString(str(m.CreationType)) || !statePattern.MatchString(str(m.LifeCycleState)) {
				return bad(action, "InvalidMemberResponse")
			}
			if _, err := time.Parse(time.RFC3339, str(m.AddTime)); err != nil {
				return bad(action, "InvalidMemberAddedAt")
			}
			seen[*m.InstanceId] = true
			out.Members = append(out.Members, Member{ID: *m.InstanceId, CreationType: *m.CreationType, Lifecycle: *m.LifeCycleState, AddedAt: *m.AddTime, Protected: *m.ProtectedFromScaleIn})
		}
		offset += int64(len(r.AutoScalingInstanceSet))
		if uint64(offset) == total {
			return nil
		}
		if len(r.AutoScalingInstanceSet) == 0 || uint64(offset) > total {
			return bad(action, "IncompleteMembersPagination")
		}
	}
	return bad(action, "MembersPaginationLimit")
}

func (c *Client) launchConfiguration(ctx context.Context, out *Snapshot) error {
	const action = "DescribeLaunchConfigurations"
	req := as.NewDescribeLaunchConfigurationsRequest()
	req.LaunchConfigurationIds = common.StringPtrs([]string{out.Group.LaunchConfiguration})
	resp, err := call(ctx, c, action, func(ctx context.Context) (*as.DescribeLaunchConfigurationsResponse, error) {
		return c.as.DescribeLaunchConfigurationsWithContext(ctx, req)
	})
	if err != nil {
		return err
	}
	if resp == nil || resp.Response == nil || resp.Response.TotalCount == nil || *resp.Response.TotalCount != 1 || len(resp.Response.LaunchConfigurationSet) != 1 || resp.Response.LaunchConfigurationSet[0] == nil {
		return bad(action, "LaunchConfigurationMissing")
	}
	r := resp.Response
	lc := r.LaunchConfigurationSet[0]
	if str(lc.LaunchConfigurationId) != out.Group.LaunchConfiguration || !statePattern.MatchString(str(lc.LaunchConfigurationStatus)) || !statePattern.MatchString(str(lc.InstanceChargeType)) || !resource(str(lc.ImageId), "img") || lc.VersionNumber == nil || *lc.VersionNumber < 1 {
		return bad(action, "InvalidLaunchConfiguration")
	}
	o := LaunchConfiguration{ID: *lc.LaunchConfigurationId, Status: *lc.LaunchConfigurationStatus, InstanceType: str(lc.InstanceType), ChargeType: *lc.InstanceChargeType, ImageID: *lc.ImageId, InstanceTypes: []string{}, SecurityGroupIDs: []string{}, VersionNumber: *lc.VersionNumber}
	if o.InstanceType != "" && !typePattern.MatchString(o.InstanceType) {
		return bad(action, "InvalidInstanceType")
	}
	for _, v := range lc.InstanceTypes {
		if !typePattern.MatchString(str(v)) {
			return bad(action, "InvalidInstanceType")
		}
		o.InstanceTypes = append(o.InstanceTypes, *v)
	}
	if o.InstanceType == "" && len(o.InstanceTypes) == 0 {
		return bad(action, "MissingInstanceType")
	}
	seen := map[string]bool{}
	for _, v := range lc.SecurityGroupIds {
		if !resource(str(v), "sg") || seen[str(v)] {
			return bad(action, "InvalidSecurityGroupID")
		}
		seen[*v] = true
		o.SecurityGroupIDs = append(o.SecurityGroupIDs, *v)
	}
	tr, err := trace(action, r.RequestId)
	if err != nil {
		return err
	}
	out.RequestIDs = append(out.RequestIDs, tr)
	out.LaunchConfiguration = o
	return nil
}

func (c *Client) policies(ctx context.Context, out *Snapshot) error {
	const action = "DescribeScalingPolicies"
	seen := map[string]bool{}
	var expected *uint64
	offset := uint64(0)
	for page := 0; page < maxPages; page++ {
		req := as.NewDescribeScalingPoliciesRequest()
		req.Filters = []*as.Filter{{Name: common.StringPtr("auto-scaling-group-id"), Values: common.StringPtrs([]string{out.Group.ID})}}
		req.Offset, req.Limit = common.Uint64Ptr(offset), common.Uint64Ptr(pageSize)
		resp, err := call(ctx, c, action, func(ctx context.Context) (*as.DescribeScalingPoliciesResponse, error) {
			return c.as.DescribeScalingPoliciesWithContext(ctx, req)
		})
		if err != nil {
			return err
		}
		if resp == nil || resp.Response == nil || resp.Response.TotalCount == nil {
			return bad(action, "IncompletePoliciesResponse")
		}
		r := resp.Response
		tr, err := trace(action, r.RequestId)
		if err != nil {
			return err
		}
		out.RequestIDs = append(out.RequestIDs, tr)
		if expected != nil && *expected != *r.TotalCount {
			return bad(action, "PoliciesChangedDuringPagination")
		}
		total := *r.TotalCount
		expected = &total
		for _, p := range r.ScalingPolicySet {
			if p == nil || str(p.AutoScalingGroupId) != out.Group.ID || !resource(str(p.AutoScalingPolicyId), "asp") || seen[str(p.AutoScalingPolicyId)] || !statePattern.MatchString(str(p.ScalingPolicyType)) {
				return bad(action, "InvalidPolicyResponse")
			}
			seen[*p.AutoScalingPolicyId] = true
			out.Policies = append(out.Policies, Policy{ID: *p.AutoScalingPolicyId, Type: *p.ScalingPolicyType})
		}
		offset += uint64(len(r.ScalingPolicySet))
		if offset == total {
			out.PolicyCount = len(out.Policies)
			return nil
		}
		if len(r.ScalingPolicySet) == 0 || offset > total {
			return bad(action, "IncompletePoliciesPagination")
		}
	}
	return bad(action, "PoliciesPaginationLimit")
}

func (c *Client) vms(ctx context.Context, out *Snapshot, tracked string) error {
	const action = "DescribeInstances"
	for _, m := range out.Members {
		out.VMs[m.ID] = VM{ID: m.ID}
	}
	if tracked != "" {
		out.VMs[tracked] = VM{ID: tracked}
	}
	ids := make([]string, 0, len(out.VMs))
	for id := range out.VMs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for start := 0; start < len(ids); start += pageSize {
		end := start + pageSize
		if end > len(ids) {
			end = len(ids)
		}
		batch := ids[start:end]
		expected := map[string]bool{}
		for _, id := range batch {
			expected[id] = true
		}
		req := cvm.NewDescribeInstancesRequest()
		req.InstanceIds = common.StringPtrs(batch)
		req.Limit = common.Int64Ptr(pageSize)
		resp, err := call(ctx, c, action, func(ctx context.Context) (*cvm.DescribeInstancesResponse, error) {
			return c.cvm.DescribeInstancesWithContext(ctx, req)
		})
		if err != nil {
			return err
		}
		if resp == nil || resp.Response == nil || resp.Response.TotalCount == nil || *resp.Response.TotalCount != int64(len(resp.Response.InstanceSet)) || len(resp.Response.InstanceSet) > len(batch) {
			return bad(action, "IncompleteCVMResponse")
		}
		r := resp.Response
		tr, err := trace(action, r.RequestId)
		if err != nil {
			return err
		}
		out.RequestIDs = append(out.RequestIDs, tr)
		seen := map[string]bool{}
		for _, v := range r.InstanceSet {
			if v == nil || !expected[str(v.InstanceId)] || seen[str(v.InstanceId)] || !statePattern.MatchString(str(v.InstanceState)) || !statePattern.MatchString(str(v.InstanceChargeType)) || v.DisableApiTermination == nil {
				return bad(action, "InvalidCVMResponse")
			}
			seen[*v.InstanceId] = true
			out.VMs[*v.InstanceId] = VM{ID: *v.InstanceId, State: *v.InstanceState, ChargeType: *v.InstanceChargeType, Protected: *v.DisableApiTermination, Returned: true}
		}
	}
	return nil
}

func (c *Client) Activity(ctx context.Context, groupID, activityID string) (out Activity, err error) {
	const action = "DescribeAutoScalingActivities"
	if !resource(groupID, "asg") || !resource(activityID, "asa") {
		return out, bad(action, "InvalidIdentity")
	}
	req := as.NewDescribeAutoScalingActivitiesRequest()
	req.ActivityIds = common.StringPtrs([]string{activityID})
	req.Limit = common.Uint64Ptr(1)
	resp, err := call(ctx, c, action, func(ctx context.Context) (*as.DescribeAutoScalingActivitiesResponse, error) {
		return c.as.DescribeAutoScalingActivitiesWithContext(ctx, req)
	})
	if err != nil {
		return out, err
	}
	if resp == nil || resp.Response == nil || resp.Response.TotalCount == nil || *resp.Response.TotalCount != 1 || len(resp.Response.ActivitySet) != 1 || resp.Response.ActivitySet[0] == nil {
		return out, bad(action, "ActivityMissingOrIncomplete")
	}
	if str(resp.Response.ActivitySet[0].ActivityId) != activityID {
		return out, bad(action, "InvalidActivityResponse")
	}
	tr, err := trace(action, resp.Response.RequestId)
	if err != nil {
		return out, err
	}
	return decodeActivity(resp.Response.ActivitySet[0], groupID, tr)
}

func decodeActivity(a *as.Activity, groupID string, tr RequestTrace) (out Activity, err error) {
	const action = "DescribeAutoScalingActivities"
	if a == nil || !resource(str(a.ActivityId), "asa") || str(a.AutoScalingGroupId) != groupID || !oneOf(str(a.StatusCode), "INIT", "RUNNING", "SUCCESSFUL", "PARTIALLY_SUCCESSFUL", "FAILED", "CANCELLED") {
		return out, bad(action, "InvalidActivityResponse")
	}
	out = Activity{ID: *a.ActivityId, GroupID: groupID, Status: *a.StatusCode, RelatedInstances: []RelatedInstance{}, RequestIDs: []RequestTrace{tr}}
	out.FailureCodes = activityFailureCodes(out.Status, a.DetailedStatusMessageSet)
	// Recovery needs type and time bounds; old activity responses can omit them.
	if a.ActivityType != nil {
		if !statePattern.MatchString(*a.ActivityType) {
			return Activity{}, bad(action, "InvalidActivityType")
		}
		out.Type = *a.ActivityType
	}
	for _, field := range []struct {
		input  *string
		target *string
	}{{a.StartTime, &out.StartedAt}, {a.EndTime, &out.EndedAt}} {
		if field.input != nil && *field.input != "" {
			at, parseErr := time.Parse(time.RFC3339, *field.input)
			if parseErr != nil {
				return Activity{}, bad(action, "InvalidActivityTime")
			}
			*field.target = at.UTC().Format(time.RFC3339)
		}
	}
	seen := map[string]bool{}
	for _, v := range a.RelatedInstanceSet {
		if v == nil || !resource(str(v.InstanceId), "ins") || seen[str(v.InstanceId)] || !oneOf(str(v.InstanceStatus), "INIT", "RUNNING", "SUCCESSFUL", "FAILED") {
			return Activity{}, bad(action, "InvalidActivityInstance")
		}
		seen[*v.InstanceId] = true
		out.RelatedInstances = append(out.RelatedInstances, RelatedInstance{ID: *v.InstanceId, Status: *v.InstanceStatus})
	}
	return out, nil
}
func oneOf(s string, allowed ...string) bool {
	for _, v := range allowed {
		if s == v {
			return true
		}
	}
	return false
}

// SetDesired sends an absolute count and never changes Min/Max. The controller
// clips to the latest group limits; provider range validation remains server-side.
// The caller must enforce expand-only use or its separately authorized contract.
func (c *Client) SetDesired(ctx context.Context, groupID string, count int64) (out WriteResult, err error) {
	const action = "ModifyDesiredCapacity"
	out.Action = action
	if !resource(groupID, "asg") || count < 0 {
		out.NotSubmitted = "client-gate"
		return out, bad(action, "InvalidMutation")
	}
	if err = ctx.Err(); err != nil {
		out.NotSubmitted = "client-gate"
		return out, sanitize(action, err, false)
	}
	out, err = retryRateLimited(ctx, c.timeout, func(ctx context.Context) (WriteResult, error) {
		return c.setDesiredAttempt(ctx, groupID, count)
	}, writeRateLimited)
	if out.Action == "" {
		out.Action = action
		out.NotSubmitted = "client-gate"
		return out, sanitize(action, err, false)
	}
	return out, err
}

func (c *Client) setDesiredAttempt(ctx context.Context, groupID string, count int64) (out WriteResult, err error) {
	defer func() { c.metrics.API("ModifyDesiredCapacity", err != nil, observedThrottle(err)) }()
	const action = "ModifyDesiredCapacity"
	out.Action = action
	req := as.NewModifyDesiredCapacityRequest()
	req.AutoScalingGroupId = common.StringPtr(groupID)
	req.DesiredCapacity = common.Uint64Ptr(uint64(count))
	ctx, submission := traceSubmission(ctx, action)
	out.Attempted = true
	resp, err := c.as.ModifyDesiredCapacityWithContext(ctx, req)
	if err != nil {
		responseID := ""
		if resp != nil && resp.Response != nil {
			responseID = str(resp.Response.RequestId)
		}
		if e := localTransportFailure(submission, action, err, responseID, ""); e != nil {
			out.NotSubmitted = "transport-before-connection"
			return out, e
		}
		e := mutationError(action, err, responseID, "")
		out.Unknown, out.RequestID = e.Unknown, e.RequestID
		return out, e
	}
	if resp == nil || resp.Response == nil || !requestPattern.MatchString(str(resp.Response.RequestId)) {
		out.Unknown = true
		return out, &Error{Action: action, Code: "MutationResponseUnknown", Unknown: true}
	}
	out.RequestID = *resp.Response.RequestId
	return out, nil
}

// RemoveExact removes one explicit ID. Only confirmed throttling rejections
// can be retried; accepted or unknown mutations are never replayed here.
func (c *Client) RemoveExact(ctx context.Context, groupID, instanceID string) (out WriteResult, err error) {
	const action = "RemoveInstances"
	out.Action = action
	if !resource(groupID, "asg") || !resource(instanceID, "ins") {
		out.NotSubmitted = "client-gate"
		return out, bad(action, "InvalidMutation")
	}
	if err = ctx.Err(); err != nil {
		out.NotSubmitted = "client-gate"
		return out, sanitize(action, err, false)
	}
	out, err = retryRateLimited(ctx, c.timeout, func(ctx context.Context) (WriteResult, error) {
		return c.removeExactAttempt(ctx, groupID, instanceID)
	}, writeRateLimited)
	if out.Action == "" {
		out.Action = action
		out.NotSubmitted = "client-gate"
		return out, sanitize(action, err, false)
	}
	return out, err
}

func (c *Client) removeExactAttempt(ctx context.Context, groupID string, instanceID string) (out WriteResult, err error) {
	defer func() { c.metrics.API("RemoveInstances", err != nil, observedThrottle(err)) }()
	const action = "RemoveInstances"
	out.Action = action
	req := as.NewRemoveInstancesRequest()
	req.AutoScalingGroupId = common.StringPtr(groupID)
	req.InstanceIds = common.StringPtrs([]string{instanceID})
	ctx, submission := traceSubmission(ctx, action)
	out.Attempted = true
	resp, err := c.as.RemoveInstancesWithContext(ctx, req)
	// Each validated identifier remains useful for read-only reconciliation,
	// even when the other identifier or the overall SDK response is unknown.
	if resp != nil && resp.Response != nil {
		if requestPattern.MatchString(str(resp.Response.RequestId)) {
			out.RequestID = *resp.Response.RequestId
		}
		if resource(str(resp.Response.ActivityId), "asa") {
			out.ActivityID = *resp.Response.ActivityId
		}
	}
	if err != nil {
		responseID, activityID := "", ""
		if resp != nil && resp.Response != nil {
			responseID, activityID = str(resp.Response.RequestId), str(resp.Response.ActivityId)
		}
		if e := localTransportFailure(submission, action, err, responseID, activityID); e != nil {
			out.NotSubmitted = "transport-before-connection"
			return out, e
		}
		e := mutationError(action, err, responseID, activityID)
		out.Unknown = e.Unknown
		if out.RequestID == "" {
			out.RequestID = e.RequestID
		}
		e.RequestID = out.RequestID
		return out, e
	}
	if out.RequestID == "" || out.ActivityID == "" {
		out.Unknown = true
		return out, &Error{Action: action, Code: "MutationResponseUnknown", RequestID: out.RequestID, Unknown: true}
	}
	return out, nil
}
