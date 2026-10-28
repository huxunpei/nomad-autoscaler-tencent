// tencent-snapshot only calls Describe APIs. It never changes cloud or Nomad state.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"os"
	"os/signal"
	"reflect"
	"regexp"
	"sort"
	"syscall"
	"time"

	as "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/as/v20180419"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
	sdkerrors "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/errors"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/profile"
	cvm "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/cvm/v20170312"
)

const pageSize = 100

type asReader interface {
	DescribeAutoScalingGroupsWithContext(context.Context, *as.DescribeAutoScalingGroupsRequest) (*as.DescribeAutoScalingGroupsResponse, error)
	DescribeAutoScalingInstancesWithContext(context.Context, *as.DescribeAutoScalingInstancesRequest) (*as.DescribeAutoScalingInstancesResponse, error)
	DescribeAutoScalingActivitiesWithContext(context.Context, *as.DescribeAutoScalingActivitiesRequest) (*as.DescribeAutoScalingActivitiesResponse, error)
}

type cvmReader interface {
	DescribeInstancesWithContext(context.Context, *cvm.DescribeInstancesRequest) (*cvm.DescribeInstancesResponse, error)
}

type groupSnapshot struct {
	ID                 *string `json:"id"`
	DesiredCapacity    *int64  `json:"desired_capacity"`
	InstanceCount      *int64  `json:"instance_count"`
	InServiceCount     *int64  `json:"in_service_count"`
	MinSize            *int64  `json:"min_size"`
	MaxSize            *int64  `json:"max_size"`
	EnabledStatus      *string `json:"enabled_status"`
	InActivityStatus   *string `json:"in_activity_status"`
	RetryPolicy        *string `json:"retry_policy"`
	AllocationPolicy   *string `json:"allocation_policy"`
	ConcurrentScaleOut *bool   `json:"concurrent_scale_out"`
}

type instanceSnapshot struct {
	ID               string  `json:"id"`
	Lifecycle        *string `json:"as_lifecycle"`
	Health           *string `json:"as_health"`
	CreationType     *string `json:"creation_type"`
	Protected        *bool   `json:"protected_from_scale_in"`
	CVMState         *string `json:"cvm_state"`
	CVMReturned      bool    `json:"cvm_returned"`
	ChargeType       *string `json:"charge_type"`
	DeleteProtection *bool   `json:"cvm_delete_protection"`
}

type relatedInstance struct {
	ID     *string `json:"id"`
	Status *string `json:"status"`
}

type activitySnapshot struct {
	ID        *string           `json:"id"`
	Type      *string           `json:"type"`
	Status    *string           `json:"status"`
	StartedAt *string           `json:"started_at"`
	EndedAt   *string           `json:"ended_at"`
	Instances []relatedInstance `json:"instances"`
}

type requestTrace struct {
	Action    string  `json:"action"`
	RequestID *string `json:"request_id"`
}

type snapshot struct {
	Region            string             `json:"region"`
	StartedAt         time.Time          `json:"started_at"`
	FinishedAt        time.Time          `json:"finished_at"`
	GroupBefore       groupSnapshot      `json:"group_before"`
	GroupAfter        groupSnapshot      `json:"group_after"`
	GroupFieldsEqual  bool               `json:"group_fields_equal"`
	ActualMemberCount int                `json:"actual_member_count"`
	Members           []instanceSnapshot `json:"members"`
	Activities        []activitySnapshot `json:"activities"`
	RequestedActivity string             `json:"requested_activity_id,omitempty"`
	Requests          []requestTrace     `json:"requests"`
	Notes             []string           `json:"notes"`
}

// Only fixed messages and validated SDK codes/RequestIds may leave the process.
// In particular, do not print SDK Error(), HTTP bodies, or request objects.
type probeError struct {
	Action    string `json:"action,omitempty"`
	Code      string `json:"code"`
	RequestID string `json:"request_id,omitempty"`
}

func (e *probeError) Error() string { return e.Code }

var safeIdentifier = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,128}$`)

func safeError(action string, err error) *probeError {
	out := &probeError{Action: action, Code: "RequestFailed"}
	var cloudErr *sdkerrors.TencentCloudSDKError
	if errors.As(err, &cloudErr) {
		if code := cloudErr.GetCode(); safeIdentifier.MatchString(code) {
			out.Code = code
		}
		if id := cloudErr.GetRequestId(); safeIdentifier.MatchString(id) {
			out.RequestID = id
		}
	} else if errors.Is(err, context.DeadlineExceeded) {
		out.Code = "RequestTimeout"
	} else if errors.Is(err, context.Canceled) {
		out.Code = "RequestCanceled"
	}
	return out
}

func invalid(code string) error { return &probeError{Code: code} }

type collector struct {
	as         asReader
	cvm        cvmReader
	timeout    time.Duration
	activityID string
	traces     []requestTrace
}

func call[T any](ctx context.Context, timeout time.Duration, action string, fn func(context.Context) (T, error)) (T, error) {
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	value, err := fn(reqCtx)
	if err != nil {
		return value, safeError(action, err)
	}
	return value, nil
}

func (c *collector) group(ctx context.Context, groupID string) (groupSnapshot, error) {
	req := as.NewDescribeAutoScalingGroupsRequest()
	req.AutoScalingGroupIds = common.StringPtrs([]string{groupID})
	req.Limit = common.Uint64Ptr(1)
	resp, err := call(ctx, c.timeout, "DescribeAutoScalingGroups", func(ctx context.Context) (*as.DescribeAutoScalingGroupsResponse, error) {
		return c.as.DescribeAutoScalingGroupsWithContext(ctx, req)
	})
	if err != nil {
		return groupSnapshot{}, err
	}
	if resp == nil || resp.Response == nil || len(resp.Response.AutoScalingGroupSet) != 1 || resp.Response.AutoScalingGroupSet[0] == nil {
		return groupSnapshot{}, invalid("GroupMissingOrUnexpectedResponse")
	}
	r := resp.Response
	c.traces = append(c.traces, requestTrace{"DescribeAutoScalingGroups", r.RequestId})
	g := r.AutoScalingGroupSet[0]
	if g.AutoScalingGroupId == nil || *g.AutoScalingGroupId != groupID {
		return groupSnapshot{}, invalid("UnexpectedGroup")
	}
	return groupSnapshot{g.AutoScalingGroupId, g.DesiredCapacity, g.InstanceCount, g.InServiceInstanceCount, g.MinSize, g.MaxSize, g.EnabledStatus, g.InActivityStatus, g.RetryPolicy, g.InstanceAllocationPolicy, g.ConcurrentScaleOutForDesiredCapacity}, nil
}

func (c *collector) members(ctx context.Context, groupID string) ([]instanceSnapshot, error) {
	out := make([]instanceSnapshot, 0)
	seen := make(map[string]bool)
	var expected *uint64
	for offset := int64(0); ; {
		req := as.NewDescribeAutoScalingInstancesRequest()
		req.Filters = []*as.Filter{{Name: common.StringPtr("auto-scaling-group-id"), Values: common.StringPtrs([]string{groupID})}}
		req.Offset, req.Limit = common.Int64Ptr(offset), common.Int64Ptr(pageSize)
		resp, err := call(ctx, c.timeout, "DescribeAutoScalingInstances", func(ctx context.Context) (*as.DescribeAutoScalingInstancesResponse, error) {
			return c.as.DescribeAutoScalingInstancesWithContext(ctx, req)
		})
		if err != nil {
			return nil, err
		}
		if resp == nil || resp.Response == nil || resp.Response.TotalCount == nil {
			return nil, invalid("IncompleteMemberResponse")
		}
		r := resp.Response
		c.traces = append(c.traces, requestTrace{"DescribeAutoScalingInstances", r.RequestId})
		if expected != nil && *expected != *r.TotalCount {
			return nil, invalid("MembersChangedDuringPagination")
		}
		expected = r.TotalCount
		for _, member := range r.AutoScalingInstanceSet {
			if member == nil || member.InstanceId == nil || *member.InstanceId == "" || member.AutoScalingGroupId == nil || *member.AutoScalingGroupId != groupID || seen[*member.InstanceId] {
				return nil, invalid("InvalidOrDuplicateMember")
			}
			seen[*member.InstanceId] = true
			out = append(out, instanceSnapshot{ID: *member.InstanceId, Lifecycle: member.LifeCycleState, Health: member.HealthStatus, CreationType: member.CreationType, Protected: member.ProtectedFromScaleIn})
		}
		offset += int64(len(r.AutoScalingInstanceSet))
		if uint64(offset) == *expected {
			break
		}
		if len(r.AutoScalingInstanceSet) == 0 || uint64(offset) > *expected {
			return nil, invalid("IncompleteMemberPagination")
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (c *collector) activities(ctx context.Context, groupID string) ([]activitySnapshot, error) {
	out := make([]activitySnapshot, 0)
	seen := make(map[string]bool)
	var expected *uint64
	for offset := uint64(0); ; {
		req := as.NewDescribeAutoScalingActivitiesRequest()
		// ActivityIds and Filters are mutually exclusive in Tencent AS.
		if c.activityID != "" {
			req.ActivityIds = common.StringPtrs([]string{c.activityID})
		} else {
			req.Filters = []*as.Filter{
				{Name: common.StringPtr("auto-scaling-group-id"), Values: common.StringPtrs([]string{groupID})},
				{Name: common.StringPtr("activity-status-code"), Values: common.StringPtrs([]string{"INIT", "RUNNING"})},
			}
		}
		req.Offset, req.Limit = common.Uint64Ptr(offset), common.Uint64Ptr(pageSize)
		resp, err := call(ctx, c.timeout, "DescribeAutoScalingActivities", func(ctx context.Context) (*as.DescribeAutoScalingActivitiesResponse, error) {
			return c.as.DescribeAutoScalingActivitiesWithContext(ctx, req)
		})
		if err != nil {
			return nil, err
		}
		if resp == nil || resp.Response == nil || resp.Response.TotalCount == nil {
			return nil, invalid("IncompleteActivityResponse")
		}
		r := resp.Response
		c.traces = append(c.traces, requestTrace{"DescribeAutoScalingActivities", r.RequestId})
		if expected != nil && *expected != *r.TotalCount {
			return nil, invalid("ActivitiesChangedDuringPagination")
		}
		expected = r.TotalCount
		for _, activity := range r.ActivitySet {
			if activity == nil || activity.ActivityId == nil || *activity.ActivityId == "" || activity.AutoScalingGroupId == nil || *activity.AutoScalingGroupId != groupID || seen[*activity.ActivityId] {
				return nil, invalid("InvalidOrDuplicateActivity")
			}
			if c.activityID != "" && *activity.ActivityId != c.activityID {
				return nil, invalid("UnexpectedActivity")
			}
			seen[*activity.ActivityId] = true
			item := activitySnapshot{ID: activity.ActivityId, Type: activity.ActivityType, Status: activity.StatusCode, StartedAt: activity.StartTime, EndedAt: activity.EndTime, Instances: make([]relatedInstance, 0)}
			for _, related := range activity.RelatedInstanceSet {
				if related != nil {
					item.Instances = append(item.Instances, relatedInstance{related.InstanceId, related.InstanceStatus})
				}
			}
			out = append(out, item)
		}
		offset += uint64(len(r.ActivitySet))
		if offset == *expected {
			break
		}
		if len(r.ActivitySet) == 0 || offset > *expected {
			return nil, invalid("IncompleteActivityPagination")
		}
	}
	if c.activityID != "" && len(out) != 1 {
		return nil, invalid("RequestedActivityMissing")
	}
	return out, nil
}

func (c *collector) cvmStates(ctx context.Context, members []instanceSnapshot) error {
	for start := 0; start < len(members); start += pageSize {
		end := min(start+pageSize, len(members))
		ids := make([]string, 0, end-start)
		indices := make(map[string]int)
		for i := start; i < end; i++ {
			ids = append(ids, members[i].ID)
			indices[members[i].ID] = i
		}
		req := cvm.NewDescribeInstancesRequest()
		req.InstanceIds, req.Limit = common.StringPtrs(ids), common.Int64Ptr(pageSize)
		resp, err := call(ctx, c.timeout, "DescribeInstances", func(ctx context.Context) (*cvm.DescribeInstancesResponse, error) {
			return c.cvm.DescribeInstancesWithContext(ctx, req)
		})
		if err != nil {
			return err
		}
		if resp == nil || resp.Response == nil || resp.Response.TotalCount == nil || *resp.Response.TotalCount != int64(len(resp.Response.InstanceSet)) {
			return invalid("IncompleteCVMResponse")
		}
		c.traces = append(c.traces, requestTrace{"DescribeInstances", resp.Response.RequestId})
		seen := make(map[string]bool)
		for _, instance := range resp.Response.InstanceSet {
			if instance == nil || instance.InstanceId == nil {
				return invalid("InvalidCVMInstance")
			}
			idx, ok := indices[*instance.InstanceId]
			if !ok || seen[*instance.InstanceId] {
				return invalid("UnexpectedOrDuplicateCVMInstance")
			}
			seen[*instance.InstanceId] = true
			members[idx].CVMReturned = true
			members[idx].CVMState = instance.InstanceState
			members[idx].ChargeType = instance.InstanceChargeType
			members[idx].DeleteProtection = instance.DisableApiTermination
		}
	}
	return nil
}

func (c *collector) collect(ctx context.Context, region, groupID string) (*snapshot, error) {
	out := &snapshot{Region: region, StartedAt: time.Now().UTC(), Notes: []string{
		"Sequential read-only observations, not an atomic cloud snapshot or Ready verdict.",
		"cvm_returned=false is ambiguous and does not prove VM deletion.",
		"No Nomad state was queried; this cannot prove P1/P2/P3.",
	}}
	var err error
	if out.GroupBefore, err = c.group(ctx, groupID); err != nil {
		return nil, err
	}
	if out.Members, err = c.members(ctx, groupID); err != nil {
		return nil, err
	}
	out.RequestedActivity = c.activityID
	if out.Activities, err = c.activities(ctx, groupID); err != nil {
		return nil, err
	}
	if err = c.cvmStates(ctx, out.Members); err != nil {
		return nil, err
	}
	if out.GroupAfter, err = c.group(ctx, groupID); err != nil {
		return nil, err
	}
	out.FinishedAt, out.ActualMemberCount = time.Now().UTC(), len(out.Members)
	out.GroupFieldsEqual = reflect.DeepEqual(out.GroupBefore, out.GroupAfter)
	out.Requests = c.traces
	return out, nil
}

func run(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("tencent-snapshot", flag.ContinueOnError)
	flags.SetOutput(io.Discard) // flag errors can echo arbitrary argument values.
	region := flags.String("region", "", "Tencent Cloud region")
	groupID := flags.String("group-id", "", "AS group ID")
	activityID := flags.String("activity-id", "", "optional AS activity ID, including completed activities")
	timeout := flags.Duration("request-timeout", 30*time.Second, "timeout for each Describe API request")
	fail := func(err error) int {
		var safe *probeError
		if !errors.As(err, &safe) {
			safe = &probeError{Code: "InternalError"}
		}
		_ = json.NewEncoder(stderr).Encode(safe)
		return 1
	}
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			_, _ = io.WriteString(stdout, "Usage: tencent-snapshot -region REGION -group-id ASG_ID [-activity-id ASA_ID] [-request-timeout 30s]\nCredentials: TENCENT_AS_SECRET_ID, TENCENT_AS_SECRET_KEY, optional TENCENT_AS_TOKEN.\nOnly Describe APIs are called. No cloud mutations or Nomad calls.\n")
			return 0
		}
		return fail(invalid("InvalidArguments"))
	}
	if !regexp.MustCompile(`^[a-z][a-z0-9-]{1,63}$`).MatchString(*region) || !regexp.MustCompile(`^asg-[A-Za-z0-9]+$`).MatchString(*groupID) || flags.NArg() != 0 || *timeout <= 0 || *timeout > 5*time.Minute {
		return fail(invalid("InvalidArguments"))
	}
	if *activityID != "" && !regexp.MustCompile(`^asa-[A-Za-z0-9]+$`).MatchString(*activityID) {
		return fail(invalid("InvalidArguments"))
	}
	secretID, secretKey := getenv("TENCENT_AS_SECRET_ID"), getenv("TENCENT_AS_SECRET_KEY")
	if secretID == "" || secretKey == "" {
		return fail(invalid("MissingExplicitCredentials"))
	}
	credential := common.NewTokenCredential(secretID, secretKey, getenv("TENCENT_AS_TOKEN"))
	clientProfile := profile.NewClientProfile()
	clientProfile.Debug = false
	clientProfile.HttpProfile.ReqTimeout = int((*timeout + time.Second - 1) / time.Second)
	asClient, err := as.NewClient(credential, *region, clientProfile)
	if err != nil {
		return fail(safeError("NewASClient", err))
	}
	cvmClient, err := cvm.NewClient(credential, *region, clientProfile)
	if err != nil {
		return fail(safeError("NewCVMClient", err))
	}
	c := collector{as: asClient, cvm: cvmClient, timeout: *timeout, activityID: *activityID}
	out, err := c.collect(ctx, *region, *groupID)
	if err != nil {
		return fail(err)
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(out); err != nil {
		return fail(invalid("OutputFailed"))
	}
	return 0
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	os.Exit(run(ctx, os.Args[1:], os.Getenv, os.Stdout, os.Stderr))
}
