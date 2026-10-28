package tencentcloud

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	as "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/as/v20180419"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
	sdkerrors "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/errors"
)

// This is a protocol candidate model, not Tencent's service implementation.
// Its partial completion follows the published final-Desired contract. The
// delayed request model must be excluded by a service guarantee before a
// client may treat an empty observation as proof that a request was not sent.
type scaleOutCandidateWire struct {
	mu                                      sync.Mutex
	count, desired                          int64
	active                                  bool
	partialFirst, blockFirst, contradictory bool
	calls                                   []scaleOutCandidateCall
	activity                                *as.Activity
	initial, pending                        int64
	arrived, release, finished              chan struct{}
	releaseOnce                             sync.Once
	server                                  *httptest.Server
}
type scaleOutCandidateCall struct {
	Action         string
	Number, Target int64
	Fields         map[string]json.RawMessage
}

func newScaleOutCandidateWire(t *testing.T) *scaleOutCandidateWire {
	t.Helper()
	f := &scaleOutCandidateWire{count: 1, desired: 1, arrived: make(chan struct{}), release: make(chan struct{}), finished: make(chan struct{})}
	f.server = httptest.NewServer(http.HandlerFunc(f.serveHTTP))
	t.Cleanup(func() { f.releaseOnce.Do(func() { close(f.release) }); f.server.Close() })
	return f
}
func (f *scaleOutCandidateWire) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.Header.Get("X-TC-Version") != "2018-04-19" || r.Header.Get("Authorization") == "" {
		http.Error(w, "invalid synthetic SDK envelope", http.StatusBadRequest)
		return
	}
	var fields map[string]json.RawMessage
	if json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&fields) != nil {
		http.Error(w, "invalid JSON", 400)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	response := func(v any) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"Response": v})
	}
	stamp := time.Now().UTC().Format(time.RFC3339)
	switch action := r.Header.Get("X-TC-Action"); action {
	case "DescribeAutoScalingGroups":
		status := "NOT_IN_ACTIVITY"
		if f.active {
			status = "IN_ACTIVITY"
		}
		response(map[string]any{"RequestId": requestID, "TotalCount": 1, "AutoScalingGroupSet": []map[string]any{{"AutoScalingGroupId": "asg-candidate", "MinSize": 0, "MaxSize": 10, "DesiredCapacity": f.desired, "InstanceCount": f.count, "InActivityStatus": status, "EnabledStatus": "ENABLED", "AutoScalingGroupStatus": "NORMAL"}}})
	case "DescribeAutoScalingActivities":
		response(map[string]any{"RequestId": requestID, "TotalCount": 1, "ActivitySet": []*as.Activity{f.activity}})
	case "ScaleOutInstances", "ModifyDesiredCapacity":
		var group string
		var number, target int64
		_ = json.Unmarshal(fields["AutoScalingGroupId"], &group)
		_ = json.Unmarshal(fields["ScaleOutNumber"], &number)
		_ = json.Unmarshal(fields["DesiredCapacity"], &target)
		if group != "asg-candidate" {
			http.Error(w, "unscoped request", 400)
			return
		}
		f.calls = append(f.calls, scaleOutCandidateCall{action, number, target, fields})
		ordinal := len(f.calls)
		if action == "ScaleOutInstances" && ordinal == 1 && f.blockFirst {
			close(f.arrived)
			f.mu.Unlock()
			<-f.release // Deliberately model a server continuing after client cancellation.
			f.mu.Lock()
			defer close(f.finished)
		}
		if f.contradictory && action == "ScaleOutInstances" {
			response(map[string]any{"ActivityId": "asa-conflict", "RequestId": requestID, "Error": map[string]string{"Code": "RequestLimitExceeded", "Message": "synthetic contradiction"}})
			return
		}
		if action == "ModifyDesiredCapacity" {
			if target < f.count || target > 10 {
				http.Error(w, "unexpected target", 400)
				return
			}
			f.count, f.desired = target, target
			response(map[string]any{"RequestId": requestID})
			return
		}
		if f.active || number < 1 || f.desired+number > 10 {
			http.Error(w, "synthetic capacity guard", 400)
			return
		}
		id := fmt.Sprintf("asa-wire%d", ordinal)
		f.activity = &as.Activity{ActivityId: common.StringPtr(id), AutoScalingGroupId: common.StringPtr(group), ActivityType: common.StringPtr("SCALE_OUT"), StatusCode: common.StringPtr("SUCCESSFUL"), StartTime: common.StringPtr(stamp), EndTime: common.StringPtr(stamp)}
		if ordinal == 1 && f.partialFirst {
			f.initial, f.pending = f.count, number
			f.desired += number
			f.active = true
			f.activity.StatusCode = common.StringPtr("RUNNING")
			f.activity.EndTime = nil
		} else {
			f.count += number
			f.desired = f.count
		}
		response(map[string]any{"RequestId": requestID, "ActivityId": id})
	default:
		http.Error(w, "unexpected action", 400)
	}
}
func (f *scaleOutCandidateWire) completePartial(successes int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.active || successes < 0 || successes >= f.pending {
		panic("invalid partial fixture")
	}
	f.count = f.initial + successes
	f.desired = f.count
	f.active = false
	f.activity.StatusCode = common.StringPtr("PARTIALLY_SUCCESSFUL")
	f.activity.EndTime = common.StringPtr(time.Now().UTC().Format(time.RFC3339))
	for i := int64(0); i < successes; i++ {
		f.activity.RelatedInstanceSet = append(f.activity.RelatedInstanceSet, &as.RelatedInstance{InstanceId: common.StringPtr(fmt.Sprintf("ins-success%d", i)), InstanceStatus: common.StringPtr("SUCCESSFUL")})
	}
}
func (f *scaleOutCandidateWire) counts() (int64, int64, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.count, f.desired, len(f.calls)
}

func scaleOutCandidateClient(t *testing.T, f *scaleOutCandidateWire, productionGuard bool) *as.Client {
	t.Helper()
	p := clientProfile(2 * time.Second)
	p.HttpProfile.Endpoint = strings.TrimPrefix(f.server.URL, "http://")
	p.HttpProfile.Scheme = "HTTP"
	c, err := as.NewClient(common.NewCredential("AKID_SYNTHETIC_CANDIDATE", "synthetic-key-for-loopback-only"), "ap-guangzhou", p)
	if err != nil {
		t.Fatal("cannot construct synthetic SDK client")
	}
	if productionGuard {
		c.WithHttpTransport(&mutationResponseTransport{base: f.server.Client().Transport})
	}
	return c
}
func scaleOutCandidateGroup(ctx context.Context, c *as.Client) (*as.AutoScalingGroup, error) {
	req := as.NewDescribeAutoScalingGroupsRequest()
	req.AutoScalingGroupIds = common.StringPtrs([]string{"asg-candidate"})
	res, err := c.DescribeAutoScalingGroupsWithContext(ctx, req)
	if err != nil {
		return nil, err
	}
	if res == nil || res.Response == nil || len(res.Response.AutoScalingGroupSet) != 1 {
		return nil, errors.New("incomplete synthetic group")
	}
	return res.Response.AutoScalingGroupSet[0], nil
}

// Candidate read-then-delta algorithm only. This is not production dispatch.
func scaleOutCandidateAbsoluteOnce(ctx context.Context, c *as.Client, target int64) (*as.ScaleOutInstancesResponse, error) {
	g, err := scaleOutCandidateGroup(ctx, c)
	if err != nil {
		return nil, err
	}
	if g.DesiredCapacity == nil || g.InstanceCount == nil || g.MinSize == nil || g.MaxSize == nil || *g.DesiredCapacity != *g.InstanceCount || str(g.InActivityStatus) != "NOT_IN_ACTIVITY" || target <= *g.DesiredCapacity || target > *g.MaxSize || target < *g.MinSize {
		return nil, errors.New("candidate requires a stable growth baseline")
	}
	req := as.NewScaleOutInstancesRequest()
	req.AutoScalingGroupId = common.StringPtr("asg-candidate")
	req.ScaleOutNumber = common.Uint64Ptr(uint64(target - *g.DesiredCapacity))
	return c.ScaleOutInstancesWithContext(ctx, req)
}

func TestScaleOutCandidateSDKActivityAndPartialContractModel(t *testing.T) {
	f := newScaleOutCandidateWire(t)
	f.partialFirst = true
	c := scaleOutCandidateClient(t, f, false)
	r, err := scaleOutCandidateAbsoluteOnce(context.Background(), c, 5)
	if err != nil || r == nil || r.Response == nil || str(r.Response.ActivityId) != "asa-wire1" || str(r.Response.RequestId) != requestID {
		t.Fatal("SDK did not return the exact synthetic activity")
	}
	f.mu.Lock()
	call := f.calls[0]
	f.mu.Unlock()
	if call.Action != "ScaleOutInstances" || call.Number != 4 || len(call.Fields) != 2 || call.Fields["ClientToken"] != nil || call.Fields["DesiredCapacity"] != nil || call.Fields["MaxSize"] != nil {
		t.Fatal("unexpected SDK request schema")
	}
	g, err := scaleOutCandidateGroup(context.Background(), c)
	if err != nil || *g.InstanceCount != 1 || *g.DesiredCapacity != 5 || str(g.InActivityStatus) != "IN_ACTIVITY" {
		t.Fatal("intermediate model observation lost")
	}
	f.completePartial(2)
	g, err = scaleOutCandidateGroup(context.Background(), c)
	if err != nil || *g.InstanceCount != 3 || *g.DesiredCapacity != 3 || str(g.InActivityStatus) != "NOT_IN_ACTIVITY" {
		t.Fatal("published partial contract model not exposed")
	}
	aReq := as.NewDescribeAutoScalingActivitiesRequest()
	aReq.ActivityIds = common.StringPtrs([]string{*r.Response.ActivityId})
	a, err := c.DescribeAutoScalingActivitiesWithContext(context.Background(), aReq)
	if err != nil || a == nil || a.Response == nil || len(a.Response.ActivitySet) != 1 || str(a.Response.ActivitySet[0].ActivityId) != "asa-wire1" || str(a.Response.ActivitySet[0].StatusCode) != "PARTIALLY_SUCCESSFUL" || len(a.Response.ActivitySet[0].RelatedInstanceSet) != 2 {
		t.Fatal("partial activity identity lost through actual SDK")
	}
	if _, err = scaleOutCandidateAbsoluteOnce(context.Background(), c, 5); err != nil {
		t.Fatal("next modeled evaluation cannot reach original goal")
	}
	count, desired, calls := f.counts()
	if count != 5 || desired != 5 || calls != 2 {
		t.Fatal("modeled retry did not use remaining capacity")
	}
	f.mu.Lock()
	number := f.calls[1].Number
	f.mu.Unlock()
	if number != 2 {
		t.Fatal("candidate replayed original delta instead of recomputing it")
	}
}

func TestScaleOutCandidateDelayedOriginalDefeatsFreshReadAndRetry(t *testing.T) {
	for _, mode := range []string{"same-delta-after-fresh-read", "absolute-fallback"} {
		t.Run(mode, func(t *testing.T) {
			f := newScaleOutCandidateWire(t)
			f.blockFirst = true
			c := scaleOutCandidateClient(t, f, false)
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			r, err := scaleOutCandidateAbsoluteOnce(ctx, c, 5)
			cancel()
			if err == nil || (r != nil && r.Response != nil && str(r.Response.ActivityId) != "") {
				t.Fatal("delayed response unexpectedly known")
			}
			select {
			case <-f.arrived:
			default:
				t.Fatal("first request did not reach modeled server")
			}
			count, desired, calls := f.counts()
			if count != 1 || desired != 1 || calls != 1 {
				t.Fatal("SDK retried implicitly or model did not preserve empty observation")
			}
			if mode == "same-delta-after-fresh-read" {
				if _, err = scaleOutCandidateAbsoluteOnce(context.Background(), c, 5); err != nil {
					t.Fatal("second candidate request failed")
				}
			} else {
				req := as.NewModifyDesiredCapacityRequest()
				req.AutoScalingGroupId = common.StringPtr("asg-candidate")
				req.DesiredCapacity = common.Uint64Ptr(5)
				if _, err = c.ModifyDesiredCapacityWithContext(context.Background(), req); err != nil {
					t.Fatal("synthetic absolute fallback failed")
				}
			}
			count, desired, calls = f.counts()
			if count != 5 || desired != 5 || calls != 2 {
				t.Fatal("second attempt did not appear to settle target")
			}
			f.releaseOnce.Do(func() { close(f.release) })
			select {
			case <-f.finished:
			case <-time.After(3 * time.Second):
				t.Fatal("late original never completed")
			}
			count, desired, calls = f.counts()
			if count != 9 || desired != 9 || calls != 2 {
				t.Fatal("candidate counterexample did not overshoot target within provider Max=10")
			}
			t.Log("model counterexample: fresh idle observation at 1, retry appears settled at 5, late original ends at 9; no real cloud call")
		})
	}
}

func TestScaleOutCandidateConflictingEnvelopeCannotBecomeConfirmedRejection(t *testing.T) {
	f := newScaleOutCandidateWire(t)
	f.contradictory = true
	c := scaleOutCandidateClient(t, f, true)
	r, err := scaleOutCandidateAbsoluteOnce(context.Background(), c, 5)
	if err == nil {
		t.Fatal("synthetic conflicting error should not succeed")
	}
	var provider *sdkerrors.TencentCloudSDKError
	if errors.As(err, &provider) && provider.GetCode() == "RequestLimitExceeded" {
		t.Fatal("conflicting raw ActivityId was discarded and became a confirmed throttling rejection")
	}
	if r != nil && r.Response != nil && str(r.Response.ActivityId) != "" {
		t.Fatal("SDK unexpectedly retained the activity in its error response; reassess this compatibility finding")
	}
	_, _, calls := f.counts()
	if calls != 1 {
		t.Fatal("configured SDK retried a rejected new action")
	}
	t.Log("conflicting raw activity/error remains an uncertain transport outcome, not a confirmed rejection")
}
