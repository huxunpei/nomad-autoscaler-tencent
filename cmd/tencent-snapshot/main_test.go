package main

import (
	"bytes"
	"context"
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

// Fake interfaces make every test offline; no SDK client is used by collection tests.
type fakeCloud struct {
	t                  *testing.T
	count              int
	memberCalls        int
	cvmCalls           int
	activityCalls      int
	duplicate          bool
	drifting           bool
	missingActivity    bool
	wrongActivityGroup bool
}

func (f *fakeCloud) checkDeadline(ctx context.Context) {
	f.t.Helper()
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > 30*time.Second || time.Until(deadline) <= 0 {
		f.t.Fatal("API call has no bounded per-request context")
	}
}

func (f *fakeCloud) DescribeAutoScalingGroupsWithContext(ctx context.Context, req *as.DescribeAutoScalingGroupsRequest) (*as.DescribeAutoScalingGroupsResponse, error) {
	f.checkDeadline(ctx)
	if len(req.AutoScalingGroupIds) != 1 || req.Filters != nil {
		f.t.Fatal("group query is not explicitly scoped")
	}
	return &as.DescribeAutoScalingGroupsResponse{Response: &as.DescribeAutoScalingGroupsResponseParams{
		AutoScalingGroupSet: []*as.AutoScalingGroup{{AutoScalingGroupId: common.StringPtr("asg-test"), InstanceCount: common.Int64Ptr(int64(f.count)), DesiredCapacity: common.Int64Ptr(int64(f.count))}},
		RequestId:           common.StringPtr("group-request"),
	}}, nil
}

func (f *fakeCloud) DescribeAutoScalingInstancesWithContext(ctx context.Context, req *as.DescribeAutoScalingInstancesRequest) (*as.DescribeAutoScalingInstancesResponse, error) {
	f.checkDeadline(ctx)
	f.memberCalls++
	if req.InstanceIds != nil || len(req.Filters) != 1 || *req.Filters[0].Values[0] != "asg-test" || *req.Limit != pageSize {
		f.t.Fatal("member query is not scoped or fully paginated")
	}
	r := &as.DescribeAutoScalingInstancesResponseParams{TotalCount: common.Uint64Ptr(uint64(f.count)), RequestId: common.StringPtr("members-request")}
	for i := int(*req.Offset); i < min(int(*req.Offset)+pageSize, f.count); i++ {
		id := i
		if f.duplicate && f.memberCalls == 2 {
			id = 0
		}
		r.AutoScalingInstanceSet = append(r.AutoScalingInstanceSet, &as.Instance{
			InstanceId: common.StringPtr(fmt.Sprintf("ins-%03d", id)), AutoScalingGroupId: common.StringPtr("asg-test"),
			LifeCycleState: common.StringPtr("CREATING"), CreationType: common.StringPtr("AUTO_CREATION"),
		})
	}
	if f.drifting && f.memberCalls == 2 {
		r.TotalCount = common.Uint64Ptr(uint64(f.count + 1))
	}
	return &as.DescribeAutoScalingInstancesResponse{Response: r}, nil
}

func (f *fakeCloud) DescribeAutoScalingActivitiesWithContext(ctx context.Context, req *as.DescribeAutoScalingActivitiesRequest) (*as.DescribeAutoScalingActivitiesResponse, error) {
	f.checkDeadline(ctx)
	f.activityCalls++
	if len(req.ActivityIds) != 0 {
		if req.Filters != nil || len(req.ActivityIds) != 1 || *req.ActivityIds[0] != "asa-single" {
			f.t.Fatal("single activity query mixed ActivityIds and Filters")
		}
		r := &as.DescribeAutoScalingActivitiesResponseParams{TotalCount: common.Uint64Ptr(0), RequestId: common.StringPtr("single-activity-request")}
		if !f.missingActivity {
			group := "asg-test"
			if f.wrongActivityGroup {
				group = "asg-other"
			}
			r.TotalCount = common.Uint64Ptr(1)
			r.ActivitySet = []*as.Activity{{ActivityId: common.StringPtr("asa-single"), AutoScalingGroupId: common.StringPtr(group), StatusCode: common.StringPtr("PARTIALLY_SUCCESSFUL"), EndTime: common.StringPtr("2026-10-15T10:00:00Z")}}
		}
		return &as.DescribeAutoScalingActivitiesResponse{Response: r}, nil
	}
	if req.ActivityIds != nil || len(req.Filters) != 2 || *req.Filters[0].Values[0] != "asg-test" || len(req.Filters[1].Values) != 2 {
		f.t.Fatal("activity query mixed ActivityIds/Filters or omitted active-state/group filter")
	}
	r := &as.DescribeAutoScalingActivitiesResponseParams{TotalCount: common.Uint64Ptr(101), RequestId: common.StringPtr("activities-request")}
	for i := int(*req.Offset); i < min(int(*req.Offset)+pageSize, 101); i++ {
		r.ActivitySet = append(r.ActivitySet, &as.Activity{
			ActivityId: common.StringPtr(fmt.Sprintf("asa-%03d", i)), AutoScalingGroupId: common.StringPtr("asg-test"), StatusCode: common.StringPtr("RUNNING"),
		})
	}
	return &as.DescribeAutoScalingActivitiesResponse{Response: r}, nil
}

func (f *fakeCloud) DescribeInstancesWithContext(ctx context.Context, req *cvm.DescribeInstancesRequest) (*cvm.DescribeInstancesResponse, error) {
	f.checkDeadline(ctx)
	f.cvmCalls++
	if len(req.InstanceIds) > pageSize || req.Filters != nil || *req.Limit != pageSize {
		f.t.Fatal("CVM request is not ID-scoped and bounded")
	}
	r := &cvm.DescribeInstancesResponseParams{RequestId: common.StringPtr("cvm-request")}
	for _, id := range req.InstanceIds {
		if *id == "ins-100" { // Missing is ambiguous, never interpreted as deletion.
			continue
		}
		r.InstanceSet = append(r.InstanceSet, &cvm.Instance{InstanceId: id, InstanceState: common.StringPtr("RUNNING")})
	}
	r.TotalCount = common.Int64Ptr(int64(len(r.InstanceSet)))
	return &cvm.DescribeInstancesResponse{Response: r}, nil
}

func TestSnapshotPaginatesAndKeepsBootingMembers(t *testing.T) {
	fake := &fakeCloud{t: t, count: 101}
	c := collector{as: fake, cvm: fake, timeout: 30 * time.Second}
	out, err := c.collect(context.Background(), "ap-guangzhou", "asg-test")
	if err != nil {
		t.Fatal(err)
	}
	if out.ActualMemberCount != 101 || fake.memberCalls != 2 || fake.cvmCalls != 2 || fake.activityCalls != 2 || len(out.Activities) != 101 {
		t.Fatalf("incomplete pagination: count=%d calls=%d/%d/%d", out.ActualMemberCount, fake.memberCalls, fake.cvmCalls, fake.activityCalls)
	}
	if out.Members[100].CVMReturned || out.Members[100].CVMState != nil || *out.Members[0].Lifecycle != "CREATING" {
		t.Fatal("missing CVM or booting member was misclassified")
	}
	if !out.GroupFieldsEqual || len(out.Requests) != 8 {
		t.Fatal("missing group bracketing or request traces")
	}
}

func TestSpecificActivityIncludesTerminalStateAndRejectsMissingOrOtherGroup(t *testing.T) {
	for _, tc := range []struct {
		name       string
		missing    bool
		otherGroup bool
	}{
		{name: "terminal"}, {name: "missing", missing: true}, {name: "other-group", otherGroup: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeCloud{t: t, missingActivity: tc.missing, wrongActivityGroup: tc.otherGroup}
			c := collector{as: fake, timeout: 30 * time.Second, activityID: "asa-single"}
			out, err := c.activities(context.Background(), "asg-test")
			if tc.missing || tc.otherGroup {
				if err == nil {
					t.Fatal("unavailable or cross-group activity accepted")
				}
				return
			}
			if err != nil || len(out) != 1 || *out[0].Status != "PARTIALLY_SUCCESSFUL" || out[0].EndedAt == nil {
				t.Fatal("terminal activity or end time missing")
			}
		})
	}
}

func TestChangingOrDuplicateMemberPagesFail(t *testing.T) {
	for _, tc := range []struct {
		name string
		dup  bool
	}{
		{"duplicate", true}, {"changed-total", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeCloud{t: t, count: 101, duplicate: tc.dup, drifting: !tc.dup}
			c := collector{as: fake, cvm: fake, timeout: 30 * time.Second}
			if _, err := c.members(context.Background(), "asg-test"); err == nil {
				t.Fatal("unstable pagination accepted as complete")
			}
		})
	}
}

func TestEmptyGroupDoesNotIssueAnUnscopedCVMQuery(t *testing.T) {
	fake := &fakeCloud{t: t}
	c := collector{as: fake, cvm: fake, timeout: 30 * time.Second}
	out, err := c.collect(context.Background(), "ap-guangzhou", "asg-test")
	if err != nil || out.ActualMemberCount != 0 || fake.cvmCalls != 0 {
		t.Fatalf("empty group caused CVM query: error=%v calls=%d", err, fake.cvmCalls)
	}
}

func TestErrorOutputDoesNotContainMessage(t *testing.T) {
	marker := "private-signature-never-print"
	err := sdkerrors.NewTencentCloudSDKError("AuthFailure.SignatureFailure", marker, "request-123")
	safe := safeError("DescribeInstances", err)
	if safe.Code != "AuthFailure.SignatureFailure" || safe.RequestID != "request-123" || strings.Contains(safe.Error(), marker) {
		t.Fatal("SDK error metadata sanitization failed")
	}
	if strings.Contains(safeError("DescribeInstances", errors.New(marker)).Error(), marker) {
		t.Fatal("raw transport error leaked")
	}
}

func TestCLIValidationNeverLoadsCredentialsForHelp(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"-h"}, func(string) string { t.Fatal("help read credentials"); return "" }, &stdout, &stderr)
	if code != 0 || !strings.Contains(stdout.String(), "Only Describe") || stderr.Len() != 0 {
		t.Fatal("help failed")
	}
	stdout.Reset()
	stderr.Reset()
	code = run(context.Background(), []string{"-region", "ap-guangzhou", "-group-id", "asg-test"}, func(string) string { return "" }, &stdout, &stderr)
	if code != 1 || !strings.Contains(stderr.String(), "MissingExplicitCredentials") || stdout.Len() != 0 {
		t.Fatal("missing explicit credentials did not fail before API calls")
	}
}

func TestCallTimeoutDoesNotExposeRawError(t *testing.T) {
	_, err := call(context.Background(), time.Millisecond, "DescribeInstances", func(ctx context.Context) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	})
	var safe *probeError
	if !errors.As(err, &safe) || safe.Code != "RequestTimeout" {
		t.Fatal("request deadline was not classified")
	}
}
