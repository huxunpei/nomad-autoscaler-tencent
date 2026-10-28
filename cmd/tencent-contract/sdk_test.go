package main

import (
	"context"
	"testing"
	"time"

	as "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/as/v20180419"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
)

// Only the two write methods are implemented; no network client is constructed.
type fakeWriteAS struct {
	asAPI
	t                 *testing.T
	expandResponse    *as.ModifyDesiredCapacityResponse
	removeResponse    *as.RemoveInstancesResponse
	expands, removals int
}

func (f *fakeWriteAS) ModifyDesiredCapacityWithContext(ctx context.Context, req *as.ModifyDesiredCapacityRequest) (*as.ModifyDesiredCapacityResponse, error) {
	f.expands++
	if stringValue(req.AutoScalingGroupId) != "asg-test" || req.DesiredCapacity == nil || *req.DesiredCapacity != 1 || req.MinSize != nil || req.MaxSize != nil {
		f.t.Fatal("expanded with unexpected group, capacity or boundary changes")
	}
	return f.expandResponse, nil
}
func (f *fakeWriteAS) RemoveInstancesWithContext(ctx context.Context, req *as.RemoveInstancesRequest) (*as.RemoveInstancesResponse, error) {
	f.removals++
	if stringValue(req.AutoScalingGroupId) != "asg-test" || len(req.InstanceIds) != 1 || stringValue(req.InstanceIds[0]) != "ins-only-new" {
		f.t.Fatal("removal did not contain exactly the confirmed ID")
	}
	return f.removeResponse, nil
}

func TestSDKExpandRequiresRequestID(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response *as.ModifyDesiredCapacityResponse
		valid    bool
	}{
		{name: "nil-response"},
		{name: "nil-body", response: &as.ModifyDesiredCapacityResponse{}},
		{name: "nil-request-id", response: &as.ModifyDesiredCapacityResponse{Response: &as.ModifyDesiredCapacityResponseParams{}}},
		{name: "empty-request-id", response: &as.ModifyDesiredCapacityResponse{Response: &as.ModifyDesiredCapacityResponseParams{RequestId: common.StringPtr("")}}},
		{name: "complete", response: &as.ModifyDesiredCapacityResponse{Response: &as.ModifyDesiredCapacityResponseParams{RequestId: common.StringPtr("request-expand")}}, valid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeWriteAS{t: t, expandResponse: tc.response}
			c := sdkCloud{as: fake, timeout: time.Second}
			_, err := c.ExpandOne(context.Background(), "asg-test")
			if tc.valid && err != nil {
				t.Fatal(err)
			}
			if !tc.valid && (err == nil || err.Error() != "ModifyDesiredResponseUnknown") {
				t.Fatal("missing RequestId accepted")
			}
			if fake.expands != 1 {
				t.Fatal("write was replayed")
			}
		})
	}
}

func TestSDKRemoveRequiresRequestIDAndActivityID(t *testing.T) {
	for _, tc := range []struct {
		name, requestID, activityID string
		valid                       bool
	}{
		{name: "missing-both"},
		{name: "missing-request", activityID: "asa-test"},
		{name: "missing-activity", requestID: "request-remove"},
		{name: "complete", requestID: "request-remove", activityID: "asa-test", valid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeWriteAS{t: t, removeResponse: &as.RemoveInstancesResponse{Response: &as.RemoveInstancesResponseParams{RequestId: common.StringPtr(tc.requestID), ActivityId: common.StringPtr(tc.activityID)}}}
			c := sdkCloud{as: fake, timeout: time.Second}
			_, err := c.RemoveExact(context.Background(), "asg-test", "ins-only-new")
			if tc.valid && err != nil {
				t.Fatal(err)
			}
			if !tc.valid && (err == nil || err.Error() != "RemoveResponseUnknown") {
				t.Fatal("missing write response ID accepted")
			}
			if fake.removals != 1 {
				t.Fatal("write was replayed")
			}
		})
	}
}
