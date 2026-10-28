package tencentcloud

import (
	"context"
	as "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/as/v20180419"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
	"testing"
)

func TestRecoveryActivityTimeAndTypeWhitelist(t *testing.T) {
	for _, name := range []string{"valid", "missing-times", "invalid-start", "invalid-type"} {
		t.Run(name, func(t *testing.T) {
			c, a, _ := fixture(t)
			a.activity = func(context.Context, *as.DescribeAutoScalingActivitiesRequest) (*as.DescribeAutoScalingActivitiesResponse, error) {
				item := &as.Activity{ActivityId: common.StringPtr("asa-test"), AutoScalingGroupId: common.StringPtr("asg-test"), ActivityType: common.StringPtr("REMOVE_INSTANCES"), StartTime: common.StringPtr("2026-10-16T08:35:07Z"), EndTime: common.StringPtr("2026-10-16T08:35:26Z"), StatusCode: common.StringPtr("SUCCESSFUL"), RelatedInstanceSet: []*as.RelatedInstance{{InstanceId: common.StringPtr("ins-one"), InstanceStatus: common.StringPtr("SUCCESSFUL")}}}
				switch name {
				case "missing-times":
					item.StartTime = nil
					item.EndTime = nil
				case "invalid-start":
					item.StartTime = common.StringPtr(secretMarker)
				case "invalid-type":
					item.ActivityType = common.StringPtr(secretMarker)
				}
				return &as.DescribeAutoScalingActivitiesResponse{Response: &as.DescribeAutoScalingActivitiesResponseParams{TotalCount: common.Uint64Ptr(1), RequestId: common.StringPtr(requestID), ActivitySet: []*as.Activity{item}}}, nil
			}
			got, err := c.Activity(context.Background(), "asg-test", "asa-test")
			if name == "invalid-start" || name == "invalid-type" {
				if err == nil {
					t.Fatal("invalid metadata")
				}
				return
			}
			if err != nil || got.Type != "REMOVE_INSTANCES" {
				t.Fatal(got, err)
			}
			if name == "valid" && (got.StartedAt != "2026-10-16T08:35:07Z" || got.EndedAt != "2026-10-16T08:35:26Z") {
				t.Fatal(got)
			}
		})
	}
}
