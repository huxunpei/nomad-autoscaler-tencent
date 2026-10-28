package tencentcloud

import (
	"context"
	"fmt"
	as "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/as/v20180419"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
	"testing"
	"time"
)

type activityHistory interface {
	ActivitiesSince(context.Context, string, time.Time) ([]Activity, error)
}

func TestActivityHistoryPaginationAndValidation(t *testing.T) {
	for _, mode := range []string{"valid", "empty", "duplicate", "total-change", "short-page", "wrong-group", "missing-start", "old-start", "invalid-end", "bad-request-id", "nil-item", "over-limit"} {
		t.Run(mode, func(t *testing.T) {
			c, a, _ := fixture(t)
			reader, ok := any(c).(activityHistory)
			if !ok {
				t.Fatal("cloud activity discovery missing")
			}
			since := time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC)
			calls := 0
			a.activity = func(ctx context.Context, r *as.DescribeAutoScalingActivitiesRequest) (*as.DescribeAutoScalingActivitiesResponse, error) {
				calls++
				if len(r.ActivityIds) != 0 || len(r.Filters) != 1 || str(r.Filters[0].Name) != "auto-scaling-group-id" || len(r.Filters[0].Values) != 1 || str(r.Filters[0].Values[0]) != "asg-test" || str(r.StartTime) != since.Format(time.RFC3339) || r.EndTime != nil || r.Limit == nil || *r.Limit != 100 || r.Offset == nil || *r.Offset != uint64((calls-1)*100) {
					t.Fatal("history query scope/pagination incorrect")
				}
				if _, ok := ctx.Deadline(); !ok {
					t.Fatal("unbounded request")
				}
				total := uint64(101)
				n := 100
				if calls == 2 {
					n = 1
				}
				if mode == "empty" {
					total = 0
					n = 0
				}
				if mode == "total-change" && calls == 2 {
					total = 102
				}
				if mode == "short-page" {
					n = 0
				}
				if mode == "over-limit" {
					total = 100001
				}
				items := []*as.Activity{}
				for i := 0; i < n; i++ {
					items = append(items, &as.Activity{ActivityId: common.StringPtr(fmt.Sprintf("asa-x%d", (calls-1)*100+i)), AutoScalingGroupId: common.StringPtr("asg-test"), ActivityType: common.StringPtr("REMOVE_INSTANCES"), StartTime: common.StringPtr(since.Format(time.RFC3339)), EndTime: common.StringPtr(since.Add(time.Second).Format(time.RFC3339)), StatusCode: common.StringPtr("SUCCESSFUL"), RelatedInstanceSet: []*as.RelatedInstance{{InstanceId: common.StringPtr("ins-one"), InstanceStatus: common.StringPtr("SUCCESSFUL")}}})
				}
				reqID := requestID
				if len(items) > 0 {
					switch mode {
					case "duplicate":
						if calls == 2 {
							items[0].ActivityId = common.StringPtr("asa-x0")
						}
					case "wrong-group":
						items[0].AutoScalingGroupId = common.StringPtr("asg-other")
					case "missing-start":
						items[0].StartTime = nil
					case "old-start":
						items[0].StartTime = common.StringPtr(since.Add(-time.Second).Format(time.RFC3339))
					case "invalid-end":
						items[0].EndTime = common.StringPtr(secretMarker)
					case "bad-request-id":
						reqID = secretMarker
					case "nil-item":
						items[0] = nil
					}
				}
				return &as.DescribeAutoScalingActivitiesResponse{Response: &as.DescribeAutoScalingActivitiesResponseParams{TotalCount: &total, RequestId: &reqID, ActivitySet: items}}, nil
			}
			got, err := reader.ActivitiesSince(context.Background(), "asg-test", since)
			if mode == "valid" {
				if err != nil || len(got) != 101 || calls != 2 || len(got[100].RequestIDs) != 1 {
					t.Fatal(len(got), calls, err)
				}
			} else if mode == "empty" {
				if err != nil || len(got) != 0 {
					t.Fatal(got, err)
				}
			} else if err == nil || len(got) != 0 {
				t.Fatal("incomplete history accepted", len(got), err)
			}
		})
	}
}

var _ activityHistory = (*Authenticated)(nil)

func TestActivityHistoryInvalidScopeAndCancellation(t *testing.T) {
	c, a, _ := fixture(t)
	calls := 0
	a.activity = func(context.Context, *as.DescribeAutoScalingActivitiesRequest) (*as.DescribeAutoScalingActivitiesResponse, error) {
		calls++
		return nil, nil
	}
	for _, scope := range []struct {
		group string
		since time.Time
	}{{"asg-test", time.Time{}}, {"wrong", time.Now()}, {"asg-test", time.Now().Add(time.Hour)}} {
		if _, err := c.ActivitiesSince(context.Background(), scope.group, scope.since); err == nil {
			t.Fatal("invalid scope accepted")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.ActivitiesSince(ctx, "asg-test", time.Now().Add(-time.Minute)); err == nil {
		t.Fatal("cancel ignored")
	}
	if calls != 0 {
		t.Fatal("invalid scope/cancellation called provider")
	}
}
