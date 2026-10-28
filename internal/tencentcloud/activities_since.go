package tencentcloud

import (
	"context"
	as "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/as/v20180419"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
	"time"
)

// ActivitiesSince reads a complete, bounded group history from a fixed lower
// bound. It does not filter by status or EndTime: doing so could hide in-flight
// activity. Empty history is not proof that a mutation was never submitted.
func (c *Client) ActivitiesSince(ctx context.Context, group string, since time.Time) ([]Activity, error) {
	const action = "DescribeAutoScalingActivities"
	if !resource(group, "asg") || since.IsZero() || since.Year() < 1970 || since.After(time.Now()) {
		return nil, bad(action, "InvalidHistoryScope")
	}
	since = since.UTC().Truncate(time.Second)
	out := []Activity{}
	seen := map[string]bool{}
	var total uint64
	for page := 0; page < maxPages; page++ {
		if err := ctx.Err(); err != nil {
			return nil, sanitize(action, err, false)
		}
		req := as.NewDescribeAutoScalingActivitiesRequest()
		req.Filters = []*as.Filter{{Name: common.StringPtr("auto-scaling-group-id"), Values: common.StringPtrs([]string{group})}}
		req.StartTime = common.StringPtr(since.Format(time.RFC3339))
		req.Limit = common.Uint64Ptr(pageSize)
		req.Offset = common.Uint64Ptr(uint64(len(out)))
		resp, err := call(ctx, c, action, func(ctx context.Context) (*as.DescribeAutoScalingActivitiesResponse, error) {
			return c.as.DescribeAutoScalingActivitiesWithContext(ctx, req)
		})
		if err != nil {
			return nil, err
		}
		if resp == nil || resp.Response == nil || resp.Response.TotalCount == nil {
			return nil, bad(action, "ActivityHistoryIncomplete")
		}
		r := resp.Response
		if page == 0 {
			total = *r.TotalCount
		}
		if total > pageSize*maxPages || total != *r.TotalCount || len(r.ActivitySet) > pageSize || uint64(len(out)+len(r.ActivitySet)) > total {
			return nil, bad(action, "ActivityHistoryChanged")
		}
		tr, err := trace(action, r.RequestId)
		if err != nil {
			return nil, err
		}
		for _, item := range r.ActivitySet {
			a, err := decodeActivity(item, group, tr)
			if err != nil {
				return nil, err
			}
			start, err := time.Parse(time.RFC3339, a.StartedAt)
			if err != nil || start.Before(since) || seen[a.ID] {
				return nil, bad(action, "InvalidActivityHistory")
			}
			if a.EndedAt != "" {
				end, _ := time.Parse(time.RFC3339, a.EndedAt)
				if end.Before(start) {
					return nil, bad(action, "InvalidActivityTime")
				}
			}
			seen[a.ID] = true
			out = append(out, a)
		}
		if uint64(len(out)) == total {
			return out, nil
		}
		if len(r.ActivitySet) != pageSize {
			return nil, bad(action, "ActivityHistoryIncomplete")
		}
	}
	return nil, bad(action, "ActivityHistoryLimitExceeded")
}
