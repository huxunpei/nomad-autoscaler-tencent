package tencentcloud

import (
	"context"
	as "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/as/v20180419"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
)

type scheduledActionsAPI interface {
	DescribeScheduledActionsWithContext(context.Context, *as.DescribeScheduledActionsRequest) (*as.DescribeScheduledActionsResponse, error)
}

func (c *Client) scheduledActions(ctx context.Context, out *Snapshot) error {
	api, ok := c.as.(scheduledActionsAPI)
	if !ok {
		return bad("DescribeScheduledActions", "ScheduledActionsUnavailable")
	}
	// Only existence/count is needed for an advisory warning. The server's
	// filtered TotalCount covers all pages; no full schedule inventory is kept.
	const action = "DescribeScheduledActions"
	req := as.NewDescribeScheduledActionsRequest()
	req.Filters = []*as.Filter{{Name: common.StringPtr("auto-scaling-group-id"), Values: common.StringPtrs([]string{out.Group.ID})}}
	req.Limit = common.Uint64Ptr(1)
	resp, err := call(ctx, c, action, func(ctx context.Context) (*as.DescribeScheduledActionsResponse, error) {
		return api.DescribeScheduledActionsWithContext(ctx, req)
	})
	if err != nil {
		return err
	}
	if resp == nil || resp.Response == nil || resp.Response.TotalCount == nil || *resp.Response.TotalCount > uint64(^uint(0)>>1) {
		return bad(action, "InvalidScheduledActionsResponse")
	}
	r := resp.Response
	tr, err := trace(action, r.RequestId)
	if err != nil {
		return err
	}
	for _, a := range r.ScheduledActionSet {
		if a == nil || str(a.AutoScalingGroupId) != out.Group.ID {
			return bad(action, "InvalidScheduledActionsResponse")
		}
	}
	out.ScheduledActionCount = int(*r.TotalCount)
	out.RequestIDs = append(out.RequestIDs, tr)
	return nil
}
