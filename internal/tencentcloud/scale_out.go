package tencentcloud

import (
	"context"

	as "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/as/v20180419"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
)

type scaleOutAPI interface {
	ScaleOutInstancesWithContext(context.Context, *as.ScaleOutInstancesRequest) (*as.ScaleOutInstancesResponse, error)
}

// ScaleOut sends exactly one increment. The controller must durably record its
// stable baseline, absolute target and increment before calling this method.
// Only proven throttling rejections can be retried; unknown increments cannot.
func (c *Client) ScaleOut(ctx context.Context, group string, number int64) (out WriteResult, err error) {
	const action = "ScaleOutInstances"
	out.Action = action
	api, ok := c.as.(scaleOutAPI)
	if !ok || !resource(group, "asg") || number < 1 || number > 2000 {
		out.NotSubmitted = "client-gate"
		return out, bad(action, "InvalidMutation")
	}
	if err = ctx.Err(); err != nil {
		out.NotSubmitted = "client-gate"
		return out, sanitize(action, err, false)
	}
	out, err = retryRateLimited(ctx, c.timeout, func(ctx context.Context) (out WriteResult, err error) {
		defer func() { c.metrics.API(action, err != nil, observedThrottle(err)) }()
		out.Action, out.Attempted = action, true
		req := as.NewScaleOutInstancesRequest()
		req.AutoScalingGroupId = common.StringPtr(group)
		req.ScaleOutNumber = common.Uint64Ptr(uint64(number))
		ctx, submission := traceSubmission(ctx, action)
		resp, err := api.ScaleOutInstancesWithContext(ctx, req)
		rid, aid := "", ""
		if resp != nil && resp.Response != nil {
			rid, aid = str(resp.Response.RequestId), str(resp.Response.ActivityId)
		}
		if requestPattern.MatchString(rid) {
			out.RequestID = rid
		}
		if resource(aid, "asa") {
			out.ActivityID = aid
		}
		if err != nil {
			if e := localTransportFailure(submission, action, err, rid, aid); e != nil {
				out.NotSubmitted = "transport-before-connection"
				return out, e
			}
			e := mutationError(action, err, rid, aid)
			if out.RequestID == "" {
				out.RequestID = e.RequestID
			}
			e.RequestID, out.Unknown = out.RequestID, e.Unknown
			return out, e
		}
		if out.RequestID == "" || out.ActivityID == "" {
			out.Unknown = true
			return out, &Error{Action: action, Code: "MutationResponseUnknown", RequestID: out.RequestID, Unknown: true}
		}
		return out, nil
	}, writeRateLimited)
	if out.Action == "" {
		out.Action, out.NotSubmitted = action, "client-gate"
		return out, sanitize(action, err, false)
	}
	return out, err
}

func (c *Authenticated) ScaleOut(ctx context.Context, group string, number int64) (WriteResult, error) {
	ctx, cancel := c.linkedContext(ctx)
	defer cancel()
	client, err := c.authClient(ctx)
	if err != nil {
		return WriteResult{Action: "ScaleOutInstances", NotSubmitted: "credential-gate"}, err
	}
	return client.ScaleOut(ctx, group, number)
}
