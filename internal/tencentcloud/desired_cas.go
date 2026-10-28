package tencentcloud

import (
	"context"

	as "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/as/v20180419"
	tchttp "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/http"
)

type commonSender interface {
	Send(tchttp.Request, tchttp.Response) error
}

// SetDesiredCAS writes an absolute target using the caller's durable baseline.
// SDK v1.3.173 does not expose AssertOriginDesiredCapacity yet. Keep the normal
// SDK signing, timeout, response validation and submission tracking via Send.
// Unknown outcomes are reconciled by the controller, never retried here.
func (c *Client) SetDesiredCAS(ctx context.Context, group string, target, origin int64) (out WriteResult, err error) {
	const action = "ModifyDesiredCapacity"
	out.Action = action
	api, ok := c.as.(commonSender)
	if !ok || !resource(group, "asg") || target < 0 || origin < 0 {
		out.NotSubmitted = "client-gate"
		return out, bad(action, "InvalidMutation")
	}
	if err = ctx.Err(); err != nil {
		out.NotSubmitted = "client-gate"
		return out, sanitize(action, err, false)
	}
	out, err = retryRateLimited(ctx, c.timeout, func(ctx context.Context) (out WriteResult, err error) {
		defer func() { c.metrics.API(action, err != nil, observedThrottle(err)) }()
		out.Action = action
		req := tchttp.NewCommonRequest("as", "2018-04-19", action)
		if req.SetActionParameters(map[string]interface{}{
			"AutoScalingGroupId": group, "DesiredCapacity": target,
			"AssertOriginDesiredCapacity": origin,
		}) != nil {
			out.NotSubmitted = "client-gate"
			return out, bad(action, "InvalidMutation")
		}
		ctx, submission := traceSubmission(ctx, action)
		req.SetContext(ctx)
		resp := as.NewModifyDesiredCapacityResponse()
		out.Attempted = true
		err = api.Send(req, resp)
		rid := ""
		if resp.Response != nil {
			rid = str(resp.Response.RequestId)
		}
		if err != nil {
			if e := localTransportFailure(submission, action, err, rid, ""); e != nil {
				out.NotSubmitted = "transport-before-connection"
				return out, e
			}
			e := mutationError(action, err, rid, "")
			out.Unknown, out.RequestID = e.Unknown, e.RequestID
			return out, e
		}
		if !requestPattern.MatchString(rid) {
			out.Unknown = true
			return out, &Error{Action: action, Code: "MutationResponseUnknown", Unknown: true}
		}
		out.RequestID = rid
		return out, nil
	}, writeRateLimited)
	if out.Action == "" {
		out.Action, out.NotSubmitted = action, "client-gate"
		return out, sanitize(action, err, false)
	}
	return out, err
}

func (c *Authenticated) SetDesiredCAS(ctx context.Context, group string, target, origin int64) (WriteResult, error) {
	ctx, cancel := c.linkedContext(ctx)
	defer cancel()
	client, err := c.authClient(ctx)
	if err != nil {
		return WriteResult{Action: "ModifyDesiredCapacity", NotSubmitted: "credential-gate"}, err
	}
	return client.SetDesiredCAS(ctx, group, target, origin)
}
