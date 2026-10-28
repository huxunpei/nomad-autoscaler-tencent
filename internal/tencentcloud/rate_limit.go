package tencentcloud

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	sdkerrors "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/errors"
)

const rateLimitAttempts = 4 // Includes the initial request, not stabilization polls.

// These public Cloud API codes indicate throttling, not resource/quota failure.
// Do not infer the safety of a new error subcode from its prefix.
func rateLimitCode(code string) bool {
	return oneOf(code, "RequestLimitExceeded", "RequestLimitExceeded.GlobalRegionUinLimitExceeded",
		"RequestLimitExceeded.IPLimitExceeded", "RequestLimitExceeded.UinLimitExceeded")
}

func readRateLimited(err error) bool {
	var e *sdkerrors.TencentCloudSDKError
	return errors.As(err, &e) && e != nil && rateLimitCode(e.GetCode()) &&
		!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}

func observedThrottle(err error) bool {
	var e *Error
	return errors.As(err, &e) && e != nil && rateLimitCode(e.Code)
}

func writeRateLimited(result WriteResult, err error) bool {
	var e *Error
	return IsRejectedWrite(result, err) && errors.As(err, &e) && rateLimitCode(e.Code)
}

// A logical API call, including its retries and waits, shares one timeout.
// Each attempt also receives a deadline and SDK-internal retries stay disabled.
// Stop with the last confirmed rejection when cancellation happens BETWEEN
// attempts: no later request was sent, so inventing an unknown result would
// strand a node that can safely have its own drain restored. Once another
// attempt starts, only that attempt's result can describe the write outcome.
func retryRateLimited[T any](ctx context.Context, timeout time.Duration, fn func(context.Context) (T, error), retry func(T, error) bool) (value T, err error) {
	budget, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err = budget.Err(); err != nil {
		return value, err
	}
	for attempt := 0; attempt < rateLimitAttempts; attempt++ {
		if stop := budget.Err(); stop != nil {
			if err != nil {
				return value, err
			}
			return value, stop
		}
		reqCtx, finish := context.WithTimeout(budget, timeout)
		value, err = fn(reqCtx)
		finish()
		if err == nil || !retry(value, err) || attempt == rateLimitAttempts-1 {
			return value, err
		}
		// Equal jitter: [250,500) ms, [500,1000) ms, [1000,2000) ms.
		// The lower bound avoids immediate replays; jitter separates controllers.
		ceiling := 500 * time.Millisecond * time.Duration(1<<attempt)
		delay := ceiling/2 + time.Duration(rand.Int64N(int64(ceiling/2)))
		timer := time.NewTimer(delay)
		select {
		case <-budget.Done():
			timer.Stop()
			return value, err
		case <-timer.C:
		}
	}
	return value, err
}
