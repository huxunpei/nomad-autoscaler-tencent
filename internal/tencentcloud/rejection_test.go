package tencentcloud

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	as "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/as/v20180419"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
	sdkerrors "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestMutationRejectionClassification(t *testing.T) {
	for _, action := range []string{"ModifyDesiredCapacity", "RemoveInstances"} {
		for _, tc := range []struct {
			name, code, request string
			known               bool
		}{
			{"authorization", "AuthFailure.UnauthorizedOperation", requestID, true},
			{"expired-token", "AuthFailure.TokenFailure", requestID, true},
			{"rate-limit", "RequestLimitExceeded", requestID, true},
			{"invalid-group", "InvalidParameterValue.InvalidAutoScalingGroupId", requestID, true},
			{"internal", "InternalError", requestID, false},
			{"generic-failure", "FailedOperation", requestID, false},
			{"capacity", "ResourcesSoldOut", requestID, false},
			{"unknown-subcode", "AuthFailure.FutureUnknownCode", requestID, false},
			{"no-request-id", "AuthFailure.UnauthorizedOperation", "", false},
			{"bad-request-id", "AuthFailure.UnauthorizedOperation", secretMarker, false},
		} {
			t.Run(action+"/"+tc.name, func(t *testing.T) {
				c, a, _ := fixture(t)
				if tc.code == "RequestLimitExceeded" {
					c.timeout = 10 * time.Millisecond
				}
				rejected := fmt.Errorf("wrapped: %w", sdkerrors.NewTencentCloudSDKError(tc.code, secretMarker, tc.request))
				a.desired = func(context.Context, *as.ModifyDesiredCapacityRequest) (*as.ModifyDesiredCapacityResponse, error) {
					return nil, rejected
				}
				a.remove = func(context.Context, *as.RemoveInstancesRequest) (*as.RemoveInstancesResponse, error) {
					return nil, rejected
				}
				var r WriteResult
				var err error
				if action == "RemoveInstances" {
					r, err = c.RemoveExact(context.Background(), "asg-test", "ins-one")
				} else {
					r, err = c.SetDesired(context.Background(), "asg-test", 2)
				}
				var e *Error
				if !errors.As(err, &e) || !r.Attempted || r.Unknown == tc.known || e.Unknown == tc.known || a.desiredCalls+a.removeCalls != 1 {
					t.Fatalf("classification=%+v %v known=%v", r, err, tc.known)
				}
				b, _ := json.Marshal([]any{r, e})
				if strings.Contains(string(b), secretMarker) {
					t.Fatal("raw error leaked")
				}
			})
		}
	}
}

func TestMutationRejectionConflictingEvidenceStaysUnknown(t *testing.T) {
	for _, name := range []string{"activity", "invalid-activity", "request-conflict", "joined-timeout", "joined-cancel", "other-action-code"} {
		t.Run(name, func(t *testing.T) {
			c, a, _ := fixture(t)
			rid, activity := requestID, ""
			var err error = sdkerrors.NewTencentCloudSDKError("AuthFailure.UnauthorizedOperation", secretMarker, requestID)
			switch name {
			case "activity":
				activity = "asa-test"
			case "invalid-activity":
				activity = secretMarker
			case "request-conflict":
				rid = "aaaaaaaa-2222-3333-4444-555555555555"
			case "joined-timeout":
				err = errors.Join(err, context.DeadlineExceeded)
			case "joined-cancel":
				err = errors.Join(err, context.Canceled)
			case "other-action-code":
				err = sdkerrors.NewTencentCloudSDKError("InvalidParameterValue.Size", secretMarker, requestID)
			}
			a.remove = func(context.Context, *as.RemoveInstancesRequest) (*as.RemoveInstancesResponse, error) {
				return &as.RemoveInstancesResponse{Response: &as.RemoveInstancesResponseParams{RequestId: common.StringPtr(rid), ActivityId: common.StringPtr(activity)}}, err
			}
			r, e := c.RemoveExact(context.Background(), "asg-test", "ins-one")
			if e == nil || !r.Unknown || a.removeCalls != 1 {
				t.Fatalf("conflict classified as rejection: %+v %v", r, e)
			}
		})
	}
}

func TestMutationRejectionThroughActualSDKHTTP(t *testing.T) {
	for _, action := range []string{"ModifyDesiredCapacity", "RemoveInstances"} {
		t.Run(action, func(t *testing.T) {
			calls := 0
			c, err := newClient(Config{Region: "ap-chengdu", SecretID: "synthetic-id", SecretKey: "synthetic-key", Timeout: time.Second}, authHTTPFunc(func(r *http.Request) (*http.Response, error) {
				calls++
				if r.Header.Get("X-TC-Action") != action {
					t.Fatal("wrong action")
				}
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"Response":{"Error":{"Code":"AuthFailure.UnauthorizedOperation","Message":"` + secretMarker + `"},"RequestId":"` + requestID + `"}}`))}, nil
			}))
			if err != nil {
				t.Fatal(err)
			}
			var r WriteResult
			if action == "RemoveInstances" {
				r, err = c.RemoveExact(context.Background(), "asg-test", "ins-one")
			} else {
				r, err = c.SetDesired(context.Background(), "asg-test", 2)
			}
			if err == nil || r.Unknown || !r.Attempted || r.RequestID != requestID || calls != 1 {
				t.Fatalf("SDK rejection: %+v %v calls=%d", r, err, calls)
			}
		})
	}
}

func TestRejectedWriteRequiresExactConsistentEvidence(t *testing.T) {
	for _, mode := range []string{"valid", "not-attempted", "result-unknown", "error-unknown", "missing-request", "different-request", "activity", "wrong-action", "no-error", "string-error", "wrong-code"} {
		t.Run(mode, func(t *testing.T) {
			r := WriteResult{Action: "RemoveInstances", RequestID: requestID, Attempted: true}
			e := &Error{Action: r.Action, RequestID: requestID, Code: "AuthFailure.UnauthorizedOperation"}
			var err error = e
			switch mode {
			case "not-attempted":
				r.Attempted = false
			case "result-unknown":
				r.Unknown = true
			case "error-unknown":
				e.Unknown = true
			case "missing-request":
				r.RequestID = ""
			case "different-request":
				e.RequestID = "aaaaaaaa-2222-3333-4444-555555555555"
			case "activity":
				r.ActivityID = "asa-test"
			case "wrong-action":
				e.Action = "ModifyDesiredCapacity"
			case "no-error":
				err = nil
			case "string-error":
				err = errors.New("AuthFailure.UnauthorizedOperation")
			case "wrong-code":
				e.Code = "InvalidParameterValue.Size"
			}
			if got := IsRejectedWrite(r, err); got != (mode == "valid") {
				t.Fatal("inconsistent rejection proof", mode, got)
			}
		})
	}
}
