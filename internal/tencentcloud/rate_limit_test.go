package tencentcloud

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	as "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/as/v20180419"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
	sdkerrors "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/errors"
)

func rateBody(code, id string) string {
	b, _ := json.Marshal(map[string]any{"Response": map[string]any{"Error": map[string]string{"Code": code, "Message": secretMarker}, "RequestId": id}})
	return string(b)
}

func rateSuccess(action string) string {
	bodies := map[string]string{
		"DescribeAutoScalingGroups":     `"TotalCount":1,"AutoScalingGroupSet":[{"AutoScalingGroupId":"asg-test","MinSize":0,"MaxSize":5,"DesiredCapacity":1,"InstanceCount":1,"EnabledStatus":"ENABLED","InActivityStatus":"NOT_IN_ACTIVITY","AutoScalingGroupStatus":"NORMAL","LaunchConfigurationId":"asc-test","RetryPolicy":"IMMEDIATE_RETRY"}]`,
		"DescribeAutoScalingInstances":  `"TotalCount":1,"AutoScalingInstanceSet":[{"InstanceId":"ins-one","AutoScalingGroupId":"asg-test","CreationType":"AUTO_CREATION","LifeCycleState":"IN_SERVICE","ProtectedFromScaleIn":false,"AddTime":"2026-10-15T01:00:00Z"}]`,
		"DescribeLaunchConfigurations":  `"TotalCount":1,"LaunchConfigurationSet":[{"LaunchConfigurationId":"asc-test","LaunchConfigurationStatus":"NORMAL","InstanceType":"S5.SMALL2","InstanceChargeType":"POSTPAID_BY_HOUR","ImageId":"img-test","SecurityGroupIds":["sg-test"],"VersionNumber":1}]`,
		"DescribeScalingPolicies":       `"TotalCount":0,"ScalingPolicySet":[]`,
		"DescribeInstances":             `"TotalCount":1,"InstanceSet":[{"InstanceId":"ins-one","InstanceState":"RUNNING","InstanceChargeType":"POSTPAID_BY_HOUR","DisableApiTermination":false}]`,
		"DescribeAutoScalingActivities": `"TotalCount":1,"ActivitySet":[{"ActivityId":"asa-test","AutoScalingGroupId":"asg-test","StatusCode":"SUCCESSFUL","ActivityType":"REMOVE","StartTime":"2026-10-15T01:00:00Z","EndTime":"2026-10-15T01:00:01Z","RelatedInstanceSet":[{"InstanceId":"ins-one","InstanceStatus":"SUCCESSFUL"}]}]`,
		"RemoveInstances":               `"ActivityId":"asa-test"`,
		"ScaleOutInstances":             `"ActivityId":"asa-test"`,
	}
	extra := bodies[action]
	if extra != "" {
		extra = "," + extra
	}
	return `{"Response":{"RequestId":"` + requestID + `"` + extra + `}}`
}

func rateClient(t *testing.T, timeout time.Duration, transport authHTTPFunc) *Client {
	t.Helper()
	c, err := newClient(Config{Region: "ap-chengdu", SecretID: "synthetic-id", SecretKey: "synthetic-key", Timeout: timeout}, transport)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func rateInvoke(t *testing.T, ctx context.Context, c *Client, action string, history bool) (WriteResult, error) {
	t.Helper()
	switch action {
	case "ModifyDesiredCapacity":
		return c.SetDesired(ctx, "asg-test", 2)
	case "ScaleOutInstances":
		return c.ScaleOut(ctx, "asg-test", 2)
	case "RemoveInstances":
		return c.RemoveExact(ctx, "asg-test", "ins-one")
	case "DescribeAutoScalingActivities":
		if history {
			_, err := c.ActivitiesSince(ctx, "asg-test", time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC))
			return WriteResult{}, err
		}
		_, err := c.Activity(ctx, "asg-test", "asa-test")
		return WriteResult{}, err
	default:
		snap, err := c.Observe(ctx, "asg-test", "")
		if err == nil && (snap.Group.Count != 1 || len(snap.Members) != 1 || len(snap.VMs) != 1) {
			t.Fatal("retry lost the complete observation")
		}
		return WriteResult{}, err
	}
}

func TestRateLimitActualSDKAllCloudEntries(t *testing.T) {
	for _, action := range []string{"DescribeAutoScalingGroups", "DescribeAutoScalingInstances", "DescribeLaunchConfigurations", "DescribeScalingPolicies", "DescribeInstances", "DescribeAutoScalingActivities", "history", "ModifyDesiredCapacity", "ScaleOutInstances", "RemoveInstances"} {
		t.Run(action, func(t *testing.T) {
			t.Parallel()
			target := action
			if target == "history" {
				target = "DescribeAutoScalingActivities"
			}
			calls := map[string]int{}
			bodies := map[string]string{}
			c := rateClient(t, 5*time.Second, func(r *http.Request) (*http.Response, error) {
				a := r.Header.Get("X-TC-Action")
				calls[a]++
				b, err := io.ReadAll(r.Body)
				if err != nil {
					t.Fatal(err)
				}
				if old, ok := bodies[a]; ok && old != string(b) {
					t.Fatal("retry changed request identity or capacity")
				}
				bodies[a] = string(b)
				if d, ok := r.Context().Deadline(); !ok || time.Until(d) > 5*time.Second {
					t.Fatal("missing shared request deadline")
				}
				if a == target && calls[a] == 1 {
					return authResponse(r, 200, rateBody("RequestLimitExceeded", requestID)), nil
				}
				return authResponse(r, 200, rateSuccess(a)), nil
			})
			result, err := rateInvoke(t, context.Background(), c, target, action == "history")
			if err != nil || result.Unknown || calls[target] != 2 {
				t.Fatalf("throttle did not recover: result=%+v err=%v calls=%v", result, err, calls)
			}
			for a, n := range calls {
				if a != target && n != 1 {
					t.Fatal("unthrottled operation repeated", a, n)
				}
			}
			if target == "ModifyDesiredCapacity" || target == "ScaleOutInstances" || target == "RemoveInstances" {
				if !result.Attempted || result.RequestID != requestID {
					t.Fatal("successful write evidence lost")
				}
			}
		})
	}
}

func TestRateLimitWritesExhaustBoundedBackoff(t *testing.T) {
	for _, action := range []string{"ModifyDesiredCapacity", "ScaleOutInstances", "RemoveInstances"} {
		t.Run(action, func(t *testing.T) {
			t.Parallel()
			var at []time.Time
			var last string
			c := rateClient(t, 6*time.Second, func(r *http.Request) (*http.Response, error) {
				at = append(at, time.Now())
				last = fmt.Sprintf("%08x-2222-3333-4444-555555555555", len(at))
				return authResponse(r, 200, rateBody("RequestLimitExceeded.UinLimitExceeded", last)), nil
			})
			out, err := rateInvoke(t, context.Background(), c, action, false)
			if len(at) != 4 || !IsRejectedWrite(out, err) || out.RequestID != last {
				t.Fatalf("exhaustion lost rejection proof: %+v %v calls=%d", out, err, len(at))
			}
			for i := 1; i < len(at); i++ {
				low := 250 * time.Millisecond * time.Duration(1<<(i-1))
				high := 2*low + 500*time.Millisecond
				if d := at[i].Sub(at[i-1]); d < low || d > high {
					t.Fatalf("unbounded/no backoff: delay[%d]=%s", i, d)
				}
			}
		})
	}
}

func TestRateLimitStoppedWaitPreservesConfirmedRefusal(t *testing.T) {
	for _, action := range []string{"ModifyDesiredCapacity", "ScaleOutInstances", "RemoveInstances"} {
		for _, mode := range []string{"deadline", "cancel"} {
			t.Run(action+"/"+mode, func(t *testing.T) {
				t.Parallel()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				var stop context.CancelFunc = func() {}
				if mode == "deadline" {
					ctx, stop = context.WithTimeout(ctx, 40*time.Millisecond)
				}
				defer stop()
				calls := 0
				c := rateClient(t, time.Second, func(r *http.Request) (*http.Response, error) {
					calls++
					if mode == "cancel" {
						time.AfterFunc(20*time.Millisecond, cancel)
					}
					return authResponse(r, 200, rateBody("RequestLimitExceeded", requestID)), nil
				})
				start := time.Now()
				out, err := rateInvoke(t, ctx, c, action, false)
				if calls != 1 || !IsRejectedWrite(out, err) || ctx.Err() == nil || time.Since(start) > time.Second {
					t.Fatalf("stopped wait became unknown or returned without waiting: %+v %v ctx=%v calls=%d", out, err, ctx.Err(), calls)
				}
			})
		}
	}
}

func TestRateLimitLaterAttemptUnknownNeverReplays(t *testing.T) {
	for _, action := range []string{"ModifyDesiredCapacity", "ScaleOutInstances", "RemoveInstances"} {
		t.Run(action, func(t *testing.T) {
			t.Parallel()
			calls := 0
			c := rateClient(t, time.Second, func(r *http.Request) (*http.Response, error) {
				calls++
				if calls == 1 {
					return authResponse(r, 200, rateBody("RequestLimitExceeded", requestID)), nil
				}
				<-r.Context().Done()
				return nil, r.Context().Err()
			})
			start := time.Now()
			out, err := rateInvoke(t, context.Background(), c, action, false)
			if calls != 2 || !out.Unknown || !out.Attempted || err == nil || IsRejectedWrite(out, err) || time.Since(start) > 2*time.Second {
				t.Fatalf("unknown retry was misclassified: %+v %v calls=%d", out, err, calls)
			}
		})
	}
}

func TestRateLimitOnlyExactStructuredCodesRetry(t *testing.T) {
	for _, code := range []string{"RequestLimitExceeded.GlobalRegionUinLimitExceeded", "RequestLimitExceeded.IPLimitExceeded", "RequestLimitExceeded.UinLimitExceeded", "RequestLimitExceeded.FutureCode", "AuthFailure.UnauthorizedOperation", "InternalError"} {
		t.Run(code, func(t *testing.T) {
			t.Parallel()
			calls := 0
			c := rateClient(t, 3*time.Second, func(r *http.Request) (*http.Response, error) {
				calls++
				if calls == 1 {
					return authResponse(r, 200, rateBody(code, requestID)), nil
				}
				return authResponse(r, 200, rateSuccess("RemoveInstances")), nil
			})
			out, err := c.RemoveExact(context.Background(), "asg-test", "ins-one")
			retry := strings.HasPrefix(code, "RequestLimitExceeded.") && code != "RequestLimitExceeded.FutureCode"
			if retry && (calls != 2 || err != nil || out.Unknown) {
				t.Fatalf("known limit not retried: %v %v", out, err)
			}
			if !retry && (calls != 1 || err == nil) {
				t.Fatal("non-whitelisted error retried", code, calls)
			}
			b, _ := json.Marshal([]any{out, err})
			if strings.Contains(string(b), secretMarker) {
				t.Fatal("raw error leaked")
			}
		})
	}
}

func TestRateLimitConflictOrMissingProofNeverRetries(t *testing.T) {
	for _, mode := range []string{"activity", "invalid-activity", "conflicting-request", "missing-request", "joined-timeout", "joined-cancel", "string-error"} {
		t.Run(mode, func(t *testing.T) {
			c, a, _ := fixture(t)
			id, returned, activity := requestID, requestID, ""
			var err error = sdkerrors.NewTencentCloudSDKError("RequestLimitExceeded", secretMarker, id)
			switch mode {
			case "activity":
				activity = "asa-test"
			case "invalid-activity":
				activity = secretMarker
			case "conflicting-request":
				returned = authRequestID
			case "missing-request":
				returned = ""
				err = sdkerrors.NewTencentCloudSDKError("RequestLimitExceeded", secretMarker, "")
			case "joined-timeout":
				err = errors.Join(err, context.DeadlineExceeded)
			case "joined-cancel":
				err = errors.Join(err, context.Canceled)
			case "string-error":
				err = errors.New("RequestLimitExceeded")
			}
			a.remove = func(context.Context, *as.RemoveInstancesRequest) (*as.RemoveInstancesResponse, error) {
				return &as.RemoveInstancesResponse{Response: &as.RemoveInstancesResponseParams{RequestId: common.StringPtr(returned), ActivityId: common.StringPtr(activity)}}, err
			}
			out, got := c.RemoveExact(context.Background(), "asg-test", "ins-one")
			if got == nil || !out.Unknown || a.removeCalls != 1 {
				t.Fatalf("ambiguous refusal replayed: %+v %v calls=%d", out, got, a.removeCalls)
			}
		})
	}
}

func TestRateLimitAuthenticationAndMetadata(t *testing.T) {
	for _, mode := range []string{"sts", "metadata-role", "metadata-credentials"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			calls, identities, environment := 0, 0, 0
			metadata := &http.Client{Transport: authHTTPFunc(func(r *http.Request) (*http.Response, error) {
				role := r.URL.String() == metadataRoleURL
				if (mode == "metadata-role" && role) || (mode == "metadata-credentials" && !role) {
					calls++
					if calls == 1 {
						return authResponse(r, 429, secretMarker), nil
					}
				}
				if role {
					return authResponse(r, 200, "existing-role"), nil
				}
				return authResponse(r, 200, fmt.Sprintf(`{"TmpSecretId":"synthetic-id","TmpSecretKey":"synthetic-key","Token":"synthetic-token","ExpiredTime":%d,"Code":"Success"}`, time.Now().Add(time.Hour).Unix())), nil
			})}
			tr := authHTTPFunc(func(r *http.Request) (*http.Response, error) {
				identities++
				if r.Header.Get("X-TC-Action") != "GetCallerIdentity" {
					t.Fatal("unexpected cloud call during auth")
				}
				if mode == "sts" && identities == 1 {
					return authResponse(r, 200, rateBody("RequestLimitExceeded", requestID)), nil
				}
				return authResponse(r, 200, identityBody("10001")), nil
			})
			c, err := newAuthenticated(context.Background(), AuthConfig{Region: "ap-guangzhou", Timeout: 4 * time.Second}, authDependencies{metadataHTTP: metadata, apiTransport: tr, getenv: func(string) string { environment++; return "" }})
			if err != nil {
				t.Fatalf("identity throttle did not recover: %v", err)
			}
			defer c.Close()
			if c.AccountID() != "10001" || environment != 0 || (mode == "sts" && identities != 2) || (mode != "sts" && (calls != 2 || identities != 1)) {
				t.Fatalf("wrong auth retries: metadata=%d sts=%d env=%d", calls, identities, environment)
			}
		})
	}
}

// NewRateLimitTestClient only exists in the test binary for the external
// adapter/engine integration test. Production configuration has no test hook.
func NewRateLimitTestClient(cfg Config, transport http.RoundTripper) (*Client, error) {
	return newClient(cfg, transport)
}

func TestRateLimitLaterResponseOwnsFinalOutcome(t *testing.T) {
	for _, action := range []string{"ModifyDesiredCapacity", "ScaleOutInstances", "RemoveInstances"} {
		for _, mode := range []string{"non-rate-refusal", "malformed-success", "transport-error", "success-despite-cancel"} {
			t.Run(action+"/"+mode, func(t *testing.T) {
				t.Parallel()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				calls := 0
				c := rateClient(t, 4*time.Second, func(r *http.Request) (*http.Response, error) {
					calls++
					if calls == 1 {
						return authResponse(r, 200, rateBody("RequestLimitExceeded", requestID)), nil
					}
					switch mode {
					case "non-rate-refusal":
						return authResponse(r, 200, rateBody("AuthFailure.UnauthorizedOperation", authRequestID)), nil
					case "malformed-success":
						return authResponse(r, 200, `{"Response":{}}`), nil
					case "transport-error":
						return nil, errors.New(secretMarker)
					default:
						cancel()
						return authResponse(r, 200, rateSuccess(action)), nil
					}
				})
				result, err := rateInvoke(t, ctx, c, action, false)
				if calls != 2 {
					t.Fatal("extra or missing attempt", calls)
				}
				switch mode {
				case "non-rate-refusal":
					if !IsRejectedWrite(result, err) || result.RequestID != authRequestID {
						t.Fatalf("lost later refusal: %+v %v", result, err)
					}
				case "success-despite-cancel":
					if err != nil || result.Unknown {
						t.Fatalf("positive acknowledgement discarded: %+v %v", result, err)
					}
				default:
					if err == nil || !result.Unknown || IsRejectedWrite(result, err) {
						t.Fatalf("earlier refusal hid later ambiguity: %+v %v", result, err)
					}
				}
				b, _ := json.Marshal([]any{result, err})
				if strings.Contains(string(b), secretMarker) {
					t.Fatal("raw error leaked")
				}
			})
		}
	}
}

func TestRateLimitReadAndMetadataBudgetAndCancel(t *testing.T) {
	for _, mode := range []string{"read-budget", "metadata-budget", "already-canceled-read", "already-canceled-write", "already-canceled-metadata"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			before := strings.HasPrefix(mode, "already-canceled")
			if before {
				cancel()
			}
			calls := 0
			timeout := 40 * time.Millisecond
			transport := authHTTPFunc(func(r *http.Request) (*http.Response, error) {
				calls++
				if strings.Contains(mode, "metadata") {
					return authResponse(r, 429, secretMarker), nil
				}
				return authResponse(r, 200, rateBody("RequestLimitExceeded", requestID)), nil
			})
			start := time.Now()
			var err error
			if strings.Contains(mode, "metadata") {
				s := instanceCredentials{http: &http.Client{Transport: transport, Timeout: timeout}}
				_, err = s.get(ctx, metadataRoleURL)
			} else {
				c := rateClient(t, timeout, transport)
				action := "DescribeAutoScalingGroups"
				if mode == "already-canceled-write" {
					action = "RemoveInstances"
				}
				out, e := rateInvoke(t, ctx, c, action, false)
				err = e
				if before && out.Attempted {
					t.Fatal("canceled request marked attempted")
				}
			}
			if err == nil || time.Since(start) > time.Second || (before && calls != 0) || (!before && calls != 1) {
				t.Fatalf("budget/cancel not enforced: %v calls=%d", err, calls)
			}
		})
	}
}

func TestRateLimitActualSDKConflictingEnvelope(t *testing.T) {
	for _, action := range []string{"ModifyDesiredCapacity", "ScaleOutInstances", "RemoveInstances"} {
		t.Run(action, func(t *testing.T) {
			calls := 0
			c := rateClient(t, 20*time.Millisecond, func(r *http.Request) (*http.Response, error) {
				calls++
				return authResponse(r, 200, `{"Response":{"Error":{"Code":"RequestLimitExceeded","Message":"synthetic"},"RequestId":"`+requestID+`","ActivityId":"asa-existing"}}`), nil
			})
			out, err := rateInvoke(t, context.Background(), c, action, false)
			if err == nil || calls != 1 || !out.Unknown || IsRejectedWrite(out, err) {
				t.Fatalf("SDK hid conflicting activity: %+v %v calls=%d", out, err, calls)
			}
		})
	}
}

type rateBodyReader struct {
	io.Reader
	closed int
}

func (b *rateBodyReader) Close() error { b.closed++; return nil }

type rateBrokenReader struct{}

func (rateBrokenReader) Read([]byte) (int, error) { return 0, errors.New(secretMarker) }

func TestRateLimitRawMutationResponseChecks(t *testing.T) {
	for _, mode := range []string{"oversize", "read-error", "null-activity", "malformed-activity", "ordinary-refusal", "error-null-success"} {
		t.Run(mode, func(t *testing.T) {
			calls := 0
			body := rateBody("RequestLimitExceeded", requestID)
			switch mode {
			case "oversize":
				body = strings.Repeat(" ", 65537) + body
			case "null-activity":
				body = strings.TrimSuffix(body, "}}") + `,"ActivityId":null}}`
			case "malformed-activity":
				body = strings.TrimSuffix(body, "}}") + `,"ActivityId":{"unexpected":true}}}`
			case "error-null-success":
				body = `{"Response":{"Error":null,"ActivityId":"asa-test","RequestId":"` + requestID + `"}}`
			}
			stream := &rateBodyReader{Reader: strings.NewReader(body)}
			if mode == "read-error" {
				stream.Reader = rateBrokenReader{}
			}
			c := rateClient(t, 20*time.Millisecond, func(r *http.Request) (*http.Response, error) {
				calls++
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: stream, Request: r}, nil
			})
			result, err := c.RemoveExact(context.Background(), "asg-test", "ins-one")
			if calls != 1 || stream.closed != 1 {
				t.Fatal("response resource leak or unsafe replay", calls, stream.closed)
			}
			switch mode {
			case "ordinary-refusal":
				if !IsRejectedWrite(result, err) {
					t.Fatalf("legitimate rate rejection lost: %+v %v", result, err)
				}
			case "error-null-success":
				if err != nil || result.Unknown || result.ActivityID != "asa-test" {
					t.Fatalf("valid accepted response changed: %+v %v", result, err)
				}
			default:
				if err == nil || !result.Unknown || IsRejectedWrite(result, err) {
					t.Fatalf("unsafe response accepted: %+v %v", result, err)
				}
			}
			b, _ := json.Marshal([]any{result, err})
			if strings.Contains(string(b), secretMarker) {
				t.Fatal("response read error leaked")
			}
		})
	}
}
