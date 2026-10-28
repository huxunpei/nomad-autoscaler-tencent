package tencentcloud

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"
)

type testScaleOutAPI interface {
	ScaleOut(context.Context, string, int64) (WriteResult, error)
}

func TestScaleOutProductionSDK(t *testing.T) {
	calls := 0
	c, err := newClient(Config{Region: "ap-test", SecretID: "synthetic-id", SecretKey: "synthetic-key", Timeout: time.Second},
		authHTTPFunc(func(req *http.Request) (*http.Response, error) {
			calls++
			var body map[string]interface{}
			if json.NewDecoder(req.Body).Decode(&body) != nil || req.Header.Get("X-TC-Action") != "ScaleOutInstances" ||
				body["AutoScalingGroupId"] != "asg-test" || body["ScaleOutNumber"] != float64(4) || len(body) != 2 {
				t.Fatal("unexpected increment request")
			}
			return authResponse(req, 200, `{"Response":{"RequestId":"`+requestID+`","ActivityId":"asa-growth"}}`), nil
		}))
	if err != nil {
		t.Fatal(err)
	}
	api, ok := interface{}(c).(testScaleOutAPI)
	if !ok {
		t.Fatal("production client has no activity-backed scale-out")
	}
	r, err := api.ScaleOut(context.Background(), "asg-test", 4)
	if err != nil || r.Action != "ScaleOutInstances" || r.ActivityID != "asa-growth" || r.RequestID != requestID || !r.Attempted || r.Unknown || calls != 1 {
		t.Fatalf("activity acknowledgement lost: %+v %v calls=%d", r, err, calls)
	}
}

func TestScaleOutProductionUnknownNeverReplays(t *testing.T) {
	calls := 0
	c, err := newClient(Config{Region: "ap-test", SecretID: "synthetic-id", SecretKey: "synthetic-key", Timeout: time.Second},
		authHTTPFunc(func(*http.Request) (*http.Response, error) {
			calls++
			return nil, errors.New("synthetic-response-lost")
		}))
	if err != nil {
		t.Fatal(err)
	}
	api, ok := interface{}(c).(testScaleOutAPI)
	if !ok {
		t.Fatal("production client has no activity-backed scale-out")
	}
	r, err := api.ScaleOut(context.Background(), "asg-test", 4)
	if err == nil || !r.Unknown || calls != 1 || r.ActivityID != "" {
		t.Fatalf("unknown increment replayed: %+v %v calls=%d", r, err, calls)
	}
}
