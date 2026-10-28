package tencentcloud

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"
)

func TestDesiredCASProductionRequestAndRejection(t *testing.T) {
	for i, response := range []string{
		`{"Response":{"RequestId":"` + requestID + `"}}`,
		`{"Response":{"Error":{"Code":"InvalidParameterValue.AssertDesiredCapacityFailed","Message":"synthetic"},"RequestId":"` + requestID + `"}}`,
	} {
		calls := 0
		c, err := newClient(Config{Region: "ap-test", SecretID: "synthetic-id", SecretKey: "synthetic-key", Timeout: time.Second}, authHTTPFunc(func(req *http.Request) (*http.Response, error) {
			calls++
			var body map[string]interface{}
			if json.NewDecoder(req.Body).Decode(&body) != nil || len(body) != 3 || body["AutoScalingGroupId"] != "asg-test" || body["DesiredCapacity"] != float64(5) || body["AssertOriginDesiredCapacity"] != float64(1) || req.Header.Get("X-TC-Action") != "ModifyDesiredCapacity" {
				t.Fatal("absolute target or CAS baseline missing", body)
			}
			return authResponse(req, 200, response), nil
		}))
		if err != nil {
			t.Fatal(err)
		}
		r, err := c.SetDesiredCAS(context.Background(), "asg-test", 5, 1)
		if calls != 1 || !r.Attempted || r.Unknown || r.RequestID != requestID {
			t.Fatal(r, err, calls)
		}
		if (i == 1) != (err != nil) {
			t.Fatal("unexpected CAS result", r, err)
		}
		if err != nil && !IsRejectedWrite(r, err) {
			t.Fatal("CAS conflict not a definite rejection", r, err)
		}
	}
}

// This models value-CAS semantics, not an actual cloud scheduling guarantee.
// Returning to the original value (ABA) is deliberately retained as a known
// limitation: passing this diagnostic does not prove delayed-request safety.
func TestDesiredCASDelayedRequestValueComparisonBoundary(t *testing.T) {
	for _, returnToOrigin := range []bool{false, true} {
		t.Run(map[bool]string{false: "changed-value-rejects", true: "ABA-is-not-fenced"}[returnToOrigin], func(t *testing.T) {
			var mu sync.Mutex
			desired, calls := int64(1), 0
			arrived, release := make(chan struct{}), make(chan struct{})
			c, err := newClient(Config{Region: "ap-test", SecretID: "synthetic-id", SecretKey: "synthetic-key", Timeout: 3 * time.Second}, authHTTPFunc(func(req *http.Request) (*http.Response, error) {
				var body struct{ DesiredCapacity, AssertOriginDesiredCapacity int64 }
				if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
					return nil, err
				}
				mu.Lock()
				calls++
				first := calls == 1
				mu.Unlock()
				if first {
					close(arrived)
					<-release
				}
				mu.Lock()
				defer mu.Unlock()
				if desired != body.AssertOriginDesiredCapacity {
					return authResponse(req, 200, `{"Response":{"Error":{"Code":"InvalidParameterValue.AssertDesiredCapacityFailed","Message":"synthetic"},"RequestId":"`+requestID+`"}}`), nil
				}
				desired = body.DesiredCapacity
				return authResponse(req, 200, `{"Response":{"RequestId":"`+requestID+`"}}`), nil
			}))
			if err != nil {
				t.Fatal(err)
			}
			var releaseOnce sync.Once
			t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
			done := make(chan error, 1)
			go func() { _, err := c.SetDesiredCAS(context.Background(), "asg-test", 5, 1); done <- err }()
			select {
			case <-arrived:
			case <-time.After(time.Second):
				t.Fatal("delayed request did not arrive")
			}
			if _, err := c.SetDesiredCAS(context.Background(), "asg-test", 5, 1); err != nil {
				t.Fatal(err)
			}
			next := int64(3)
			if returnToOrigin {
				next = 1
			}
			if _, err := c.SetDesiredCAS(context.Background(), "asg-test", next, 5); err != nil {
				t.Fatal(err)
			}
			releaseOnce.Do(func() { close(release) })
			select {
			case err := <-done:
				mu.Lock()
				observed := desired
				mu.Unlock()
				if returnToOrigin {
					if err != nil || observed != 5 {
						t.Fatal("ABA diagnostic no longer models value CAS", observed, err)
					}
				} else if err == nil || observed != 3 {
					t.Fatal("stale assertion overwrote a different value", observed, err)
				}
			case <-time.After(time.Second):
				t.Fatal("delayed request did not finish")
			}
		})
	}
}

func TestDesiredCASUnknownNeverRetriesInAdapter(t *testing.T) {
	calls := 0
	c, err := newClient(Config{Region: "ap-test", SecretID: "synthetic-id", SecretKey: "synthetic-key", Timeout: time.Second}, authHTTPFunc(func(*http.Request) (*http.Response, error) {
		calls++
		return nil, errors.New("synthetic-response-lost")
	}))
	if err != nil {
		t.Fatal(err)
	}
	r, err := c.SetDesiredCAS(context.Background(), "asg-test", 5, 1)
	if err == nil || !r.Unknown || calls != 1 {
		t.Fatal("adapter replayed unknown CAS", r, err, calls)
	}
}
