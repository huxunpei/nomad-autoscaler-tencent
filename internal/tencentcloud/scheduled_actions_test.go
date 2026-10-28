package tencentcloud

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"
)

func TestScheduledActionsDiagnosticReadsFilteredTotal(t *testing.T) {
	c, err := newClient(Config{Region: "ap-test", SecretID: "synthetic-id", SecretKey: "synthetic-key", Timeout: time.Second}, authHTTPFunc(func(req *http.Request) (*http.Response, error) {
		var body struct {
			Filters []struct {
				Name   string
				Values []string
			}
			Limit uint64
		}
		if json.NewDecoder(req.Body).Decode(&body) != nil || req.Header.Get("X-TC-Action") != "DescribeScheduledActions" || body.Limit != 1 || len(body.Filters) != 1 || body.Filters[0].Name != "auto-scaling-group-id" || len(body.Filters[0].Values) != 1 || body.Filters[0].Values[0] != "asg-test" {
			t.Fatal("unscoped schedule query")
		}
		return authResponse(req, 200, `{"Response":{"RequestId":"`+requestID+`","TotalCount":3,"ScheduledActionSet":[{"AutoScalingGroupId":"asg-test","ScheduledActionId":"asst-test"}]}}`), nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	s := Snapshot{Group: Group{ID: "asg-test"}}
	if err := c.scheduledActions(context.Background(), &s); err != nil || s.ScheduledActionCount != 3 || len(s.RequestIDs) != 1 {
		t.Fatal(s, err)
	}
}
