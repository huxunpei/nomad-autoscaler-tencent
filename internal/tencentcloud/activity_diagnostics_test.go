package tencentcloud

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	as "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/as/v20180419"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
)

func TestActivityFailureDiagnosticsThroughSDK(t *testing.T) {
	for _, status := range []string{"FAILED", "PARTIALLY_SUCCESSFUL", "CANCELLED", "SUCCESSFUL", "RUNNING"} {
		t.Run(status, func(t *testing.T) {
			at := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
			details := []any{
				map[string]any{"Code": "LimitExceeded.SpotQuota", "Message": secretMarker},
				map[string]any{"Code": "ResourcesSoldOut.SpecifiedInstanceType", "Message": secretMarker},
				// Exact code observed in a partially successful launch-hook rollback.
				map[string]any{"Code": "LifecycleHookActionAbandon", "Message": secretMarker},
				map[string]any{"Code": "LifecycleHookActionAbandon"},
				map[string]any{"Code": "LifecycleHookActionAbandon." + secretMarker},
				// Recorded public API shape: AS may localize Code as well as Message.
				map[string]any{"Code": "云主机售罄", "Message": secretMarker},
				map[string]any{"Code": "云主机售罄" + secretMarker},
				map[string]any{"Code": "LimitExceeded.SpotQuota"},
				map[string]any{"Code": "InternalError." + secretMarker},
				map[string]any{"Code": "InternalError.PRIVATE_DIAGNOSTIC_CANARY"},
				map[string]any{"Code": secretMarker}, nil,
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("X-TC-Action") != "DescribeAutoScalingActivities" {
					t.Error("unexpected API call")
					http.Error(w, "unexpected action", 400)
					return
				}
				item := map[string]any{"ActivityId": "asa-test", "AutoScalingGroupId": "asg-test", "ActivityType": "SCALE_OUT", "StatusCode": status, "StartTime": at.Format(time.RFC3339), "EndTime": at.Add(time.Second).Format(time.RFC3339), "RelatedInstanceSet": []any{}, "StatusMessage": secretMarker, "DetailedStatusMessageSet": details}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{"Response": map[string]any{"RequestId": requestID, "TotalCount": 1, "ActivitySet": []any{item}}})
			}))
			defer server.Close()
			p := clientProfile(time.Second)
			p.HttpProfile.Endpoint = strings.TrimPrefix(server.URL, "http://")
			p.HttpProfile.Scheme = "http"
			sdk, err := as.NewClient(common.NewCredential("synthetic-id", "synthetic-key"), "ap-test", p)
			if err != nil {
				t.Fatal(err)
			}
			client := &Client{as: sdk, region: "ap-test", timeout: time.Second}
			one, err := client.Activity(context.Background(), "asg-test", "asa-test")
			if err != nil {
				t.Fatal(err)
			}
			many, err := client.ActivitiesSince(context.Background(), "asg-test", at)
			if err != nil || len(many) != 1 {
				t.Fatal("history lookup failed", err)
			}
			for _, activity := range []Activity{one, many[0]} {
				raw, _ := json.Marshal(activity)
				if strings.Contains(string(raw), secretMarker) || strings.Contains(string(raw), "PRIVATE_DIAGNOSTIC_CANARY") || strings.Contains(string(raw), "Message") {
					t.Fatal("raw provider details escaped the adapter")
				}
				var decoded struct {
					FailureCodes []string `json:"failure_codes"`
				}
				_ = json.Unmarshal(raw, &decoded)
				var want []string
				if status == "FAILED" || status == "PARTIALLY_SUCCESSFUL" || status == "CANCELLED" {
					want = []string{"InternalError", "LifecycleHookActionAbandon", "LimitExceeded.SpotQuota", "ResourcesSoldOut", "ResourcesSoldOut.SpecifiedInstanceType", "Unclassified"}
				}
				if !reflect.DeepEqual(decoded.FailureCodes, want) {
					t.Fatalf("diagnostics: got %v, want %v", decoded.FailureCodes, want)
				}
			}
		})
	}
}

func TestActivityFailureDiagnosticsAreBoundedAndCanonical(t *testing.T) {
	var details []*as.DetailedStatusMessage
	for i := 0; i < 2000; i++ {
		details = append(details, &as.DetailedStatusMessage{Code: common.StringPtr(fmt.Sprintf("ResourcesSoldOut.PRIVATE_VARIANT_%d", i))})
	}
	got := activityFailureCodes("FAILED", details)
	if !reflect.DeepEqual(got, []string{"ResourcesSoldOut"}) {
		t.Fatal("unbounded provider code values escaped")
	}
	for _, codes := range [][]string{nil, {"Unclassified"}, {"LifecycleHookActionAbandon"}, {"LifecycleHookActionAbandon", "Unclassified"}, {"ResourcesSoldOut"}, {"LimitExceeded.SpotQuota", "ResourcesSoldOut"}} {
		if !(Activity{Status: "FAILED", FailureCodes: codes}).ValidFailureCodes() {
			t.Fatal("valid or legacy diagnostics refused")
		}
	}
	for _, codes := range [][]string{{""}, {"LifecycleHookActionAbandon.PRIVATE_VARIANT"}, {"LifecycleHookActionAbandonSuffix"}, {"LifecycleHookActionAbandon", "LifecycleHookActionAbandon"}, {"云主机售罄"}, {"ResourcesSoldOut.PRIVATE_VARIANT"}, {"LimitExceeded", "LimitExceeded"}, {"ResourcesSoldOut", "LimitExceeded"}, {secretMarker}, make([]string, 65)} {
		if (Activity{Status: "FAILED", FailureCodes: codes}).ValidFailureCodes() {
			t.Fatal("untrusted/noncanonical diagnostics accepted")
		}
	}
	if (Activity{Status: "SUCCESSFUL", FailureCodes: got}).ValidFailureCodes() {
		t.Fatal("success record relabeled as failure")
	}
}
