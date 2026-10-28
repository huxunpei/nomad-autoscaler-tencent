package live

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

func TestFailedGrowthDiagnosticsPersistThroughRestartAndSettlement(t *testing.T) {
	for _, tc := range []struct {
		name  string
		codes []any
	}{
		{"capacity", []any{"LimitExceeded.SpotQuota", "ResourcesSoldOut.SpecifiedInstanceType"}},
		{"launch_hook_abandon", []any{"LifecycleHookActionAbandon"}},
		{"legacy_unclassified", []any{"Unclassified"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, c, n, e := failedGrowthFixture(t, false)
			var logs bytes.Buffer
			cfg.Logger = hclog.New(&hclog.LoggerOptions{Output: &logs, JSONFormat: true, Level: hclog.Info})
			e.cfg.Logger = cfg.Logger
			// Decode the adapter's optional wire shape so the pre-fix test compiles and
			// fails on missing behavior, rather than on a missing Go declaration.
			raw, err := json.Marshal(map[string]any{"failure_codes": tc.codes})
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(raw, &c.history[0]); err != nil {
				t.Fatal(err)
			}
			step(t, e)
			e.Close()
			logs.Reset()
			e = openMulti(t, cfg, c, n)
			c.snap = copyJSON(c.complete)
			step(t, e)
			found := false
			for _, row := range lifecycleRows(t, &logs) {
				if row["result"] == "failed" {
					found = true
					if !reflect.DeepEqual(row["provider_failure_codes"], tc.codes) {
						t.Fatalf("failure diagnostics lost across restart: %v", row)
					}
				}
			}
			if !found || e.state.LastCompletedAt != 0 || c.writeCount() != 1 || e.checkRecoveryAudit() != nil {
				t.Fatal("missing failed result, false success or replayed cloud mutation")
			}
		})
	}
}

func TestFailureDiagnosticsCannotAlterRecoveryProof(t *testing.T) {
	at := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	a := tencentcloud.Activity{ID: "asa-diag", GroupID: testGroup, Type: "REMOVE_INSTANCES", Status: "FAILED", StartedAt: at.Format(time.RFC3339), EndedAt: at.Add(time.Second).Format(time.RFC3339), RelatedInstances: []tencentcloud.RelatedInstance{{ID: "ins-diag", Status: "FAILED"}}, RequestIDs: []tencentcloud.RequestTrace{{Action: "DescribeAutoScalingActivities", RequestID: testRequest}}}
	b := copyJSON(a)
	b.FailureCodes = []string{"LimitExceeded.SpotQuota"}
	if !sameActivity(a, b) || !validHistoryActivity(b, testGroup, time.Now().UTC()) || !validActivity(b, testGroup, b.ID, "ins-diag") {
		t.Fatal("late diagnostic detail changed the operational evidence")
	}
	b.Status = "SUCCESSFUL"
	if sameActivity(a, b) || validHistoryActivity(b, testGroup, time.Now().UTC()) {
		t.Fatal("diagnostics masked an outcome change")
	}
	b = copyJSON(a)
	b.FailureCodes = []string{"PRIVATE_CODE"}
	if sameActivity(a, b) || validHistoryActivity(b, testGroup, time.Now().UTC()) || validActivity(b, testGroup, b.ID, "ins-diag") {
		t.Fatal("unsafe diagnostics entered recovery evidence")
	}
}
