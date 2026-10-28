package live

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

type loggingCloud struct {
	*settlementCloud
	removals int
	failAt   int
}

func (c *loggingCloud) Activity(ctx context.Context, g, id string) (tencentcloud.Activity, error) {
	if c.activities[id].Status == "RUNNING" {
		c.removals++
		c.terminal(c.removals != c.failAt)
		if c.removals == c.failAt {
			for i := range c.snap.Members {
				if c.snap.Members[i].ID == c.activities[id].RelatedInstances[0].ID {
					c.snap.Members[i].Lifecycle = "TERMINATION_FAILED"
				}
			}
		}
	}
	return c.multiCloud.Activity(ctx, g, id)
}

func lifecycleRows(t *testing.T, b *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(b.String(), "\n") {
		if line == "" {
			continue
		}
		var row map[string]any
		if json.Unmarshal([]byte(line), &row) != nil {
			t.Fatal("invalid JSON log")
		}
		if row["@message"] == "scaling lifecycle" {
			out = append(out, row)
		}
	}
	return out
}

func TestLifecyclePartialFailureAndRestartLogsActualCounts(t *testing.T) {
	cfg, c, n, e := settlementFixture(t, 3)
	e.Close()
	var output bytes.Buffer
	cfg.Logger = hclog.New(&hclog.LoggerOptions{Output: &output, JSONFormat: true, Level: hclog.Info})
	cfg.StabilizationInterval = time.Millisecond
	cloud := &loggingCloud{settlementCloud: c, failAt: 2}
	e = openMulti(t, cfg, cloud, n)
	id := "11111111-2222-3333-4444-555555555555"
	if changed, err := e.ScaleAndWaitWithPolicy(1, 15, id); !changed || err == nil {
		t.Fatal("partial failure not returned")
	}
	s, _, _ := e.Snapshot()
	if s.Batch == nil || s.Pending == nil || len(s.Members) != 2 {
		t.Fatal("wrong partial state")
	}
	batchID := s.Batch.ID
	found := false
	for _, row := range lifecycleRows(t, &output) {
		if row["result"] == "failed-pending-reconciliation" {
			found = true
			if row["policy_id"] != id || row["operation_id"] != batchID || row["old_count"] != float64(3) || row["requested_count"] != float64(1) || row["effective_count"] != float64(1) || row["observed_new_count"] != float64(2) || len(row["removed_nodes"].([]any)) != 1 {
				t.Fatalf("wrong partial facts: %v", row)
			}
		}
	}
	if !found {
		t.Fatal("missing partial failure lifecycle")
	}
	e.Close()
	output.Reset()
	e = openMulti(t, cfg, cloud, n)
	laterDeletion(t, e, c)
	step(t, e)
	step(t, e)
	s, _, _ = e.Snapshot()
	if s.Pending != nil || s.LastBatch == nil || s.LastBatch.Phase != "failed" {
		t.Fatal("retry did not settle")
	}
	found = false
	for _, row := range lifecycleRows(t, &output) {
		if row["result"] == "failed" {
			found = true
			if row["policy_id"] != id || row["operation_id"] != batchID || row["observed_new_count"] != float64(1) || len(row["removed_nodes"].([]any)) != 2 {
				t.Fatalf("restart lost original context: %v", row)
			}
		}
	}
	if !found {
		t.Fatal("missing retry settlement log")
	}
}

func TestLifecycleClampedRequestAndUnknownIdentity(t *testing.T) {
	for _, id := range []string{"11111111-2222-3333-4444-555555555555", "PRIVATE_POLICY\nSECRET"} {
		t.Run("identity", func(t *testing.T) {
			_, _, _, e := completionFixture(t)
			var output bytes.Buffer
			e.cfg.Logger = hclog.New(&hclog.LoggerOptions{Output: &output, JSONFormat: true})
			if changed, err := e.ScaleAndWaitWithPolicy(7, 15, id); !changed || err != nil {
				t.Fatal(changed, err)
			}
			found := false
			for _, row := range lifecycleRows(t, &output) {
				if row["result"] == "completed" {
					found = true
					if row["old_count"] != float64(0) || row["requested_count"] != float64(7) || row["effective_count"] != float64(5) || row["observed_new_count"] != float64(5) {
						t.Fatalf("target confused with actual: %v", row)
					}
				}
			}
			if !found || strings.Contains(output.String(), "PRIVATE_") {
				t.Fatal("missing result or leaked identity")
			}
		})
	}
}

func TestLifecycleUnknownObservationAndRepeatedSave(t *testing.T) {
	_, _, _, e := completionFixture(t)
	var output bytes.Buffer
	e.cfg.Logger = hclog.New(&hclog.LoggerOptions{Output: &output, JSONFormat: true})
	if _, err := e.Scale(2); err != nil {
		t.Fatal(err)
	}
	output.Reset()
	e.observeError = code("PRIVATE_ERROR")
	if err := e.save(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if err := e.save(); err != nil {
			t.Fatal(err)
		}
	}
	rows := lifecycleRows(t, &output)
	if len(rows) != 1 || rows[0]["observed_count_available"] != false || rows[0]["observed_new_count"] != nil || strings.Contains(output.String(), "PRIVATE_ERROR") {
		t.Fatal("stale count or repeated log")
	}
}

func TestLifecycleSourceCompatibilityAndTerminalLogNotReassigned(t *testing.T) {
	cfg, c, n, e := completionFixture(t)
	var output bytes.Buffer
	cfg.Logger = hclog.New(&hclog.LoggerOptions{Output: &output, JSONFormat: true})
	e.cfg.Logger = cfg.Logger
	if _, err := e.ScaleAndWaitWithPolicy(2, 15, "11111111-2222-3333-4444-555555555555"); err != nil {
		t.Fatal(err)
	}
	source := e.state.LastBatch.Source
	for _, invalid := range []*ActionSource{{PolicyID: "PRIVATE_VALUE", RequestedCount: 2}, {RequestedCount: -1}} {
		e.state.LastBatch.Source = invalid
		if e.validateState() == nil {
			t.Fatal("invalid source accepted")
		}
	}
	e.state.LastBatch.Source = nil
	if e.validateState() != nil {
		t.Fatal("legacy missing context rejected")
	}
	e.state.LastBatch.Source = source
	output.Reset()
	if err := e.save(); err != nil {
		t.Fatal(err)
	}
	if output.Len() != 0 {
		t.Fatal("terminal batch logged again")
	}
	e.Close()
	e = openMulti(t, cfg, c, n)
	step(t, e)
	if output.Len() != 0 {
		t.Fatal("old completion reassigned to restart observation")
	}
}

func TestLifecycleDoesNotPublishUnpersistedAction(t *testing.T) {
	_, c, _, e := completionFixture(t)
	var output bytes.Buffer
	e.cfg.Logger = hclog.New(&hclog.LoggerOptions{Output: &output, JSONFormat: true})
	e.cfg.Dir = e.cfg.Dir + "/missing"
	if _, err := e.ScaleAndWaitWithPolicy(2, 15, ""); err == nil {
		t.Fatal("failed persistence ignored")
	}
	if len(lifecycleRows(t, &output)) != 0 || len(c.writes) != 0 {
		t.Fatal("unpersisted operation logged or sent")
	}
}
