package nomadsdk

import (
	"context"
	"testing"
	"time"
)

func TestSDKPostResultUsesExactReadback(t *testing.T) {
	for _, outcome := range []string{"purge-disabled", "purged", "already-absent", "lost-response", "forbidden", "unavailable"} {
		t.Run(outcome, func(t *testing.T) {
			f := &fixture{drained: true, purged: outcome == "already-absent", purgeLost: outcome == "lost-response", authFailed: false}
			if outcome == "forbidden" {
				f.nodeReadStatus = 403
			}
			if outcome == "unavailable" {
				f.nodeReadStatus = 503
			}
			enabled := "true"
			if outcome == "purge-disabled" {
				enabled = "false"
			}
			a := setup(t, f, map[string]string{"node_purge": enabled})
			start := time.Now()
			result, err := a.PostScaleInWithEvidence(context.Background(), identity(second), operation, func() error { f.barrier.Store(true); return nil })
			if outcome == "lost-response" || outcome == "forbidden" || outcome == "unavailable" {
				if err == nil || result.Outcome != "" || !result.ObservedAt.IsZero() {
					t.Fatal("failed post fabricated success receipt")
				}
				if outcome == "forbidden" || outcome == "unavailable" {
					if f.purgeWrites != 0 {
						t.Fatal("purged forbidden node")
					}
					return
				}
				result, err = a.PostScaleInWithEvidence(context.Background(), identity(second), operation, func() error { t.Error("replayed purge"); return nil })
				outcome = "already-absent"
			}
			if err != nil || result.Outcome != outcome || result.Identity != identity(second) || result.OperationID != operation || result.PolicySHA256 != a.PolicyDigest() || result.Purge != (enabled == "true") || result.ObservedAt.Before(start) || result.ObservedAt.After(time.Now()) {
				t.Fatalf("bad receipt: %+v %v", result, err)
			}
			expected := 0
			if outcome == "purged" || f.purgeLost {
				expected = 1
			}
			if f.purgeWrites != expected {
				t.Fatal("wrong purge write count", f.purgeWrites, expected)
			}
		})
	}
}
