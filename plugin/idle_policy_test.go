package plugin

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/live"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func idlePolicyFixture(t *testing.T) (string, *Target, *routingCloud) {
	t.Helper()
	root := t.TempDir()
	c := &routingCloud{account: "10001", region: "ap-guangzhou"}
	p := routeTarget(t, root, c, nil)
	readyRoute(t, p, map[string]string{"tencent_group_id": "asg-test"})
	return filepath.Join(root, "accounts", c.account, c.region, "asg-test"), p, c
}
func nextIdlePolicy() map[string]string {
	return map[string]string{"tencent_group_id": "asg-test", "node_drain_deadline": "10m", "node_class": "new-class", "node_selector_strategy": "newest_create_index", "scale_in_protection": "true"}
}
func TestIdlePolicyUpdateRestartAndGuards(t *testing.T) {
	dir, p, c := idlePolicyFixture(t)
	if _, err := PreviewIdlePolicyUpdate(dir, nextIdlePolicy(), "test", "unit/idle"); err == nil {
		t.Fatal("running controller not locked")
	}
	p.Close()
	plan, err := PreviewIdlePolicyUpdate(dir, nextIdlePolicy(), "test", "unit/idle")
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"plan", "confirm"} {
		q := plan
		confirm := plan.Digest
		if key == "plan" {
			q.Directory += "-other"
		} else {
			confirm = "bad"
		}
		if ApplyIdlePolicyUpdate(q, confirm) == nil {
			t.Fatal("invalid confirmation accepted")
		}
	}
	if err = ApplyIdlePolicyUpdate(plan, plan.Digest); err != nil {
		t.Fatal(err)
	}
	if err = ApplyIdlePolicyUpdate(plan, plan.Digest); err != nil {
		t.Fatal("idempotent apply", err)
	}
	root := filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(dir))))
	q := routeTarget(t, root, c, nil)
	status := readyRoute(t, q, nextIdlePolicy())
	if !status.Ready || status.Count != 0 || c.writes.Load() != 0 {
		t.Fatal("new policy failed restart", status)
	}
	q.Close()
	raw, _ := os.ReadFile(filepath.Join(dir, "live.json"))
	var s live.State
	_ = json.Unmarshal(raw, &s)
	if s.DrainDeadlineNanoseconds != int64(10*time.Minute) || s.PolicyAuditSHA256 != plan.Change.AuditID || s.LastCompletedAt != 0 {
		t.Fatal("state or history changed")
	}
	if _, err = PreviewIdlePolicyUpdate(dir, map[string]string{"tencent_group_id": "asg-test", "instance_id_meta_key": "other"}, "test", "unit/map"); err == nil {
		t.Fatal("unverified mapping change allowed")
	}
}
func TestIdlePolicyUpdateCrashBoundaries(t *testing.T) {
	for _, at := range []string{"audit", "marker", "live.json", "route.json", "receipt"} {
		t.Run(at, func(t *testing.T) {
			dir, p, c := idlePolicyFixture(t)
			p.Close()
			plan, err := PreviewIdlePolicyUpdate(dir, nextIdlePolicy(), "test", "unit/crash")
			if err != nil {
				t.Fatal(err)
			}
			if err = applyIdlePolicyUpdate(plan, plan.Digest, func(stage string) error {
				if stage == at {
					return errors.New("SyntheticInterrupted")
				}
				return nil
			}); err == nil {
				t.Fatal("checkpoint not reached")
			}
			if at == "audit" {
				if err = ApplyIdlePolicyUpdate(plan, plan.Digest); err != nil {
					t.Fatal(err)
				}
			}
			// Startup completes only a durable local update marker, then loads new policy.
			root := filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(dir))))
			q := routeTarget(t, root, c, nil)
			s := readyRoute(t, q, nextIdlePolicy())
			q.Close()
			if !s.Ready || c.writes.Load() != 0 {
				t.Fatal("restart mutated cloud or lost readiness")
			}
			if _, err = os.Stat(filepath.Join(dir, policyMarker)); !os.IsNotExist(err) {
				t.Fatal("marker not cleared")
			}
		})
	}
}
func TestIdlePolicyUpdateChangedStateAndRecoveryRefused(t *testing.T) {
	for _, mode := range []string{"journal-changed", "recovery-marker", "queue", "mapping", "wrong-group", "pending", "expired", "unsafe-file"} {
		t.Run(mode, func(t *testing.T) {
			dir, p, _ := idlePolicyFixture(t)
			p.Close()
			before, _ := os.ReadFile(filepath.Join(dir, "live.json"))
			plan, err := PreviewIdlePolicyUpdate(dir, nextIdlePolicy(), "test", "unit/reject")
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "journal-changed":
				_ = os.WriteFile(filepath.Join(dir, "live.json"), append(before, ' '), 0600)
			case "recovery-marker":
				_ = os.WriteFile(filepath.Join(dir, "recovery-pending.json"), []byte("{}"), 0600)
			case "queue":
				_ = os.WriteFile(filepath.Join(dir, "journal-save-pending.json"), []byte("{}"), 0600)
			case "expired":
				plan.CreatedAt = time.Now().Add(-time.Hour)
				plan.ExpiresAt = plan.CreatedAt.Add(5 * time.Minute)
				plan.Digest = policyDigest(plan)
			case "unsafe-file":
				_ = os.Chmod(filepath.Join(dir, "live.json"), 0644)
			case "pending":
				var s live.State
				_ = json.Unmarshal(before, &s)
				s.ManualReview = true
				_ = os.WriteFile(filepath.Join(dir, "live.json"), policyJSON(s), 0600)
			case "mapping", "wrong-group":
				v := nextIdlePolicy()
				if mode == "mapping" {
					v["instance_id_meta_key"] = "other"
				} else {
					v["tencent_group_id"] = "asg-other"
				}
				if _, err = PreviewIdlePolicyUpdate(dir, v, "test", "unit/reject"); err == nil {
					t.Fatal("scope accepted")
				}
				return
			}
			current, _ := os.ReadFile(filepath.Join(dir, "live.json"))
			if ApplyIdlePolicyUpdate(plan, plan.Digest) == nil {
				t.Fatal("unsafe update accepted")
			}
			after, _ := os.ReadFile(filepath.Join(dir, "live.json"))
			if !bytes.Equal(current, after) {
				t.Fatal("rejected update mutated journal")
			}
		})
	}
}

func TestIdlePolicyProcessCrash(t *testing.T) {
	if path := os.Getenv("NOMAD_POLICY_CRASH_PLAN"); path != "" {
		p, err := ReadIdlePolicyPlan(path)
		if err != nil {
			t.Fatal(err)
		}
		err = applyIdlePolicyUpdate(p, p.Digest, func(stage string) error {
			if stage == os.Getenv("NOMAD_POLICY_CRASH_STAGE") {
				os.Exit(97)
			}
			return nil
		})
		t.Fatal("checkpoint missing", err)
	}
	for _, at := range []string{"marker", "live.json", "route.json", "receipt"} {
		t.Run(at, func(t *testing.T) {
			dir, p, c := idlePolicyFixture(t)
			p.Close()
			plan, err := PreviewIdlePolicyUpdate(dir, nextIdlePolicy(), "test", "unit/process-crash")
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "plan.json")
			if err = WriteIdlePolicyPlan(path, plan); err != nil {
				t.Fatal(err)
			}
			child := exec.Command(os.Args[0], "-test.run=^TestIdlePolicyProcessCrash$")
			child.Env = append(os.Environ(), "NOMAD_POLICY_CRASH_PLAN="+path, "NOMAD_POLICY_CRASH_STAGE="+at)
			if out, err := child.CombinedOutput(); err == nil || child.ProcessState.ExitCode() != 97 {
				t.Fatalf("crash failed: %v %s", err, out)
			}
			root := filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(dir))))
			q := routeTarget(t, root, c, nil)
			readyRoute(t, q, nextIdlePolicy())
			q.Close()
			if c.writes.Load() != 0 {
				t.Fatal("cloud mutation during policy recovery")
			}
			if _, err = os.Lstat(filepath.Join(dir, policyMarker)); !os.IsNotExist(err) {
				t.Fatal("unfinished policy marker")
			}
		})
	}
}
