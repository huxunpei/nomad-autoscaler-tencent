package live

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func launchFixture(t *testing.T) (Config, *fakeCloud, *sdkProbe, State) {
	t.Helper()
	cfg := config(t)
	cfg.GroupContract = true
	c := newCloud()
	c.snap.Group.AllocationPolicy = "LAUNCH_CONFIGURATION"
	c.addMember()
	n := &sdkProbe{fakeNomad: newNomad()}
	e := openSDKProbe(t, cfg, c, n)
	// A completed real lifecycle supplies cooldown, batch and operation history.
	if _, err := e.Scale(0); err != nil {
		t.Fatal(err)
	}
	n.drained()
	step(t, e)
	c.complete()
	step(t, e)
	step(t, e)
	before := cloneRetryState(e.state)
	e.Close()
	if before.LastCompletedAt == 0 || before.Pending != nil || before.LastBatch == nil {
		t.Fatal("fixture not complete")
	}
	c.snap.LaunchConfiguration.ID = "asc-next"
	c.snap.LaunchConfiguration.ImageID = "img-next"
	c.snap.LaunchConfiguration.VersionNumber++
	c.snap.Group.LaunchConfiguration = "asc-next"
	c.writes = nil
	n.writes = nil
	return cfg, c, n, before
}
func launchRecovery(t *testing.T, cfg Config, c *fakeCloud, n *sdkProbe) *Recovery {
	t.Helper()
	r, err := OpenRecovery(cfg, c, n)
	if err != nil {
		t.Fatal(err)
	}
	r.delay = time.Millisecond
	t.Cleanup(r.Close)
	return r
}
func launchPreview(t *testing.T, r *Recovery) RecoveryPlan {
	t.Helper()
	p, err := r.PreviewLaunchConfiguration(context.Background(), "test", "change/lc", strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func TestLaunchConfigurationPreservesHistoryAndRestarts(t *testing.T) {
	cfg, c, n, before := launchFixture(t)
	r := launchRecovery(t, cfg, c, n)
	p := launchPreview(t, r)
	if _, err := r.ApplyLaunchConfiguration(context.Background(), p, p.Digest, p.MaintenanceRouteSHA256); err != nil {
		t.Fatal(err)
	}
	after := cloneRetryState(r.e.state)
	after.LaunchConfiguration = before.LaunchConfiguration
	after.RecoveryAudit = before.RecoveryAudit
	if !reflect.DeepEqual(after, before) {
		t.Fatal("acceptance rewrote history, membership, policy or cooldown")
	}
	if len(c.writes) != 0 || len(n.writes) != 0 {
		t.Fatal("maintenance mutated provider")
	}
	r.Close()
	e := openSDKProbe(t, cfg, c, n)
	if !e.ready {
		t.Fatal("accepted LC did not restore readiness", e.state.ErrorCode)
	}
	e.Close()
	// A second explicit acceptance retains a chain and supports a return to an old LC.
	c.snap.LaunchConfiguration = *before.LaunchConfiguration
	c.snap.Group.LaunchConfiguration = before.LaunchConfiguration.ID
	r = launchRecovery(t, cfg, c, n)
	p = launchPreview(t, r)
	if _, err := r.ApplyLaunchConfiguration(context.Background(), p, p.Digest, p.MaintenanceRouteSHA256); err != nil {
		t.Fatal(err)
	}
	r.Close()
	e = openSDKProbe(t, cfg, c, n)
	if !e.ready {
		t.Fatal("second acceptance not ready")
	}
	e.Close()
	// Corrupting the immutable evidence must block the next startup.
	raw, _ := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
	path := auditPath(cfg.Dir, *e.state.RecoveryAudit)
	_ = os.WriteFile(path, []byte("{}"), 0600)
	if opened, err := Open(cfg, c, n); err == nil {
		opened.Close()
		t.Fatal("corrupted acceptance audit accepted")
	}
	got, _ := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
	if !bytes.Equal(got, raw) {
		t.Fatal("invalid audit rewrote journal")
	}
}
func TestLaunchConfigurationRejectsDriftAndUnsafeState(t *testing.T) {
	for _, mode := range []string{"confirmation", "expired", "route", "journal", "cloud-lc", "cloud-busy", "cloud-desired", "cloud-spot", "cloud-policy", "cloud-prepaid", "unknown", "recovery-marker", "retry-marker", "closed", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			cfg, c, n, _ := launchFixture(t)
			r := launchRecovery(t, cfg, c, n)
			p := launchPreview(t, r)
			confirm := p.Digest
			route := p.MaintenanceRouteSHA256
			ctx := context.Background()
			switch mode {
			case "confirmation":
				confirm = "bad"
			case "expired":
				p.CreatedAt = time.Now().Add(-6 * time.Minute)
				p.ExpiresAt = p.CreatedAt.Add(5 * time.Minute)
				p.Digest = planDigest(p)
				confirm = p.Digest
			case "route":
				route = strings.Repeat("b", 64)
			case "journal":
				raw, _ := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
				_ = os.WriteFile(filepath.Join(cfg.Dir, "live.json"), append(raw, ' '), 0600)
			case "cloud-lc":
				c.snap.LaunchConfiguration.VersionNumber++
			case "cloud-busy":
				c.snap.Group.Activity = "IN_ACTIVITY"
			case "cloud-desired":
				c.snap.Group.Desired++
			case "cloud-spot":
				c.snap.Group.AllocationPolicy = "SPOT_MIXED"
			case "cloud-policy":
				c.snap.PolicyCount = 1
			case "cloud-prepaid":
				c.snap.LaunchConfiguration.ChargeType = "PREPAID"
			case "unknown":
				r.e.state.ManualReview = true
			case "recovery-marker":
				_ = os.WriteFile(filepath.Join(cfg.Dir, recoveryMarkerName), []byte("{}"), 0600)
			case "retry-marker":
				_ = os.WriteFile(filepath.Join(cfg.Dir, journalRetryQueueName), []byte("{}"), 0600)
			case "closed":
				r.Close()
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			raw, _ := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
			if _, err := r.ApplyLaunchConfiguration(ctx, p, confirm, route); err == nil {
				t.Fatal("unsafe maintenance accepted")
			}
			got, _ := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
			if !bytes.Equal(raw, got) || len(c.writes) > 0 || len(n.writes) > 0 {
				t.Fatal("rejection mutated state/provider")
			}
		})
	}
}
func TestLaunchConfigurationNonemptyMembersAndDrain(t *testing.T) {
	for _, mode := range []string{"ready", "draining", "ineligible", "missing", "changed-node", "manual-member", "terminating", "stopped", "pending"} {
		t.Run(mode, func(t *testing.T) {
			cfg := config(t)
			cfg.GroupContract = true
			c := newCloud()
			c.snap.Group.AllocationPolicy = "LAUNCH_CONFIGURATION"
			c.addMember()
			n := &sdkProbe{fakeNomad: newNomad()}
			e := openSDKProbe(t, cfg, c, n)
			if mode == "pending" {
				if _, err := e.Scale(0); err != nil {
					t.Fatal(err)
				}
			}
			e.Close()
			c.snap.LaunchConfiguration.VersionNumber++
			switch mode {
			case "draining":
				n.node.Drain = true
			case "ineligible":
				n.node.Eligibility = "ineligible"
			case "missing":
				n.missing = true
			case "changed-node":
				n.node.NodeID = "22222222-2222-2222-2222-222222222222"
			case "manual-member":
				c.snap.Members[0].CreationType = "MANUAL_ATTACH"
			case "terminating":
				c.snap.Members[0].Lifecycle = "TERMINATING"
			case "stopped":
				vm := c.snap.VMs[testInstance]
				vm.State = "STOPPED"
				c.snap.VMs[testInstance] = vm
			}
			r := launchRecovery(t, cfg, c, n)
			p, err := r.PreviewLaunchConfiguration(context.Background(), "test", "change/lc", strings.Repeat("a", 64))
			if mode == "ready" {
				if err != nil {
					t.Fatal(err)
				}
				if _, err = r.ApplyLaunchConfiguration(context.Background(), p, p.Digest, p.MaintenanceRouteSHA256); err != nil {
					t.Fatal(err)
				}
			} else if err == nil {
				t.Fatal("unstable member/pending accepted")
			}
		})
	}
}
func TestLaunchConfigurationCrashRepair(t *testing.T) {
	for _, stage := range []string{"prepared", "journal", "receipt"} {
		t.Run(stage, func(t *testing.T) {
			cfg, c, n, before := launchFixture(t)
			r := launchRecovery(t, cfg, c, n)
			p := launchPreview(t, r)
			r.checkpoint = func(at string) error {
				if at == stage {
					return errors.New("synthetic interruption")
				}
				return nil
			}
			if _, err := r.ApplyLaunchConfiguration(context.Background(), p, p.Digest, p.MaintenanceRouteSHA256); err == nil {
				t.Fatal("crash not injected")
			}
			r.Close()
			if x, err := Open(cfg, c, n); err == nil {
				x.Close()
				t.Fatal("manual incomplete transaction not blocked")
			}
			result, err := RepairRecovery(cfg, p.Digest)
			if err != nil {
				t.Fatal(err)
			}
			expected := "committed"
			if stage == "prepared" {
				expected = "aborted"
			}
			if result.Status != expected {
				t.Fatal(result.Status)
			}
			e := openSDKProbe(t, cfg, c, n)
			if stage == "prepared" {
				if e.ready || !reflect.DeepEqual(e.state.LaunchConfiguration, before.LaunchConfiguration) {
					t.Fatal("aborted transaction accepted LC")
				}
			} else if !e.ready {
				t.Fatal("committed repair not ready", e.state.ErrorCode)
			}
			e.Close()
			if len(c.writes) > 0 || len(n.writes) > 0 {
				t.Fatal("repair mutated provider")
			}
		})
	}
}
