package live

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

type growthRegistrationCloud struct{ *activityGrowthCloud }

func (c *growthRegistrationCloud) ActivitiesSince(context.Context, string, time.Time) ([]tencentcloud.Activity, error) {
	out := []tencentcloud.Activity{}
	for _, a := range c.activities {
		out = append(out, copyJSON(a))
	}
	return out, nil
}
func growthRegistrationFixture(t *testing.T, lost bool) (Config, *growthRegistrationCloud, *registrationNomad, *Engine) {
	t.Helper()
	cfg, base, old, e := activityGrowthFixture(t, "SUCCESSFUL", 0)
	e.Close()
	cfg.RegistrationGracePeriod = 30 * time.Minute
	c := &growthRegistrationCloud{base}
	c.activityOnly = !lost
	c.lost = lost
	delete(old.nodes, "ins-multi1")
	delete(old.nodes, "ins-multi2")
	n := &registrationNomad{multiNomad: old}
	e = openMulti(t, cfg, c, n)
	if _, err := legacyActivityScale(t, e, 3, false); err == nil || len(c.increments) != 1 {
		t.Fatal("lost acknowledgement not established")
	}
	return cfg, c, n, e
}
func requireGrowthRegistrationRecovered(t *testing.T, e *Engine, c *growthRegistrationCloud) {
	t.Helper()
	s, ready, err := e.Snapshot()
	if err != nil || s.Pending != nil || s.Batch != nil || s.LastBatch == nil || s.LastBatch.Phase != "completed" || s.LastCompletedAt == 0 || len(s.Members) != 3 || ready || len(c.increments) != 1 || s.RecoveryAudit == nil || e.checkRecoveryAudit() != nil {
		t.Fatal("confirmed cloud growth remained unknown or registration was reported ready", err, ready)
	}
	record, err := readRecord(e.cfg.Dir, *s.RecoveryAudit)
	if err != nil || record.Before.Pending == nil || record.After.Pending != nil || len(record.Evidence) != 2 {
		t.Fatal("missing durable recovery proof", err)
	}
	for _, ev := range record.Evidence {
		if ev.Cloud.Group.Count != 3 || ev.Activity.Status != "SUCCESSFUL" {
			t.Fatal("incomplete cloud completion proof")
		}
	}
}
func TestGrowthRegistrationRecoveryWaitExpiryAndLateJoin(t *testing.T) {
	for _, lost := range []bool{false, true} {
		name := "recorded"
		if lost {
			name = "discovered"
		}
		t.Run(name, func(t *testing.T) {
			cfg, c, n, e := growthRegistrationFixture(t, lost)
			e.Close()
			e = openMulti(t, cfg, c, n)
			step(t, e)
			requireGrowthRegistrationRecovered(t, e, c)
			step(t, e)
			s, ready, err := e.Snapshot()
			if err != nil || ready || s.Members["ins-multi1"].RegistrationState != "waiting" || s.Members["ins-multi2"].RegistrationState != "waiting" {
				t.Fatal("initial registration wait not preserved")
			}
			completed := s.LastCompletedAt
			e.Close()
			cfg.RegistrationGracePeriod = time.Nanosecond
			e = openMulti(t, cfg, c, n)
			s, ready, err = e.Snapshot()
			if err != nil || !ready || s.Members["ins-multi1"].RegistrationState != "timed_out" || s.LastCompletedAt != completed {
				t.Fatal("expiry did not release stable cloud without replay/reset")
			}
			n.nodes["ins-multi1"] = nomadclient.Node{Identity: nomadclient.Identity{NodeID: "22222222-1111-1111-1111-111111111111", InstanceID: "ins-multi1", GroupID: testGroup, Region: testRegion}, Status: "ready", Eligibility: "eligible"}
			step(t, e)
			s, ready, err = e.Snapshot()
			if err != nil || !ready || !s.Members["ins-multi1"].EverJoined || s.Members["ins-multi1"].RegistrationState != "registered" || s.LastCompletedAt != completed || len(c.increments) != 1 || len(n.writes) != 0 {
				t.Fatal("late join did not recover without provider writes")
			}
		})
	}
}
func TestGrowthRegistrationRecoveryManualPreviewAndApply(t *testing.T) {
	cfg, c, n, e := growthRegistrationFixture(t, false)
	e.Close()
	r, err := OpenRecovery(cfg, c, n)
	if err != nil {
		t.Fatal(err)
	}
	r.delay = 0
	defer r.Close()
	plan, err := r.Preview(context.Background(), "asa-grow1", "operator", "test/registration")
	if err != nil {
		t.Fatal("manual cloud success recovery requires Nomad join", err)
	}
	if _, err = r.Apply(context.Background(), plan, plan.Digest); err != nil {
		t.Fatal(err)
	}
	r.Close()
	e = openMulti(t, cfg, c, n)
	requireGrowthRegistrationRecovered(t, e, c)
	if e.state.Members["ins-multi1"].RegistrationState != "waiting" || len(n.writes) != 0 {
		t.Fatal("manual recovery bypassed join wait")
	}
}
func TestGrowthRegistrationRecoveryPreservesMissingPreviouslyJoinedNode(t *testing.T) {
	cfg, c, n, e := growthRegistrationFixture(t, false)
	original := n.nodes["ins-multi0"]
	delete(n.nodes, "ins-multi0")
	_ = e.reconcileOnce()
	requireGrowthRegistrationRecovered(t, e, c)
	e.Close()
	cfg.RegistrationGracePeriod = time.Nanosecond
	e = openMulti(t, cfg, c, n)
	s, ready, _ := e.Snapshot()
	if ready || !s.Members["ins-multi0"].EverJoined || s.Members["ins-multi0"].RegistrationState != "unknown" {
		t.Fatal("previous registration disappeared across recovery")
	}
	n.nodes["ins-multi0"] = original
	step(t, e)
	if _, ready, err := e.Snapshot(); err != nil || !ready {
		t.Fatal("restored original identity did not release readiness", err)
	}
}
func TestGrowthRegistrationRecoveryNomadFailureDoesNotInventReadiness(t *testing.T) {
	for _, mode := range []string{"read-failure", "duplicate", "ineligible"} {
		t.Run(mode, func(t *testing.T) {
			_, c, n, e := growthRegistrationFixture(t, false)
			switch mode {
			case "read-failure":
				n.readError = errors.New("synthetic unavailable")
			case "duplicate":
				n.duplicate = true
			case "ineligible":
				v := n.nodes["ins-multi0"]
				v.Eligibility = "ineligible"
				n.nodes[v.InstanceID] = v
			}
			_ = e.reconcileOnce()
			requireGrowthRegistrationRecovered(t, e, c)
			_ = e.reconcileOnce()
			if _, ready, _ := e.Snapshot(); ready || len(c.increments) != 1 || len(n.writes) != 0 {
				t.Fatal("cloud completion invented healthy Nomad state")
			}
		})
	}
}
func TestGrowthRegistrationRecoveryStillRequiresCloudEvidence(t *testing.T) {
	for _, mode := range []string{"booting", "desired", "wrong-member", "running-activity"} {
		t.Run(mode, func(t *testing.T) {
			_, c, _, e := growthRegistrationFixture(t, false)
			switch mode {
			case "booting":
				v := c.snap.VMs["ins-multi1"]
				v.State = "STARTING"
				c.snap.VMs[v.ID] = v
			case "desired":
				c.snap.Group.Desired = 4
			case "wrong-member":
				a := c.activities["asa-grow1"]
				a.RelatedInstances[0].ID = "ins-unrelated"
				c.activities[a.ID] = a
			case "running-activity":
				a := c.activities["asa-grow1"]
				a.Status = "RUNNING"
				a.EndedAt = ""
				c.activities[a.ID] = a
			}
			_ = e.reconcileOnce()
			if e.state.Pending == nil || !e.state.Pending.Unknown || e.state.LastCompletedAt != 0 || len(c.increments) != 1 {
				t.Fatal("incomplete cloud proof released unknown growth")
			}
		})
	}
}

func TestGrowthRegistrationRecoveryAuditResumesAfterInterruption(t *testing.T) {
	for _, point := range []string{"prepared", "journal", "receipt"} {
		t.Run(point, func(t *testing.T) {
			cfg, c, n, e := growthRegistrationFixture(t, false)
			hit := false
			e.recoveryCheckpoint = func(at string) error {
				if at == point {
					hit = true
					return errors.New("synthetic interruption")
				}
				return nil
			}
			_ = e.reconcileOnce()
			if !hit {
				t.Fatal("audit checkpoint not reached")
			}
			e.Close()
			e = openMulti(t, cfg, c, n)
			if e.state.Pending != nil {
				step(t, e)
			}
			requireGrowthRegistrationRecovered(t, e, c)
			step(t, e)
			if e.state.Members["ins-multi1"].RegistrationState != "waiting" || len(n.writes) != 0 {
				t.Fatal("interrupted recovery lost registration guard")
			}
		})
	}
}
func TestGrowthRegistrationRecoveryOldAbsoluteIntentKeepsItsAction(t *testing.T) {
	cfg, base, old, e := groupContractFixture(t, 1, 0, 5, false)
	e.Close()
	c := &discoveryCloud{multiCloud: base, dir: cfg.Dir}
	delete(old.nodes, "ins-multi1")
	delete(old.nodes, "ins-multi2")
	n := &registrationNomad{multiNomad: old}
	e = openMulti(t, cfg, c, n)
	startUnidentified(t, e, c, false)
	completeUnidentified(e, c, false)
	step(t, e)
	step(t, e)
	s, ready, err := e.Snapshot()
	if err != nil || ready || s.Pending != nil || s.LastCompletedAt == 0 || s.LastOperation.CloudIntent.Action != "ModifyDesiredCapacity" || len(c.writes) != 1 || s.Members["ins-multi1"].RegistrationState != "waiting" {
		t.Fatal("old absolute intent did not retain its cloud completion contract")
	}
}
func TestGrowthRegistrationRecoveryLegacyMappingRemainsStrict(t *testing.T) {
	cfg, c, n := recoveryFixture(t, false)
	n.missing = true
	r := recoverySession(t, cfg, c, n)
	if _, err := r.Preview(context.Background(), testActivity, "operator", "test/legacy"); err == nil {
		t.Fatal("legacy recovery lost its strict mapping contract")
	}
}
