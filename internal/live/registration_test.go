package live

import (
	"bytes"
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/telemetry"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

type registrationNomad struct {
	*multiNomad
	readError error
	duplicate bool
}

func (n *registrationNomad) ObserveMembers(_ context.Context, _, _ string, ids []string) ([]nomadclient.Node, error) {
	if n.readError != nil {
		return nil, n.readError
	}
	out := []nomadclient.Node{}
	for _, id := range ids {
		if node, found := n.nodes[id]; found {
			out = append(out, copyJSON(node))
		}
	}
	if n.duplicate && len(out) > 0 {
		out = append(out, out[0])
	}
	return out, nil
}

func registrationFixture(t *testing.T) (Config, *multiCloud, *registrationNomad) {
	t.Helper()
	c := &multiCloud{fakeCloud: newCloud(), activities: map[string]tencentcloud.Activity{}}
	c.snap.Group.Max = 5
	_, _ = c.SetDesired(context.Background(), testGroup, 2)
	c.writes = nil
	for i := range c.snap.Members {
		c.snap.Members[i].AddedAt = time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)
	}
	n := &registrationNomad{multiNomad: &multiNomad{nodes: map[string]nomadclient.Node{
		"ins-multi1": {Identity: nomadclient.Identity{InstanceID: "ins-multi1", NodeID: testNode, GroupID: testGroup, Region: testRegion}, Status: "ready", Eligibility: "eligible"},
	}, load: map[string]int{}}}
	cfg := config(t)
	cfg.GroupContract = true
	cfg.StabilizationInterval = time.Millisecond
	return cfg, c, n
}

func TestRegistrationGraceClockBoundary(t *testing.T) {
	now := time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name       string
		age, grace time.Duration
		state      string
		invalid    bool
	}{
		{"before-default", 30*time.Minute - time.Nanosecond, 30 * time.Minute, "waiting", false},
		{"exact-default", 30 * time.Minute, 30 * time.Minute, "timed_out", false},
		{"custom", 20 * time.Minute, 15 * time.Minute, "timed_out", false},
		{"long-custom", time.Hour, 2 * time.Hour, "waiting", false},
		{"future", -time.Second, 30 * time.Minute, "unknown", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			added := now.Add(-tc.age).Format(time.RFC3339Nano)
			e := &Engine{cfg: Config{RegistrationGracePeriod: tc.grace}, state: State{Members: map[string]Member{testInstance: {ID: testInstance, RegistrationState: "unknown"}}}}
			e.classifyMissingRegistrations(tencentcloud.Snapshot{At: now, Members: []tencentcloud.Member{{ID: testInstance, AddedAt: added}}})
			if e.state.Members[testInstance].RegistrationState != tc.state || (e.observeError != nil) != tc.invalid {
				t.Fatal(e.state.Members, e.observeError)
			}
		})
	}
}

func TestRegistrationGraceWaitExpiryRecoveryAndDiagnostics(t *testing.T) {
	cfg, c, n := registrationFixture(t)
	cfg.RegistrationGracePeriod = 2 * time.Hour
	cfg.Metrics = telemetry.New()
	var logs bytes.Buffer
	cfg.Logger = hclog.New(&hclog.LoggerOptions{Output: &logs, Level: hclog.Debug})
	e := openMulti(t, cfg, c, n)
	state, ready, err := e.Snapshot()
	if err != nil || ready || state.Members["ins-multi0"].RegistrationState != "waiting" || len(state.Members) != 2 {
		t.Fatal("grace did not protect new member")
	}
	if changed, err := e.Scale(3); changed || !errors.Is(err, ErrNotReady) {
		t.Fatal("waiting gate bypassed")
	}
	e.Close()
	// Changing the configured budget recomputes from the same AS timestamp.
	cfg.RegistrationGracePeriod = 30 * time.Minute
	e = openMulti(t, cfg, c, n)
	state, ready, err = e.Snapshot()
	if err != nil || !ready || state.Members["ins-multi0"].RegistrationState != "timed_out" {
		t.Fatal("expiry did not release group")
	}
	if !strings.Contains(logs.String(), "ins-multi0") || !strings.Contains(logs.String(), "waited_seconds") {
		t.Fatal("missing safe timeout diagnostic")
	}
	for i := 0; i < 3; i++ {
		step(t, e)
	}
	if strings.Count(logs.String(), "grace period expired") != 1 {
		t.Fatal("timeout warning repeated every poll")
	}
	scrape := func() string {
		w := httptest.NewRecorder()
		cfg.Metrics.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
		return w.Body.String()
	}
	if !strings.Contains(scrape(), `tencent_as_registration_members{state="timed_out"} 1`) {
		t.Fatal("missing timeout gauge")
	}
	n.readError = errors.New("PRIVATE_ERROR")
	_ = e.reconcileOnce()
	state, ready, err = e.Snapshot()
	if err != nil || ready || state.Members["ins-multi0"].RegistrationState != "unknown" ||
		!strings.Contains(scrape(), `tencent_as_registration_members{state="timed_out"} 0`) ||
		!strings.Contains(scrape(), "tencent_as_registration_observation_failed_groups 1") {
		t.Fatal("failed read became timeout")
	}
	n.readError = nil
	n.nodes["ins-multi0"] = nomadclient.Node{Identity: nomadclient.Identity{NodeID: "22222222-1111-1111-1111-111111111111", InstanceID: "ins-multi0", GroupID: testGroup, Region: testRegion}, Status: "ready", Eligibility: "eligible"}
	step(t, e)
	state, ready, err = e.Snapshot()
	if err != nil || !ready || state.Members["ins-multi0"].RegistrationState != "registered" ||
		!strings.Contains(logs.String(), "Nomad registration recovered") || strings.Contains(logs.String(), "PRIVATE_ERROR") {
		t.Fatal("late join not recovered")
	}
	if c.writeCount() != 0 || len(n.writes) != 0 {
		t.Fatal("observer mutated providers")
	}
	n.duplicate = true
	_ = e.reconcileOnce()
	state, ready, err = e.Snapshot()
	if err != nil || ready || state.Members["ins-multi0"].RegistrationState != "unknown" {
		t.Fatal("duplicate mapping released group")
	}
	e.Close()
	if !strings.Contains(scrape(), `tencent_as_registration_members{state="unknown"} 0`) {
		t.Fatal("closed route leaked gauges")
	}
}

func TestRegistrationGraceExpiredMemberNeverSelectedAndOtherScalingContinues(t *testing.T) {
	for _, direction := range []string{"grow", "shrink"} {
		t.Run(direction, func(t *testing.T) {
			cfg, c, n := registrationFixture(t)
			e := openMulti(t, cfg, c, n)
			if changed, err := e.Scale(0); changed || err == nil || c.writeCount() != 0 || len(n.writes) != 0 {
				t.Fatal("insufficient safe candidates changed providers")
			}
			if direction == "grow" {
				if changed, err := e.Scale(3); !changed || err != nil {
					t.Fatal("expired member blocked grow", err)
				}
				step(t, e)
				if c.writeCount() != 1 || len(n.writes) != 0 {
					t.Fatal("grow replayed or drained")
				}
			} else {
				if changed, err := e.Scale(1); !changed || err != nil {
					t.Fatal("expired member blocked safe shrink", err)
				}
				if multiState(t, e).Pending.InstanceID != "ins-multi1" {
					t.Fatal("missing VM selected")
				}
				completeMultiDelete(t, e, c)
				step(t, e) // Readiness is refreshed after terminal publication.
				s, ready, err := e.Snapshot()
				if err != nil || !ready || s.Batch != nil || len(s.Members) != 1 || s.Members["ins-multi0"].Joined {
					t.Fatal("unrelated shrink did not settle")
				}
				if len(c.writes) != 1 || c.writes[0] != "remove:ins-multi1" {
					t.Fatal("wrong cloud deletion")
				}
			}
		})
	}
}

func TestRegistrationGraceCloudAndExistingNodeGuards(t *testing.T) {
	for _, which := range []string{"activity", "desired", "booting", "manual", "external-drain", "future", "read-failure"} {
		t.Run(which, func(t *testing.T) {
			cfg, c, n := registrationFixture(t)
			switch which {
			case "activity":
				c.snap.Group.Activity = "IN_ACTIVITY"
			case "desired":
				c.snap.Group.Desired = 3
			case "booting":
				v := c.snap.VMs["ins-multi0"]
				v.State = "STARTING"
				c.snap.VMs[v.ID] = v
			case "manual":
				c.snap.Members[0].CreationType = "MANUAL_ATTACH"
			case "external-drain":
				v := n.nodes["ins-multi1"]
				v.Eligibility = "ineligible"
				n.nodes[v.InstanceID] = v
			case "future":
				c.snap.Members[0].AddedAt = time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
			case "read-failure":
				n.readError = errors.New("synthetic read failure")
			}
			e := openMulti(t, cfg, c, n)
			if _, ready, _ := e.Snapshot(); ready {
				t.Fatal("registration expiry bypassed guard")
			}
			if changed, err := e.Scale(3); changed || err == nil || c.writeCount() != 0 || len(n.writes) != 0 {
				t.Fatal("guard allowed write")
			}
		})
	}
}
