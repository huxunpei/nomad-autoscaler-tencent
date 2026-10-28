package live

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
	"os"
	"path/filepath"
	"testing"

	"github.com/hashicorp/go-hclog"
)

func groupContractFixture(t *testing.T, count, min, max int64, protect bool) (Config, *multiCloud, *multiNomad, *Engine) {
	t.Helper()
	c := &multiCloud{fakeCloud: newCloud(), activities: map[string]tencentcloud.Activity{}}
	c.snap.Group.Min = min
	c.snap.Group.Max = max
	n := &multiNomad{nodes: map[string]nomadclient.Node{}, load: map[string]int{}}
	for i := int64(0); i < max+1; i++ {
		id := fmt.Sprintf("ins-multi%d", i)
		n.nodes[id] = nomadclient.Node{Identity: nomadclient.Identity{NodeID: fmt.Sprintf("%08d-1111-1111-1111-111111111111", i+1), InstanceID: id, GroupID: testGroup, Region: testRegion}, Status: "ready", Eligibility: "eligible"}
	}
	if count > 0 {
		_, _ = c.SetDesired(context.Background(), testGroup, count)
		c.writes = nil
	}
	cfg := config(t)
	cfg.GroupContract = true
	cfg.ScaleInProtection = protect
	e := openMulti(t, cfg, c, n)
	return cfg, c, n, e
}
func TestGroupContractClampsLiveBoundsAndSurvivesChangedLimits(t *testing.T) {
	cfg, c, n, e := groupContractFixture(t, 1, 1, 5, false)
	var warnings bytes.Buffer
	e.cfg.Logger = hclog.New(&hclog.LoggerOptions{Output: &warnings, JSONFormat: true})
	if changed, err := e.Scale(0); changed || err != nil {
		t.Fatalf("below Min should be current-count no-op: %v %v", changed, err)
	}
	if changed, err := e.Scale(99); !changed || err != nil {
		t.Fatal(changed, err)
	}
	if c.snap.Group.Desired != 5 || c.snap.Group.Min != 1 || c.snap.Group.Max != 5 || len(c.writes) != 1 {
		t.Fatal("did not clamp target without changing cloud limits")
	}
	decoder := json.NewDecoder(&warnings)
	for _, want := range [][2]int64{{0, 1}, {99, 5}} {
		var entry map[string]interface{}
		if err := decoder.Decode(&entry); err != nil {
			t.Fatal(err)
		}
		if entry["@level"] != "warn" || entry["group"] != testGroup || entry["old_count"] != float64(1) || entry["requested_count"] != float64(want[0]) || entry["effective_count"] != float64(want[1]) || entry["min"] != float64(1) || entry["max"] != float64(5) {
			t.Fatalf("missing clamp warning context: %#v", entry)
		}
	}
	step(t, e)
	e.Close()
	c.snap.Group.Max = 150
	e = openMulti(t, cfg, c, n)
	if changed, err := e.Scale(0); !changed || err != nil {
		t.Fatal(changed, err)
	}
	if e.state.Batch.Target != 1 || len(e.state.Batch.Selected) != 4 {
		t.Fatal("restart used stale bounds or selected below Min")
	}
}
func TestGroupContractNoPrototypeCapacityCeiling(t *testing.T) {
	const capacity = 2000
	cfg, c, n, e := groupContractFixture(t, 0, 0, capacity, false)
	if changed, err := e.Scale(3000); !changed || err != nil {
		t.Fatal(changed, err)
	}
	if c.snap.Group.Desired != capacity || len(c.snap.Members) != capacity {
		t.Fatal("legal cloud capacity was rejected or truncated")
	}
	step(t, e)
	e.Close()
	e = openMulti(t, cfg, c, n)
	if s := multiState(t, e); s.Version != 3 || len(s.Members) != capacity || s.MaxCapacity != 0 {
		t.Fatal("large group journal did not survive restart")
	}
}
func TestGroupContractZeroCapacityGroup(t *testing.T) {
	_, c, _, e := groupContractFixture(t, 0, 0, 0, false)
	if changed, err := e.Scale(10); changed || err != nil || c.writeCount() != 0 {
		t.Fatalf("zero-sized group: %v %v", changed, err)
	}
}
func TestGroupContractProtectionFiltersInstancesNotWholeGroup(t *testing.T) {
	for _, tc := range []struct {
		name            string
		as, vm, enabled bool
		want            string
	}{{"as-on", true, false, true, "ins-multi1"}, {"as-off", true, false, false, "ins-multi0"}, {"cvm-on", false, true, true, "ins-multi1"}, {"cvm-off", false, true, false, "ins-multi1"}} {
		t.Run(tc.name, func(t *testing.T) {
			_, c, _, e := groupContractFixture(t, 2, 0, 2, tc.enabled)
			c.snap.Members[0].Protected = tc.as
			vm := c.snap.VMs["ins-multi0"]
			vm.Protected = tc.vm
			c.snap.VMs[vm.ID] = vm
			if changed, err := e.Scale(1); !changed || err != nil {
				t.Fatal(changed, err)
			}
			if e.state.Pending.InstanceID != tc.want {
				t.Fatalf("selected %s, want %s", e.state.Pending.InstanceID, tc.want)
			}
			completeMultiDelete(t, e, c)
			if len(c.writes) != 1 || c.writes[0] != "remove:"+tc.want {
				t.Fatal("wrong deletion or duplicate capacity write")
			}
		})
	}
}
func TestGroupContractRechecksLimitsAndProtectionAfterDrain(t *testing.T) {
	for _, mode := range []string{"min", "max", "as", "cvm"} {
		t.Run(mode, func(t *testing.T) {
			cfg, c, n, e := groupContractFixture(t, 2, 0, 2, true)
			if _, err := e.Scale(1); err != nil {
				t.Fatal(err)
			}
			id := e.state.Pending.InstanceID
			switch mode {
			case "min":
				c.snap.Group.Min = 2
			case "max":
				c.snap.Group.Max = 0
			case "as":
				for i := range c.snap.Members {
					if c.snap.Members[i].ID == id {
						c.snap.Members[i].Protected = true
					}
				}
			case "cvm":
				vm := c.snap.VMs[id]
				vm.Protected = true
				c.snap.VMs[id] = vm
			}
			e.Close()
			e = openMulti(t, cfg, c, n)
			_ = e.reconcileOnce()
			if c.writeCount() != 0 || e.state.Pending != nil || n.nodes[id].Eligibility != "eligible" || e.state.LastBatch == nil || e.state.LastBatch.Phase != "failed" {
				t.Fatal("guard change did not abort deletion and restore the drained node")
			}
		})
	}
}
func TestGroupContractCloudRetryDoesNotBlockOrDuplicateWrites(t *testing.T) {
	_, c, _, e := groupContractFixture(t, 2, 0, 2, false)
	c.snap.Group.RetryPolicy = "INCREMENTAL_INTERVALS"
	if _, err := e.Scale(1); err != nil {
		t.Fatal(err)
	}
	step(t, e)
	if c.writeCount() != 1 {
		t.Fatal("retry policy prevented precise delete")
	}
	for i := 0; i < 3; i++ {
		_ = e.reconcileOnce()
		if changed, _ := e.Scale(1); changed {
			t.Fatal("pending cloud activity stacked an action")
		}
	}
	if c.writeCount() != 1 {
		t.Fatal("plugin resubmitted cloud retry")
	}
}
func TestGroupContractIdleMigrationPreservesHistoryAndRejectsPendingLegacy(t *testing.T) {
	for _, active := range []bool{false, true} {
		t.Run(fmt.Sprint(active), func(t *testing.T) {
			cfg, c, n, e := multiFixture(t, 2, 2)
			if active {
				if _, err := e.Scale(1); err != nil {
					t.Fatal(err)
				}
			}
			e.Close()
			path := filepath.Join(cfg.Dir, "live.json")
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			cfg.GroupContract = true
			candidate, err := Open(cfg, c, n)
			if active {
				if candidate != nil {
					candidate.Close()
				}
				if err == nil {
					t.Fatal("reinterpreted active legacy operation")
				}
				after, _ := os.ReadFile(path)
				if string(after) != string(before) {
					t.Fatal("rejected migration changed legacy journal")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer candidate.Close()
			backup, err := os.ReadFile(filepath.Join(cfg.Dir, "live-v2-before-group-contract.backup.json"))
			if err != nil || string(backup) != string(before) {
				t.Fatal("migration did not retain exact old state")
			}
			var state State
			raw, _ := os.ReadFile(path)
			if json.Unmarshal(raw, &state) != nil || state.Version != 3 || state.MaxCapacity != 0 {
				t.Fatal("missing group contract journal")
			}
			candidate.Close()
			cfg.GroupContract = false
			old, err := Open(cfg, c, n)
			if old != nil {
				old.Close()
			}
			if err == nil {
				t.Fatal("legacy controller accepted new semantics")
			}
		})
	}
}
