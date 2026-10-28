package live

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadsdk"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/targetconfig"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/tencentcloud"
)

const legacyMappedKey = "custom_cvm_identity"

func legacyMappedSource(t *testing.T, name string, d time.Duration) (Config, *settlementCloud, *legacyHTTPHarness, State, *nomadsdk.Adapter, []byte) {
	t.Helper()
	t.Setenv("NOMAD_ALLOC_ID", "")
	old, model, raw := historicalCustomInput(t, name, 5*time.Second)
	cfg := Config{Dir: filepath.Join(t.TempDir(), "journal"), GroupID: old.GroupID, Region: old.Region, GroupContract: true, DrainDeadline: d, ReconcileInterval: time.Hour, DisableReconciliation: true}
	if err := os.Mkdir(cfg.Dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cfg.Dir, "live.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	c := &settlementCloud{multiCloud: &multiCloud{fakeCloud: &fakeCloud{snap: model.Snapshot}, activities: model.Activities}}
	h := &legacyHTTPHarness{nodes: map[string]nomadclient.Node{}, purged: map[string]bool{}, mapping: map[string]string{}, metaKey: legacyMappedKey, cloud: c, directory: cfg.Dir, deadline: d, t: t}
	for _, n := range model.Nodes {
		n.ModifyIndex = 101
		h.nodes[n.InstanceID] = n
	}
	server := httptest.NewServer(http.HandlerFunc(h.serve))
	t.Cleanup(server.Close)
	client, err := nomadclient.New(nomadclient.Config{Address: server.URL, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	client, err = client.WithScope(old.GroupID, old.Region, legacyMappedKey)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := (targetconfig.Agent{}).Policy(map[string]string{"tencent_group_id": old.GroupID, "node_drain_deadline": d.String(), "node_purge": "true", "node_drain_ignore_system_jobs": "true", "instance_id_meta_key": legacyMappedKey})
	if err != nil {
		t.Fatal(err)
	}
	return cfg, c, h, old, nomadsdk.New(client, policy), raw
}

func TestLegacyCustomMappingAdoptionAndEffect(t *testing.T) {
	for _, d := range []time.Duration{15 * time.Minute, 0, -time.Second} {
		for _, name := range []string{"v1-grow", "v2-grow", "v1-draining", "v1-removing", "v1-delete-unknown", "v2-removing", "v2-delete-unknown", "v2-between-children"} {
			t.Run(d.String()+"/"+name, func(t *testing.T) {
				cfg, c, h, old, adapter, raw := legacyMappedSource(t, name, d)
				e, err := Open(cfg, c, adapter)
				if err != nil {
					t.Fatal("custom mapping adoption missing", err)
				}
				if e.state.NomadInstanceIDMetaKey != legacyMappedKey || c.writeCount() != 0 || len(h.writes) != 0 || e.checkRecoveryAudit() != nil {
					e.Close()
					t.Fatal("adoption rewrote provider or missing audit")
				}
				if e.state.Pending != nil && old.Pending != nil && (e.state.Pending.NodeID != old.Pending.NodeID || e.state.Pending.InstanceID != old.Pending.InstanceID || e.state.Pending.SDKLifecycle) {
					e.Close()
					t.Fatal("original identity reselected")
				}
				backup, err := os.ReadFile(filepath.Join(cfg.Dir, "live-v"+string(rune('0'+old.Version))+"-before-group-contract.backup.json"))
				if err != nil || !reflect.DeepEqual(backup, raw) {
					e.Close()
					t.Fatal("source backup changed")
				}
				e.Close()
				reader, err := OpenRecovery(cfg, c, adapter)
				if err != nil {
					t.Fatal(err)
				}
				reader.Close()
				e, err = Open(cfg, c, adapter)
				if err != nil {
					t.Fatal(err)
				}
				defer e.Close()
				if name == "v1-draining" {
					h.mu.Lock()
					n := h.nodes[testInstance]
					n.Drain = false
					n.HasDrainStrategy = false
					n.LastDrain.Status = "complete"
					h.nodes[testInstance] = n
					h.mu.Unlock()
				}
				runLegacyToTerminal(t, e, c)
				if e.state.LastBatch.Phase != "completed" || e.state.LastCompletedAt == 0 || e.checkRecoveryAudit() != nil {
					t.Fatal("custom mapping lifecycle not completed")
				}
				if old.Batch != nil && len(old.Batch.Results) > 0 && !reflect.DeepEqual(e.state.LastBatch.Results[:len(old.Batch.Results)], old.Batch.Results) {
					t.Fatal("old prefix rewritten")
				}
				if name == "v1-removing" && c.writeCount() != 0 {
					t.Fatal("known deletion replayed")
				}
				completed := e.state.LastCompletedAt
				e.Close()
				reader, err = OpenRecovery(cfg, c, adapter)
				if err != nil {
					t.Fatal("completed mapped recovery reader", err)
				}
				if reader.e.state.LastCompletedAt != completed {
					t.Fatal("completed mapped timestamp changed")
				}
				reader.Close()

			})
		}
	}
}

func TestLegacyCustomMappingAllHistoricalShapes(t *testing.T) {
	entries, err := os.ReadDir("testdata/historical-custom-d24948f")
	if err != nil || len(entries) != 13 {
		t.Fatal("missing historical sources")
	}
	for _, d := range []time.Duration{15 * time.Minute, 0, -time.Second} {
		for _, entry := range entries {
			t.Run(d.String()+"/"+entry.Name(), func(t *testing.T) {
				cfg, c, h, old, adapter, _ := legacyMappedSource(t, entry.Name(), d)
				e, err := Open(cfg, c, adapter)
				if err != nil {
					t.Fatal(err)
				}
				defer e.Close()
				proof := e.state.LegacyContract.Custom.MappingProof
				if !legacyMappingBindingMatches(old, *e.state.LegacyContract.Custom) || proof == nil || c.writeCount() != 0 || len(h.writes) != 0 || e.state.LastCompletedAt != 0 {
					t.Fatal("missing proof or manufactured effects")
				}
			})
		}
	}
}

func TestLegacyCustomMappingAmbiguityRejectedAndCanRetry(t *testing.T) {
	for _, mode := range []string{"missing", "different-instance", "different-node", "duplicate", "unavailable", "revision-change"} {
		t.Run(mode, func(t *testing.T) {
			cfg, c, h, _, adapter, raw := legacyMappedSource(t, "v1-draining", 0)
			original := copyJSON(h.nodes)
			h.mu.Lock()
			n := h.nodes[testInstance]
			switch mode {
			case "missing":
				h.mapping[n.NodeID] = ""
			case "different-instance":
				h.mapping[n.NodeID] = "ins-other"
			case "different-node":
				n.NodeID = "22222222-2222-2222-2222-222222222222"
				h.nodes[testInstance] = n
			case "duplicate":
				n.NodeID = "22222222-2222-2222-2222-222222222222"
				h.nodes["alias"] = n
			case "unavailable":
				h.readUnavailable = true
			case "revision-change":
				h.unstable = true
			}
			h.mu.Unlock()
			e, err := Open(cfg, c, adapter)
			if err == nil {
				e.Close()
				t.Fatal("ambiguous mapping adopted")
			}
			if err.Error() != "LegacyMappingUnavailable" {
				t.Fatal("wrong mapping diagnostic", err)
			}
			after, _ := os.ReadFile(filepath.Join(cfg.Dir, "live.json"))
			if !reflect.DeepEqual(after, raw) || c.writeCount() != 0 || len(h.writes) != 0 {
				t.Fatal("failed proof changed journal/provider")
			}
			if _, err := os.Lstat(filepath.Join(cfg.Dir, "live-v1-before-group-contract.backup.json")); !os.IsNotExist(err) {
				t.Fatal("unproven mapping published migration backup")
			}
			h.mu.Lock()
			h.nodes = original
			h.mapping = map[string]string{}
			h.readUnavailable = false
			h.unstable = false
			h.mu.Unlock()
			e, err = Open(cfg, c, adapter)
			if err != nil {
				t.Fatal("repaired mapping did not initialize", err)
			}
			e.Close()
		})
	}
}

func TestLegacyCustomMappingProofDoesNotAuthorizeLaterIdentityChange(t *testing.T) {
	cfg, c, h, _, adapter, _ := legacyMappedSource(t, "v1-draining", -time.Second)
	e, err := Open(cfg, c, adapter)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	h.mu.Lock()
	h.mapping[h.nodes[testInstance].NodeID] = "ins-other"
	h.mu.Unlock()
	e.EnableReconciliation()
	_ = e.reconcileOnce()
	if c.writeCount() != 0 || len(h.writes) != 0 || e.state.Pending == nil {
		t.Fatal("stale mapping proof authorized a mutation")
	}
	h.mu.Lock()
	h.mapping = map[string]string{}
	h.mu.Unlock()
	runLegacyToTerminal(t, e, c)
	if c.writeCount() != 0 || e.state.LastBatch.Phase != "failed" || e.state.LastCompletedAt != 0 || !reflect.DeepEqual(h.writes, []string{"restore:" + testInstance}) {
		t.Fatal("failed mapping did not safely restore original node", e.state.LastEvent, h.writes)
	}
}

func TestLegacyCustomMappingMissingNodeStillNeedsCloudSettlement(t *testing.T) {
	cfg, c, h, _, adapter, _ := legacyMappedSource(t, "v1-removing", 0)
	h.purged[testInstance] = true
	c.terminal(true)
	e, err := Open(cfg, c, adapter)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	p := e.state.LegacyContract.Custom.MappingProof
	if len(p.Nodes) != 1 || !p.Nodes[0].Missing || p.Nodes[0].ModifyIndex != 0 {
		t.Fatal("missing node not represented explicitly")
	}
	runLegacyToTerminal(t, e, c)
	if c.writeCount() != 0 || len(h.writes) != 0 || e.state.LastBatch.Phase != "completed" || e.state.LastCompletedAt == 0 || e.checkRecoveryAudit() != nil {
		t.Fatal("already absent node caused replay or lost post")
	}
}

func TestLegacyCustomMappingAuditRejectsForgedProof(t *testing.T) {
	cfg, c, _, _, adapter, _ := legacyMappedSource(t, "v1-draining", 0)
	e, err := Open(cfg, c, adapter)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	record, err := readRecord(cfg.Dir, *e.state.RecoveryAudit)
	if err != nil || !validLegacyCustomRecord(record) {
		t.Fatal("missing valid migration record", err)
	}
	for _, mode := range []string{"source-key", "destination-key", "instance", "node", "revision", "missing-revision", "empty", "future", "stale"} {
		t.Run(mode, func(t *testing.T) {
			r := copyJSON(record)
			p := r.After.LegacyContract.Custom.MappingProof
			switch mode {
			case "source-key":
				p.SourceKey = "other"
			case "destination-key":
				p.DestinationKey = "other"
			case "instance":
				p.Nodes[0].Identity.InstanceID = "ins-other"
			case "node":
				p.Nodes[0].Identity.NodeID = "22222222-2222-2222-2222-222222222222"
			case "revision":
				p.Nodes[0].ModifyIndex = 0
			case "missing-revision":
				p.Nodes[0].Missing = true
			case "empty":
				p.Nodes = nil
			case "future":
				p.ObservedAt = r.Plan.CreatedAt.Add(time.Nanosecond)
			case "stale":
				p.ObservedAt = r.Plan.CreatedAt.Add(-31 * time.Second)
			}
			if validLegacyCustomRecord(r) {
				t.Fatal("forged proof accepted")
			}
		})
	}
}

type legacyMappingCrashInput struct {
	Directory, At string
	Deadline      time.Duration
	Nodes         map[string]nomadclient.Node
	Snapshot      tencentcloud.Snapshot
	Activities    map[string]tencentcloud.Activity
}

func TestLegacyCustomMappingCrashHelper(t *testing.T) {
	path := os.Getenv("NOMAD_MAPPING_CRASH_INPUT")
	if path == "" {
		t.Skip("subprocess only")
	}
	raw, err := os.ReadFile(path)
	var in legacyMappingCrashInput
	if err != nil || json.Unmarshal(raw, &in) != nil {
		t.Fatal("bad mapping crash input")
	}
	cfg := Config{Dir: in.Directory, GroupID: testGroup, Region: testRegion, GroupContract: true, DrainDeadline: in.Deadline, ReconcileInterval: time.Hour, DisableReconciliation: true}
	c := &settlementCloud{multiCloud: &multiCloud{fakeCloud: &fakeCloud{snap: in.Snapshot}, activities: in.Activities}}
	h := &legacyHTTPHarness{nodes: in.Nodes, purged: map[string]bool{}, mapping: map[string]string{}, metaKey: legacyMappedKey, cloud: c, directory: cfg.Dir, deadline: in.Deadline, t: t}
	server := httptest.NewServer(http.HandlerFunc(h.serve))
	defer server.Close()
	client, err := nomadclient.New(nomadclient.Config{Address: server.URL, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	client, err = client.WithScope(testGroup, testRegion, legacyMappedKey)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := (targetconfig.Agent{}).Policy(map[string]string{"tencent_group_id": testGroup, "node_drain_deadline": in.Deadline.String(), "node_purge": "true", "node_drain_ignore_system_jobs": "true", "instance_id_meta_key": legacyMappedKey})
	if err != nil {
		t.Fatal(err)
	}
	e := &Engine{cfg: cfg, cloud: c, nomad: nomadsdk.New(client, policy), ctx: context.Background()}
	if err := e.lockDir(); err != nil {
		t.Fatal(err)
	}
	defer e.release()
	if _, err := e.load(); err != nil {
		t.Fatal(err)
	}
	e.recoveryCheckpoint = func(stage string) error {
		if stage == in.At {
			if c.writeCount() != 0 || len(h.writes) != 0 {
				t.Fatal("provider write before mapping adoption")
			}
			os.Exit(61)
		}
		return nil
	}
	if err := e.upgradeGroupContract(); err != nil {
		t.Fatal(err)
	}
	t.Fatal("mapping crash checkpoint missed")
}
func TestLegacyCustomMappingAbruptExitRetainsProof(t *testing.T) {
	for _, at := range []string{"before-record", "record", "prepared", "journal", "receipt", "cleared"} {
		t.Run(at, func(t *testing.T) {
			cfg, c, h, old, adapter, raw := legacyMappedSource(t, "v1-draining", 0)
			input := filepath.Join(t.TempDir(), "input.json")
			if err := os.WriteFile(input, jsonBytes(legacyMappingCrashInput{cfg.Dir, at, cfg.DrainDeadline, h.nodes, c.snap, c.activities}), 0600); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestLegacyCustomMappingCrashHelper$")
			cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"), "TMPDIR=" + os.Getenv("TMPDIR"), "NOMAD_MAPPING_CRASH_INPUT=" + input}
			out, err := cmd.CombinedOutput()
			if err == nil || cmd.ProcessState == nil || cmd.ProcessState.ExitCode() != 61 {
				t.Fatalf("expected exit61: %v %s", err, out)
			}
			e, err := Open(cfg, c, adapter)
			if err != nil {
				t.Fatal("mapping adoption restart", err)
			}
			if !legacyMappingBindingMatches(old, *e.state.LegacyContract.Custom) || e.state.Pending.ID != old.Pending.ID || e.state.Pending.SDKLifecycle || c.writeCount() != 0 || len(h.writes) != 0 || e.checkRecoveryAudit() != nil {
				e.Close()
				t.Fatal("mapping proof or original intent changed")
			}
			e.Close()
			backup, err := os.ReadFile(filepath.Join(cfg.Dir, "live-v1-before-group-contract.backup.json"))
			if err != nil || !reflect.DeepEqual(backup, raw) {
				t.Fatal("mapping backup changed")
			}
			reader, err := OpenRecovery(cfg, c, adapter)
			if err != nil {
				t.Fatal(err)
			}
			reader.Close()
		})
	}
}

func TestLegacyCustomMappingFailedSurvivorKeepsAsyncDeletionSemantics(t *testing.T) {
	for _, d := range []time.Duration{15 * time.Minute, 0, -time.Second} {
		t.Run(d.String(), func(t *testing.T) {
			cfg, c, h, old, adapter, _ := legacyMappedSource(t, "v1-failed-restored", d)
			e, err := Open(cfg, c, adapter)
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			e.EnableReconciliation()
			_ = e.reconcileOnce()
			_ = e.reconcileOnce()
			if e.state.Pending == nil || e.state.Pending.DrainOutcome != "restored" || c.writeCount() != 0 || len(h.writes) != 0 || e.state.LastCompletedAt != 0 {
				t.Fatal("already restored mapped survivor changed")
			}
			laterDeletion(t, e, c)
			runLegacyToTerminal(t, e, c)
			if e.state.LastBatch.Phase != "failed" || e.state.LastCompletedAt != 0 || e.state.LastOperation.ActivityID != old.LastOperation.ActivityID || c.writeCount() != 0 || !reflect.DeepEqual(h.writes, []string{"post:" + testInstance}) || e.checkRecoveryAudit() != nil {
				t.Fatal("mapped failure fabricated success or replayed deletion")
			}
			if ok, err := e.Scale(1); !ok || err != nil {
				t.Fatal("next mapped action blocked", err)
			}
		})
	}
}
