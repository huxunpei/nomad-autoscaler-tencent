package nomadclient

import (
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadtest"

	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const firstID = "11111111-1111-1111-1111-111111111111"
const secondID = "22222222-2222-2222-2222-222222222222"

var identity = Identity{NodeID: firstID, InstanceID: "ins-test1", GroupID: "asg-test1", Region: "ap-chengdu"}

func raw(i Identity) rawNode {
	return rawNode{ID: i.NodeID, Status: "ready", SchedulingEligibility: "eligible", Meta: map[string]string{"cloud_instance_id": i.InstanceID, "tencent_group_id": i.GroupID, "tencent_region": i.Region, "private_value": "must-not-be-returned"}}
}
func clientFor(t *testing.T, h http.HandlerFunc) *Client {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if nomadtest.DisabledACL(w, r) {
			return
		}
		h(w, r)
	}))
	t.Cleanup(s.Close)
	c, err := New(Config{Address: s.URL, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}
func code(t *testing.T, err error, want string) {
	t.Helper()
	e, ok := err.(*Error)
	if !ok || e.Code != want {
		t.Fatalf("got %v, want %s", err, want)
	}
}

func TestAddressValidation(t *testing.T) {
	for _, a := range []string{"http://secret@localhost:4646", "http://localhost:4646/prefix", "http://localhost:4646/?token=secret", "http://localhost:4646/#secret", "ftp://localhost", "http:///missing", "http://localhost:4646/%2f"} {
		t.Run(a, func(t *testing.T) { _, err := New(Config{Address: a}); code(t, err, "InvalidAddress") })
	}
	if _, err := New(Config{Address: "http://127.0.0.1:4646", Token: "bad\nvalue"}); err == nil {
		t.Fatal("accepted header injection")
	}
}

func TestReadDoesNotReturnArbitraryMetadata(t *testing.T) {
	c := clientFor(t, func(w http.ResponseWriter, r *http.Request) { _ = json.NewEncoder(w).Encode(raw(identity)) })
	n, err := c.ReadNode(context.Background(), firstID)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(n)
	if strings.Contains(string(b), "must-not-be-returned") {
		t.Fatal("arbitrary metadata leaked")
	}
	if err = CheckIdentity(n, identity); err != nil {
		t.Fatal(err)
	}
}

func TestResolveMembersPagesAndFullInfo(t *testing.T) {
	second := Identity{NodeID: secondID, InstanceID: "ins-test2", GroupID: identity.GroupID, Region: identity.Region}
	var listCalls, infoCalls atomic.Int32
	c := clientFor(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/nodes":
			listCalls.Add(1)
			if r.URL.Query().Get("per_page") != "100" {
				t.Error("missing page size")
			}
			if r.URL.Query().Get("next_token") == "" {
				w.Header().Set("X-Nomad-NextToken", "next")
				fmt.Fprintf(w, `[{"ID":%q}]`, firstID)
			} else {
				fmt.Fprintf(w, `[{"ID":%q}]`, secondID)
			}
		case "/v1/node/" + firstID:
			infoCalls.Add(1)
			_ = json.NewEncoder(w).Encode(raw(identity))
		case "/v1/node/" + secondID:
			infoCalls.Add(1)
			_ = json.NewEncoder(w).Encode(raw(second))
		default:
			t.Error("unexpected path")
			w.WriteHeader(404)
		}
	})
	got, err := c.ResolveMembers(context.Background(), identity.GroupID, identity.Region, []string{second.InstanceID, identity.InstanceID})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].InstanceID != "ins-test1" || listCalls.Load() != 2 || infoCalls.Load() != 2 {
		t.Fatalf("mapping/pagination not proven: %+v", got)
	}
}

func TestResolveMembersRejectsDuplicateScopeAndMissing(t *testing.T) {
	for _, tc := range []struct{ name, instance, group, region, want string }{
		{"duplicate", identity.InstanceID, identity.GroupID, identity.Region, "DuplicateMemberMapping"},
		{"cross-group", "ins-other", "asg-other", identity.Region, "MemberScopeMismatch"},
		{"cross-region", "ins-other", identity.GroupID, "ap-guangzhou", "MemberScopeMismatch"},
		{"missing", "ins-other", identity.GroupID, identity.Region, "MemberNotRegistered"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			second := Identity{NodeID: secondID, InstanceID: tc.instance, GroupID: tc.group, Region: tc.region}
			c := clientFor(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/nodes" {
					fmt.Fprintf(w, `[{"ID":%q},{"ID":%q}]`, firstID, secondID)
					return
				}
				n := raw(identity)
				if strings.HasSuffix(r.URL.Path, secondID) {
					n = raw(second)
				}
				_ = json.NewEncoder(w).Encode(n)
			})
			members := []string{identity.InstanceID}
			if tc.name != "duplicate" {
				members = append(members, "ins-other")
			}
			if tc.name == "missing" {
				members = append(members, "ins-absent")
			}
			_, err := c.ResolveMembers(context.Background(), identity.GroupID, identity.Region, members)
			code(t, err, tc.want)
		})
	}
}

func TestListRejectsRepeatedPagination(t *testing.T) {
	c := clientFor(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/nodes" {
			w.Header().Set("X-Nomad-NextToken", "same")
			fmt.Fprintf(w, `[{"ID":%q}]`, firstID)
			return
		}
		_ = json.NewEncoder(w).Encode(raw(identity))
	})
	_, err := c.ListNodes(context.Background())
	code(t, err, "InvalidNodePage")
}

type fixture struct {
	mu          sync.Mutex
	node        rawNode
	writes      int
	mode        string
	lastPayload map[string]any
}

func (f *fixture) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.Method == http.MethodGet {
		_ = json.NewEncoder(w).Encode(f.node)
		return
	}
	f.writes++
	if r.URL.Path != "/v1/node/"+firstID+"/drain" {
		w.WriteHeader(404)
		return
	}
	_ = json.NewDecoder(r.Body).Decode(&f.lastPayload)
	if f.mode == "malformed" {
		fmt.Fprint(w, "credential-must-not-leak")
		return
	}
	if f.mode == "server-error" {
		w.WriteHeader(500)
		fmt.Fprint(w, "credential-must-not-leak")
		return
	}
	if f.mode == "disconnect" {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			conn.Close()
		}
		return
	}
	meta := f.lastPayload["Meta"].(map[string]any)
	f.node.LastDrain = &struct {
		Status string
		Meta   map[string]string
	}{Status: "draining", Meta: map[string]string{"drained_by": fmt.Sprint(meta["drained_by"]), operationKey: fmt.Sprint(meta[operationKey])}}
	if f.lastPayload["DrainSpec"] == nil {
		f.node.Drain = false
		f.node.DrainStrategy = nil
		f.node.SchedulingEligibility = "eligible"
		f.node.LastDrain.Status = "canceled"
	} else {
		f.node.Drain = true
		f.node.DrainStrategy = json.RawMessage(`{"Deadline":1000000000}`)
		f.node.SchedulingEligibility = "ineligible"
	}
	fmt.Fprint(w, `{"NodeModifyIndex":42}`)
}

func TestStartRestoreAndIdempotentRestore(t *testing.T) {
	f := &fixture{node: raw(identity)}
	c := clientFor(t, f.serve)
	out, err := c.StartOwnedDrain(context.Background(), identity, "op-one", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !out.WriteAttempted || out.Unknown || out.After.Eligibility != "ineligible" || !owns(*out.After, "op-one") {
		t.Fatalf("invalid drain result %+v", out)
	}
	out, err = c.RestoreOwnedDrain(context.Background(), identity, "op-one")
	if err != nil {
		t.Fatal(err)
	}
	if !out.WriteAttempted || out.After.Drain || out.After.Eligibility != "eligible" {
		t.Fatalf("invalid restore %+v", out)
	}
	out, err = c.RestoreOwnedDrain(context.Background(), identity, "op-one")
	if err != nil || out.WriteAttempted {
		t.Fatalf("restore should be read-only no-op: %+v %v", out, err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.writes != 2 || f.lastPayload["DrainSpec"] != nil || f.lastPayload["MarkEligible"] != true {
		t.Fatalf("wrong mutation count or restore payload: %d %+v", f.writes, f.lastPayload)
	}
}

func TestProtectExistingDrainIneligibilityIdentityAndOwnership(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		restore    bool
		change     func(*rawNode)
	}{
		{"drained", "NodeAlreadyUnavailable", false, func(n *rawNode) { n.Drain = true }},
		{"strategy", "NodeAlreadyUnavailable", false, func(n *rawNode) { n.DrainStrategy = json.RawMessage(`{}`) }},
		{"ineligible", "NodeAlreadyUnavailable", false, func(n *rawNode) { n.SchedulingEligibility = "ineligible" }},
		{"identity", "IdentityMismatch", false, func(n *rawNode) { n.Meta["cloud_instance_id"] = "ins-other" }},
		{"external-owner", "DrainOwnershipMismatch", true, func(n *rawNode) {
			n.LastDrain = &struct {
				Status string
				Meta   map[string]string
			}{"draining", map[string]string{"drained_by": "operator", operationKey: "op-one"}}
		}},
		{"other-operation", "DrainOwnershipMismatch", true, func(n *rawNode) {
			n.LastDrain = &struct {
				Status string
				Meta   map[string]string
			}{"draining", map[string]string{"drained_by": owner, operationKey: "op-other"}}
		}},
		{"missing-owner", "DrainOwnershipMismatch", true, func(n *rawNode) {}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &fixture{node: raw(identity)}
			tc.change(&f.node)
			c := clientFor(t, f.serve)
			var err error
			if tc.restore {
				_, err = c.RestoreOwnedDrain(context.Background(), identity, "op-one")
			} else {
				_, err = c.StartOwnedDrain(context.Background(), identity, "op-one", time.Minute)
			}
			code(t, err, tc.want)
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.writes != 0 {
				t.Fatal("protected node was mutated")
			}
		})
	}
}

func TestUnknownResponsesAreNotRetriedOrLeaked(t *testing.T) {
	for _, mode := range []string{"malformed", "server-error", "disconnect"} {
		t.Run(mode, func(t *testing.T) {
			f := &fixture{node: raw(identity), mode: mode}
			c := clientFor(t, f.serve)
			out, err := c.StartOwnedDrain(context.Background(), identity, "op-one", time.Minute)
			if err == nil || !out.Unknown || !out.WriteAttempted {
				t.Fatalf("unknown mutation not classified: %+v %v", out, err)
			}
			b, _ := json.Marshal(struct {
				Result MutationResult
				Error  error
			}{out, err})
			if strings.Contains(string(b), "credential-must-not-leak") {
				t.Fatal("response body leaked")
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.writes != 1 {
				t.Fatalf("mutation was retried %d times", f.writes)
			}
		})
	}
}

func TestRedirectDoesNotForwardToken(t *testing.T) {
	var received atomic.Int32
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { received.Add(1); w.WriteHeader(200) }))
	defer destination.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, destination.URL+"/token-leak", http.StatusTemporaryRedirect)
	}))
	defer source.Close()
	c, err := New(Config{Address: source.URL, Token: "private-test-token", Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, err = c.ReadNode(context.Background(), firstID)
	code(t, err, "HTTPFailure")
	if received.Load() != 0 || strings.Contains(err.Error(), "private-test-token") || strings.Contains(err.Error(), source.URL) {
		t.Fatal("redirect followed or sensitive error leaked")
	}
}

func TestCanceledContextAndInvalidUUIDDoNotMutate(t *testing.T) {
	f := &fixture{node: raw(identity)}
	c := clientFor(t, f.serve)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	out, err := c.StartOwnedDrain(ctx, identity, "op-one", time.Minute)
	if err == nil || out.WriteAttempted {
		t.Fatal("canceled request proceeded")
	}
	_, err = c.ReadNode(context.Background(), "../secret")
	code(t, err, "InvalidNodeID")
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.writes != 0 {
		t.Fatal("unexpected mutation")
	}
}

func TestLongPositiveDrainDeadlineIsSentWithoutTruncation(t *testing.T) {
	f := &fixture{node: raw(identity)}
	c := clientFor(t, f.serve)
	result, err := c.StartOwnedDrain(context.Background(), identity, "op-long", 48*time.Hour)
	if err != nil || !result.WriteAttempted || result.After == nil || !result.After.Drain {
		t.Fatal("valid long deadline rejected", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	spec, ok := f.lastPayload["DrainSpec"].(map[string]any)
	if !ok || spec["Deadline"] != float64(48*time.Hour) || f.writes != 1 {
		t.Fatal("long duration changed on wire")
	}
}

func TestNonPositiveSDKDrainValuesRemainWireValues(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second, time.Duration(-1 << 63)} {
		t.Run(d.String(), func(t *testing.T) {
			f := &fixture{node: raw(identity)}
			c := clientFor(t, f.serve)
			result, err := c.StartOwnedDrain(context.Background(), identity, "op-nonpositive", d)
			if err != nil || !result.WriteAttempted || result.After == nil || !result.After.Drain {
				t.Fatal("valid SDK mode rejected", err)
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			spec, ok := f.lastPayload["DrainSpec"].(map[string]any)
			if !ok || spec["Deadline"] != float64(d) || f.lastPayload["MarkEligible"] != false {
				t.Fatal("deadline mode became default or restore")
			}
		})
	}
}
