package nomadclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// Metadata is shaped like the native Nomad 2.0.7 read-policy response. The
// allocation list alone deliberately looks safe even when ACLs filtered it.
func TestAllocationVisibilityRejectsFilteredSuccess(t *testing.T) {
	for _, name := range []string{"full", "partial", "exact-deny", "exact-no-read", "exact-read", "union", "second-deny", "management", "disabled", "unknown-400", "missing-parsed", "revision-mismatch", "duplicate-policy", "paginated", "changed-during-read", "metadata-denied", "unknown-token-type", "null-policy", "native-disabled", "native-disabled-unconfirmed", "native-disabled-wrong-type", "native-disabled-wrong-revision", "anonymous"} {
		t.Run(name, func(t *testing.T) {
			var allocations, metadata atomic.Int32
			namespace := func(n string, caps ...string) map[string]any { return map[string]any{"Name": n, "Capabilities": caps} }
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				send := func(v any) { json.NewEncoder(w).Encode(v) }
				switch {
				case r.URL.Path == "/v1/acl/token/self":
					metadata.Add(1)
					if name == "disabled" {
						http.Error(w, "ACL support disabled", 400)
						return
					}
					if name == "unknown-400" {
						http.Error(w, "proxy says ACL support disabled but not authoritative", 400)
						return
					}
					typ := "client"
					if name == "management" {
						typ = "management"
					}
					if name == "unknown-token-type" {
						typ = "unknown"
					}
					accessor := firstID
					revision := uint64(5)
					if strings.HasPrefix(name, "native-disabled") {
						accessor = "acls-disabled"
						revision = 0
					}
					if name == "native-disabled-wrong-type" {
						typ = "management"
					}
					if name == "native-disabled-wrong-revision" {
						revision = 5
					}
					if name == "anonymous" {
						accessor = "anonymous"
						revision = 0
					}
					send(map[string]any{"Type": typ, "AccessorID": accessor, "ModifyIndex": revision, "SecretID": "synthetic-ignored-secret"})
				case r.URL.Path == "/v1/acl/policies":
					metadata.Add(1)
					if name == "native-disabled" {
						http.Error(w, "ACL support disabled", 400)
						return
					}
					if name == "metadata-denied" {
						http.Error(w, "synthetic-private-body", 403)
						return
					}
					if name == "paginated" {
						w.Header().Set("X-Nomad-NextToken", "more")
					}
					index := uint64(7)
					if name == "changed-during-read" && allocations.Load() > 0 {
						index = 8
					}
					rows := []any{map[string]any{"Name": "reader", "ModifyIndex": index}}
					if name == "duplicate-policy" {
						rows = append(rows, rows[0])
					}
					if name == "union" || name == "second-deny" {
						rows = append(rows, map[string]any{"Name": "second", "ModifyIndex": uint64(9)})
					}
					send(rows)
				case strings.HasPrefix(r.URL.Path, "/v1/acl/policy/"):
					metadata.Add(1)
					policy := strings.TrimPrefix(r.URL.Path, "/v1/acl/policy/")
					index := uint64(7)
					if name == "changed-during-read" && allocations.Load() > 0 {
						index = 8
					}
					if name == "revision-mismatch" {
						index = 6
					}
					if policy == "second" {
						index = 9
					}
					ns := []any{namespace("*", "read-job")}
					switch name {
					case "partial":
						ns = []any{namespace("default", "read-job")}
					case "exact-deny":
						ns = append(ns, namespace("hidden", "deny"))
					case "exact-no-read":
						ns = append(ns, namespace("hidden", "list-jobs"))
					case "exact-read":
						ns = append(ns, namespace("hidden", "read-job"))
					case "union":
						if policy == "reader" {
							ns = append(ns, namespace("hidden", "list-jobs"))
						} else {
							ns = []any{namespace("hidden", "read-job")}
						}
					case "second-deny":
						if policy == "second" {
							ns = []any{namespace("hidden", "deny")}
						}
					}
					out := map[string]any{"Name": policy, "ModifyIndex": index, "RulesJSON": map[string]any{"Namespaces": ns}}
					if name == "missing-parsed" {
						delete(out, "RulesJSON")
					}
					if name == "null-policy" {
						send(nil)
						return
					}
					send(out)
				case r.URL.Path == "/v1/node/"+firstID+"/allocations":
					allocations.Add(1)
					if r.URL.Query().Get("namespace") != "*" {
						t.Error("lost all-namespace query")
					}
					send([]any{})
				default:
					t.Errorf("unexpected endpoint %s", r.URL.Path)
					w.WriteHeader(404)
				}
			}))
			defer server.Close()
			c, err := New(Config{Address: server.URL})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			stopped, err := c.TasksStopped(context.Background(), firstID, false)
			valid := name == "full" || name == "exact-read" || name == "union" || name == "management" || name == "disabled" || name == "native-disabled" || name == "anonymous"
			if valid {
				if err != nil || !stopped {
					t.Fatalf("complete visibility rejected: stopped=%t err=%v", stopped, err)
				}
			} else if err == nil || stopped {
				t.Fatalf("filtered/unproven view accepted: stopped=%t err=%v", stopped, err)
			}
			if valid && metadata.Load() == 0 {
				t.Error("allocation success used without ACL proof")
			}
			if !valid && name != "changed-during-read" && allocations.Load() != 0 {
				t.Error("unproven allocation view queried")
			}
			if err != nil && (strings.Contains(err.Error(), "synthetic") || strings.Contains(err.Error(), "http")) {
				t.Fatal("unsafe error contains upstream details")
			}
		})
	}
}
