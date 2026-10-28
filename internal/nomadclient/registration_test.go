package nomadclient

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
)

func TestRegistrationObservationPartialOnlyAfterCompleteRead(t *testing.T) {
	for _, mode := range []string{"complete", "second-page-error", "detail-error", "duplicate", "bad-metadata"} {
		t.Run(mode, func(t *testing.T) {
			c := clientFor(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/nodes" {
					if r.URL.Query().Get("next_token") == "" {
						w.Header().Set("X-Nomad-NextToken", "page-two")
						fmt.Fprintf(w, `[{"ID":%q}]`, firstID)
					} else if mode == "second-page-error" {
						http.Error(w, "synthetic forbidden", 403)
					} else if mode == "duplicate" {
						fmt.Fprintf(w, `[{"ID":%q}]`, secondID)
					} else {
						fmt.Fprint(w, `[]`)
					}
					return
				}
				if mode == "detail-error" {
					http.Error(w, "synthetic not found", 404)
					return
				}
				n := raw(identity)
				if r.URL.Path == "/v1/node/"+secondID {
					n.ID = secondID
				}
				if mode == "bad-metadata" {
					n.Meta["cloud_instance_id"] = "bad\nidentity"
				}
				_ = json.NewEncoder(w).Encode(n)
			})
			ids := []string{identity.InstanceID, "ins-absent"}
			nodes, err := c.ObserveMembers(context.Background(), identity.GroupID, identity.Region, ids)
			if mode == "complete" {
				if err != nil || len(nodes) != 1 || nodes[0].Identity != identity {
					t.Fatal("partial presence not returned", err)
				}
				if _, err := c.ResolveMembers(context.Background(), identity.GroupID, identity.Region, ids); err == nil {
					t.Fatal("strict drain resolver accepted missing member")
				}
			} else if err == nil || len(nodes) != 0 {
				t.Fatal("incomplete or ambiguous read became absence evidence", err)
			}
		})
	}
}
