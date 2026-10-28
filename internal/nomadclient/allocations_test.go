package nomadclient

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
)

func TestActiveAllocationsCountsOnlyNonterminalAndValidatesScope(t *testing.T) {
	for _, mode := range []string{"normal", "foreign", "duplicate", "unknown-status", "truncated"} {
		t.Run(mode, func(t *testing.T) {
			c := clientFor(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" || r.URL.Path != "/v1/node/"+firstID+"/allocations" {
					t.Error("unexpected request")
				}
				rows := []map[string]string{{"ID": firstID, "NodeID": firstID, "ClientStatus": "running"}, {"ID": secondID, "NodeID": firstID, "ClientStatus": "complete"}}
				switch mode {
				case "foreign":
					rows[0]["NodeID"] = secondID
				case "duplicate":
					rows[1]["ID"] = firstID
				case "unknown-status":
					rows[0]["ClientStatus"] = "bad"
				case "truncated":
					w.Header().Set("X-Nomad-NextToken", "more")
				}
				_ = json.NewEncoder(w).Encode(rows)
			})
			count, err := c.ActiveAllocations(context.Background(), firstID)
			if mode == "normal" {
				if err != nil || count != 1 {
					t.Fatal(count, err)
				}
			} else if err == nil {
				t.Fatal("invalid response accepted")
			}
		})
	}
}
