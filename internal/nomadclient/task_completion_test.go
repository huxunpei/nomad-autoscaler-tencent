package nomadclient

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
)

func allocationForTasks() map[string]any {
	return map[string]any{
		"ID": secondID, "NodeID": firstID, "Namespace": "other-team", "ClientStatus": "complete", "TaskGroup": "service",
		"Job":        map[string]any{"Type": "service", "TaskGroups": []any{map[string]any{"Name": "service", "Tasks": []any{map[string]any{"Name": "app"}, map[string]any{"Name": "cleanup"}}}}},
		"TaskStates": map[string]any{"app": map[string]any{"State": "dead"}, "cleanup": map[string]any{"State": "dead"}},
	}
}

func TestTasksStoppedValidatesTaskCompletionAndSystemException(t *testing.T) {
	for _, tt := range []struct {
		name                     string
		ignore, stopped, wantErr bool
	}{
		{"completed", false, true, false}, {"poststop-running", false, false, false},
		{"system-ignored", true, true, false}, {"system-required", false, false, false},
		{"missing-state", false, false, true}, {"null-task", false, false, true}, {"unknown-task-state", false, false, true},
		{"missing-job", false, false, true}, {"missing-task-definition", false, false, true}, {"null-list", false, false, true},
		{"foreign-node", false, false, true}, {"duplicate-allocation", false, false, true}, {"paginated", false, false, true},
		{"acl-denied", false, false, true}, {"empty", false, true, false}, {"running-allocation", false, false, false},
		{"unknown-allocation", false, false, true}, {"system-missing-job-type", true, false, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := clientFor(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" || r.URL.Path != "/v1/node/"+firstID+"/allocations" || r.URL.Query().Get("namespace") != "*" {
					t.Error("completion check must read all namespaces for exact node")
				}
				a := allocationForTasks()
				states := a["TaskStates"].(map[string]any)
				job := a["Job"].(map[string]any)
				switch tt.name {
				case "poststop-running":
					states["cleanup"] = map[string]any{"State": "running"}
				case "system-ignored", "system-required":
					job["Type"] = "system"
					a["ClientStatus"] = "running"
					states["cleanup"] = map[string]any{"State": "running"}
				case "missing-state":
					delete(states, "cleanup")
				case "null-task":
					states["cleanup"] = nil
				case "unknown-task-state":
					states["cleanup"] = map[string]any{"State": "mystery"}
				case "missing-job":
					delete(a, "Job")
				case "missing-task-definition":
					delete(job, "TaskGroups")
				case "foreign-node":
					a["NodeID"] = secondID
				case "paginated":
					w.Header().Set("X-Nomad-NextToken", "more")
				case "acl-denied":
					http.Error(w, "private-response", 403)
					return
				case "empty":
					_, _ = w.Write([]byte("[]"))
					return
				case "null-list":
					_, _ = w.Write([]byte("null"))
					return
				case "running-allocation":
					a["ClientStatus"] = "running"
				case "unknown-allocation":
					a["ClientStatus"] = "unknown"
				case "system-missing-job-type":
					delete(job, "Type")
				}
				rows := []any{a}
				if tt.name == "duplicate-allocation" {
					rows = append(rows, a)
				}
				_ = json.NewEncoder(w).Encode(rows)
			})
			guard, ok := any(c).(interface {
				TasksStopped(context.Context, string, bool) (bool, error)
			})
			if !ok {
				t.Fatal("task completion guard is missing")
			}
			stopped, err := guard.TasksStopped(context.Background(), firstID, tt.ignore)
			if stopped != tt.stopped || (err != nil) != tt.wantErr {
				t.Fatalf("got stopped=%t error=%v; want stopped=%t error=%t", stopped, err, tt.stopped, tt.wantErr)
			}
		})
	}
}
