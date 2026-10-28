package nomadclient

import (
	"context"
	"net/http"
	"net/url"
)

// taskAllocation deliberately excludes task configuration, environment and
// secrets. Completion needs both the task definitions and the task states:
// an allocation becoming terminal does not imply its poststop tasks are dead.
type taskAllocation struct {
	ID, NodeID, Namespace, ClientStatus, TaskGroup string
	Job                                            *struct {
		Type       string
		TaskGroups []*struct {
			Name  string
			Tasks []*struct{ Name string }
		}
	}
	TaskStates map[string]*struct{ State string }
}

// TasksStopped checks all namespaces on one node. Deployments must grant
// complete allocation visibility; a namespace-scoped token is insufficient.
// Missing or unrecognised state is an error, never evidence of task completion.
// System tasks are exempt only when the policy explicitly requests it.
func (c *Client) TasksStopped(ctx context.Context, nodeID string, ignoreSystemJobs bool) (bool, error) {
	if !uuid.MatchString(nodeID) {
		return false, failure("InvalidNodeID")
	}
	proof, err := c.allocationVisibilityProof(ctx)
	if err != nil {
		return false, err
	}
	var rows []taskAllocation
	headers, err := c.request(ctx, http.MethodGet, "/v1/node/"+nodeID+"/allocations", url.Values{"namespace": {"*"}}, nil, &rows)
	if err != nil {
		return false, err
	}
	if headers.Get("X-Nomad-NextToken") != "" {
		return false, failure("IncompleteAllocations")
	}
	if rows == nil {
		return false, failure("InvalidAllocations")
	}
	seen := make(map[string]bool, len(rows))
	stopped := true
	for _, a := range rows {
		if !uuid.MatchString(a.ID) || a.NodeID != nodeID || seen[a.ID] || a.Namespace == "" || a.Job == nil {
			return false, failure("InvalidAllocations")
		}
		seen[a.ID] = true
		switch a.Job.Type {
		case "system":
			if ignoreSystemJobs {
				continue
			}
		case "service", "batch", "sysbatch":
		default:
			return false, failure("InvalidAllocationJob")
		}
		switch a.ClientStatus {
		case "complete", "failed":
		case "pending", "running":
			stopped = false
		default:
			return false, failure("UnconfirmedAllocationStatus")
		}
		tasks := map[string]bool{}
		matched := false
		for _, g := range a.Job.TaskGroups {
			if g == nil || g.Name == "" {
				return false, failure("InvalidTaskDefinitions")
			}
			if g.Name != a.TaskGroup {
				continue
			}
			if matched {
				return false, failure("InvalidTaskDefinitions")
			}
			matched = true
			for _, task := range g.Tasks {
				if task == nil || task.Name == "" || tasks[task.Name] {
					return false, failure("InvalidTaskDefinitions")
				}
				tasks[task.Name] = true
			}
		}
		if !matched || len(tasks) == 0 || len(tasks) != len(a.TaskStates) {
			return false, failure("IncompleteTaskStates")
		}
		for name := range tasks {
			state, ok := a.TaskStates[name]
			if !ok || state == nil {
				return false, failure("IncompleteTaskStates")
			}
			switch state.State {
			case "dead":
			case "pending", "running":
				stopped = false
			default:
				return false, failure("InvalidTaskState")
			}
		}
	}
	if stopped {
		after, err := c.allocationVisibilityProof(ctx)
		if err != nil {
			return false, err
		}
		if after != proof {
			return false, failure("AllocationVisibilityChanged")
		}
	}
	return stopped, nil
}
