// Package nomadclient implements the small Nomad API surface needed by a
// Tencent target. Callers must durably persist an intent before each mutation.
// Read-then-write ownership checks are not CAS: deployments need one writer.
package nomadclient

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"
)

const owner = "nomad-autoscaler-tencent"
const operationKey = "tencent_operation_id"
const maxBody = 2 << 20

var uuid = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
var identifier = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,128}$`)

// Error deliberately omits response bodies, URLs, credentials and transport errors.
type Error struct {
	Code       string `json:"code"`
	HTTPStatus int    `json:"http_status,omitempty"`
	Unknown    bool   `json:"mutation_outcome_unknown,omitempty"`
}

func (e *Error) Error() string  { return e.Code }
func failure(code string) error { return &Error{Code: code} }

type Config struct {
	Address                               string
	Token                                 string `json:"-"`
	Region, Namespace                     string
	HTTPAuth                              string `json:"-"`
	CACert, CAPath, ClientCert, ClientKey string `json:"-"`
	TLSServerName                         string
	SkipVerify                            bool
	WaitTime                              time.Duration
	Timeout                               time.Duration
}
type Client struct {
	base                                 *url.URL
	token                                string
	region, namespace, httpAuth          string
	scopeGroup, scopeRegion, instanceKey string
	waitTime                             time.Duration
	http                                 *http.Client
}

func New(cfg Config) (*Client, error) {
	u, err := validateAddress(cfg.Address)
	if err != nil {
		return nil, err
	}
	if strings.ContainsAny(cfg.Token, "\x00\r\n") {
		return nil, failure("InvalidToken")
	}
	for _, value := range []string{cfg.Region, cfg.Namespace, cfg.HTTPAuth, cfg.TLSServerName} {
		if strings.ContainsAny(value, "\x00\r\n") {
			return nil, failure("InvalidNomadConnectionOption")
		}
	}
	if cfg.WaitTime < 0 {
		return nil, failure("InvalidNomadWaitTime")
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.Timeout < time.Millisecond || cfg.Timeout > 30*time.Second {
		return nil, failure("InvalidTimeout")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	if err = configureTLS(transport, cfg, u.Scheme); err != nil {
		return nil, err
	}
	return &Client{base: u, token: cfg.Token, region: cfg.Region, namespace: cfg.Namespace, httpAuth: cfg.HTTPAuth, waitTime: cfg.WaitTime, http: &http.Client{Transport: transport, Timeout: cfg.Timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}
func (c *Client) Close() { c.http.CloseIdleConnections() }

type Identity struct {
	NodeID     string `json:"node_id"`
	InstanceID string `json:"instance_id"`
	GroupID    string `json:"group_id"`
	Region     string `json:"region"`
}

func (i Identity) Validate() error {
	if !uuid.MatchString(i.NodeID) || !identifier.MatchString(i.InstanceID) || !identifier.MatchString(i.GroupID) || !identifier.MatchString(i.Region) {
		return failure("InvalidIdentity")
	}
	return nil
}
func ValidateOperationID(id string) error {
	if !identifier.MatchString(id) {
		return failure("InvalidOperationID")
	}
	return nil
}

type DrainRecord struct {
	Status      string `json:"status"`
	Owner       string `json:"owner"`
	OperationID string `json:"operation_id"`
}

// Node is a whitelist, never the full API response or arbitrary metadata.
type Node struct {
	ModifyIndex uint64 `json:"modify_index,omitempty"`
	Identity
	Status           string       `json:"status"`
	Drain            bool         `json:"drain"`
	HasDrainStrategy bool         `json:"has_drain_strategy"`
	Eligibility      string       `json:"eligibility"`
	LastDrain        *DrainRecord `json:"last_drain,omitempty"`
}
type rawNode struct {
	ModifyIndex           uint64
	ID                    string
	Meta                  map[string]string
	Status                string
	Drain                 bool
	DrainStrategy         json.RawMessage
	SchedulingEligibility string
	LastDrain             *struct {
		Status string
		Meta   map[string]string
	}
}

func (c *Client) request(ctx context.Context, method, path string, query url.Values, payload any, out any) (http.Header, error) {
	return c.requestWithACLStatus(ctx, method, path, query, payload, out, false)
}

func (c *Client) requestWithACLStatus(ctx context.Context, method, path string, query url.Values, payload any, out any, aclStatus bool) (http.Header, error) {
	u := *c.base
	q := url.Values{}
	for k, v := range query {
		q[k] = append([]string(nil), v...)
	}
	if _, ok := q["region"]; !ok && c.region != "" {
		q.Set("region", c.region)
	}
	if _, ok := q["namespace"]; !ok && c.namespace != "" {
		q.Set("namespace", c.namespace)
	}
	if q.Get("index") != "" && q.Get("wait") == "" && c.waitTime > 0 {
		q.Set("wait", c.waitTime.String())
	}
	u.Path, u.RawPath, u.RawQuery = path, "", q.Encode()
	var body io.Reader
	if payload != nil {
		b, err := json.Marshal(payload)
		if err != nil {
			return nil, failure("EncodeFailed")
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return nil, failure("RequestInvalid")
	}
	req.Header.Set("Accept", "application/json")
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.token != "" {
		req.Header.Set("X-Nomad-Token", c.token)
	}
	if c.httpAuth != "" {
		user, pass, _ := strings.Cut(c.httpAuth, ":")
		req.SetBasicAuth(user, pass)
	}
	resp, err := c.http.Do(req)
	write := method != http.MethodGet
	if err != nil {
		return nil, &Error{Code: "RequestFailed", Unknown: write}
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if aclStatus && method == http.MethodGet && (path == "/v1/acl/token/self" || path == "/v1/acl/policies") && resp.StatusCode == http.StatusBadRequest {
			b, readErr := io.ReadAll(io.LimitReader(resp.Body, 257))
			if readErr == nil && len(b) <= 256 && strings.TrimSpace(string(b)) == "ACL support disabled" {
				return nil, &Error{Code: "ACLDisabled", HTTPStatus: resp.StatusCode}
			}
		}
		// A non-2xx response is not proof that a reverse proxy did not forward
		// a mutation. Reconcile reads; do not replay the request automatically.
		return nil, &Error{Code: "HTTPFailure", HTTPStatus: resp.StatusCode, Unknown: write}
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if err != nil || len(b) > maxBody {
		return nil, &Error{Code: "ResponseUnreadable", Unknown: write}
	}
	if err = json.Unmarshal(b, out); err != nil {
		return nil, &Error{Code: "ResponseInvalid", Unknown: write}
	}
	return resp.Header, nil
}

func (c *Client) ReadNode(ctx context.Context, nodeID string) (Node, error) {
	var n Node
	if !uuid.MatchString(nodeID) {
		return n, failure("InvalidNodeID")
	}
	var raw rawNode
	if _, err := c.request(ctx, http.MethodGet, "/v1/node/"+nodeID, nil, nil, &raw); err != nil {
		return n, err
	}
	if raw.ID != nodeID {
		return n, failure("NodeIDMismatch")
	}
	if (raw.Status != "ready" && raw.Status != "down" && raw.Status != "initializing" && raw.Status != "disconnected") || (raw.SchedulingEligibility != "eligible" && raw.SchedulingEligibility != "ineligible") {
		return n, failure("NodeStateInvalid")
	}
	n = Node{ModifyIndex: raw.ModifyIndex, Identity: Identity{NodeID: raw.ID, InstanceID: raw.Meta["cloud_instance_id"], GroupID: raw.Meta["tencent_group_id"], Region: raw.Meta["tencent_region"]}, Status: raw.Status, Drain: raw.Drain, HasDrainStrategy: len(raw.DrainStrategy) > 0 && string(raw.DrainStrategy) != "null", Eligibility: raw.SchedulingEligibility}
	if c.scopeGroup != "" {
		n.GroupID, n.Region, n.InstanceID = c.scopeGroup, c.scopeRegion, raw.Meta[c.instanceKey]
		// Other providers may use this key for unrelated IDs or URIs.
		if !identifier.MatchString(n.InstanceID) {
			n.InstanceID = ""
		}
	}
	// Unrelated nodes may lack the Tencent metadata. Never return arbitrary
	// strings supplied in these fields (including errors or echoed tokens).
	for _, v := range []string{n.InstanceID, n.GroupID, n.Region} {
		if v != "" && !identifier.MatchString(v) {
			return Node{}, failure("NodeMetadataInvalid")
		}
	}
	if raw.LastDrain != nil {
		status := raw.LastDrain.Status
		if status != "draining" && status != "complete" && status != "canceled" {
			status = "unknown"
		}
		n.LastDrain = &DrainRecord{Status: status}
		if raw.LastDrain.Meta["drained_by"] == owner {
			n.LastDrain.Owner = owner
			if id := raw.LastDrain.Meta[operationKey]; identifier.MatchString(id) {
				n.LastDrain.OperationID = id
			}
		}
	}
	return n, nil
}

func CheckIdentity(n Node, want Identity) error {
	if err := want.Validate(); err != nil {
		return err
	}
	if n.Identity != want {
		return failure("IdentityMismatch")
	}
	return nil
}

// ListNodes paginates list stubs then fetches each full node for metadata.
func (c *Client) ListNodes(ctx context.Context) ([]Node, error) {
	out := []Node{}
	seenIDs := map[string]bool{}
	seenTokens := map[string]bool{}
	next := ""
	for page := 0; page < 1000; page++ {
		q := url.Values{"per_page": {"100"}}
		if next != "" {
			q.Set("next_token", next)
		}
		var stubs []struct{ ID string }
		h, err := c.request(ctx, http.MethodGet, "/v1/nodes", q, nil, &stubs)
		if err != nil {
			return nil, err
		}
		for _, stub := range stubs {
			if !uuid.MatchString(stub.ID) || seenIDs[stub.ID] {
				return nil, failure("InvalidNodePage")
			}
			seenIDs[stub.ID] = true
			n, err := c.ReadNode(ctx, stub.ID)
			if err != nil {
				return nil, err
			}
			out = append(out, n)
		}
		next = h.Get("X-Nomad-NextToken")
		if next == "" {
			return out, nil
		}
		if len(next) > 4096 || seenTokens[next] || len(stubs) == 0 {
			return nil, failure("InvalidPagination")
		}
		seenTokens[next] = true
	}
	return nil, failure("PaginationLimit")
}

// ResolveMembers requires every supplied current AS member to map uniquely to
// a node in the expected group/region. It does not infer cloud membership.
func (c *Client) ResolveMembers(ctx context.Context, group, region string, members []string) ([]Node, error) {
	nodes, err := c.ObserveMembers(ctx, group, region, members)
	if err != nil {
		return nil, err
	}
	if len(nodes) != len(members) {
		return nil, failure("MemberNotRegistered")
	}
	return nodes, nil
}

// ObserveMembers returns present members only after a complete successful node
// enumeration. Missing members are not errors; failed reads and ambiguous
// identities are errors and never become evidence of absence.
func (c *Client) ObserveMembers(ctx context.Context, group, region string, members []string) ([]Node, error) {
	if !identifier.MatchString(group) || !identifier.MatchString(region) {
		return nil, failure("InvalidGroupScope")
	}
	if c.scopeGroup != "" && (group != c.scopeGroup || region != c.scopeRegion) {
		return nil, failure("MemberScopeMismatch")
	}
	expected := map[string]bool{}
	for _, id := range members {
		if !identifier.MatchString(id) || expected[id] {
			return nil, failure("InvalidMembers")
		}
		expected[id] = true
	}
	nodes, err := c.ListNodes(ctx)
	if err != nil {
		return nil, err
	}
	found := map[string]Node{}
	for _, n := range nodes {
		if !expected[n.InstanceID] {
			continue
		}
		if n.GroupID != group || n.Region != region {
			return nil, failure("MemberScopeMismatch")
		}
		if _, ok := found[n.InstanceID]; ok {
			return nil, failure("DuplicateMemberMapping")
		}
		found[n.InstanceID] = n
	}
	out := make([]Node, 0, len(found))
	for _, n := range found {
		out = append(out, n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].InstanceID < out[j].InstanceID })
	return out, nil
}

type MutationResult struct {
	Before               *Node  `json:"before,omitempty"`
	After                *Node  `json:"after,omitempty"`
	WriteAttempted       bool   `json:"write_attempted"`
	Unknown              bool   `json:"mutation_outcome_unknown"`
	NodeModifyIndex      uint64 `json:"node_modify_index,omitempty"`
	MonitoringIncomplete bool   `json:"monitoring_incomplete,omitempty"`
}

func owns(n Node, op string) bool {
	return n.LastDrain != nil && n.LastDrain.Owner == owner && n.LastDrain.OperationID == op
}

// StartOwnedDrain requires a ready, eligible, undrained node. The caller must
// persist intent first and prevent competing writers; this API has no CAS.
func (c *Client) StartOwnedDrain(ctx context.Context, want Identity, op string, deadline time.Duration) (MutationResult, error) {
	return c.mutate(ctx, want, op, deadline, false)
}

// RestoreOwnedDrain cancels only this operation's drain and marks eligibility.
// A cloud caller must independently prove the VM still runs and either deletion
// was never submitted or a known deletion failure permits restoring work. This
// does not prohibit later cloud retries.
func (c *Client) RestoreOwnedDrain(ctx context.Context, want Identity, op string) (MutationResult, error) {
	return c.mutate(ctx, want, op, 0, true)
}

func (c *Client) mutate(ctx context.Context, want Identity, op string, deadline time.Duration, restore bool) (out MutationResult, err error) {
	if err = want.Validate(); err != nil {
		return out, err
	}
	if err = ValidateOperationID(op); err != nil {
		return out, err
	}
	before, err := c.ReadNode(ctx, want.NodeID)
	if err != nil {
		return out, err
	}
	out.Before = &before
	if err = CheckIdentity(before, want); err != nil {
		return out, err
	}
	if before.Status != "ready" {
		return out, failure("NodeNotReady")
	}
	if restore {
		if !owns(before, op) {
			return out, failure("DrainOwnershipMismatch")
		}
		if !before.Drain && !before.HasDrainStrategy && before.Eligibility == "eligible" {
			out.After = &before
			return out, nil
		}
	} else {
		if before.Drain || before.HasDrainStrategy || before.Eligibility != "eligible" {
			return out, failure("NodeAlreadyUnavailable")
		}
		if owns(before, op) {
			return out, failure("OperationAlreadyUsed")
		}
	}
	type spec struct {
		Deadline         int64
		IgnoreSystemJobs bool
	}
	payload := struct {
		DrainSpec    *spec
		MarkEligible bool
		Meta         map[string]string
	}{MarkEligible: restore, Meta: map[string]string{"drained_by": owner, operationKey: op}}
	if !restore {
		payload.DrainSpec = &spec{Deadline: int64(deadline), IgnoreSystemJobs: false}
	}
	out.WriteAttempted = true
	var response struct{ NodeModifyIndex uint64 }
	_, err = c.request(ctx, http.MethodPost, "/v1/node/"+want.NodeID+"/drain", nil, payload, &response)
	if err != nil {
		out.Unknown = true
		return out, err
	}
	out.NodeModifyIndex = response.NodeModifyIndex
	if response.NodeModifyIndex == 0 {
		out.Unknown = true
		return out, &Error{Code: "MutationResponseInvalid", Unknown: true}
	}
	after, err := c.ReadNode(ctx, want.NodeID)
	if err != nil {
		out.Unknown = true
		return out, &Error{Code: "MutationReadbackFailed", Unknown: true}
	}
	out.After = &after
	if CheckIdentity(after, want) != nil || !owns(after, op) || after.Status != "ready" {
		out.Unknown = true
		return out, &Error{Code: "MutationReadbackMismatch", Unknown: true}
	}
	if restore {
		if after.Drain || after.HasDrainStrategy || after.Eligibility != "eligible" {
			out.Unknown = true
			return out, &Error{Code: "RestoreNotObserved", Unknown: true}
		}
	} else if after.Eligibility != "ineligible" || (!after.Drain && after.LastDrain.Status != "complete") {
		out.Unknown = true
		return out, &Error{Code: "DrainNotObserved", Unknown: true}
	}
	return out, nil
}

// ActiveAllocations counts work still assigned to the node, including pending
// and unknown work. It never returns raw job specifications or task secrets.
func (c *Client) ActiveAllocations(ctx context.Context, nodeID string) (int, error) {
	if !uuid.MatchString(nodeID) {
		return 0, failure("InvalidNodeID")
	}
	proof, err := c.allocationVisibilityProof(ctx)
	if err != nil {
		return 0, err
	}
	var rows []struct{ ID, NodeID, ClientStatus string }
	h, err := c.request(ctx, http.MethodGet, "/v1/node/"+nodeID+"/allocations", url.Values{"namespace": {"*"}}, nil, &rows)
	if err != nil {
		return 0, err
	}
	if h.Get("X-Nomad-NextToken") != "" {
		return 0, failure("IncompleteAllocations")
	}
	seen := map[string]bool{}
	count := 0
	for _, a := range rows {
		if !uuid.MatchString(a.ID) || a.NodeID != nodeID || seen[a.ID] {
			return 0, failure("InvalidAllocations")
		}
		seen[a.ID] = true
		switch a.ClientStatus {
		case "complete", "failed", "lost":
		case "pending", "running", "unknown":
			count++
		default:
			return 0, failure("InvalidAllocationStatus")
		}
	}
	after, err := c.allocationVisibilityProof(ctx)
	if err != nil {
		return 0, err
	}
	if after != proof {
		return 0, failure("AllocationVisibilityChanged")
	}
	return count, nil
}
