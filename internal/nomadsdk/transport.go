package nomadsdk

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"regexp"
	"strings"
	"sync/atomic"

	"github.com/hashicorp/nomad/api"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadclient"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/targetconfig"
)

const maxBody = 2 << 20

var uuid = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// This view is local to one SDK invocation. It neither modifies Nomad metadata
// nor exposes unrelated group allocations. All mutations require a durable
// caller barrier and an exact node identity; it is not a replacement for CAS.
type scopedTransport struct {
	ctx                                      context.Context
	next                                     http.RoundTripper
	client                                   *nomadclient.Client
	policy                                   targetconfig.Policy
	origin, mode, op, selfAlloc              string
	allowed                                  map[string]nomadclient.Identity
	before                                   func() error
	unfiltered                               bool
	selfVerified, readFailed, writeAttempted atomic.Bool
	modifyIndex                              atomic.Uint64
}

func (s *scopedTransport) wrap(next http.RoundTripper) http.RoundTripper { s.next = next; return s }

func (s *scopedTransport) RoundTrip(input *http.Request) (resp *http.Response, err error) {
	write := input.Method != http.MethodGet
	defer func() {
		if err != nil && !write {
			s.readFailed.Store(true)
		}
	}()
	if input.URL.Scheme+"://"+input.URL.Host != s.origin || input.URL.RawPath != "" || input.URL.User != nil {
		return nil, failure("SDKRequestOutsideScope")
	}
	ctx, cancel := context.WithCancel(input.Context())
	stop := context.AfterFunc(s.ctx, cancel)
	defer func() { stop(); cancel() }()
	if s.ctx.Err() != nil {
		return nil, failure("SDKContextEnded")
	}
	r := input.Clone(ctx)
	path := r.URL.Path
	ownAlloc := s.selfAlloc != "" && path == "/v1/allocation/"+s.selfAlloc
	nodeID, suffix := "", ""
	if strings.HasPrefix(path, "/v1/node/") {
		parts := strings.Split(strings.TrimPrefix(path, "/v1/node/"), "/")
		nodeID = parts[0]
		if len(parts) == 2 {
			suffix = parts[1]
		} else if len(parts) != 1 {
			return nil, failure("SDKRequestOutsideScope")
		}
		if _, ok := s.allowed[nodeID]; !ok {
			return nil, failure("SDKRequestOutsideScope")
		}
	}
	if !write {
		if path != "/v1/nodes" && !ownAlloc && (nodeID == "" || (suffix != "" && suffix != "allocations")) {
			return nil, failure("SDKReadOutsideScope")
		}
		if suffix == "allocations" || ownAlloc {
			q := r.URL.Query()
			q.Set("namespace", "*")
			r.URL.RawQuery = q.Encode()
		}
	} else {
		if r.Method != http.MethodPut || nodeID == "" || !((s.mode == "pre" && suffix == "drain") || (s.mode == "post" && suffix == "purge")) {
			return nil, failure("SDKWriteOutsideIntent")
		}
		if err = s.prepareWrite(r, nodeID, suffix); err != nil {
			return nil, err
		}
	}
	resp, b, err := s.fetch(r)
	if err != nil {
		return nil, err
	}
	switch {
	case path == "/v1/nodes":
		var nodes []*api.NodeListStub
		nodes, err = s.nodePages(r, resp, b)
		if err == nil {
			b, err = json.Marshal(nodes)
			resp.Header.Del("X-Nomad-NextToken")
		}
	case ownAlloc:
		var a api.Allocation
		if json.Unmarshal(b, &a) != nil || a.ID != s.selfAlloc || !uuid.MatchString(a.NodeID) {
			err = failure("SDKSelfIdentityUnavailable")
		} else {
			s.selfVerified.Store(true)
		}
	case !write && suffix == "":
		var n api.Node
		want := s.allowed[nodeID]
		if json.Unmarshal(b, &n) != nil || n.ID != nodeID || n.Meta[s.policy.InstanceIDMetaKey] != want.InstanceID {
			err = failure("SDKMappingChanged")
		}
	case !write && suffix == "allocations":
		if resp.Header.Get("X-Nomad-NextToken") != "" {
			err = failure("SDKIncompleteAllocations")
		} else {
			err = validateAllocations(b, nodeID)
		}
	case write && suffix == "drain":
		var ack struct{ NodeModifyIndex uint64 }
		if json.Unmarshal(b, &ack) != nil || ack.NodeModifyIndex == 0 {
			err = failure("SDKDrainResponseInvalid")
		} else {
			s.modifyIndex.Store(ack.NodeModifyIndex)
		}
	}
	if err != nil {
		return nil, err
	}
	resp.Body = io.NopCloser(bytes.NewReader(b))
	resp.ContentLength = int64(len(b))
	resp.Header.Del("Content-Length")
	return resp, nil
}

// fetch buffers and closes each upstream response while both the outer action
// context and SDK monitor's child context are still active. Errors never carry
// upstream text, URLs or credentials to callers or SDK logs.
func (s *scopedTransport) fetch(r *http.Request) (*http.Response, []byte, error) {
	resp, err := s.next.RoundTrip(r)
	if err != nil {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		return nil, nil, failure("SDKRequestFailed")
	}
	if resp == nil || resp.Body == nil {
		return nil, nil, failure("SDKResponseInvalid")
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if err != nil || len(b) > maxBody {
		return nil, nil, failure("SDKResponseUnreadable")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, nil, failure("SDKHTTPFailure")
	}
	// The Nomad API client advertises gzip explicitly, so net/http leaves
	// decoding to that client. This scoped view validates/rewrites JSON before
	// the API client receives it: decode here and remove the header to prevent
	// a second decompression. Bound both the wire and decoded representations.
	switch strings.ToLower(strings.TrimSpace(resp.Header.Get("Content-Encoding"))) {
	case "", "identity":
	case "gzip":
		z, decodeErr := gzip.NewReader(bytes.NewReader(b))
		if decodeErr != nil {
			return nil, nil, failure("SDKResponseUnreadable")
		}
		decoded, decodeErr := io.ReadAll(io.LimitReader(z, maxBody+1))
		closeErr := z.Close()
		if decodeErr != nil || closeErr != nil || len(decoded) > maxBody {
			return nil, nil, failure("SDKResponseUnreadable")
		}
		b = decoded
		resp.Header.Del("Content-Encoding")
		resp.Uncompressed = true
	default:
		return nil, nil, failure("SDKResponseEncodingUnsupported")
	}
	return resp, b, nil
}

func (s *scopedTransport) nodePages(r *http.Request, resp *http.Response, b []byte) ([]*api.NodeListStub, error) {
	selected := make([]*api.NodeListStub, 0, len(s.allowed))
	seenNodes := map[string]bool{}
	seenTokens := map[string]bool{}
	for page := 0; page < 100; page++ {
		var rows []*api.NodeListStub
		if json.Unmarshal(b, &rows) != nil || rows == nil {
			return nil, failure("SDKNodeListInvalid")
		}
		for _, n := range rows {
			if n == nil || !uuid.MatchString(n.ID) || seenNodes[n.ID] {
				return nil, failure("SDKNodeListInvalid")
			}
			seenNodes[n.ID] = true
			if len(seenNodes) > 10000 {
				return nil, failure("SDKNodeListTooLarge")
			}
			if _, ok := s.allowed[n.ID]; !ok {
				continue
			}
			// A marker exists only in this SDK response when all configured filters are
			// empty. Real metadata and configured SDK AND-filters are left untouched.
			if s.unfiltered {
				n.NodeClass = scopeClass
			}
			selected = append(selected, n)
		}
		next := resp.Header.Get("X-Nomad-NextToken")
		if next == "" {
			if len(selected) != len(s.allowed) {
				return nil, failure("SDKScopedNodeMissing")
			}
			resp.Header.Del("X-Nomad-NextToken")
			return selected, nil
		}
		if len(next) > 4096 || seenTokens[next] || len(rows) == 0 {
			return nil, failure("SDKPaginationInvalid")
		}
		seenTokens[next] = true
		req := r.Clone(r.Context())
		q := req.URL.Query()
		q.Set("next_token", next)
		req.URL.RawQuery = q.Encode()
		var err error
		resp, b, err = s.fetch(req)
		if err != nil {
			return nil, err
		}
	}
	return nil, failure("SDKPaginationLimit")
}

func validateAllocations(b []byte, nodeID string) error {
	var rows []*api.Allocation
	if json.Unmarshal(b, &rows) != nil || rows == nil {
		return failure("SDKAllocationsInvalid")
	}
	seen := map[string]bool{}
	for _, a := range rows {
		if a == nil || !uuid.MatchString(a.ID) || a.NodeID != nodeID || a.Namespace == "" || seen[a.ID] || a.Job == nil || a.Job.Type == nil {
			return failure("SDKAllocationsInvalid")
		}
		seen[a.ID] = true
		switch a.ClientStatus {
		case "pending", "running", "unknown", "complete", "failed", "lost":
		default:
			return failure("SDKAllocationsInvalid")
		}
		if a.ClientStatus == "running" && (a.Resources == nil || a.Resources.CPU == nil || a.Resources.MemoryMB == nil || *a.Resources.CPU < 0 || *a.Resources.MemoryMB < 0) {
			return failure("SDKAllocationResourcesInvalid")
		}
	}
	return nil
}

func (s *scopedTransport) prepareWrite(r *http.Request, nodeID, suffix string) error {
	if s.before == nil || s.writeAttempted.Load() {
		return failure("SDKWriteOutsideIntent")
	}
	want := s.allowed[nodeID]
	n, err := s.client.ReadNode(r.Context(), nodeID)
	if err != nil || nomadclient.CheckIdentity(n, want) != nil {
		return failure("SDKWriteIdentityChanged")
	}
	if suffix == "drain" {
		if n.Status != "ready" || n.Eligibility != "eligible" || n.Drain || n.HasDrainStrategy || owned(n, s.op) {
			return failure("SDKDrainPreconditionFailed")
		}
		if r.Body == nil {
			return failure("SDKDrainPayloadInvalid")
		}
		b, err := io.ReadAll(io.LimitReader(r.Body, maxBody+1))
		r.Body.Close()
		if err != nil || len(b) > maxBody {
			return failure("SDKDrainPayloadInvalid")
		}
		var p struct {
			NodeID       string
			DrainSpec    *api.DrainSpec
			MarkEligible bool
			Meta         map[string]string
		}
		if json.Unmarshal(b, &p) != nil || p.DrainSpec == nil || (p.NodeID != "" && p.NodeID != nodeID) || p.MarkEligible || p.DrainSpec.Deadline != s.policy.DrainDeadline || p.DrainSpec.IgnoreSystemJobs != s.policy.IgnoreSystemJobs {
			return failure("SDKDrainPayloadInvalid")
		}
		var payload map[string]json.RawMessage
		if json.Unmarshal(b, &payload) != nil {
			return failure("SDKDrainPayloadInvalid")
		}
		payload["Meta"], err = json.Marshal(map[string]string{"drained_by": owner, "tencent_operation_id": s.op})
		if err != nil {
			return failure("SDKDrainPayloadInvalid")
		}
		b, err = json.Marshal(payload)
		if err != nil {
			return failure("SDKDrainPayloadInvalid")
		}
		r.Body = io.NopCloser(bytes.NewReader(b))
		r.ContentLength = int64(len(b))
		r.GetBody = nil
	} else if !owned(n, s.op) {
		return failure("SDKPurgeOwnershipLost")
	}
	if s.before() != nil {
		return failure("SDKDurableBarrierFailed")
	}
	if r.Context().Err() != nil {
		return failure("SDKContextEnded")
	}
	if !s.writeAttempted.CompareAndSwap(false, true) {
		return failure("SDKDuplicateWrite")
	}
	return nil
}
