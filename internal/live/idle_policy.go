package live

import (
	"os"
	"path/filepath"
	"reflect"
	"time"
)

// Historical results retain their original deadline when an idle controller
// adopts a new policy. IDs prevent applying this context to any future action.
type PolicyHistoryContext struct {
	OperationID   string        `json:"operation_id,omitempty"`
	BatchID       string        `json:"batch_id,omitempty"`
	DrainDeadline time.Duration `json:"drain_deadline_nanoseconds"`
}
type IdlePolicyAudit struct {
	Version         int       `json:"version"`
	Before          State     `json:"before"`
	After           State     `json:"after_without_policy_audit_head"`
	OldPolicySHA256 string    `json:"old_policy_sha256"`
	NewPolicySHA256 string    `json:"new_policy_sha256"`
	At              time.Time `json:"at"`
	Operator        string    `json:"operator"`
	Reference       string    `json:"reference"`
}
type IdlePolicyChange struct {
	Before  []byte `json:"before"`
	After   []byte `json:"after"`
	Audit   []byte `json:"audit"`
	AuditID string `json:"audit_id"`
}

// The caller owns the route lock first; this second lock excludes live.Open and
// the recovery CLI. No provider client is constructed by this maintenance API.
func LockIdlePolicyState(dir string) (func(), error) {
	e := &Engine{cfg: Config{Dir: dir}}
	if !filepath.IsAbs(dir) || filepath.Clean(dir) == "/" {
		return nil, code("InvalidPolicyDirectory")
	}
	if err := e.lockDir(); err != nil {
		return nil, err
	}
	return e.release, nil
}
func idlePolicyAfter(before State, deadline time.Duration, digest string) (State, error) {
	if before.Version != 3 || before.Pending != nil || before.Batch != nil || before.ManualReview || before.LegacyContract != nil || !digestPattern.MatchString(before.NomadPolicySHA256) || !digestPattern.MatchString(digest) || !validControllerDrainDeadline(Config{GroupContract: true, DrainDeadline: deadline}) {
		return State{}, code("PolicyChangeRequiresIdleController")
	}
	after := cloneRetryState(before)
	if deadline != time.Duration(before.DrainDeadlineNanoseconds) {
		// An idle state's last operation and last batch always refer to the same
		// last outcome. A previously archived context remains authoritative.
		if before.LastOperation != nil || before.LastBatch != nil {
			d := time.Duration(before.DrainDeadlineNanoseconds)
			if h := before.PolicyHistory; h != nil && ((before.LastOperation != nil && h.OperationID == before.LastOperation.ID) || (before.LastBatch != nil && h.BatchID == before.LastBatch.ID)) {
				d = h.DrainDeadline
			}
			h := &PolicyHistoryContext{DrainDeadline: d}
			if before.LastOperation != nil {
				h.OperationID = before.LastOperation.ID
			}
			if before.LastBatch != nil {
				h.BatchID = before.LastBatch.ID
			}
			after.PolicyHistory = h
		}
	}
	after.DrainDeadlineNanoseconds = int64(deadline)
	after.NomadPolicySHA256 = digest
	return after, nil
}
func ValidateIdlePolicyChange(change IdlePolicyChange) error {
	var a IdlePolicyAudit
	var before, after State
	if sha(change.Audit) != change.AuditID || decodeStrict(change.Audit, &a) != nil || decodeStrict(change.Before, &before) != nil || decodeStrict(change.After, &after) != nil || a.Version != 1 || a.At.IsZero() || !actorPattern.MatchString(a.Operator) || !referencePattern.MatchString(a.Reference) || !reflect.DeepEqual(a.Before, before) || a.OldPolicySHA256 != before.NomadPolicySHA256 || a.NewPolicySHA256 != a.After.NomadPolicySHA256 {
		return code("InvalidIdlePolicyAudit")
	}
	expected, err := idlePolicyAfter(before, time.Duration(a.After.DrainDeadlineNanoseconds), a.NewPolicySHA256)
	if err != nil || !reflect.DeepEqual(expected, a.After) {
		return code("InvalidIdlePolicyAudit")
	}
	expected.PolicyAuditSHA256 = change.AuditID
	if !reflect.DeepEqual(expected, after) {
		return code("InvalidIdlePolicyAudit")
	}
	for _, s := range []State{before, after} {
		e := &Engine{cfg: Config{GroupContract: true, GroupID: s.GroupID, Region: s.Region, DrainDeadline: time.Duration(s.DrainDeadlineNanoseconds)}, state: s}
		if e.validateState() != nil {
			return code("InvalidIdlePolicyState")
		}
	}
	return nil
}

// Prepare requires both locks. Mapping changes deliberately require a separate
// identity migration; this offline operation never guesses a node identity.
func PrepareIdlePolicyChange(cfg Config, digest, key, operator, reference string, deadline time.Duration) (IdlePolicyChange, error) {
	var out IdlePolicyChange
	e := &Engine{cfg: cfg}
	for _, name := range []string{recoveryMarkerName, journalRetryQueueName} {
		if _, err := os.Lstat(filepath.Join(cfg.Dir, name)); !os.IsNotExist(err) {
			return out, code("PolicyChangeRecoveryPending")
		}
	}
	exists, err := e.load()
	if err != nil {
		return out, err
	}
	if !exists {
		return out, code("PolicyChangeJournalMissing")
	}
	if err = e.checkRecoveryAudit(); err != nil {
		return out, err
	}
	if key != e.state.NomadInstanceIDMetaKey {
		return out, code("PolicyMappingChangeRequiresIdentityVerification")
	}
	after, err := idlePolicyAfter(e.state, deadline, digest)
	if err != nil {
		return out, err
	}
	a := IdlePolicyAudit{Version: 1, Before: e.state, After: after, OldPolicySHA256: e.state.NomadPolicySHA256, NewPolicySHA256: digest, At: time.Now().UTC(), Operator: operator, Reference: reference}
	raw := jsonBytes(a)
	after.PolicyAuditSHA256 = sha(raw)
	out = IdlePolicyChange{Before: append([]byte(nil), e.durableJournal...), After: jsonBytes(after), Audit: raw, AuditID: sha(raw)}
	return out, ValidateIdlePolicyChange(out)
}
func (e *Engine) checkPolicyAudit() error {
	head := e.state.PolicyAuditSHA256
	if head == "" {
		if e.state.PolicyHistory != nil {
			return code("PolicyHistoryAuditMissing")
		}
		return nil
	}
	seen := map[string]bool{}
	for head != "" {
		if !digestPattern.MatchString(head) || seen[head] || len(seen) >= 10000 {
			return code("InvalidIdlePolicyAudit")
		}
		seen[head] = true
		dir := filepath.Join(e.cfg.Dir, "policy-audit")
		info, err := os.Lstat(dir)
		if err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
			return code("InvalidIdlePolicyAudit")
		}
		raw, err := readPrivate(filepath.Join(dir, head+".json"), 2*stateFileLimit)
		var a IdlePolicyAudit
		if err != nil || sha(raw) != head || decodeStrict(raw, &a) != nil {
			return code("InvalidIdlePolicyAudit")
		}
		after := cloneRetryState(a.After)
		after.PolicyAuditSHA256 = head
		if ValidateIdlePolicyChange(IdlePolicyChange{Before: jsonBytes(a.Before), After: jsonBytes(after), Audit: raw, AuditID: head}) != nil || a.Before.GroupID != e.cfg.GroupID || a.Before.Region != e.cfg.Region {
			return code("InvalidIdlePolicyAudit")
		}
		if head == e.state.PolicyAuditSHA256 && (e.state.NomadPolicySHA256 != after.NomadPolicySHA256 || e.state.NomadInstanceIDMetaKey != after.NomadInstanceIDMetaKey || e.state.DrainDeadlineNanoseconds != after.DrainDeadlineNanoseconds || !reflect.DeepEqual(e.state.PolicyHistory, after.PolicyHistory)) {
			return code("PolicyHistoryAuditMismatch")
		}
		head = a.Before.PolicyAuditSHA256
	}
	return nil
}
func (e *Engine) historicalPolicyEngine(operation, batch string) *Engine {
	cfg := e.cfg
	if h := e.state.PolicyHistory; h != nil && ((operation != "" && h.OperationID == operation) || (batch != "" && h.BatchID == batch)) {
		cfg.DrainDeadline = h.DrainDeadline
	}
	return &Engine{cfg: cfg, state: e.state}
}

// A prepared policy transaction must finish before any engine or recovery work.
func (e *Engine) checkPolicyPending() error {
	if _, err := os.Lstat(filepath.Join(e.cfg.Dir, "policy-update-pending.json")); !os.IsNotExist(err) {
		return code("PolicyUpdatePending")
	}
	return nil
}
