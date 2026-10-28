package live

import (
	"reflect"
	"time"
)

// The old result prefix stays byte-for-byte unchanged. This optional binding
// distinguishes it from SDK children started after nonpositive-mode adoption.
// It survives as part of the completed batch and is checked against its audit.
type LegacyBatchDrainOrigin struct {
	SourceVersion int           `json:"source_version"`
	JournalSHA256 string        `json:"journal_sha256"`
	DrainDeadline time.Duration `json:"drain_deadline_nanoseconds"`
	PrefixCount   int           `json:"prefix_count"`
	PrefixSHA256  string        `json:"prefix_sha256"`
	OperationID   string        `json:"operation_id,omitempty"`
}

func legacyPrefixDigest(b *Batch, count int) string {
	return sha(jsonBytes(append([]Operation{}, b.Results[:count]...)))
}

func legacyCustomWaitBindingMatches(cfg Config, c *LegacyCustomBinding, b *Batch) bool {
	if cfg.DrainDeadline > 0 || b == nil || b.Kind != "shrink" {
		return c.DrainWait == nil
	}
	return c.DrainWait != nil && b.LegacyDrainOrigin != nil && reflect.DeepEqual(c.DrainWait, b.DrainWait)
}

func adoptLegacyCustomDrainWait(cfg Config, s *State, source string, oldDeadline time.Duration, version int, c *LegacyCustomBinding) error {
	b := s.Batch
	bad := code("InvalidLegacyCustomDrainWait")
	if cfg.DrainDeadline > 0 || b.Kind != "shrink" {
		if c.DrainWait != nil {
			return bad
		}
		return nil
	}
	w := c.DrainWait
	interval := cfg.StabilizationInterval
	if interval == 0 {
		interval = 5 * time.Second
	}
	if w != nil {
		interval = w.Interval
	}
	budget, err := completionBudgetFor(cfg.DrainDeadline, interval, b.InitialCount, b.Target, defaultDrainWaitAttempts)
	if err != nil {
		return bad
	}
	want := DrainWaitWindow{Until: b.StartedAt.Add(budget).UTC(), Attempts: defaultDrainWaitAttempts, Interval: interval}
	if w != nil && !reflect.DeepEqual(*w, want) {
		return bad
	}
	c.DrainWait = &want
	copy := want
	b.DrainWait = &copy
	origin := &LegacyBatchDrainOrigin{SourceVersion: version, JournalSHA256: source, DrainDeadline: oldDeadline, PrefixCount: len(b.Results), PrefixSHA256: legacyPrefixDigest(b, len(b.Results))}
	if s.Pending != nil {
		origin.OperationID = s.Pending.ID
	}
	b.LegacyDrainOrigin = origin
	return nil
}

func (e *Engine) validLegacyBatchDrainOrigin(b *Batch) bool {
	o := b.LegacyDrainOrigin
	if o == nil || !e.nonpositiveDrainMode() || e.state.Version != 3 || b.Kind != "shrink" || (o.SourceVersion != 1 && o.SourceVersion != 2) || !digestPattern.MatchString(o.JournalSHA256) || o.DrainDeadline <= 0 || o.DrainDeadline > 24*time.Hour || o.PrefixCount < 0 || o.PrefixCount > len(b.Results) || !digestPattern.MatchString(o.PrefixSHA256) || legacyPrefixDigest(b, o.PrefixCount) != o.PrefixSHA256 || (o.OperationID != "" && !operationID.MatchString(o.OperationID)) {
		return false
	}
	for i := 0; i < o.PrefixCount; i++ {
		op := b.Results[i]
		if op.LegacyOrigin != nil || op.DrainWait != nil || op.SDKLifecycle || op.Kind != "shrink" || op.Phase != "completed" {
			return false
		}
	}
	found := o.OperationID == ""
	check := func(op *Operation) bool {
		if op == nil || op.LegacyOrigin == nil {
			return true
		}
		v := op.LegacyOrigin
		if op.ID != o.OperationID || v.JournalSHA256 != o.JournalSHA256 || v.SourceVersion != o.SourceVersion || v.DrainDeadline != o.DrainDeadline || op.DrainWait != nil || op.SDKLifecycle {
			return false
		}
		found = true
		return true
	}
	for i := o.PrefixCount; i < len(b.Results); i++ {
		if !check(&b.Results[i]) {
			return false
		}
	}
	if e.state.Batch != nil && e.state.Batch.ID == b.ID && !check(e.state.Pending) {
		return false
	}
	return found
}

func (e *Engine) legacyCustomPrefixResult(op *Operation) bool {
	if op == nil || !e.nonpositiveDrainMode() {
		return false
	}
	for _, b := range []*Batch{e.state.Batch, e.state.LastBatch} {
		if b == nil || b.LegacyDrainOrigin == nil || !e.validLegacyBatchDrainOrigin(b) {
			continue
		}
		for i := 0; i < b.LegacyDrainOrigin.PrefixCount; i++ {
			if reflect.DeepEqual(*op, b.Results[i]) {
				return true
			}
		}
	}
	return false
}
