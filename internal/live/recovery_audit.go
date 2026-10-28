package live

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"time"
)

type RecoveryAuditRef struct {
	ID     string `json:"id"`
	SHA256 string `json:"sha256"`
}
type recoveryRecord struct {
	Version      int          `json:"version"`
	Automatic    bool         `json:"automatic,omitempty"`
	Plan         RecoveryPlan `json:"plan"`
	AppliedAt    time.Time    `json:"applied_at"`
	ExecutorUID  int          `json:"executor_uid"`
	ExecutorEUID int          `json:"executor_euid"`
	BeforeSHA256 string       `json:"before_sha256"`
	Before       State        `json:"before"`
	// After retains the previous audit head, avoiding a self-referential hash.
	After    State              `json:"after_without_new_audit_head"`
	Evidence []RecoveryEvidence `json:"applied_evidence"`
}
type recoveryMarker struct {
	Audit        RecoveryAuditRef `json:"audit"`
	BeforeSHA256 string           `json:"before_sha256"`
	AfterSHA256  string           `json:"after_sha256"`
}
type RecoveryResult struct {
	Audit         RecoveryAuditRef `json:"audit"`
	Status        string           `json:"status"`
	JournalSHA256 string           `json:"journal_sha256"`
	At            time.Time        `json:"at"`
	ExecutorUID   int              `json:"executor_uid"`
	ExecutorEUID  int              `json:"executor_euid"`
}

const recoveryMarkerName = "recovery-pending.json"

func readPrivate(path string, limit int64) ([]byte, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, code("RecoveryFileUnavailable")
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > limit {
		return nil, code("RecoveryFileUnsafe")
	}
	b, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil || int64(len(b)) > limit {
		return nil, code("RecoveryFileReadFailed")
	}
	return b, nil
}
func decodeStrict(b []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if d.Decode(v) != nil {
		return code("RecoveryFileInvalid")
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return code("RecoveryFileInvalid")
	}
	return nil
}

// Publish a fully synced inode atomically with no replacement. A crash cannot
// leave a partial record at the final name, and retry never overwrites evidence.
func writeOnce(path string, b []byte) error {
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, ".recovery-*")
	if err != nil {
		return code("RecoveryAuditWriteFailed")
	}
	defer os.Remove(f.Name())
	_, err = f.Write(b)
	if err == nil {
		err = f.Sync()
	}
	ce := f.Close()
	if err == nil {
		err = ce
	}
	if err == nil {
		err = os.Link(f.Name(), path)
	}
	if err == nil {
		err = syncDir(dir)
	}
	if err != nil {
		return code("RecoveryAuditWriteFailed")
	}
	return nil
}
func (e *Engine) checkRecoveryPending() error {
	_, err := os.Lstat(filepath.Join(e.cfg.Dir, recoveryMarkerName))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return code("RecoveryAuditRepairRequired")
}
func validRef(ref *RecoveryAuditRef) bool {
	return ref != nil && digestPattern.MatchString(ref.ID) && digestPattern.MatchString(ref.SHA256)
}
func auditPath(dir string, ref RecoveryAuditRef) string {
	return filepath.Join(dir, "recovery-audit", ref.ID+".json")
}
func resultPath(dir string, ref RecoveryAuditRef) string {
	return filepath.Join(dir, "recovery-audit", ref.ID+".result.json")
}
func readRecord(dir string, ref RecoveryAuditRef) (recoveryRecord, error) {
	var record recoveryRecord
	if !validRef(&ref) {
		return record, code("RecoveryAuditInvalid")
	}
	// Do not allow a symlink in the audit directory path.
	info, err := os.Lstat(filepath.Join(dir, "recovery-audit"))
	if err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return record, code("RecoveryAuditInvalid")
	}
	b, err := readPrivate(auditPath(dir, ref), 32<<20)
	if err != nil || sha(b) != ref.SHA256 || decodeStrict(b, &record) != nil {
		return record, code("RecoveryAuditInvalid")
	}
	if !validRecoveryRecordOrigin(record) || record.Plan.Digest != ref.ID || planDigest(record.Plan) != ref.ID || record.BeforeSHA256 != record.Plan.JournalSHA256 || !reflect.DeepEqual(record.Before.RecoveryAudit, record.After.RecoveryAudit) || record.Before.GroupID != record.After.GroupID || record.Before.Region != record.After.Region || record.Plan.Scope.GroupID != record.Before.GroupID || record.Plan.Scope.Region != record.Before.Region {
		return record, code("RecoveryAuditInvalid")
	}
	return record, nil
}
func resultState(record recoveryRecord, ref RecoveryAuditRef) State {
	s := record.After
	s.RecoveryAudit = &ref
	return s
}
func (e *Engine) checkRecoveryAudit() error {
	ref := e.state.RecoveryAudit
	seen := map[string]bool{}
	migrationFound := e.state.LegacyContract == nil
	launchChecked := false
	drainOrigins := map[string]bool{}
	for _, b := range []*Batch{e.state.Batch, e.state.LastBatch} {
		if b != nil && b.LegacyDrainOrigin != nil {
			drainOrigins[b.ID] = false
		}
	}

	for ref != nil {
		if !validRef(ref) || seen[ref.ID] || len(seen) >= 10000 {
			return code("RecoveryAuditInvalid")
		}
		seen[ref.ID] = true
		record, err := readRecord(e.cfg.Dir, *ref)
		if err != nil {
			return err
		}
		if record.Version == 3 && (record.Plan.Decision.Action == "migrate-v2-sdk-contract" || record.Plan.Decision.Action == "migrate-v1-sdk-operation" || record.Plan.Decision.Action == "migrate-legacy-custom-contract") && e.state.Batch != nil && record.After.Batch != nil && e.state.Batch.ID == record.After.Batch.ID {
			if !reflect.DeepEqual(e.state.LegacyContract, record.After.LegacyContract) {
				return code("LegacyMigrationAuditMismatch")
			}
			migrationFound = true
		}
		if record.Version == 3 && record.Plan.Decision.Action == "migrate-legacy-custom-contract" && record.After.Batch != nil {
			for _, b := range []*Batch{e.state.Batch, e.state.LastBatch} {
				if b != nil && b.LegacyDrainOrigin != nil && b.ID == record.After.Batch.ID {
					if !reflect.DeepEqual(b.LegacyDrainOrigin, record.After.Batch.LegacyDrainOrigin) || !reflect.DeepEqual(b.DrainWait, record.After.Batch.DrainWait) {
						return code("LegacyMigrationAuditMismatch")
					}
					drainOrigins[b.ID] = true
				}
			}
		}
		if record.Version == 4 && !launchChecked {
			if err := e.checkLaunchConfigurationAudit(record); err != nil {
				return err
			}
			launchChecked = true
		}
		if record.Before.GroupID != e.cfg.GroupID || record.Before.Region != e.cfg.Region {
			return code("RecoveryAuditScopeMismatch")
		}
		b, err := readPrivate(resultPath(e.cfg.Dir, *ref), 1<<20)
		var receipt RecoveryResult
		if err != nil || decodeStrict(b, &receipt) != nil || receipt.Audit != *ref || receipt.Status != "committed" || receipt.JournalSHA256 != sha(jsonBytes(resultState(record, *ref))) {
			return code("RecoveryAuditReceiptInvalid")
		}
		ref = record.Before.RecoveryAudit
	}
	for _, found := range drainOrigins {
		if !found {
			return code("LegacyMigrationAuditMissing")
		}
	}
	if !migrationFound {
		return code("LegacyMigrationAuditMissing")
	}
	return nil
}
func (r *Recovery) checkpointAt(stage string) error {
	if r.checkpoint != nil {
		return r.checkpoint(stage)
	}
	return nil
}
func (r *Recovery) commit(p RecoveryPlan, evidence []RecoveryEvidence, after State, beforeHash string) (RecoveryResult, error) {
	var out RecoveryResult
	e := r.e
	dir := filepath.Join(e.cfg.Dir, "recovery-audit")
	if err := privateDir(dir); err != nil {
		return out, code("RecoveryAuditDirectoryFailed")
	}
	info, err := os.Lstat(dir)
	if err != nil || info.Mode().Perm()&0077 != 0 {
		return out, code("RecoveryAuditDirectoryUnsafe")
	}
	record := recoveryRecord{Version: 1, Plan: p, AppliedAt: time.Now().UTC(), ExecutorUID: os.Getuid(), ExecutorEUID: os.Geteuid(), BeforeSHA256: beforeHash, Before: e.state, After: after, Evidence: evidence}
	if r.automatic {
		record.Version = 2
		if r.workflowAudit {
			record.Version = 3
		}
		record.Automatic = true
	}
	if p.Version == 2 && p.Decision.Action == acceptLaunchConfiguration {
		record.Version = 4
		if !validLaunchConfigurationRecord(record) {
			return out, code("InvalidLaunchConfigurationAudit")
		}
	}
	b := jsonBytes(record)
	ref := RecoveryAuditRef{ID: p.Digest, SHA256: sha(b)}
	after = resultState(record, ref)
	afterHash := sha(jsonBytes(after))
	marker := recoveryMarker{Audit: ref, BeforeSHA256: beforeHash, AfterSHA256: afterHash}
	if r.automatic {
		e.automaticAudit = &marker
	}
	if r.automatic {
		if err = r.checkpointAt("before-record"); err != nil {
			return out, err
		}
	}
	if err = writeOnce(auditPath(e.cfg.Dir, ref), b); err != nil {
		return out, err
	}
	if r.automatic {
		if err = r.checkpointAt("record"); err != nil {
			return out, err
		}
	}
	if err = writeOnce(filepath.Join(e.cfg.Dir, recoveryMarkerName), jsonBytes(marker)); err != nil {
		return out, err
	}
	if err = r.checkpointAt("prepared"); err != nil {
		return out, err
	}
	current, readErr := readPrivate(filepath.Join(e.cfg.Dir, "live.json"), stateFileLimit)
	if readErr != nil || sha(current) != beforeHash {
		return out, code("RecoveryJournalConflict")
	}
	e.state = after
	if err = e.save(); err != nil {
		return out, err
	}
	if err = r.checkpointAt("journal"); err != nil {
		return out, err
	}
	out = RecoveryResult{Audit: ref, Status: "committed", JournalSHA256: afterHash, At: time.Now().UTC(), ExecutorUID: os.Getuid(), ExecutorEUID: os.Geteuid()}
	if err = writeOnce(resultPath(e.cfg.Dir, ref), jsonBytes(out)); err != nil {
		return out, err
	}
	if err = r.checkpointAt("receipt"); err != nil {
		return out, err
	}
	if err = clearMarker(e.cfg.Dir); err != nil {
		return out, err
	}
	if r.automatic {
		if err = r.checkpointAt("cleared"); err != nil {
			return out, err
		}
	}
	if r.automatic {
		e.automaticAudit = nil
	}
	return out, nil
}
func clearMarker(dir string) error {
	if err := os.Remove(filepath.Join(dir, recoveryMarkerName)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return code("RecoveryAuditRepairRequired")
	}
	if syncDir(dir) != nil {
		return code("RecoveryAuditRepairRequired")
	}
	return nil
}

// Repair only finishes/aborts local bookkeeping. It never writes live.json or
// calls cloud/Nomad. If the journal was not committed, unknown stays frozen.
func RepairRecovery(cfg Config, confirmation string) (RecoveryResult, error) {
	var out RecoveryResult
	if !digestPattern.MatchString(confirmation) {
		return out, code("RecoveryConfirmationRequired")
	}
	e, err := recoveryEngine(cfg)
	if err != nil {
		return out, err
	}
	defer e.release()
	b, err := readPrivate(filepath.Join(e.cfg.Dir, recoveryMarkerName), 1<<20)
	var marker recoveryMarker
	if err != nil || decodeStrict(b, &marker) != nil || marker.Audit.ID != confirmation {
		return out, code("RecoveryMarkerMismatch")
	}
	return e.finishRecoveryBookkeeping(marker)
}

// finishRecoveryBookkeeping changes audit bookkeeping only, under the existing
// directory lock. It never rewrites the journal or calls cloud/Nomad.
func (e *Engine) finishRecoveryBookkeeping(marker recoveryMarker) (RecoveryResult, error) {
	var out RecoveryResult
	record, err := readRecord(e.cfg.Dir, marker.Audit)
	if err != nil {
		return out, err
	}
	if scope(e.cfg) != record.Plan.Scope || marker.BeforeSHA256 != record.BeforeSHA256 || marker.AfterSHA256 != sha(jsonBytes(resultState(record, marker.Audit))) {
		return out, code("RecoveryMarkerMismatch")
	}
	b, err := readPrivate(filepath.Join(e.cfg.Dir, "live.json"), stateFileLimit)
	if err != nil {
		return out, err
	}
	current := sha(b)
	status := ""
	switch current {
	case marker.BeforeSHA256:
		status = "aborted"
	case marker.AfterSHA256:
		status = "committed"
	default:
		return out, code("RecoveryJournalConflict")
	}
	loaded := &Engine{cfg: e.cfg}
	exists, err := loaded.load()
	if err != nil || !exists {
		return out, code("RecoveryJournalInvalid")
	}
	out = RecoveryResult{Audit: marker.Audit, Status: status, JournalSHA256: current, At: time.Now().UTC(), ExecutorUID: os.Getuid(), ExecutorEUID: os.Geteuid()}
	path := resultPath(e.cfg.Dir, marker.Audit)
	if _, err = os.Lstat(path); err == nil {
		prior, readErr := readPrivate(path, 1<<20)
		var receipt RecoveryResult
		if readErr != nil || decodeStrict(prior, &receipt) != nil || receipt.Audit != out.Audit || receipt.Status != out.Status || receipt.JournalSHA256 != current {
			return out, code("RecoveryReceiptConflict")
		}
		out = receipt
	} else if !errors.Is(err, os.ErrNotExist) {
		return out, code("RecoveryReceiptConflict")
	} else if err = writeOnce(path, jsonBytes(out)); err != nil {
		return out, err
	}
	if err = loaded.checkRecoveryAudit(); err != nil {
		return out, err
	}
	if err = clearMarker(e.cfg.Dir); err != nil {
		return out, err
	}
	e.state = loaded.state
	e.durableJournal = append([]byte(nil), loaded.durableJournal...)
	return out, nil
}

// ReadRecoveryPlan accepts only a private, bounded, regular file and strict JSON.
func ReadRecoveryPlan(path string) (RecoveryPlan, error) {
	var p RecoveryPlan
	b, err := readPrivate(path, 32<<20)
	if err != nil {
		return p, err
	}
	err = decodeStrict(b, &p)
	return p, err
}

// WriteRecoveryPlan refuses replacement, including symlinks.
func WriteRecoveryPlan(path string, p RecoveryPlan) error { return writeOnce(path, jsonBytes(p)) }
