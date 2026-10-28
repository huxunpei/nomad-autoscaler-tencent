package plugin

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"time"

	"github.com/huxunpei/nomad-autoscaler-tencent/internal/live"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/nomadsdk"
	"github.com/huxunpei/nomad-autoscaler-tencent/internal/targetconfig"
)

const policyMarker = "policy-update-pending.json"
const policyFileLimit int64 = 256 << 20

// This explicit offline maintenance plan contains policy/state, never credentials.
type IdlePolicyPlan struct {
	Version     int                   `json:"version"`
	Directory   string                `json:"directory"`
	CreatedAt   time.Time             `json:"created_at"`
	ExpiresAt   time.Time             `json:"expires_at"`
	BeforeRoute []byte                `json:"before_route"`
	AfterRoute  []byte                `json:"after_route"`
	Change      live.IdlePolicyChange `json:"change"`
	Digest      string                `json:"digest"`
}

func policyJSON(v any) []byte              { b, _ := json.MarshalIndent(v, "", "  "); return append(b, '\n') }
func policyHash(b []byte) string           { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func policyDigest(p IdlePolicyPlan) string { p.Digest = ""; return policyHash(policyJSON(p)) }
func policyDecode(b []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if d.Decode(v) != nil {
		return errors.New("InvalidPolicyUpdateFile")
	}
	var tail any
	if d.Decode(&tail) != io.EOF {
		return errors.New("InvalidPolicyUpdateFile")
	}
	return nil
}
func policyRead(path string) ([]byte, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, errors.New("PolicyUpdateFileUnavailable")
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()
	s, err := f.Stat()
	if err != nil || !s.Mode().IsRegular() || s.Mode().Perm()&0077 != 0 || s.Size() > policyFileLimit {
		return nil, errors.New("UnsafePolicyUpdateFile")
	}
	b, err := io.ReadAll(io.LimitReader(f, policyFileLimit+1))
	if err != nil || int64(len(b)) > policyFileLimit {
		return nil, errors.New("PolicyUpdateFileUnavailable")
	}
	return b, nil
}
func policyWrite(path string, b []byte, once bool) error {
	d := filepath.Dir(path)
	f, err := os.CreateTemp(d, ".policy-update-*")
	if err != nil {
		return errors.New("PolicyUpdatePersistenceFailed")
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
		if once {
			err = os.Link(f.Name(), path)
		} else {
			err = os.Rename(f.Name(), path)
		}
	}
	if err == nil {
		err = routeSyncDir(d)
	}
	if err != nil {
		return errors.New("PolicyUpdatePersistenceFailed")
	}
	return nil
}
func policyConfig(dir string, r routeRecord) live.Config {
	return live.Config{Dir: dir, GroupID: r.Policy.GroupID, Region: r.Region, GroupContract: true, DrainDeadline: r.Policy.DrainDeadline, ScaleInProtection: r.Policy.ScaleInProtection}
}
func validPolicyRoute(r routeRecord) bool {
	a := targetconfig.Agent{Region: r.Region, ScaleInProtection: r.Policy.ScaleInProtection}
	v := r.Policy.SDKConfig()
	v["tencent_group_id"] = r.Policy.GroupID
	v["instance_id_meta_key"] = r.Policy.InstanceIDMetaKey
	p, err := a.Policy(v)
	return r.Version == 2 && r.MaxCapacity == 0 && verifiedAccountPattern.MatchString(r.Account) && r.Region != "" && err == nil && p == r.Policy
}
func validateIdlePolicyPlan(p IdlePolicyPlan) error {
	var old, next routeRecord
	if p.Version != 1 || !filepath.IsAbs(p.Directory) || filepath.Clean(p.Directory) != p.Directory || p.CreatedAt.IsZero() || !p.ExpiresAt.Equal(p.CreatedAt.Add(5*time.Minute)) || p.Digest != policyDigest(p) || policyDecode(p.BeforeRoute, &old) != nil || policyDecode(p.AfterRoute, &next) != nil || !validPolicyRoute(old) || !validPolicyRoute(next) || old.Account != next.Account || old.Region != next.Region || old.Policy.GroupID != next.Policy.GroupID || old.Policy.InstanceIDMetaKey != next.Policy.InstanceIDMetaKey || old.Policy == next.Policy || filepath.Base(p.Directory) != old.Policy.GroupID || filepath.Base(filepath.Dir(p.Directory)) != old.Region || filepath.Base(filepath.Dir(filepath.Dir(p.Directory))) != old.Account || live.ValidateIdlePolicyChange(p.Change) != nil {
		return errors.New("InvalidIdlePolicyPlan")
	}
	var before, after live.State
	_ = json.Unmarshal(p.Change.Before, &before)
	_ = json.Unmarshal(p.Change.After, &after)
	if before.NomadPolicySHA256 != nomadsdk.New(nil, old.Policy).PolicyDigest() || after.NomadPolicySHA256 != nomadsdk.New(nil, next.Policy).PolicyDigest() || before.NomadInstanceIDMetaKey != old.Policy.InstanceIDMetaKey || after.NomadInstanceIDMetaKey != next.Policy.InstanceIDMetaKey || before.DrainDeadlineNanoseconds != int64(old.Policy.DrainDeadline) || after.DrainDeadlineNanoseconds != int64(next.Policy.DrainDeadline) || before.GroupID != old.Policy.GroupID || before.Region != old.Region {
		return errors.New("PolicyUpdateContractMismatch")
	}
	return nil
}

// Preview takes both existing locks and performs no provider/network calls.
func PreviewIdlePolicyUpdate(dir string, values map[string]string, operator, reference string) (IdlePolicyPlan, error) {
	var p IdlePolicyPlan
	dir = filepath.Clean(dir)
	if _, err := os.Lstat(filepath.Join(dir, "route.json")); err != nil {
		return p, errors.New("PolicyUpdateRouteMissing")
	}
	lock, err := lockRouteDir(dir)
	if err != nil {
		return p, err
	}
	defer releaseRouteLock(lock)
	unlock, err := live.LockIdlePolicyState(dir)
	if err != nil {
		return p, err
	}
	defer unlock()
	if _, err = os.Lstat(filepath.Join(dir, policyMarker)); !os.IsNotExist(err) {
		return p, errors.New("PolicyUpdatePending")
	}
	raw, err := policyRead(filepath.Join(dir, "route.json"))
	if err != nil {
		return p, err
	}
	var old routeRecord
	if policyDecode(raw, &old) != nil || !validPolicyRoute(old) {
		return p, errors.New("InvalidRouteRecord")
	}
	next, err := (targetconfig.Agent{Region: old.Region, ScaleInProtection: old.Policy.ScaleInProtection}).Policy(values)
	if err != nil {
		return p, err
	}
	if old.Policy.GroupID != next.GroupID {
		return p, errors.New("PolicyUpdateGroupChanged")
	}
	if old.Policy == next {
		return p, errors.New("PolicyUpdateNoChange")
	}
	change, err := live.PrepareIdlePolicyChange(policyConfig(dir, old), nomadsdk.New(nil, next).PolicyDigest(), next.InstanceIDMetaKey, operator, reference, next.DrainDeadline)
	if err != nil {
		return p, err
	}
	updated := old
	updated.Policy = next
	now := time.Now().UTC()
	p = IdlePolicyPlan{Version: 1, Directory: dir, CreatedAt: now, ExpiresAt: now.Add(5 * time.Minute), BeforeRoute: raw, AfterRoute: policyJSON(updated), Change: change}
	p.Digest = policyDigest(p)
	return p, validateIdlePolicyPlan(p)
}
func WriteIdlePolicyPlan(path string, p IdlePolicyPlan) error {
	if err := validateIdlePolicyPlan(p); err != nil {
		return err
	}
	return policyWrite(path, policyJSON(p), true)
}
func ReadIdlePolicyPlan(path string) (IdlePolicyPlan, error) {
	var p IdlePolicyPlan
	b, err := policyRead(path)
	if err != nil {
		return p, err
	}
	if err = policyDecode(b, &p); err == nil {
		err = validateIdlePolicyPlan(p)
	}
	return p, err
}
func ReadIdlePolicyValues(path string) (map[string]string, error) {
	b, err := policyRead(path)
	if err != nil {
		return nil, err
	}
	var v map[string]string
	if policyDecode(b, &v) != nil || v == nil {
		return nil, errors.New("InvalidPolicyUpdateValues")
	}
	return v, nil
}

// A durable marker commits intent to the two exact local file versions. Until
// it is cleared the runtime must finish this local transaction before opening
// any engine. Conflicting bytes are never overwritten.
func ApplyIdlePolicyUpdate(p IdlePolicyPlan, confirm string) error {
	return applyIdlePolicyUpdate(p, confirm, nil)
}
func applyIdlePolicyUpdate(p IdlePolicyPlan, confirm string, checkpoint func(string) error) error {
	if err := validateIdlePolicyPlan(p); err != nil {
		return err
	}
	if confirm != p.Digest {
		return errors.New("PolicyUpdateConfirmationRequired")
	}
	lock, err := lockRouteDir(p.Directory)
	if err != nil {
		return err
	}
	defer releaseRouteLock(lock)
	unlock, err := live.LockIdlePolicyState(p.Directory)
	if err != nil {
		return err
	}
	defer unlock()
	marker := filepath.Join(p.Directory, policyMarker)
	if b, readErr := policyRead(marker); readErr == nil {
		var pending IdlePolicyPlan
		if policyDecode(b, &pending) != nil || !reflect.DeepEqual(p, pending) {
			return errors.New("PolicyUpdateConflict")
		}
		return finishIdlePolicyUpdate(p, checkpoint)
	} else if _, err = os.Lstat(marker); !os.IsNotExist(err) {
		return errors.New("UnsafePolicyUpdateMarker")
	}
	receipt := filepath.Join(p.Directory, "policy-audit", p.Change.AuditID+".result.json")
	if b, readErr := policyRead(receipt); readErr == nil {
		if string(b) != string(policyJSON(map[string]string{"digest": p.Digest, "status": "committed"})) {
			return errors.New("PolicyUpdateReceiptConflict")
		}
		return nil
	}
	if time.Now().Before(p.CreatedAt) || !time.Now().Before(p.ExpiresAt) {
		return errors.New("PolicyUpdatePlanExpired")
	}
	for name, want := range map[string][]byte{"route.json": p.BeforeRoute, "live.json": p.Change.Before} {
		b, err := policyRead(filepath.Join(p.Directory, name))
		if err != nil || !bytes.Equal(b, want) {
			return errors.New("PolicyUpdateStateChanged")
		}
	}
	for _, name := range []string{"recovery-pending.json", "journal-save-pending.json"} {
		if _, err := os.Lstat(filepath.Join(p.Directory, name)); !os.IsNotExist(err) {
			return errors.New("PolicyChangeRecoveryPending")
		}
	}
	// Revalidate the current recovery and policy audit chains before preparing.
	var old routeRecord
	_ = policyDecode(p.BeforeRoute, &old)
	var after live.State
	_ = json.Unmarshal(p.Change.After, &after)
	var a live.IdlePolicyAudit
	_ = json.Unmarshal(p.Change.Audit, &a)
	if _, err = live.PrepareIdlePolicyChange(policyConfig(p.Directory, old), after.NomadPolicySHA256, after.NomadInstanceIDMetaKey, a.Operator, a.Reference, time.Duration(after.DrainDeadlineNanoseconds)); err != nil {
		return err
	}
	auditDir := filepath.Join(p.Directory, "policy-audit")
	if err = routePrivateDir(auditDir); err != nil {
		return err
	}
	auditPath := filepath.Join(auditDir, p.Change.AuditID+".json")
	if b, readErr := policyRead(auditPath); readErr == nil {
		if !bytes.Equal(b, p.Change.Audit) {
			return errors.New("PolicyUpdateAuditConflict")
		}
	} else if err = policyWrite(auditPath, p.Change.Audit, true); err != nil {
		return err
	}
	if checkpoint != nil {
		if err = checkpoint("audit"); err != nil {
			return err
		}
	}
	if err = policyWrite(marker, policyJSON(p), true); err != nil {
		return err
	}
	if checkpoint != nil {
		if err = checkpoint("marker"); err != nil {
			return err
		}
	}
	return finishIdlePolicyUpdate(p, checkpoint)
}
func finishIdlePolicyUpdate(p IdlePolicyPlan, checkpoint func(string) error) error {
	if err := validateIdlePolicyPlan(p); err != nil {
		return err
	}
	audit, err := policyRead(filepath.Join(p.Directory, "policy-audit", p.Change.AuditID+".json"))
	if err != nil || !bytes.Equal(audit, p.Change.Audit) {
		return errors.New("PolicyUpdateAuditConflict")
	}
	pairs := []struct {
		name          string
		before, after []byte
	}{{"live.json", p.Change.Before, p.Change.After}, {"route.json", p.BeforeRoute, p.AfterRoute}}
	// Check both files before modifying either one.
	for _, pair := range pairs {
		b, err := policyRead(filepath.Join(p.Directory, pair.name))
		if err != nil || (!bytes.Equal(b, pair.before) && !bytes.Equal(b, pair.after)) {
			return errors.New("PolicyUpdateStateChanged")
		}
	}
	for _, pair := range pairs {
		path := filepath.Join(p.Directory, pair.name)
		b, _ := policyRead(path)
		if !bytes.Equal(b, pair.after) {
			if err := policyWrite(path, pair.after, false); err != nil {
				return err
			}
		}
		if checkpoint != nil {
			if err := checkpoint(pair.name); err != nil {
				return err
			}
		}
	}
	receipt := filepath.Join(p.Directory, "policy-audit", p.Change.AuditID+".result.json")
	want := policyJSON(map[string]string{"digest": p.Digest, "status": "committed"})
	if b, err := policyRead(receipt); err == nil {
		if !bytes.Equal(b, want) {
			return errors.New("PolicyUpdateReceiptConflict")
		}
	} else if err = policyWrite(receipt, want, true); err != nil {
		return err
	}
	if checkpoint != nil {
		if err := checkpoint("receipt"); err != nil {
			return err
		}
	}
	if err := os.Remove(filepath.Join(p.Directory, policyMarker)); err != nil && !os.IsNotExist(err) {
		return errors.New("PolicyUpdatePersistenceFailed")
	}
	return routeSyncDir(p.Directory)
}
func resumeIdlePolicyUpdate(dir string) error {
	if _, err := os.Lstat(filepath.Join(dir, policyMarker)); os.IsNotExist(err) {
		return nil
	}
	lock, err := lockRouteDir(dir)
	if err != nil {
		return err
	}
	defer releaseRouteLock(lock)
	unlock, err := live.LockIdlePolicyState(dir)
	if err != nil {
		return err
	}
	defer unlock()
	b, err := policyRead(filepath.Join(dir, policyMarker))
	if err != nil {
		return err
	}
	var p IdlePolicyPlan
	if policyDecode(b, &p) != nil || p.Directory != dir {
		return errors.New("InvalidIdlePolicyPlan")
	}
	return finishIdlePolicyUpdate(p, nil)
}
