// Package simulation implements an explicitly synthetic cloud and Nomad model.
// It never connects to a cloud or Nomad API. Its results cannot validate the
// actual Tencent Cloud RemoveInstances/DesiredCapacity contract.
package simulation

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"syscall"
	"time"
)

var (
	ErrUnknown = errors.New("simulated deletion result unknown; reconciliation pending")
	ErrPartial = errors.New("simulated deletion partially failed; running node restored")
	ErrBusy    = errors.New("operation pending; refusing another capacity change")
)

type Member struct {
	ID        string `json:"id"`
	Running   bool   `json:"running"`
	Joined    bool   `json:"joined"`
	Protected bool   `json:"protected"`
	Eligible  bool   `json:"eligible"`
	DrainedBy string `json:"drained_by,omitempty"`
}

type Operation struct {
	ID       string   `json:"id"`
	Phase    string   `json:"phase"`
	Target   int64    `json:"target"`
	Selected []string `json:"selected"`
	// Results are an injected observation, not proof obtained from a cloud API.
	Results  map[string]string `json:"results"`
	Restored map[string]bool   `json:"restored"`
}

type State struct {
	Version   int               `json:"version"`
	GroupID   string            `json:"group_id"`
	Desired   int64             `json:"desired"`
	Members   map[string]Member `json:"members"`
	Pending   *Operation        `json:"pending,omitempty"`
	LastEvent string            `json:"last_event,omitempty"`
	Sequence  uint64            `json:"sequence"`
	NextID    int64             `json:"next_id"`
	FaultUsed bool              `json:"fault_used"`
}

type Config struct {
	Dir               string
	GroupID           string
	Initial           int64
	Min               int64
	Max               int64
	Protect           bool
	Fault             string
	ReconcileInterval time.Duration
}

type Engine struct {
	mu        sync.Mutex
	cfg       Config
	state     State
	lock      *os.File
	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
	lastError error
}

func Open(cfg Config) (*Engine, error) {
	if !filepath.IsAbs(cfg.Dir) || cfg.GroupID == "" || cfg.Min < 0 || cfg.Max < cfg.Min || cfg.Initial < cfg.Min || cfg.Initial > cfg.Max || cfg.ReconcileInterval <= 0 {
		return nil, errors.New("invalid simulation configuration")
	}
	switch cfg.Fault {
	case "none", "grow-failure", "partial-delete", "unknown-delete":
	default:
		return nil, errors.New("invalid simulation fault")
	}
	if err := os.MkdirAll(cfg.Dir, 0700); err != nil {
		return nil, err
	}
	lk, err := os.OpenFile(filepath.Join(cfg.Dir, ".lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(lk.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lk.Close()
		return nil, fmt.Errorf("state directory already in use: %w", err)
	}
	e := &Engine{cfg: cfg, lock: lk, stop: make(chan struct{}), done: make(chan struct{})}
	if err := e.load(); err != nil {
		e.release()
		return nil, err
	}
	if e.state.Version == 0 {
		e.state = State{Version: 1, GroupID: cfg.GroupID, Desired: cfg.Initial, Members: map[string]Member{}}
		e.addTo(cfg.Initial)
		if err := e.save(); err != nil {
			e.release()
			return nil, err
		}
	}
	if e.state.GroupID != cfg.GroupID {
		e.release()
		return nil, errors.New("state belongs to another group")
	}
	go e.worker()
	return e, nil
}

func (e *Engine) load() error {
	f, err := os.Open(filepath.Join(e.cfg.Dir, "simulation.json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	dec := json.NewDecoder(io.LimitReader(f, 4<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&e.state); err != nil {
		return fmt.Errorf("invalid state: %w", err)
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return errors.New("invalid trailing state")
	}
	if e.state.Version != 1 || e.state.Members == nil || e.state.Desired < 0 || e.state.NextID < 0 {
		return errors.New("unsupported or incomplete state")
	}
	for id, member := range e.state.Members {
		if id == "" || member.ID != id {
			return errors.New("invalid member identity in state")
		}
	}
	if op := e.state.Pending; op != nil {
		if op.ID == "" || len(op.Selected) == 0 || op.Results == nil || op.Restored == nil {
			return errors.New("invalid pending operation")
		}
		if op.Phase != "selected" && op.Phase != "draining" && op.Phase != "delete-pending" {
			return errors.New("invalid operation phase")
		}
		selected := map[string]bool{}
		for _, id := range op.Selected {
			if id == "" || selected[id] {
				return errors.New("invalid selected instance set")
			}
			selected[id] = true
			member, exists := e.state.Members[id]
			if !exists && op.Results[id] != "deleted" {
				return errors.New("pending member missing without confirmed deletion")
			}
			if op.Restored[id] && (op.Results[id] != "failed" || !exists || member.DrainedBy == op.ID) {
				return errors.New("inconsistent restoration marker")
			}
		}
		for id, result := range op.Results {
			if !selected[id] || (result != "unknown" && result != "failed" && result != "deleted") {
				return errors.New("invalid instance result")
			}
		}
		for id := range op.Restored {
			if !selected[id] {
				return errors.New("restored instance is not selected")
			}
		}
	}
	return nil
}

func (e *Engine) save() (err error) {
	defer func() {
		if err != nil {
			e.lastError = fmt.Errorf("state persistence failed: %w", err)
		}
	}()
	b, err := json.MarshalIndent(e.state, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(e.cfg.Dir, ".simulation-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(b); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(f.Name(), filepath.Join(e.cfg.Dir, "simulation.json"))
	}
	if err == nil {
		d, openErr := os.Open(e.cfg.Dir)
		if openErr != nil {
			err = openErr
		} else {
			err = d.Sync()
			d.Close()
		}
	}
	return err
}

func (e *Engine) release() {
	_ = syscall.Flock(int(e.lock.Fd()), syscall.LOCK_UN)
	_ = e.lock.Close()
}

func (e *Engine) Close() {
	e.closeOnce.Do(func() { close(e.stop); <-e.done; e.release() })
}

func (e *Engine) worker() {
	defer close(e.done)
	t := time.NewTicker(e.cfg.ReconcileInterval)
	defer t.Stop()
	for {
		select {
		case <-e.stop:
			return
		case <-t.C:
			e.mu.Lock()
			if e.lastError == nil && e.state.Pending != nil {
				if err := e.reconcile(); err != nil && !errors.Is(err, ErrUnknown) {
					e.lastError = err
				}
			}
			e.mu.Unlock()
		}
	}
}

// Snapshot returns a copy. Status callers never persist or advance an operation.
func (e *Engine) Snapshot() (State, bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	b, err := json.Marshal(e.state)
	if err != nil {
		return State{}, false, err
	}
	var out State
	if err := json.Unmarshal(b, &out); err != nil {
		return State{}, false, err
	}
	ready := out.Pending == nil && e.lastError == nil
	for _, m := range out.Members {
		if !m.Running || m.DrainedBy != "" {
			ready = false
		}
	}
	return out, ready, e.lastError
}

func (e *Engine) addTo(target int64) {
	for int64(len(e.state.Members)) < target {
		e.state.NextID++
		id := fmt.Sprintf("ins-sim-%04d", e.state.NextID)
		if _, exists := e.state.Members[id]; exists {
			continue
		}
		e.state.Members[id] = Member{ID: id, Running: true, Joined: true, Eligible: true}
	}
}

// Scale models the proposed workflow. Real APIs may have different semantics.
// A dry-run caller must skip this function entirely.
func (e *Engine) Scale(target int64) (bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.lastError != nil {
		return false, e.lastError
	}
	if e.state.Pending != nil {
		return false, ErrBusy
	}
	for _, member := range e.state.Members {
		if !member.Running || member.DrainedBy != "" {
			return false, ErrBusy
		}
	}
	if target < 0 {
		return false, errors.New("negative target")
	}
	if target < e.cfg.Min {
		target = e.cfg.Min
	}
	if target > e.cfg.Max {
		target = e.cfg.Max
	}
	n := int64(len(e.state.Members))
	if n == target && e.state.Desired == target {
		return false, nil
	}
	if target >= n {
		e.state.Desired = target
		if e.cfg.Fault == "grow-failure" && !e.state.FaultUsed {
			e.state.FaultUsed = true
			if err := e.save(); err != nil {
				return false, err
			}
			return false, errors.New("simulated quota failure; same absolute target may be retried")
		}
		e.addTo(target)
		e.state.LastEvent = strconv.FormatInt(time.Now().UnixNano(), 10)
		return true, e.save()
	}
	if e.state.Desired != n {
		return false, errors.New("capacity mismatch; reconcile before scale-in")
	}
	ids := make([]string, 0, len(e.state.Members))
	for id, m := range e.state.Members {
		if m.Running && m.Joined && m.Eligible && m.DrainedBy == "" && (!e.cfg.Protect || !m.Protected) {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids) // Deterministic simulation selector; not production least_busy.
	k := int(n - target)
	if len(ids) < k {
		return false, errors.New("not enough eligible joined simulation nodes")
	}
	e.state.Sequence++
	op := &Operation{ID: fmt.Sprintf("sim-%d", e.state.Sequence), Phase: "selected", Target: target, Selected: append([]string(nil), ids[:k]...), Results: map[string]string{}, Restored: map[string]bool{}}
	e.state.Pending = op
	// Persist selection and operation identity before changing scheduling state.
	if err := e.save(); err != nil {
		return false, err
	}
	for _, id := range op.Selected {
		m := e.state.Members[id]
		m.DrainedBy = op.ID
		m.Eligible = false
		e.state.Members[id] = m
	}
	op.Phase = "draining"
	if err := e.save(); err != nil {
		return false, err
	}
	op.Phase = "delete-pending"
	if err := e.save(); err != nil {
		return false, err
	}
	fault := "none"
	if !e.state.FaultUsed {
		fault = e.cfg.Fault
		e.state.FaultUsed = true
	}
	for i, id := range op.Selected {
		result := "deleted"
		if fault == "unknown-delete" {
			result = "unknown"
		}
		if fault == "partial-delete" && i == len(op.Selected)-1 {
			result = "failed"
		}
		op.Results[id] = result
	}
	if err := e.save(); err != nil {
		return false, err
	}
	err := e.reconcile()
	if err != nil {
		return false, err
	}
	if fault == "partial-delete" {
		return false, ErrPartial
	}
	return true, nil
}

func (e *Engine) reconcile() error {
	op := e.state.Pending
	if op == nil {
		return nil
	}
	if op.Phase != "delete-pending" {
		for _, id := range op.Selected {
			m, exists := e.state.Members[id]
			if !exists {
				return errors.New("pre-delete member missing; manual reconciliation required")
			}
			if m.DrainedBy == "" && m.Eligible {
				continue
			}
			if m.DrainedBy != op.ID || !m.Running {
				return errors.New("cannot safely recover interrupted drain")
			}
			m.DrainedBy = ""
			m.Eligible = true
			e.state.Members[id] = m
		}
		e.state.Pending = nil
		return e.save()
	}
	unknown := false
	for _, id := range op.Selected {
		m, exists := e.state.Members[id]
		if !exists {
			continue
		}
		switch op.Results[id] {
		case "deleted":
			delete(e.state.Members, id)
		case "failed":
			if op.Restored[id] {
				continue
			}
			if !m.Running {
				unknown = true
				continue
			}
			if m.DrainedBy != op.ID {
				return errors.New("drain ownership changed; manual reconciliation required")
			}
			m.DrainedBy = ""
			m.Eligible = true
			e.state.Members[id] = m
			op.Restored[id] = true
		default:
			unknown = true
		}
	}
	if !unknown {
		// The simulator chooses D=actual survivors. P1 must prove or adapt the
		// real provider's decrement/compensation contract; this is NOT evidence.
		e.state.Desired = int64(len(e.state.Members))
		e.state.LastEvent = strconv.FormatInt(time.Now().UnixNano(), 10)
		e.state.Pending = nil
	}
	if err := e.save(); err != nil {
		return err
	}
	if unknown {
		return ErrUnknown
	}
	return nil
}

// Observe injects a later per-instance observation for deterministic tests.
// It does not reconcile; the independent worker does so without Status/Scale.
func (e *Engine) Observe(results map[string]string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.lastError != nil {
		return e.lastError
	}
	if e.state.Pending == nil {
		return errors.New("no pending operation")
	}
	allowed := map[string]bool{}
	for _, id := range e.state.Pending.Selected {
		allowed[id] = true
	}
	for id, result := range results {
		if !allowed[id] || (result != "failed" && result != "deleted") {
			return errors.New("invalid simulated observation")
		}
		if old := e.state.Pending.Results[id]; old != "" && old != "unknown" && old != result {
			return errors.New("cannot change a confirmed result")
		}
	}
	for id, result := range results {
		e.state.Pending.Results[id] = result
	}
	return e.save()
}
