// Package policybounds provides advisory comparisons of file policies and
// current cloud limits. It never supplies capacity targets or routing options.
package policybounds

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/hcl/v2/hclsimple"
	"github.com/hashicorp/nomad-autoscaler/sdk"
)

type boundary struct {
	file, name, group string
	min, max          int64
}

type Checker struct {
	mu       sync.Mutex
	dir      string
	logger   hclog.Logger
	nextRead time.Time
	policies []boundary
	warned   map[string]map[string]bool
}

func New(dir string, logger hclog.Logger) *Checker {
	c := &Checker{dir: dir, logger: logger, warned: map[string]map[string]bool{}}
	if dir == "" {
		if logger != nil {
			logger.Info("policy boundary diagnostics skipped; configure plugin policy_dir to match the Agent")
		}
	} else {
		c.read(time.Now().UTC())
	}
	return c
}

func (c *Checker) warn(message string, args ...interface{}) {
	if c.logger != nil {
		c.logger.Warn(message, args...)
	}
}

func (c *Checker) read(now time.Time) {
	c.nextRead = now.Add(time.Minute)
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		c.policies = nil
		c.warn("policy directory unavailable; boundary diagnostics skipped")
		return
	}
	var policies []boundary
	for _, entry := range entries {
		ext := filepath.Ext(entry.Name())
		if ext != ".hcl" && ext != ".json" {
			continue
		}
		// Do not follow a symlink or read a FIFO from a diagnostic path.
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() || info.Size() > 1<<20 {
			c.warn("policy file cannot be read for boundary diagnostics", "file", entry.Name())
			continue
		}
		data, err := os.ReadFile(filepath.Join(c.dir, entry.Name()))
		if err != nil {
			c.warn("policy file cannot be read for boundary diagnostics", "file", entry.Name())
			continue
		}
		var decoded sdk.FileDecodeScalingPolicies
		// Decode the same SDK structs with the same HCL/JSON decoder as Agent.
		// Never log parser errors: they can contain arbitrary source values.
		if hclsimple.Decode(entry.Name(), data, nil, &decoded) != nil {
			c.warn("policy file could not be parsed; boundary diagnostics skipped", "file", entry.Name())
			continue
		}
		for _, p := range decoded.ScalingPolicies {
			if p == nil || !p.Enabled || (p.Type != "" && p.Type != sdk.ScalingPolicyTypeCluster) || p.Doc == nil || p.Doc.Target == nil || p.Doc.Target.Name != "tencent-as" {
				continue
			}
			group := p.Doc.Target.Config["tencent_group_id"]
			if group == "" {
				continue
			}
			policies = append(policies, boundary{file: entry.Name(), name: p.Name, group: group, min: p.Min, max: p.Max})
		}
	}
	c.policies = policies
}

func (c *Checker) Check(group string, min, max int64, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.dir == "" {
		return
	}
	if !now.Before(c.nextRead) {
		c.read(now)
	}
	current := map[string]bool{}
	for _, p := range c.policies {
		if p.group != group || (p.min >= min && p.max <= max) {
			continue
		}
		key := fmt.Sprintf("%d/%d/%d/%d", min, max, p.min, p.max)
		current[key] = true
		if c.warned[group][key] {
			continue
		}
		var names []string
		for _, other := range c.policies {
			if other.group == group && other.min == p.min && other.max == p.max {
				names = append(names, other.file+"#"+other.name)
			}
		}
		c.warn("policy bounds exceed cloud group limits; scaling uses cloud limits", "policies", strings.Join(names, ","), "group", group, "policy_min", p.min, "policy_max", p.max, "group_min", min, "group_max", max)
		if c.warned[group] == nil {
			c.warned[group] = map[string]bool{}
		}
		c.warned[group][key] = true
	}
	c.warned[group] = current
}
