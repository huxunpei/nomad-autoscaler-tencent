package policybounds

import (
	"bytes"
	"fmt"
	"github.com/hashicorp/go-hclog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writePolicy(t *testing.T, dir, name string, min, max int, enabled bool, kind, target string) {
	t.Helper()
	body := fmt.Sprintf(`scaling %q {
 enabled = %t
 type = %q
 min = %d
 max = %d
 policy {
   target %q {
     tencent_group_id = "asg-test"
   }
 }
}`, name, enabled, kind, min, max, target)
	if err := os.WriteFile(filepath.Join(dir, name+".hcl"), []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestPolicyBoundariesWarnInsideTargetAndReload(t *testing.T) {
	dir := t.TempDir()
	var logs bytes.Buffer
	logger := hclog.New(&hclog.LoggerOptions{Output: &logs, JSONFormat: true})
	writePolicy(t, dir, "wide", 1, 20, true, "cluster", "tencent-as")
	writePolicy(t, dir, "inside", 2, 10, true, "cluster", "tencent-as")
	writePolicy(t, dir, "disabled", 0, 30, false, "cluster", "tencent-as")
	writePolicy(t, dir, "other-provider", 0, 30, true, "cluster", "aws-asg")
	c := New(dir, logger)
	now := time.Now()
	// No target/count input: policy 1..20 must warn even when target is 5.
	c.Check("asg-test", 2, 10, now)
	if strings.Count(logs.String(), "policy bounds exceed") != 1 || !strings.Contains(logs.String(), "wide.hcl#wide") || strings.Contains(logs.String(), "disabled.hcl") {
		t.Fatal(logs.String())
	}
	c.Check("asg-test", 2, 10, now)
	if strings.Count(logs.String(), "policy bounds exceed") != 1 {
		t.Fatal("duplicate warning")
	}
	writePolicy(t, dir, "second", 0, 11, true, "cluster", "tencent-as")
	c.Check("asg-test", 2, 10, now.Add(2*time.Minute))
	if strings.Count(logs.String(), "policy bounds exceed") != 2 || !strings.Contains(logs.String(), "second.hcl#second") {
		t.Fatal(logs.String())
	}
	logs.Reset()
	c.Check("asg-test", 0, 30, now.Add(3*time.Minute))
	if logs.Len() != 0 {
		t.Fatal("in-range policies warned", logs.String())
	}
	c.Check("asg-test", 2, 10, now.Add(4*time.Minute))
	if strings.Count(logs.String(), "policy bounds exceed") != 2 {
		t.Fatal("boundary changes not re-warned", logs.String())
	}
}

func TestPolicyBoundariesJSONAndMalformedFilesAreAdvisory(t *testing.T) {
	dir := t.TempDir()
	var logs bytes.Buffer
	if err := os.WriteFile(filepath.Join(dir, "policy.json"), []byte(`{"scaling":{"json":{"enabled":true,"type":"cluster","min":1,"max":20,"policy":{"target":{"tencent-as":{"tencent_group_id":"asg-test"}}}}}}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "bad.hcl"), []byte(`PRIVATE-SYNTHETIC-FILE-CONTENT`), 0600); err != nil {
		t.Fatal(err)
	}
	c := New(dir, hclog.New(&hclog.LoggerOptions{Output: &logs, JSONFormat: true}))
	c.Check("asg-test", 2, 10, time.Now())
	if !strings.Contains(logs.String(), "policy.json#json") || !strings.Contains(logs.String(), "could not be parsed") || strings.Contains(logs.String(), "PRIVATE-SYNTHETIC") {
		t.Fatal(logs.String())
	}
}

func TestPolicyBoundariesMissingDirectoryDoesNotFail(t *testing.T) {
	var logs bytes.Buffer
	c := New(filepath.Join(t.TempDir(), "missing"), hclog.New(&hclog.LoggerOptions{Output: &logs}))
	c.Check("asg-test", 2, 10, time.Now())
	if !strings.Contains(logs.String(), "directory unavailable") {
		t.Fatal(logs.String())
	}
}
