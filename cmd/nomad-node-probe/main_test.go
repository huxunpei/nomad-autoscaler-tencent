package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
)

const testNodeID = "11111111-1111-1111-1111-111111111111"

func TestMutationActionDefaultsToReadOnly(t *testing.T) {
	var writes atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			writes.Add(1)
		}
		fmt.Fprintf(w, `{"ID":%q,"Status":"ready","Drain":false,"SchedulingEligibility":"eligible","Meta":{"cloud_instance_id":"ins-test","tencent_group_id":"asg-test","tencent_region":"lab"}}`, testNodeID)
	}))
	defer s.Close()
	out, exit := run([]string{"--nomad-address", s.URL, "--action", "start-drain", "--node-id", testNodeID, "--expected-instance", "ins-test", "--expected-group", "asg-test", "--expected-region", "lab", "--operation-id", "op-test"})
	if exit != 0 || !out.Preview || out.Mutation != nil || writes.Load() != 0 {
		t.Fatalf("default preview wrote: %+v", out)
	}
}

func TestExecuteRefusesExistingEvidenceDirectoryBeforeMutation(t *testing.T) {
	var writes atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			writes.Add(1)
		}
		fmt.Fprintf(w, `{"ID":%q,"Status":"ready","Drain":false,"SchedulingEligibility":"eligible","Meta":{"cloud_instance_id":"ins-test","tencent_group_id":"asg-test","tencent_region":"lab"}}`, testNodeID)
	}))
	defer s.Close()
	out, exit := run([]string{"--nomad-address", s.URL, "--action", "start-drain", "--execute", "--node-id", testNodeID, "--expected-instance", "ins-test", "--expected-group", "asg-test", "--expected-region", "lab", "--operation-id", "op-test", "--evidence-dir", filepath.Clean(t.TempDir())})
	if exit == 0 || out.Error == nil || out.Error.Code != "NewEvidenceDirectoryRequired" || writes.Load() != 0 {
		t.Fatalf("existing intent was replayed: %+v", out)
	}
}

func TestInvalidFlagsDoNotEchoValues(t *testing.T) {
	out, exit := run([]string{"--unknown-secret-flag", "sensitive-value"})
	if exit == 0 || out.Error == nil || out.Error.Error() != "InvalidArguments" {
		t.Fatalf("invalid arguments not sanitized: %+v", out)
	}
}
