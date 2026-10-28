package telemetry

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestMetricsConcurrentCountersAndBoundedLabels(t *testing.T) {
	c := New()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); c.API("RemoveInstances", true, true); c.Scale("success") }()
	}
	wg.Wait()
	c.API("PRIVATE_ACTION", true, false)
	c.Scale("PRIVATE_RESULT")
	w := httptest.NewRecorder()
	c.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if w.Code != 200 || !strings.Contains(w.Header().Get("Content-Type"), "version=0.0.4") {
		t.Fatal("invalid exposition")
	}
	body := w.Body.String()
	for _, line := range []string{`tencent_as_api_attempts_total{action="RemoveInstances"} 100`, `tencent_as_api_errors_total{action="RemoveInstances"} 100`, `tencent_as_api_throttles_total{action="RemoveInstances"} 100`, `tencent_as_scale_requests_total{result="success"} 100`, `tencent_as_scale_requests_total{result="error"} 1`, `tencent_as_api_errors_total{action="other"} 1`} {
		if !strings.Contains(body, line+"\n") {
			t.Errorf("missing %s", line)
		}
	}
	if strings.Contains(body, "PRIVATE_") {
		t.Fatal("unbounded label leaked")
	}
	for i := 0; i < 10; i++ {
		r := httptest.NewRecorder()
		c.ServeHTTP(r, httptest.NewRequest("GET", "/metrics", nil))
		if r.Body.String() != body {
			t.Fatal("scrape mutated counters")
		}
	}
	fresh := httptest.NewRecorder()
	New().ServeHTTP(fresh, httptest.NewRequest("GET", "/metrics", nil))
	if strings.Contains(fresh.Body.String(), `action="RemoveInstances"} 100`) {
		t.Fatal("collectors shared process global state")
	}
}

func TestMetricsHTTPAndListenerLifecycle(t *testing.T) {
	c := New()
	for _, tc := range []struct {
		method, path string
		status       int
	}{{"POST", "/metrics", 405}, {"GET", "/", 404}, {"GET", "/metrics/", 404}} {
		w := httptest.NewRecorder()
		c.ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, nil))
		if w.Code != tc.status {
			t.Fatalf("%s %s: %d", tc.method, tc.path, w.Code)
		}
	}
	for _, addr := range []string{"0.0.0.0:9091", "[::]:9091", "192.0.2.1:9091", "localhost:9091", "127.0.0.1", "127.0.0.1:65536", "127.0.0.1:90\nPRIVATE"} {
		if ValidAddress(addr) {
			t.Fatalf("unsafe address accepted: %q", addr)
		}
		if s, err := c.Start(addr); err == nil || s != nil {
			t.Fatal("invalid listener started")
		}
	}
	for _, addr := range []string{"", "127.0.0.1:0", "127.0.0.1:9091", "[::1]:9091"} {
		if !ValidAddress(addr) {
			t.Fatal("valid loopback rejected")
		}
	}
	if s, err := c.Start(""); err != nil || s != nil {
		t.Fatal("empty configuration created listener")
	}
	server, err := c.Start("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	client := &http.Client{Timeout: time.Second}
	response, err := client.Get("http://" + server.Address() + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil || response.StatusCode != 200 || !strings.Contains(string(body), "# TYPE tencent_as_api_errors_total counter") {
		t.Fatal("listener did not expose counters")
	}
	if second, err := c.Start(server.Address()); err == nil || second != nil {
		t.Fatal("occupied address ignored")
	}
	server.Close()
	server.Close()
	if response, err = client.Get("http://" + server.Address() + "/metrics"); err == nil {
		response.Body.Close()
		t.Fatal("listener survived close")
	}
}

func TestMetricsAllOutcomeSeries(t *testing.T) {
	c := New()
	for _, r := range []string{"success", "error", "no-op", "dry-run"} {
		c.Scale(r)
	}
	w := httptest.NewRecorder()
	c.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	for _, r := range []string{"success", "error", "no_op", "dry_run"} {
		if !strings.Contains(w.Body.String(), fmt.Sprintf("tencent_as_scale_requests_total{result=%q} 1\n", r)) {
			t.Fatal("missing outcome", r)
		}
	}
	var absent *Collector
	absent.API("RemoveInstances", true, true)
	absent.Scale("error")
}
