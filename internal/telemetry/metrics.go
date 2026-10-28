// Package telemetry exposes bounded per-plugin counters. Scraping never reads
// credentials, cloud APIs, Nomad, or the operation journal.
package telemetry

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var actions = [...]string{"DescribeAutoScalingGroups", "DescribeAutoScalingInstances", "DescribeLaunchConfigurations", "DescribeScalingPolicies", "DescribeAutoScalingActivities", "DescribeInstances", "ModifyDesiredCapacity", "ScaleOutInstances", "RemoveInstances", "GetCallerIdentity", "InstanceMetadataRole", "InstanceMetadataCredentials", "other"}
var outcomes = [...]string{"success", "error", "no_op", "dry_run"}

type apiCounters struct{ attempts, failures, throttles atomic.Uint64 }
type Collector struct {
	registrationMu sync.Mutex
	registration   map[string]RegistrationCounts
	started        int64
	api            [len(actions)]apiCounters
	scale          [len(outcomes)]atomic.Uint64
}

func New() *Collector { return &Collector{started: time.Now().Unix()} }

// API counts an SDK/metadata attempt, including a failure before an HTTP
// response arrives. Validation failures before an attempt are not API errors.
func (c *Collector) API(action string, failed, throttled bool) {
	if c == nil {
		return
	}
	index := len(actions) - 1
	for i, v := range actions {
		if v == action {
			index = i
			break
		}
	}
	counter := &c.api[index]
	counter.attempts.Add(1)
	if failed || throttled {
		counter.failures.Add(1)
	}
	if throttled {
		counter.throttles.Add(1)
	}
}

// Scale counts the synchronous plugin request result, not a second completion
// when a background worker later settles the same failed request.
func (c *Collector) Scale(result string) {
	if c == nil {
		return
	}
	index := 1
	switch result {
	case "success":
		index = 0
	case "no-op":
		index = 2
	case "dry-run":
		index = 3
	}
	c.scale[index].Add(1)
}

func (c *Collector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/metrics" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var b strings.Builder
	b.WriteString("# HELP tencent_as_api_attempts_total SDK and metadata attempts since plugin initialization.\n# TYPE tencent_as_api_attempts_total counter\n")
	// Capture throttles before failures before attempts so each row respects the
	// subset relation even while another goroutine increments these counters.
	type counts struct{ attempts, failures, throttles uint64 }
	snapshot := make([]counts, len(actions))
	for i := range actions {
		v := &c.api[i]
		snapshot[i].throttles = v.throttles.Load()
		snapshot[i].failures = v.failures.Load()
		snapshot[i].attempts = v.attempts.Load()
	}
	for i, action := range actions {
		fmt.Fprintf(&b, "tencent_as_api_attempts_total{action=%q} %d\n", action, snapshot[i].attempts)
	}
	b.WriteString("# HELP tencent_as_api_errors_total Failed SDK and metadata attempts, including throttling.\n# TYPE tencent_as_api_errors_total counter\n")
	for i, action := range actions {
		fmt.Fprintf(&b, "tencent_as_api_errors_total{action=%q} %d\n", action, snapshot[i].failures)
	}
	b.WriteString("# HELP tencent_as_api_throttles_total Attempts with an exact known throttling response.\n# TYPE tencent_as_api_throttles_total counter\n")
	for i, action := range actions {
		fmt.Fprintf(&b, "tencent_as_api_throttles_total{action=%q} %d\n", action, snapshot[i].throttles)
	}
	b.WriteString("# HELP tencent_as_scale_requests_total Synchronous target Scale results; no-op and dry-run are separate.\n# TYPE tencent_as_scale_requests_total counter\n")
	for i, result := range outcomes {
		fmt.Fprintf(&b, "tencent_as_scale_requests_total{result=%q} %d\n", result, c.scale[i].Load())
	}
	b.WriteString("# HELP tencent_as_process_start_time_seconds Collector initialization time; counters reset on plugin restart.\n# TYPE tencent_as_process_start_time_seconds gauge\n")
	fmt.Fprintf(&b, "tencent_as_process_start_time_seconds %d\n", c.started)
	c.writeRegistrationMetrics(&b)
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write([]byte(b.String()))
}

func ValidAddress(address string) bool {
	if address == "" {
		return true
	}
	parsed, err := netip.ParseAddrPort(address)
	return err == nil && parsed.Addr().IsLoopback() && parsed.Addr().Zone() == ""
}

type Server struct {
	server  *http.Server
	address string
	done    chan struct{}
	once    sync.Once
}

func (c *Collector) Start(address string) (*Server, error) {
	if !ValidAddress(address) {
		return nil, errors.New("InvalidMetricsListenAddress")
	}
	if address == "" {
		return nil, nil
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, errors.New("MetricsListenerUnavailable")
	}
	s := &Server{server: &http.Server{Handler: c, ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8 << 10}, address: listener.Addr().String(), done: make(chan struct{})}
	go func() { defer close(s.done); _ = s.server.Serve(listener) }()
	return s, nil
}
func (s *Server) Address() string { return s.address }
func (s *Server) Close() {
	if s != nil {
		s.once.Do(func() { _ = s.server.Close(); <-s.done })
	}
}
