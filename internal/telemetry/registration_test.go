package telemetry

import (
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestRegistrationMetricsAggregateReplaceAndClose(t *testing.T) {
	c := New()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c.SetRegistration(fmt.Sprint("PRIVATE_ROUTE", i), RegistrationCounts{TimedOut: 1, Waiting: 2})
			w := httptest.NewRecorder()
			c.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
		}(i)
	}
	wg.Wait()
	c.SetRegistration("PRIVATE_ROUTE0", RegistrationCounts{Registered: 1, Unknown: 2, ObservationFailed: true})
	c.ForgetRegistration("PRIVATE_ROUTE1")
	w := httptest.NewRecorder()
	c.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	body := w.Body.String()
	for _, line := range []string{`tencent_as_registration_members{state="timed_out"} 18`, `tencent_as_registration_members{state="waiting"} 36`, `tencent_as_registration_members{state="unknown"} 2`, `tencent_as_registration_members{state="registered"} 1`, "tencent_as_registration_observation_failed_groups 1"} {
		if !strings.Contains(body, line+"\n") {
			t.Fatal("missing", line)
		}
	}
	if strings.Contains(body, "PRIVATE_ROUTE") {
		t.Fatal("route identity became label")
	}
	var absent *Collector
	absent.SetRegistration("none", RegistrationCounts{})
	absent.ForgetRegistration("none")
}
