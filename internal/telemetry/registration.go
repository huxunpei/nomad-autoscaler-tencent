package telemetry

import (
	"fmt"
	"strings"
)

// Keys are internal controller identities, never metric labels. There is one
// entry per live route, removed on close; scrapes only aggregate cached counts.
type RegistrationCounts struct {
	Registered, Waiting, TimedOut, Unknown uint64
	ObservationFailed                      bool
}

func (c *Collector) SetRegistration(key string, counts RegistrationCounts) {
	if c == nil {
		return
	}
	c.registrationMu.Lock()
	defer c.registrationMu.Unlock()
	if c.registration == nil {
		c.registration = map[string]RegistrationCounts{}
	}
	c.registration[key] = counts
}

func (c *Collector) ForgetRegistration(key string) {
	if c == nil {
		return
	}
	c.registrationMu.Lock()
	defer c.registrationMu.Unlock()
	delete(c.registration, key)
}

func (c *Collector) writeRegistrationMetrics(b *strings.Builder) {
	var total RegistrationCounts
	var failures uint64
	c.registrationMu.Lock()
	for _, counts := range c.registration {
		total.Registered += counts.Registered
		total.Waiting += counts.Waiting
		total.TimedOut += counts.TimedOut
		total.Unknown += counts.Unknown
		if counts.ObservationFailed {
			failures++
		}
	}
	c.registrationMu.Unlock()
	b.WriteString("# HELP tencent_as_registration_members Members by last registration observation; failed observations are unknown.\n# TYPE tencent_as_registration_members gauge\n")
	for _, row := range []struct {
		state string
		count uint64
	}{
		{"registered", total.Registered}, {"waiting", total.Waiting},
		{"timed_out", total.TimedOut}, {"unknown", total.Unknown},
	} {
		fmt.Fprintf(b, "tencent_as_registration_members{state=%q} %d\n", row.state, row.count)
	}
	b.WriteString("# HELP tencent_as_registration_observation_failed_groups Controllers whose latest cloud or Nomad observation failed.\n# TYPE tencent_as_registration_observation_failed_groups gauge\n")
	fmt.Fprintf(b, "tencent_as_registration_observation_failed_groups %d\n", failures)
}
