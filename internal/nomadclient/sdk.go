package nomadclient

import (
	"github.com/hashicorp/nomad/api"
	"net/http"
	"strings"
	"time"
)

// SDKConfig shares the validated TLS transport and connection identity.
// Never persist or log this configuration: the SDK uses in-memory credentials.
func (c *Client) SDKConfig(wrap func(http.RoundTripper) http.RoundTripper) *api.Config {
	h := *c.http
	if wrap != nil {
		h.Transport = wrap(c.http.Transport)
	}
	// Nomad blocking queries default to a server wait of several minutes.
	// A shorter HTTP timeout would abort an otherwise healthy drain monitor.
	// Keep a margin for Nomad's server-side wait jitter and the response body;
	// the SDK can issue another bounded read until the action deadline ends.
	maxWait := c.http.Timeout / 2
	if maxWait < time.Millisecond {
		maxWait = time.Millisecond // API wait is serialized in whole milliseconds.
	}
	wait := c.waitTime
	if wait == 0 || wait > maxWait {
		wait = maxWait
	}
	cfg := &api.Config{Address: c.base.String(), Region: c.region, Namespace: "*", SecretID: c.token, WaitTime: wait, HttpClient: &h, TLSConfig: &api.TLSConfig{}}
	if c.httpAuth != "" {
		user, pass, _ := strings.Cut(c.httpAuth, ":")
		cfg.HttpAuth = &api.HttpBasicAuth{Username: user, Password: pass}
	}
	return cfg
}

// PostScaleInResult records the adapter's exact post-task readback, without
// credentials, arbitrary node metadata, or raw API errors.
type PostScaleInResult struct {
	Identity
	OperationID  string    `json:"operation_id"`
	PolicySHA256 string    `json:"policy_sha256"`
	Purge        bool      `json:"purge"`
	Outcome      string    `json:"outcome"`
	ObservedAt   time.Time `json:"observed_at"`
}
