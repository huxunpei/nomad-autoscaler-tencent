package nomadclient

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/hashicorp/nomad/api"
)

// A real blocking API request stays quiet until the server's wait expires.
// Client.Timeout must not terminate a healthy SDK drain monitor first.
func TestSDKBlockingQueryReturnsBeforeRequestTimeout(t *testing.T) {
	for _, wait := range []time.Duration{0, 20 * time.Millisecond, 2 * time.Second} {
		t.Run(wait.String(), func(t *testing.T) {
			const timeout = 300 * time.Millisecond
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				delay := 600 * time.Millisecond
				if raw := r.URL.Query().Get("wait"); raw != "" {
					var err error
					delay, err = time.ParseDuration(raw)
					if err != nil || delay <= 0 {
						t.Error("invalid effective wait")
					}
					delay += 20 * time.Millisecond
				}
				select {
				case <-r.Context().Done():
					return
				case <-time.After(delay):
				}
				w.Header().Set("X-Nomad-Index", "101")
				_, _ = w.Write([]byte(`{"ID":"11111111-1111-1111-1111-111111111111"}`))
			}))
			defer server.Close()
			raw, err := New(Config{Address: server.URL, Timeout: timeout, WaitTime: wait})
			if err != nil {
				t.Fatal(err)
			}
			defer raw.Close()
			sdk, err := api.NewClient(raw.SDKConfig(nil))
			if err != nil {
				t.Fatal(err)
			}
			node, _, err := sdk.Nodes().Info("11111111-1111-1111-1111-111111111111", &api.QueryOptions{WaitIndex: 100})
			if err != nil || node.ID != "11111111-1111-1111-1111-111111111111" {
				t.Fatal("healthy blocking read exceeded per-request timeout", err)
			}
		})
	}
}
