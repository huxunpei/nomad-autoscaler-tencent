package plugin

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAgentObservationDoesNotRecordInheritedCredentials(t *testing.T) {
	var body string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		body = string(raw)
		io.WriteString(w, "true")
	}))
	defer server.Close()
	p := &observedAgentTarget{endpoint: server.URL}
	values := map[string]string{"nomad_token": "synthetic-private-token", "nomad_http-auth": "synthetic-user:synthetic-password", "nomad_address": "https://example.invalid:4646"}
	if err := p.observeRequest("SetConfig", values, nil); err != nil {
		t.Fatal(err)
	}
	for _, v := range []string{"synthetic-private-token", "synthetic-user:synthetic-password"} {
		if strings.Contains(body, v) {
			t.Fatal("observation recorded credential")
		}
	}
	if !strings.Contains(body, "CredentialKeys") || !strings.Contains(body, "nomad_address") || values["nomad_token"] != "synthetic-private-token" {
		t.Fatal("redaction changed input or lost non-secret evidence")
	}
	// Unexpected deletion stays a fixture error in pre-existing growth fixtures.
	_, err := (&agentFixtureCloud{url: server.URL}).RemoveExact(context.Background(), "asg-example", "ins-example")
	if err == nil {
		t.Fatal("unexpected unstructured cloud result accepted")
	}
}
