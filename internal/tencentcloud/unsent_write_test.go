package tencentcloud

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"
)

func TestUnsentWriteAuthenticationProvenance(t *testing.T) {
	path := filepath.Join(t.TempDir(), "credentials.json")
	authFile(t, path, "synthetic-a")
	calls := 0
	c, err := newAuthenticated(context.Background(), AuthConfig{Region: "ap-guangzhou", Method: "api_key", CredentialsFile: path}, authDependencies{apiTransport: authHTTPFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		return authResponse(r, 200, identityBody("10001")), nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = os.Remove(path)
	r, err := c.SetDesired(context.Background(), "asg-test", 3)
	if !IsNotSubmittedWrite(r, err) || calls != 1 {
		t.Fatal("missing pre-call proof", r, err, calls)
	}
	r, err = c.RemoveExact(context.Background(), "asg-test", "ins-test")
	if !IsNotSubmittedWrite(r, err) || calls != 1 {
		t.Fatal("missing pre-delete proof", r, err, calls)
	}
	for _, mode := range []string{"zero", "attempted", "unknown", "request", "activity", "source", "action", "nil-error"} {
		q := r
		e := err
		switch mode {
		case "zero":
			q = WriteResult{}
		case "attempted":
			q.Attempted = true
		case "unknown":
			q.Unknown = true
		case "request":
			q.RequestID = authRequestID
		case "activity":
			q.ActivityID = "asa-test"
		case "source":
			q.NotSubmitted = "other"
		case "action":
			q.Action = "DescribeInstances"
		case "nil-error":
			e = nil
		}
		if IsNotSubmittedWrite(q, e) {
			t.Fatal("contradictory evidence accepted", mode)
		}
	}
	if IsNotSubmittedWrite(WriteResult{}, errors.New("LocalCredentialGateNotSubmitted")) {
		t.Fatal("error text became proof")
	}
}
