package tencentcloud

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

// The SDK decodes an Error envelope before decoding the typed response. As a
// result an ActivityId alongside Error is otherwise invisible to mutationError.
// Check the raw envelope before the SDK can turn contradictory evidence into
// an apparently safe rejection. Neither raw bodies nor credentials are exposed.
type mutationResponseTransport struct{ base http.RoundTripper }

func (t *mutationResponseTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(req)
	if err != nil {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		return nil, err
	}
	if !oneOf(req.Header.Get("X-TC-Action"), "ModifyDesiredCapacity", "RemoveInstances", "ScaleOutInstances") {
		return resp, nil
	}
	if resp == nil || resp.Body == nil {
		return nil, errors.New("MutationResponseUnavailable")
	}
	defer resp.Body.Close()
	const limit = 64 << 10
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil || len(body) > limit {
		return nil, errors.New("MutationResponseUnavailable")
	}
	var envelope struct{ Response map[string]json.RawMessage }
	if json.Unmarshal(body, &envelope) == nil {
		raw, hasError := envelope.Response["Error"]
		_, hasActivity := envelope.Response["ActivityId"]
		if hasError && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) && hasActivity {
			return nil, errors.New("ConflictingMutationResponse")
		}
	}
	clone := *resp
	clone.Body = io.NopCloser(bytes.NewReader(body))
	clone.ContentLength = int64(len(body))
	return &clone, nil
}
