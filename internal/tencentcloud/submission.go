package tencentcloud

import (
	"context"
	"net/http"
	"net/http/httptrace"
	"sync"
)

type submissionKey struct{}

// One SDK attempt owns one trace. Only our native Transport can contribute
// evidence; an opaque RoundTripper or an error's wording cannot establish that
// no mutation was sent. A successful connection is conservatively ambiguous,
// even when a subsequent failure might have happened before writing any bytes.
type submissionTrace struct {
	mu                sync.Mutex
	action            string
	started, finished int
	unsafe            bool
}

func traceSubmission(ctx context.Context, action string) (context.Context, *submissionTrace) {
	s := &submissionTrace{action: action}
	return context.WithValue(ctx, submissionKey{}, s), s
}

func (s *submissionTrace) connected() {
	s.mu.Lock()
	s.unsafe = true
	s.mu.Unlock()
}

func (s *submissionTrace) notSubmitted() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.started > 0 && s.started == s.finished && !s.unsafe
}

// Constructed only around a privately owned, standard Transport. Do not wrap
// an injected transport, http.DefaultTransport, registered alternate protocols,
// or a custom TLS dialer with this type: those can send without the trace hooks.
type submissionTransport struct{ base *http.Transport }

func newSubmissionTransport() *submissionTransport {
	return &submissionTransport{base: authTransport(true)}
}

func (t *submissionTransport) CloseIdleConnections() { t.base.CloseIdleConnections() }

func (t *submissionTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	s, _ := req.Context().Value(submissionKey{}).(*submissionTrace)
	if s == nil {
		return t.base.RoundTrip(req)
	}
	s.mu.Lock()
	s.started++
	if req.Method != http.MethodPost || req.Header.Get("X-TC-Action") != s.action {
		s.unsafe = true
	}
	getConn := false
	s.mu.Unlock()
	trace := &httptrace.ClientTrace{
		GetConn: func(string) {
			s.mu.Lock()
			getConn = true
			s.mu.Unlock()
		},
		GotConn:              func(httptrace.GotConnInfo) { s.connected() },
		WroteHeaders:         s.connected,
		WroteRequest:         func(httptrace.WroteRequestInfo) { s.connected() },
		GotFirstResponseByte: s.connected,
	}
	resp, err := t.base.RoundTrip(req.WithContext(httptrace.WithClientTrace(req.Context(), trace)))
	s.mu.Lock()
	s.finished++
	if err == nil || resp != nil || !getConn {
		s.unsafe = true
	}
	s.mu.Unlock()
	return resp, err
}

func localTransportFailure(s *submissionTrace, action string, err error, responseID, activityID string) *Error {
	// Even malformed returned identifiers contradict local non-submission.
	if err != nil && responseID == "" && activityID == "" &&
		sanitize(action, err, true).RequestID == "" && s.notSubmitted() {
		return &Error{Action: action, Code: "LocalTransportNotSubmitted"}
	}
	return nil
}
