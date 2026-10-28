// Package nomadtest contains HTTP fixtures used only by test files.
package nomadtest

import "net/http"

// DisabledACL models the authoritative response of an ACL-disabled Nomad.
// It must never wrap a real Nomad endpoint or any production transport.
func DisabledACL(w http.ResponseWriter, r *http.Request) bool {
	if r.Method == http.MethodGet && r.URL.Path == "/v1/acl/token/self" {
		http.Error(w, "ACL support disabled", http.StatusBadRequest)
		return true
	}
	return false
}
