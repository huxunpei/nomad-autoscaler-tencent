package tencentcloud

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"

	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
	tchttp "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/http"
)

var accountPattern = regexp.MustCompile(`^[1-9][0-9]{0,19}$`)

// GetCallerIdentity checks that the selected credentials actually authenticate,
// before a group is known. It does not create/attach roles or enumerate groups.
func verifyCaller(ctx context.Context, cfg AuthConfig, values credentialValues, transport http.RoundTripper) (string, error) {
	profile := clientProfile(cfg.Timeout)
	profile.HttpProfile.Endpoint = "sts.tencentcloudapi.com"
	profile.HttpProfile.ReqMethod = "POST"
	client := common.NewCommonClient(common.NewTokenCredential(values.id, values.key, values.token), cfg.Region, profile)
	client.WithHttpTransport(&authBoundTransport{base: transport, hosts: map[string]bool{"sts.tencentcloudapi.com": true}, limit: credentialLimit})
	response, err := call(ctx, &Client{timeout: cfg.Timeout, metrics: cfg.Metrics}, "GetCallerIdentity", func(ctx context.Context) (*tchttp.CommonResponse, error) {
		request := tchttp.NewCommonRequest("sts", "2018-08-13", "GetCallerIdentity")
		request.SetContext(ctx)
		response := tchttp.NewCommonResponse()
		return response, client.Send(request, response)
	})
	if err != nil {
		return "", err
	}
	var body struct {
		Response *struct {
			AccountID string `json:"AccountId"`
			RequestID string `json:"RequestId"`
		} `json:"Response"`
	}
	if json.Unmarshal(response.GetBody(), &body) != nil || body.Response == nil || !accountPattern.MatchString(body.Response.AccountID) || !requestPattern.MatchString(body.Response.RequestID) {
		return "", bad("GetCallerIdentity", "InvalidCallerIdentity")
	}
	return body.Response.AccountID, nil
}

// An SDK-owned http.Client follows redirects by default. Deny them before the
// client can forward a signed request, and bound responses before SDK decoding.
type authBoundTransport struct {
	base  http.RoundTripper
	hosts map[string]bool
	limit int64
}

func (t *authBoundTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Scheme != "https" || !t.hosts[r.URL.Host] || r.URL.User != nil {
		return nil, errors.New("CloudEndpointDenied")
	}
	resp, err := t.base.RoundTrip(r)
	if err != nil {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		return nil, errors.New("CloudTransportUnavailable")
	}
	if resp == nil || resp.Body == nil {
		return nil, errors.New("CloudResponseUnavailable")
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		return nil, errors.New("CloudRedirectDenied")
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, t.limit+1))
	if err != nil || int64(len(body)) > t.limit {
		return nil, errors.New("CloudResponseUnavailable")
	}
	clone := *resp
	clone.Body = io.NopCloser(bytes.NewReader(body))
	clone.ContentLength = int64(len(body))
	return &clone, nil
}
