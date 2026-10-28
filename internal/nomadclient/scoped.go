package nomadclient

import "strings"

// WithScope binds identity mapping to a cloud-validated group scope.
// The caller supplies membership from the cloud, never from Nomad metadata.
func (c *Client) WithScope(group, region, key string) (*Client, error) {
	if !identifier.MatchString(group) || !identifier.MatchString(region) || strings.TrimSpace(key) == "" || strings.ContainsAny(key, "\x00\r\n") {
		return nil, failure("InvalidMappingScope")
	}
	out := *c
	out.scopeGroup = group
	out.scopeRegion = region
	out.instanceKey = key
	return &out, nil
}
