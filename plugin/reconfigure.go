package plugin

import (
	"crypto/sha256"
	"encoding/binary"
	"sort"
)

// agentConfigDigest is never logged, persisted or included in policy state.
// Length prefixes preserve exact strings, including non-UTF8 bytes and NULs;
// serialization must not normalize distinct credential/configuration values.
func agentConfigDigest(config map[string]string) [32]byte {
	keys := make([]string, 0, len(config))
	for key := range config {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	h := sha256.New()
	var length [8]byte
	for _, key := range keys {
		for _, value := range []string{key, config[key]} {
			binary.BigEndian.PutUint64(length[:], uint64(len(value)))
			h.Write(length[:])
			h.Write([]byte(value))
		}
	}
	var digest [32]byte
	copy(digest[:], h.Sum(nil))
	return digest
}
