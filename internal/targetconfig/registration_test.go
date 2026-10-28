package targetconfig

import (
	"strings"
	"testing"
	"time"
)

func TestRegistrationGraceOption(t *testing.T) {
	for _, duration := range []string{"30m", "15m", "2h", "1s"} {
		got, err := ParseAgent(map[string]string{"tencent_region": "ap-guangzhou", "tencent_registration_grace_period": duration})
		want, _ := time.ParseDuration(duration)
		if err != nil || got.RegistrationGracePeriod != want {
			t.Fatalf("valid registration grace rejected: %v", err)
		}
	}
	for _, duration := range []string{"", "0s", "-1s", "PRIVATE_BAD", "999999999999h"} {
		_, err := ParseAgent(map[string]string{"tencent_region": "ap-guangzhou", "tencent_registration_grace_period": duration})
		if err == nil || strings.Contains(err.Error(), "PRIVATE_BAD") {
			t.Fatal("invalid duration accepted or echoed")
		}
	}
	a, _ := ParseAgent(map[string]string{"tencent_region": "ap-guangzhou"})
	if a.RegistrationGracePeriod != 30*time.Minute {
		t.Fatal("wrong default grace period")
	}
	if _, err := a.Policy(map[string]string{"tencent_group_id": "asg-test", "tencent_registration_grace_period": "30m"}); err == nil {
		t.Fatal("Agent setting accepted in policy")
	}
}
