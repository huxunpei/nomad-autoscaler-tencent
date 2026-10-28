# Development template. Keep policies disabled until deployment acceptance.
log_level = "INFO"
log_json = true
plugin_dir = "/opt/nomad-autoscaler/plugins"
http {
  bind_address = "127.0.0.1"
  bind_port = 8080
}
nomad {
  address = "http://127.0.0.1:4646"
  namespace = "default"
  # For a remote API, use HTTPS and configure the existing CA/client identity.
  # Supply NOMAD_TOKEN in the runtime environment; never put its value here.
}
# Each policy must set target restart_cooldown to that policy's cooldown.
# Without it, an Agent restart can scale again before the remaining cooldown ends.
policy {
  dir = "/etc/nomad-autoscaler/policies"
  source "nomad" { enabled = false }
  source "file" { enabled = true }
}
apm "prometheus" {
  driver = "prometheus"
  config = { address = "http://127.0.0.1:9090" }
}
target "tencent-as" {
  driver = "tencent-as"
  config = {
    tencent_region = "ap-guangzhou"
    tencent_state_dir = "/var/lib/nomad-autoscaler/tencent-as"
    # Repeat policy.dir for advisory min/max comparisons; never controls scaling.
    policy_dir = "/etc/nomad-autoscaler/policies"
    # Default: use the existing CVM instance identity. No static fallback.
    # tencent_auth_method = "api_key"
    # Optional explicit path; no search or fallback when selected:
    # tencent_credentials_file = "/etc/nomad-autoscaler/tencent-credentials.json"
    # tencent_registration_grace_period = "30m"
    # retry_attempts = "15"
    # scale_in_protection = "false"
    # tencent_metrics_listen_address = "127.0.0.1:9273"
  }
}
