# Install only ONE policy for this ASG. Replace the synthetic ASG ID.
# Disabled by default; syntax validation does not authorize cloud operations.
scaling "tencent-workers-fixed" {
  enabled = false
  min = 1
  max = 5
  policy {
    cooldown = "2m"
    evaluation_interval = "1m"
    check "capacity" {
      strategy "fixed-value" { value = 1 }
    }
    target "tencent-as" {
      tencent_group_id = "asg-example"
      node_drain_deadline = "10m"
      # Must match cooldown above. An Agent restart can scale immediately if this is omitted.
      restart_cooldown = "2m"
    }
  }
}
