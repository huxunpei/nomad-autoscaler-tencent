# Alternative to cluster-fixed.hcl; never install both for the same ASG.
# Requires Nomad client metrics with a matching node_class label.
scaling "tencent-workers-cpu" {
  enabled = false
  min = 1
  max = 5
  policy {
    cooldown = "2m"
    evaluation_interval = "1m"
    on_check_error = "fail"
    check "allocated_cpu_percentage" {
      source = "prometheus"
      query = "sum(nomad_client_allocated_cpu{node_class=\"workers\"}*100/(nomad_client_unallocated_cpu{node_class=\"workers\"}+nomad_client_allocated_cpu{node_class=\"workers\"}))/count(nomad_client_allocated_cpu{node_class=\"workers\"})"
      strategy "target-value" { target = 70 }
    }
    target "tencent-as" {
      tencent_group_id = "asg-example"
      node_class = "workers"
      node_selector_strategy = "least_busy"
      node_drain_deadline = "10m"
      node_drain_ignore_system_jobs = "false"
      node_purge = "false"
      instance_id_meta_key = "cloud_instance_id"
      scale_in_protection = "false"
      # Must match cooldown above. An Agent restart can scale immediately if this is omitted.
      restart_cooldown = "2m"
    }
  }
}
