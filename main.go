package main

import (
	"github.com/hashicorp/nomad-autoscaler/plugins"
	"github.com/huxunpei/nomad-autoscaler-tencent/plugin"
)

func main() { plugins.Serve(plugin.Factory) }
