package plugin

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/nomad-autoscaler/plugins/base"
	"github.com/hashicorp/nomad-autoscaler/plugins/shared"
	"github.com/hashicorp/nomad-autoscaler/plugins/target"
	"github.com/hashicorp/nomad-autoscaler/sdk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// This probe verifies the actual upstream RPC client/server boundary, without
// changing the Agent or making cloud calls. The live engine has separate tests.
type waitingRPCProbe struct {
	entered, settled chan struct{}
	ready            atomic.Bool
}

func (*waitingRPCProbe) SetConfig(map[string]string) error { return nil }
func (*waitingRPCProbe) PluginInfo() (*base.PluginInfo, error) {
	return &base.PluginInfo{Name: Name, PluginType: sdk.PluginTypeTarget}, nil
}
func (p *waitingRPCProbe) Status(map[string]string) (*sdk.TargetStatus, error) {
	return &sdk.TargetStatus{Count: 1, Ready: p.ready.Load()}, nil
}
func (p *waitingRPCProbe) Scale(sdk.ScalingAction, map[string]string) error {
	close(p.entered)
	<-p.settled
	p.ready.Store(true)
	return nil
}

func TestSDKRPCScaleWaitDoesNotBlockStatus(t *testing.T) {
	probe := &waitingRPCProbe{entered: make(chan struct{}), settled: make(chan struct{})}
	impl := &target.PluginTarget{Impl: probe}
	listener := bufconn.Listen(1 << 20)
	defer listener.Close()
	server := grpc.NewServer()
	defer server.Stop()
	if err := impl.GRPCServer(nil, server); err != nil {
		t.Fatal(err)
	}
	go func() { _ = server.Serve(listener) }()
	conn, err := grpc.NewClient("passthrough:///contract", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	raw, err := impl.GRPCClient(ctx, nil, conn)
	if err != nil {
		t.Fatal(err)
	}
	client := raw.(target.Target)
	done := make(chan error, 1)
	go func() {
		done <- client.Scale(sdk.ScalingAction{Count: 5}, map[string]string{shared.PluginConfigKeyGRPCTimeout: "10ms"})
	}()
	select {
	case <-probe.entered:
	case <-ctx.Done():
		t.Fatal("Scale did not reach server")
	}
	defer func() {
		select {
		case <-probe.settled:
		default:
			close(probe.settled)
		}
	}()
	// The same status timeout key must not impose an early success/timeout on Scale.
	select {
	case err := <-done:
		t.Fatalf("Scale ended before convergence: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	before := time.Now()
	status, err := client.Status(map[string]string{shared.PluginConfigKeyGRPCTimeout: "250ms"})
	if err != nil || status == nil || status.Ready || time.Since(before) >= 250*time.Millisecond {
		t.Fatal("Status was blocked by Scale", status, err)
	}
	close(probe.settled)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(errors.New("Scale failed to finish after convergence"))
	}
}
