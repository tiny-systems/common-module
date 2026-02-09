package inject_test

import (
	"context"
	"testing"

	"github.com/tiny-systems/common-module/components/inject"
	"github.com/tiny-systems/common-module/internal/testharness"
)

func TestBasicFlow(t *testing.T) {
	h := testharness.New((&inject.Component{}).Instance())
	ctx := context.Background()

	h.Handle(ctx, "config", inject.Config{
		Data: map[string]any{"labelSelector": "app=nginx", "namespace": "production"},
	})
	h.Handle(ctx, "message", inject.Message{Context: "tick-1"})

	outs := h.PortOutputs("output")
	if len(outs) != 1 {
		t.Fatalf("expected 1 output, got %d", len(outs))
	}
	o := outs[0].(inject.Output)
	if o.Context != "tick-1" {
		t.Errorf("context: got %v, want 'tick-1'", o.Context)
	}
	cfg := o.Config.(map[string]any)
	if cfg["labelSelector"] != "app=nginx" {
		t.Errorf("labelSelector: got %v, want 'app=nginx'", cfg["labelSelector"])
	}
	if cfg["namespace"] != "production" {
		t.Errorf("namespace: got %v, want 'production'", cfg["namespace"])
	}
}

func TestMessageBeforeConfig(t *testing.T) {
	h := testharness.New((&inject.Component{}).Instance())
	ctx := context.Background()

	h.Handle(ctx, "message", inject.Message{Context: "early"})

	outs := h.PortOutputs("output")
	if len(outs) != 1 {
		t.Fatalf("expected 1 output, got %d", len(outs))
	}
	o := outs[0].(inject.Output)
	if o.Config != nil {
		t.Errorf("expected nil config before any config sent, got %v", o.Config)
	}
}

func TestPodRestart(t *testing.T) {
	ctx := context.Background()
	pod1 := testharness.New((&inject.Component{}).Instance())

	// Pod 1 receives config
	pod1.Handle(ctx, "config", inject.Config{
		Data: map[string]any{"ns": "prod", "label": "app=api"},
	})

	if pod1.Metadata["inject-config"] == "" {
		t.Fatal("config not persisted to metadata")
	}

	// Pod 2: fresh instance with pod1's metadata
	pod2 := pod1.NewPod()
	pod2.Reconcile(ctx)

	pod2.Handle(ctx, "message", inject.Message{Context: "from-cron"})

	outs := pod2.PortOutputs("output")
	if len(outs) != 1 {
		t.Fatalf("pod2: expected 1 output, got %d", len(outs))
	}
	o := outs[0].(inject.Output)
	if o.Config == nil {
		t.Fatal("pod2: config is nil after reconcile restore")
	}
	cfg := o.Config.(map[string]any)
	if cfg["ns"] != "prod" {
		t.Errorf("pod2: ns: got %v, want 'prod'", cfg["ns"])
	}
	if cfg["label"] != "app=api" {
		t.Errorf("pod2: label: got %v, want 'app=api'", cfg["label"])
	}
}

func TestStaleReconcileDoesNotOverwrite(t *testing.T) {
	ctx := context.Background()
	h := testharness.New((&inject.Component{}).Instance())

	// Fresh config via port
	h.Handle(ctx, "config", inject.Config{
		Data: map[string]any{"version": "v2"},
	})

	// Stale reconcile arrives with old metadata
	stale := h.NewPod()
	stale.Metadata["inject-config"] = `{"version":"v1"}`

	// Feed stale metadata to the SAME component (not the new pod)
	// This simulates reconcile arriving after config port already set settingsFromPort=true
	h.Metadata = stale.Metadata
	h.Reconcile(ctx)

	h.Handle(ctx, "message", inject.Message{Context: "test"})

	outs := h.PortOutputs("output")
	if len(outs) != 1 {
		t.Fatalf("expected 1 output, got %d", len(outs))
	}
	o := outs[0].(inject.Output)
	cfg := o.Config.(map[string]any)
	if cfg["version"] != "v2" {
		t.Errorf("stale reconcile overwrote fresh config: got %v, want 'v2'", cfg["version"])
	}
}

func TestConfigUpdate(t *testing.T) {
	ctx := context.Background()
	h := testharness.New((&inject.Component{}).Instance())

	h.Handle(ctx, "config", inject.Config{Data: map[string]any{"v": 1}})
	h.Handle(ctx, "message", inject.Message{Context: "a"})

	h.Handle(ctx, "config", inject.Config{Data: map[string]any{"v": 2}})
	h.Handle(ctx, "message", inject.Message{Context: "b"})

	outs := h.PortOutputs("output")
	if len(outs) != 2 {
		t.Fatalf("expected 2 outputs, got %d", len(outs))
	}

	o1 := outs[0].(inject.Output)
	// JSON numbers deserialize as float64 in Go
	if v, ok := o1.Config.(map[string]any)["v"]; !ok || v != 1 {
		t.Errorf("first output: v=%v, want 1", v)
	}

	o2 := outs[1].(inject.Output)
	if v, ok := o2.Config.(map[string]any)["v"]; !ok || v != 2 {
		t.Errorf("second output: v=%v, want 2", v)
	}
}

func TestMetadataPersistence(t *testing.T) {
	ctx := context.Background()
	h := testharness.New((&inject.Component{}).Instance())

	h.Handle(ctx, "config", inject.Config{
		Data: map[string]any{"key": "value"},
	})

	raw, ok := h.Metadata["inject-config"]
	if !ok {
		t.Fatal("inject-config not in metadata")
	}
	if raw == "" || raw == "null" {
		t.Fatalf("metadata value is empty or null: %q", raw)
	}
}

func TestMultipleMessagesShareConfig(t *testing.T) {
	ctx := context.Background()
	h := testharness.New((&inject.Component{}).Instance())

	h.Handle(ctx, "config", inject.Config{
		Data: map[string]any{"env": "staging"},
	})

	for i := 0; i < 5; i++ {
		h.Handle(ctx, "message", inject.Message{Context: i})
	}

	outs := h.PortOutputs("output")
	if len(outs) != 5 {
		t.Fatalf("expected 5 outputs, got %d", len(outs))
	}

	for i, out := range outs {
		o := out.(inject.Output)
		if o.Config == nil {
			t.Errorf("output %d: config is nil", i)
			continue
		}
		cfg := o.Config.(map[string]any)
		if cfg["env"] != "staging" {
			t.Errorf("output %d: env=%v, want 'staging'", i, cfg["env"])
		}
	}
}

func TestPodRestartMultipleRounds(t *testing.T) {
	ctx := context.Background()

	// Pod 1: set config
	pod1 := testharness.New((&inject.Component{}).Instance())
	pod1.Handle(ctx, "config", inject.Config{
		Data: map[string]any{"round": 1},
	})

	// Pod 2: restore, update config
	pod2 := pod1.NewPod()
	pod2.Reconcile(ctx)
	pod2.Handle(ctx, "config", inject.Config{
		Data: map[string]any{"round": 2},
	})

	// Pod 3: restore from pod2's metadata
	pod3 := pod2.NewPod()
	pod3.Reconcile(ctx)
	pod3.Handle(ctx, "message", inject.Message{Context: "final"})

	outs := pod3.PortOutputs("output")
	if len(outs) != 1 {
		t.Fatalf("expected 1 output, got %d", len(outs))
	}
	o := outs[0].(inject.Output)
	cfg := o.Config.(map[string]any)
	// JSON round-trip turns int to float64
	if cfg["round"] != float64(2) {
		t.Errorf("pod3: round=%v, want 2 (from pod2's update)", cfg["round"])
	}
}
