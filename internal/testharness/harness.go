package testharness

import (
	"context"
	"sync"

	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/pkg/utils"
)

// PortMsg captures a single output sent by a component via handler()
type PortMsg struct {
	Port string
	Data any
}

// Harness wraps a component for testing.
// Captures all port outputs and manages metadata persistence.
type Harness struct {
	component module.Component
	mu        sync.Mutex
	Metadata  map[string]string
	Outputs   []PortMsg
}

// New creates a harness for the given component instance.
func New(c module.Component) *Harness {
	return &Harness{
		component: c,
		Metadata:  map[string]string{},
	}
}

// Handler implements module.Handler, capturing outputs and metadata writes.
func (h *Harness) Handler(ctx context.Context, port string, data any) any {
	if port == v1alpha1.ReconcilePort {
		if fn, ok := data.(func(*v1alpha1.TinyNode) error); ok {
			h.mu.Lock()
			node := &v1alpha1.TinyNode{
				Status: v1alpha1.TinyNodeStatus{Metadata: h.Metadata},
			}
			if err := fn(node); err != nil {
				h.mu.Unlock()
				return err
			}
			h.Metadata = node.Status.Metadata
			h.mu.Unlock()
		}
		return nil
	}
	h.mu.Lock()
	h.Outputs = append(h.Outputs, PortMsg{Port: port, Data: data})
	h.mu.Unlock()
	return nil
}

// LeaderHandler returns a handler with IsLeader=true in context.
func (h *Harness) LeaderHandler(ctx context.Context, port string, data any) any {
	return h.Handler(utils.WithLeader(ctx, true), port, data)
}

// Handle sends a message to the component on the given port.
func (h *Harness) Handle(ctx context.Context, port string, msg any) any {
	return h.component.Handle(ctx, h.Handler, port, msg)
}

// HandleAsLeader sends a message with IsLeader=true in context.
func (h *Harness) HandleAsLeader(ctx context.Context, port string, msg any) any {
	return h.component.Handle(utils.WithLeader(ctx, true), h.Handler, port, msg)
}

// Reconcile simulates a reconcile event with current metadata.
func (h *Harness) Reconcile(ctx context.Context) any {
	h.mu.Lock()
	meta := copyMap(h.Metadata)
	h.mu.Unlock()
	return h.component.Handle(ctx, h.Handler, v1alpha1.ReconcilePort, v1alpha1.TinyNode{
		Status: v1alpha1.TinyNodeStatus{Metadata: meta},
	})
}

// ReconcileAsLeader simulates a reconcile with IsLeader=true.
func (h *Harness) ReconcileAsLeader(ctx context.Context) any {
	h.mu.Lock()
	meta := copyMap(h.Metadata)
	h.mu.Unlock()
	return h.component.Handle(utils.WithLeader(ctx, true), h.Handler, v1alpha1.ReconcilePort, v1alpha1.TinyNode{
		Status: v1alpha1.TinyNodeStatus{Metadata: meta},
	})
}

// NewPod simulates a pod restart: fresh component instance, same metadata.
func (h *Harness) NewPod() *Harness {
	h.mu.Lock()
	meta := copyMap(h.Metadata)
	h.mu.Unlock()
	return &Harness{
		component: h.component.Instance(),
		Metadata:  meta,
	}
}

// PortOutputs returns all data sent to the given port.
func (h *Harness) PortOutputs(port string) []any {
	h.mu.Lock()
	defer h.mu.Unlock()
	var result []any
	for _, o := range h.Outputs {
		if o.Port == port {
			result = append(result, o.Data)
		}
	}
	return result
}

// Reset clears captured outputs (but keeps metadata).
func (h *Harness) Reset() {
	h.mu.Lock()
	h.Outputs = nil
	h.mu.Unlock()
}

// Ports returns the component's current port definitions.
func (h *Harness) Ports() []module.Port {
	return h.component.Ports()
}

func copyMap(m map[string]string) map[string]string {
	cp := make(map[string]string, len(m))
	for k, v := range m {
		cp[k] = v
	}
	return cp
}
