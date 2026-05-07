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

// New creates a harness for the given component instance. The harness
// mirrors the scheduler's capability injection so components written
// against the new lifecycle interfaces (OnSettings, OnReconcile, OnControl,
// OnEmitter, etc.) work in tests the same way they do in the runner.
func New(c module.Component) *Harness {
	h := &Harness{
		component: c,
		Metadata:  map[string]string{},
	}
	// Inject the long-lived emit handler for components that need to publish
	// from goroutines or system-port handlers.
	if e, ok := c.(module.EmitterAware); ok {
		e.OnEmitter(h.Handler)
	}
	return h
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

// Handle sends a message to the component on the given port. For system
// ports, dispatches through the matching capability interface when the
// component implements one (mirroring scheduler.Update's Phase 1). For
// other ports, falls through to Component.Handle.
func (h *Harness) Handle(ctx context.Context, port string, msg any) any {
	return h.dispatch(ctx, port, msg)
}

// HandleAsLeader sends a message with IsLeader=true in context.
func (h *Harness) HandleAsLeader(ctx context.Context, port string, msg any) any {
	return h.dispatch(utils.WithLeader(ctx, true), port, msg)
}

// Reconcile simulates a reconcile event with current metadata.
func (h *Harness) Reconcile(ctx context.Context) any {
	h.mu.Lock()
	meta := copyMap(h.Metadata)
	h.mu.Unlock()
	return h.dispatch(ctx, v1alpha1.ReconcilePort, v1alpha1.TinyNode{
		Status: v1alpha1.TinyNodeStatus{Metadata: meta},
	})
}

// ReconcileAsLeader simulates a reconcile with IsLeader=true.
func (h *Harness) ReconcileAsLeader(ctx context.Context) any {
	h.mu.Lock()
	meta := copyMap(h.Metadata)
	h.mu.Unlock()
	return h.dispatch(utils.WithLeader(ctx, true), v1alpha1.ReconcilePort, v1alpha1.TinyNode{
		Status: v1alpha1.TinyNodeStatus{Metadata: meta},
	})
}

// dispatch routes the call to a capability interface for system ports when
// the component implements one, or to Component.Handle otherwise. This
// mirrors what scheduler.Update + dispatchCapability do at runtime.
func (h *Harness) dispatch(ctx context.Context, port string, msg any) any {
	switch port {
	case v1alpha1.SettingsPort:
		if c, ok := h.component.(module.SettingsHandler); ok {
			if err := c.OnSettings(ctx, msg); err != nil {
				return err
			}
			return nil
		}
	case v1alpha1.ReconcilePort:
		if c, ok := h.component.(module.ReconcileHandler); ok {
			node, _ := msg.(v1alpha1.TinyNode)
			if err := c.OnReconcile(ctx, node); err != nil {
				return err
			}
			return nil
		}
	case v1alpha1.ControlPort:
		if c, ok := h.component.(module.ControlHandler); ok {
			if err := c.OnControl(ctx, msg); err != nil {
				return err
			}
			return nil
		}
	case v1alpha1.IdentityPort:
		if c, ok := h.component.(module.IdentityAware); ok {
			id, _ := msg.(v1alpha1.NodeIdentity)
			c.OnIdentity(id)
			return nil
		}
	case v1alpha1.ClientPort:
		if c, ok := h.component.(module.ClientAware); ok {
			client, _ := msg.(module.K8sClient)
			c.OnClient(client)
			return nil
		}
	}
	return h.component.Handle(ctx, h.Handler, port, msg)
}

// NewPod simulates a pod restart: fresh component instance, same metadata.
// Reinjects EmitterAware on the new instance so it can publish from
// background loops just like a freshly scheduled runner would.
func (h *Harness) NewPod() *Harness {
	h.mu.Lock()
	meta := copyMap(h.Metadata)
	h.mu.Unlock()
	pod := &Harness{
		component: h.component.Instance(),
		Metadata:  meta,
	}
	if e, ok := pod.component.(module.EmitterAware); ok {
		e.OnEmitter(pod.Handler)
	}
	return pod
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
