package async

import (
	"context"
	"fmt"

	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
	"go.opentelemetry.io/otel/trace"
)

const (
	ComponentName        = "async"
	InPort        string = "in"
	OutPort       string = "out"

	// defaultMaxConcurrency limits concurrent goroutines to prevent memory exhaustion
	defaultMaxConcurrency = 100
)

type Context any

type InMessage struct {
	Context Context `json:"context" configurable:"true" required:"true" title:"Context" description:"Arbitrary message to be modified"`
}

type Settings struct {
	MaxConcurrency int `json:"maxConcurrency" title:"Max Concurrency" description:"Maximum number of concurrent async operations. 0 means use default (100)."`
}

type Component struct {
	settings  Settings
	semaphore chan struct{}
}

func (t *Component) Instance() module.Component {
	return &Component{
		settings:  Settings{MaxConcurrency: defaultMaxConcurrency},
		semaphore: make(chan struct{}, defaultMaxConcurrency),
	}
}

func (t *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Async",
		Info:        "Non-blocking pass-through. Returns immediately (unblocks sender), then emits context on Out in a goroutine. Limits concurrent goroutines via maxConcurrency setting to prevent memory issues.",
		Tags:        []string{"SDK"},
	}
}

func (t *Component) Handle(ctx context.Context, handler module.Handler, port string, msg interface{}) any {
	switch port {
	case v1alpha1.SettingsPort:
		in, ok := msg.(Settings)
		if !ok {
			return fmt.Errorf("invalid settings")
		}
		t.settings = in
		// Recreate semaphore with new capacity
		maxConc := in.MaxConcurrency
		if maxConc <= 0 {
			maxConc = defaultMaxConcurrency
		}
		t.semaphore = make(chan struct{}, maxConc)
		return nil

	case InPort:
		in, ok := msg.(InMessage)
		if !ok {
			return fmt.Errorf("invalid message")
		}

		// Try to acquire semaphore slot (non-blocking for async behavior)
		select {
		case t.semaphore <- struct{}{}:
			// Got a slot, spawn goroutine
			go func() {
				defer func() { <-t.semaphore }() // Release slot when done
				_ = handler(trace.ContextWithSpanContext(context.Background(), trace.SpanContextFromContext(ctx)), OutPort, in.Context)
			}()
		default:
			// Semaphore full - run synchronously to apply backpressure
      return handler(trace.ContextWithSpanContext(context.Background(), trace.SpanContextFromContext(ctx)), OutPort, in.Context)
		}
		return nil

	default:
		return fmt.Errorf("port %s is not supported", port)
	}
}

func (t *Component) Ports() []module.Port {
	return []module.Port{
		{
			Name:          v1alpha1.SettingsPort,
			Label:         "Settings",
			Configuration: t.settings,
		},
		{
			Name:          InPort,
			Label:         "In",
			Configuration: InMessage{},
			Position:      module.Left,
		},
		{
			Name:          OutPort,
			Label:         "Out",
			Source:        true,
			Configuration: new(Context),
			Position:      module.Right,
		},
	}
}

var _ module.Component = (*Component)(nil)

func init() {
	registry.Register((&Component{}).Instance())
}
