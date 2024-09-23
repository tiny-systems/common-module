package async

import (
	"context"
	"fmt"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
	"go.opentelemetry.io/otel/trace"
)

const (
	ComponentName        = "common_async"
	InPort        string = "in"
	OutPort       string = "out"
)

type Context any

type InMessage struct {
	Context Context `json:"context" configurable:"true" required:"true" title:"Context" description:"Arbitrary message to be modified"`
}

type Component struct {
}

func (t *Component) Instance() module.Component {
	return &Component{}
}

func (t *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Async",
		Info:        "Asynchronously sends a new message after incoming message received",
		Tags:        []string{"SDK"},
	}
}

func (t *Component) Handle(ctx context.Context, handler module.Handler, port string, msg interface{}) error {
	if in, ok := msg.(InMessage); ok {
		go func() {
			_ = handler(trace.ContextWithSpanContext(context.Background(), trace.SpanContextFromContext(ctx)), OutPort, in.Context)
		}()
		return nil
	}
	return fmt.Errorf("invalid message")
}

func (t *Component) Ports() []module.Port {
	return []module.Port{
		{
			Name:          InPort,
			Label:         "In",
			Source:        true,
			Configuration: InMessage{},
			Position:      module.Left,
		},
		{
			Name:          OutPort,
			Label:         "Out",
			Source:        false,
			Configuration: new(Context),
			Position:      module.Right,
		},
	}
}

var _ module.Component = (*Component)(nil)

func init() {
	registry.Register(&Component{})
}
