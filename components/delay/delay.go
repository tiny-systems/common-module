package delay

import (
	"context"
	"fmt"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
	"time"
)

const (
	ComponentName        = "delay"
	OutPort       string = "out"
	InPort        string = "in"
)

type Context any

type Request struct {
	Context Context `json:"context" configurable:"true" title:"Context" description:"Arbitrary message to be delayed"`
	Delay   int     `json:"delay" required:"true" title:"Component (ms)"`
}

type Component struct {
}

func (t *Component) Instance() module.Component {
	return &Component{}
}

func (t *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Delay",
		Info:        "Timed pause. Receives context + delay (ms) on In, sleeps for specified duration (blocking upstream), then emits context on Out. Use for rate limiting or adding pauses between operations.",
		Tags:        []string{"SDK"},
	}
}

func (t *Component) Handle(ctx context.Context, handler module.Handler, port string, msg interface{}) any {

	in, ok := msg.(Request)
	if !ok {
		return fmt.Errorf("invalid message")
	}
	if in.Delay <= 0 {
		return fmt.Errorf("invalid delay")
	}

	time.Sleep(time.Millisecond * time.Duration(in.Delay))
	// Return handler result to propagate responses back through the call chain
	// (critical for blocking I/O patterns like HTTP Server)
	return handler(ctx, OutPort, in.Context)
}

func (t *Component) Ports() []module.Port {
	return []module.Port{
		{
			Name:  InPort,
			Label: "In",
			Configuration: Request{
				Delay: 1000,
			},
			Position: module.Left,
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
