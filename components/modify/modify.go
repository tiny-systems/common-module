package modify

import (
	"context"
	"fmt"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName        = "transform"
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
		Description: "Modify",
		Info:        "Data transformer for connecting output back to input on the SAME node (e.g., http_server request→response loops). AVOID using modify between DIFFERENT nodes - configure data mapping directly on the edge instead. Edge configuration can set literal values and map fields without needing modify in between.",
		Tags:        []string{"SDK"},
	}
}

func (t *Component) Handle(ctx context.Context, handler module.Handler, port string, msg interface{}) any {
	if in, ok := msg.(InMessage); ok {
		return handler(ctx, OutPort, in.Context)
	}
	return fmt.Errorf("invalid message")
}

func (t *Component) Ports() []module.Port {
	return []module.Port{
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
