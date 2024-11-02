package split

import (
	"context"
	"fmt"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName        = "split"
	OutPort       string = "out"
	InPort        string = "in"
)

type Context any

type ItemContext any

type InMessage struct {
	Context Context       `json:"context" title:"Context" configurable:"true"  description:"Message to be send further with each item"  configurable:"true"`
	Array   []ItemContext `json:"array" title:"Array" default:"null" description:"Array of items to be split" required:"true"`
}

type OutMessage struct {
	Context Context     `json:"context"`
	Item    ItemContext `json:"item"`
}

type Component struct {
}

func (t *Component) Instance() module.Component {
	return &Component{}
}

func (t *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Split Array",
		Info:        "Splits any array into chunks and send further as separate messages",
		Tags:        []string{"SDK", "ARRAY"},
	}
}

func (t *Component) Handle(ctx context.Context, handler module.Handler, port string, msg interface{}) error {
	if in, ok := msg.(InMessage); ok {
		for _, item := range in.Array {
			if err := handler(ctx, OutPort, OutMessage{
				Context: in.Context,
				Item:    item,
			}); err != nil {
				return err
			}
		}
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
			Configuration: OutMessage{},
			Position:      module.Right,
		},
	}
}

func init() {
	registry.Register(&Component{})
}
