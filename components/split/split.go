package split

import (
	"context"
	"fmt"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName        = "array_split"
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
		Info:        "Array iterator. Input: context + array. Emits one message per array element on Out, each containing {context, item}. Elements are processed sequentially - next item sent after previous Out completes. Use to process lists item by item.",
		Tags:        []string{"SDK", "ARRAY"},
	}
}

func (t *Component) Handle(ctx context.Context, handler module.Handler, _ string, msg interface{}) any {
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
			Configuration: InMessage{},
			Position:      module.Left,
		},
		{
			Name:          OutPort,
			Label:         "Out",
			Source:        true,
			Configuration: OutMessage{},
			Position:      module.Right,
		},
	}
}

func init() {
	registry.Register((&Component{}).Instance())
}
