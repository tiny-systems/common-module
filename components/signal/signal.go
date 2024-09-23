package signal

import (
	"context"
	"fmt"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName        = "signal"
	OutPort       string = "out"
)

type Context any

type Settings struct {
	Context Context `json:"context" required:"true" configurable:"true" title:"Context" description:"Arbitrary message to send"`
	Auto    bool    `json:"auto" title:"Auto send" required:"true" description:"Send signal automatically"`
}

type Component struct {
	settings Settings
}

type Control struct {
	Context Context `json:"context" required:"true" title:"Context"`
	Send    bool    `json:"send" format:"button" title:"Send" required:"true"`
}

func (t *Component) Instance() module.Component {
	return &Component{
		settings: Settings{},
	}
}

func (t *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Signal",
		Info:        "Sends any message when flow starts",
		Tags:        []string{"SDK"},
	}
}

func (t *Component) Handle(ctx context.Context, handler module.Handler, port string, msg interface{}) error {

	switch port {
	case module.ControlPort:
		in, ok := msg.(Control)
		if !ok {
			return fmt.Errorf("invalid input msg")
		}

		t.settings.Context = in.Context
		_ = handler(ctx, module.ReconcilePort, nil)
		_ = handler(ctx, OutPort, in.Context)

	case module.SettingsPort:
		in, ok := msg.(Settings)
		if !ok {
			return fmt.Errorf("invalid settings")
		}
		t.settings = in

		if t.settings.Auto {
			return handler(ctx, OutPort, in.Context)
		}
	}
	return nil
}

func (t *Component) Ports() []module.Port {

	return []module.Port{
		{
			Name:          module.SettingsPort,
			Label:         "Settings",
			Source:        true,
			Configuration: t.settings,
		},
		{
			Name:          OutPort,
			Label:         "Out",
			Source:        false,
			Position:      module.Right,
			Configuration: new(Context),
		},
		{
			Name:  module.ControlPort,
			Label: "Control",
			Configuration: Control{
				Context: t.settings.Context,
			},
		},
	}
}

var _ module.Component = (*Component)(nil)

func init() {
	registry.Register(&Component{})
}
