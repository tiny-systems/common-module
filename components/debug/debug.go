package debug

import (
	"context"
	"fmt"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName        = "debug"
	InPort        string = "in"
)

type Context any

type Settings struct {
	Context Context `json:"context" configurable:"true" required:"true" title:"Context" description:"Component message"`
}

type InMessage struct {
	Context Context `json:"context" configurable:"false" required:"true" title:"Context" title:"Context"`
}

type Control struct {
	Context Context `json:"context" readonly:"true" required:"true" title:"Context"`
}

type Component struct {
	settings Settings
}

func (t *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Debug",
		Info:        "Consumes any data without sending it anywhere. Displays the latest message using control port.",
		Tags:        []string{"SDK"},
	}
}

func (t *Component) Handle(ctx context.Context, output module.Handler, port string, msg interface{}) error {

	switch port {
	case module.SettingsPort:
		in, ok := msg.(Settings)
		if !ok {
			return fmt.Errorf("invalid settings")
		}
		t.settings = in
		return nil
	case InPort:
		if in, ok := msg.(InMessage); ok {
			t.settings.Context = in.Context
			return output(ctx, module.ReconcilePort, nil)
		}
		return fmt.Errorf("invalid message in")
	}

	return fmt.Errorf("unknown port: %s", port)
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
			Name:  module.ControlPort,
			Label: "Control",
			Configuration: Control{
				Context: t.settings.Context,
			},
		},
		{
			Name:          module.SettingsPort,
			Label:         "Settings",
			Source:        true,
			Configuration: t.settings,
		},
	}
}

func (t *Component) Instance() module.Component {
	return &Component{}
}

var _ module.Component = (*Component)(nil)

func init() {
	registry.Register(&Component{})
}
