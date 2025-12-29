package debug

import (
	"context"
	"fmt"
	"github.com/tiny-systems/module/api/v1alpha1"
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
		Info:        "Message sink for inspection. Receives messages on In (no output ports). Displays last received message in Control port. Use as flow endpoint to inspect data or terminate unused branches.",
		Tags:        []string{"SDK"},
	}
}

func (t *Component) Handle(ctx context.Context, output module.Handler, port string, msg interface{}) any {

	switch port {
	case v1alpha1.SettingsPort:
		in, ok := msg.(Settings)
		if !ok {
			return fmt.Errorf("invalid settings")
		}
		t.settings = in
		return nil
	case InPort:
		if in, ok := msg.(InMessage); ok {
			t.settings.Context = in.Context
			_ = output(ctx, v1alpha1.ReconcilePort, nil)
			return nil

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
			Configuration: InMessage{},
			Position:      module.Left,
		},
		{
			Name:   v1alpha1.ControlPort,
			Label:  "Control",
			Source: true,
			Configuration: Control{
				Context: t.settings.Context,
			},
		},
		{
			Name:          v1alpha1.SettingsPort,
			Label:         "Settings",
			Configuration: t.settings,
		},
	}
}

func (t *Component) Instance() module.Component {
	return &Component{}
}

var _ module.Component = (*Component)(nil)

func init() {
	registry.Register((&Component{}).Instance())
}
