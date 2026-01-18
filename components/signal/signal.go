package signal

import (
	"context"
	"fmt"

	"github.com/rs/zerolog/log"
	"github.com/swaggest/jsonschema-go"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/pkg/utils"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName        = "signal"
	OutPort       string = "out"
)

type Context any

type Settings struct {
	Context Context `json:"context" required:"true" configurable:"true" title:"Context" description:"Arbitrary message to send"`
}

type Component struct {
	settings Settings
	nodeName string

	// Handler reference for control actions
	handler module.Handler

	// Running state (local tracking for UI)
	isRunning bool
}

type Control struct {
	Context     Context `json:"context" required:"true" title:"Context"`
	Send        bool    `json:"send" format:"button" title:"Send" required:"true" colSpan:"col-span-6"`
	Reset       bool    `json:"reset" format:"button" title:"Reset" required:"true" colSpan:"col-span-6"`
	ResetEnable bool    `json:"-" jsonschema:"-"`
}

func (c Control) PrepareJSONSchema(schema *jsonschema.Schema) error {
	if c.ResetEnable {
		delete(schema.Properties, "send")
		return nil
	}

	delete(schema.Properties, "reset")
	return nil
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
		Info:        "Flow trigger. Click Send to emit configured context on Out port and start the flow. The Out port is a blocking port - Signal keeps running (edge animated) until the connected component stops or Reset is clicked. Use as entry point - connect Out to components you want to activate when flow starts.",
		Tags:        []string{"SDK"},
	}
}

func (t *Component) Handle(ctx context.Context, handler module.Handler, port string, msg interface{}) any {
	// Store handler for goroutine use
	t.handler = handler

	switch port {
	case v1alpha1.ReconcilePort:
		// Receive TinyNode for node name
		if node, ok := msg.(v1alpha1.TinyNode); ok {
			t.nodeName = node.Name
		}
		return nil

	case v1alpha1.ControlPort:
		log.Info().
			Bool("isLeader", utils.IsLeader(ctx)).
			Interface("msg", msg).
			Str("msgType", fmt.Sprintf("%T", msg)).
			Msg("signal component: ControlPort received")

		// Only leader processes control commands
		if !utils.IsLeader(ctx) {
			return nil
		}

		in, ok := msg.(Control)
		if !ok {
			log.Error().
				Str("msgType", fmt.Sprintf("%T", msg)).
				Msg("signal component: type assertion failed")
			return fmt.Errorf("invalid input msg: expected Control, got %T", msg)
		}

		log.Info().
			Bool("send", in.Send).
			Bool("reset", in.Reset).
			Msg("signal component: Control parsed")

		if in.Reset {
			log.Info().Msg("signal component: reset requested")
			// Reset doesn't need to do anything special now
			// The blocking edge will be cancelled by context when the TinySignal is deleted
			t.isRunning = false

			// Trigger reconcile to update UI
			_ = handler(context.Background(), v1alpha1.ReconcilePort, nil)

			return nil
		}

		if in.Send {
			log.Info().Msg("signal component: send requested, calling OutPort")

			t.isRunning = true

			// Trigger reconcile to update UI
			_ = handler(context.Background(), v1alpha1.ReconcilePort, nil)

			// Call the blocking OutPort - this will create a TinyState for the destination
			// and block until the destination component completes or the TinyState is deleted
			// The handler returns when the blocking edge completes
			result := handler(ctx, OutPort, in.Context)

			log.Info().
				Interface("result", result).
				Msg("signal component: OutPort returned, send complete")

			t.isRunning = false

			// Trigger reconcile to update UI
			_ = handler(context.Background(), v1alpha1.ReconcilePort, nil)

			return nil
		}

		return nil

	case v1alpha1.SettingsPort:
		in, ok := msg.(Settings)
		if !ok {
			return fmt.Errorf("invalid settings")
		}
		t.settings = in
		return nil
	}
	return nil
}

func (t *Component) Ports() []module.Port {
	log.Info().
		Bool("isRunning", t.isRunning).
		Msg("signal component: Ports() called")

	// Get context from settings
	controlContext := t.settings.Context

	return []module.Port{
		{
			Name:          v1alpha1.SettingsPort,
			Label:         "Settings",
			Configuration: t.settings,
		},
		{
			Name:          OutPort,
			Label:         "Out",
			Source:        true,
			Position:      module.Right,
			Configuration: new(Context),
			Blocking:      true, // Use TinyState for blocking edges
		},
		{
			Name:   v1alpha1.ControlPort,
			Label:  "Control",
			Source: true,
			Configuration: Control{
				Context:     controlContext,
				ResetEnable: t.isRunning, // Show Reset when running, Send when not
			},
		},
	}
}

var _ module.Component = (*Component)(nil)

func init() {
	registry.Register((&Component{}).Instance())
}
