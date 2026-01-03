package signal

import (
	"context"
	"fmt"
	"github.com/swaggest/jsonschema-go"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/pkg/utils"
	"github.com/tiny-systems/module/registry"
	"sync"
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
	settings       Settings
	controlContext Context // Store control context separately from settings
	cancelFuncLock *sync.Mutex
	cancelFunc     context.CancelFunc
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
		cancelFuncLock: &sync.Mutex{},
		settings:       Settings{},
	}
}

func (t *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Signal",
		Info:        "Flow trigger. Click Send to emit configured context on Out port and start the flow. Signal keeps running (maintaining the trigger) until Reset is clicked. Use as entry point - connect Out to components you want to activate when flow starts.",
		Tags:        []string{"SDK"},
	}
}

func (t *Component) Handle(ctx context.Context, handler module.Handler, port string, msg interface{}) any {

	switch port {
	case v1alpha1.ControlPort:

		if !utils.IsLeader(ctx) {
			// only leader propagates request further to avoid x (amount of replicas) multiply
			return nil
		}

		in, ok := msg.(Control)
		if !ok {
			return fmt.Errorf("invalid input msg")
		}

		if in.Reset {
			t.controlContext = nil
		} else {
			t.controlContext = in.Context
		}

		t.cancelFuncLock.Lock()

		if t.cancelFunc != nil {
			t.cancelFunc()
			t.cancelFunc = nil
		}
		t.cancelFuncLock.Unlock()

		if in.Reset {
			_ = handler(context.Background(), v1alpha1.ReconcilePort, nil)

			// so signal controller do not try bomb us all the time we stay put
			<-ctx.Done()
			// we block signal
			return ctx.Err()
		}

		t.cancelFuncLock.Lock()
		ctx, t.cancelFunc = context.WithCancel(ctx)
		t.cancelFuncLock.Unlock()

		//
		_ = handler(context.Background(), v1alpha1.ReconcilePort, nil)

		// we blocked by requested resource
		_ = handler(ctx, OutPort, in.Context)

		return ctx.Err()

	case v1alpha1.SettingsPort:
		in, ok := msg.(Settings)
		if !ok {
			return fmt.Errorf("invalid settings")
		}
		t.settings = in

	}
	return nil
}

func (t *Component) Ports() []module.Port {

	t.cancelFuncLock.Lock()
	defer t.cancelFuncLock.Unlock()

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
		},
		{
			Name:   v1alpha1.ControlPort,
			Label:  "Control",
			Source: true,
			Configuration: Control{
				Context:     t.getControlContext(),
				ResetEnable: t.cancelFunc != nil,
			},
		},
	}
}

// getControlContext returns the control context if set, otherwise falls back to settings context
func (t *Component) getControlContext() Context {
	if t.controlContext != nil {
		return t.controlContext
	}
	return t.settings.Context
}

var _ module.Component = (*Component)(nil)

func init() {
	registry.Register((&Component{}).Instance())
}
