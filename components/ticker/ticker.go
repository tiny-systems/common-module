package ticker

import (
	"context"
	"fmt"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
	"time"
)

const (
	ComponentName        = "ticker"
	OutPort       string = "out"
)

type Context any

type Settings struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context" description:"Arbitrary message to be send each period of time"`
	Delay   int     `json:"delay" required:"true" title:"Delay (ms)" description:"Delay between signals" minimum:"0" default:"1000"`
	Auto    bool    `json:"auto" title:"Auto send" required:"true" description:"Start sending as soon as component configured"`
}

type Component struct {
	settings Settings
	cancel   context.CancelFunc
	ctx      context.Context
}

func (t *Component) Instance() module.Component {
	return &Component{
		settings: Settings{
			Delay: 1000,
		},
	}
}

type Control struct {
	Context Context `json:"context" required:"true" title:"Context"`
	Start   bool    `json:"start" format:"button" colSpan:"col-span-6" title:"Start" required:"true"`
	Stop    bool    `json:"stop" format:"button" colSpan:"col-span-6" title:"Stop" required:"true"`
}

func (t *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Ticker",
		Info:        "Sends messages periodically with defined delay. Next message being sent as soon as port unblocked",
		Tags:        []string{"SDK"},
	}
}

// Emit non a pointer receiver copies Component with copy of settings
func (t *Component) emit(ctx context.Context, handler module.Handler) error {

	var runCtx context.Context
	runCtx, t.cancel = context.WithCancel(ctx)

	// redraw
	_ = handler(ctx, module.ReconcilePort, nil)

	for {
		timer := time.NewTimer(time.Duration(t.settings.Delay) * time.Millisecond)
		select {
		case <-timer.C:
			_ = handler(ctx, OutPort, t.settings.Context)

		case <-runCtx.Done():
			timer.Stop()
			return runCtx.Err()
		}
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

		if in.Stop && t.cancel != nil {
			// we are running and asked to stop
			t.cancel()
			t.cancel = nil
			// redraw
			_ = handler(ctx, module.ReconcilePort, nil)
			return nil
		}
		if in.Start && t.cancel == nil {
			//asked to start and not running
			return t.emit(ctx, handler)
		}
	case module.SettingsPort:
		in, ok := msg.(Settings)
		if !ok {
			return fmt.Errorf("invalid settings")
		}
		t.settings = in

		if t.settings.Auto {

			// stop if its already running
			if t.cancel != nil {
				t.cancel()
				t.cancel = nil
			}
			return t.emit(ctx, handler)
		}
	}
	return nil
}

func (t *Component) getControl() Control {
	c := Control{
		Stop:    t.cancel == nil,
		Start:   t.cancel != nil,
		Context: t.settings.Context,
	}
	return c
}

func (t *Component) Ports() []module.Port {

	ports := []module.Port{
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
			Name:          module.ControlPort,
			Label:         "Control",
			Configuration: t.getControl(),
		},
	}

	return ports
}

var _ module.Component = (*Component)(nil)

func init() {
	registry.Register(&Component{})
}
