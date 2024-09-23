package ticker

import (
	"context"
	"fmt"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
	"sync/atomic"
	"time"
)

const (
	ComponentName        = "ticker"
	OutPort       string = "out"
	StatusPort    string = "status"
)

type Context any

type Status struct {
	Status string `json:"status" readonly:"true" title:"Status" colSpan:"col-span-6"`
	Reset  bool   `json:"reset" format:"button" title:"Reset" required:"true" colSpan:"col-span-6"`
}

type Settings struct {
	Context          Context `json:"context,omitempty" configurable:"true" title:"Context" description:"Arbitrary message to be send each period of time"`
	Period           int     `json:"period" required:"true" title:"Periodicity (ms)" minimum:"10" default:"1000"`
	EnableStatusPort bool    `json:"enableStatusPort" required:"true" title:"Enable status port" description:"Status port"`
}

type Component struct {
	counter  int64
	settings Settings
}

func (t *Component) Instance() module.Component {
	return &Component{
		settings: Settings{
			Period: 1000,
		},
	}
}

type Control struct {
	Start bool `json:"start" required:"true" title:"Component state"`
}

func (t *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Ticker",
		Info:        "Sends messages periodically",
		Tags:        []string{"SDK"},
	}
}

// Emit non a pointer receiver copies Component with copy of settings
func (t *Component) emit(ctx context.Context, handler module.Handler) error {
	ticker := time.NewTicker(time.Duration(t.settings.Period) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:

			atomic.AddInt64(&t.counter, 1)
			_ = handler(ctx, OutPort, t.settings.Context)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (t *Component) Handle(ctx context.Context, handler module.Handler, port string, msg interface{}) error {
	if port == module.SettingsPort {
		settings, ok := msg.(Settings)
		if !ok {
			return fmt.Errorf("invalid settings")
		}
		if settings.Period < 10 {
			return fmt.Errorf("period should be more than 10 milliseconds")
		}
		t.settings = settings
		return nil
	}

	return fmt.Errorf("invalid message")
}

func (t *Component) Ports() []module.Port {
	ports := []module.Port{
		{
			Name:   module.SettingsPort,
			Label:  "Settings",
			Source: true,

			Configuration: Settings{
				Period: 1000,
			},
		},
		{
			Name:          OutPort,
			Label:         "Out",
			Source:        false,
			Position:      module.Right,
			Configuration: new(Context),
		},
	}

	if t.settings.EnableStatusPort {
		ports = append(ports, module.Port{
			Name:     StatusPort,
			Label:    "Status",
			Source:   true,
			Position: module.Bottom,
			Configuration: Status{
				Status: fmt.Sprintf("All good: %d", t.counter),
			},
		})
	}
	return ports
}

var _ module.Component = (*Component)(nil)

func init() {
	registry.Register(&Component{})
}
