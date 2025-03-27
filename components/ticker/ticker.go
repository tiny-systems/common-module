package ticker

import (
	"context"
	"errors"
	"fmt"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
	"go.opentelemetry.io/otel/trace"
	"sync"
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
	ctx      context.Context

	cancelFunc     context.CancelFunc
	cancelFuncLock *sync.Mutex

	runLock *sync.Mutex
}

func (t *Component) Instance() module.Component {
	return &Component{
		cancelFuncLock: &sync.Mutex{},
		runLock:        &sync.Mutex{},
		settings: Settings{
			Delay: 1000,
		},
	}
}

type StartControl struct {
	Context Context `json:"context" required:"true" title:"Context"`
	Status  string  `json:"status" title:"Status" readonly:"true"`
	Start   bool    `json:"start" format:"button" title:"Start" required:"true"`
}

type StopControl struct {
	Context Context `json:"context" required:"true" title:"Context"`
	Status  string  `json:"status" title:"Status" readonly:"true"`
	Stop    bool    `json:"stop" format:"button" title:"Stop" required:"true"`
}

func (t *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Ticker",
		Info:        "Sends messages periodically with defined delay. Next message being sent as soon as port unblocked.",
		Tags:        []string{"SDK"},
	}
}

// Emit non a pointer receiver copies Component with copy of settings
func (t *Component) emit(ctx context.Context, handler module.Handler) error {

	t.runLock.Lock()
	defer t.runLock.Unlock()

	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()

	t.setCancelFunc(runCancel)
	// reconcile so show we are listening
	_ = handler(context.Background(), module.ReconcilePort, nil)

	defer func() {
		t.setCancelFunc(nil)
		_ = handler(context.Background(), module.ReconcilePort, nil)
	}()

	for {
		timer := time.NewTimer(time.Duration(t.settings.Delay) * time.Millisecond)
		select {
		case <-timer.C:
			_ = handler(trace.ContextWithSpanContext(runCtx, trace.NewSpanContext(trace.SpanContextConfig{})), OutPort, t.settings.Context)

		case <-runCtx.Done():
			timer.Stop()

			err := runCtx.Err()
			if errors.Is(err, context.Canceled) {
				return nil
			}

			return err
		}
	}
}

func (t *Component) Handle(ctx context.Context, handler module.Handler, port string, msg interface{}) error {

	switch port {
	case module.SettingsPort:
		in, ok := msg.(Settings)
		if !ok {
			return fmt.Errorf("invalid settings")
		}
		t.settings = in

		if t.settings.Auto {
			// stop if its already running
			_ = t.stop()
			return t.emit(ctx, handler)
		}

		return nil

	case module.ControlPort:
		if msg == nil {
			break
		}
		switch msg.(type) {
		case StartControl:
			t.settings.Context = msg.(StartControl).Context
			return t.emit(ctx, handler)
		case StopControl:
			return t.stop()
		}
	}

	return fmt.Errorf("invalid port: %s", port)
}

func (t *Component) setCancelFunc(f func()) {
	t.cancelFuncLock.Lock()
	defer t.cancelFuncLock.Unlock()
	t.cancelFunc = f
}

func (t *Component) isRunning() bool {
	t.cancelFuncLock.Lock()
	defer t.cancelFuncLock.Unlock()

	return t.cancelFunc != nil
}

func (t *Component) stop() error {
	t.cancelFuncLock.Lock()
	defer t.cancelFuncLock.Unlock()
	if t.cancelFunc == nil {
		return nil
	}
	t.cancelFunc()
	return nil
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

func (t *Component) getControl() interface{} {
	if t.isRunning() {
		return StopControl{
			Status:  "Running",
			Context: t.settings.Context,
		}
	}
	return StartControl{
		Context: t.settings.Context,
		Status:  "Not running",
	}
}

var _ module.Component = (*Component)(nil)

func init() {
	registry.Register(&Component{})
}
