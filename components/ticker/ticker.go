package ticker

import (
	"context"
	"errors"
	"fmt"
	"github.com/swaggest/jsonschema-go"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/pkg/utils"
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

type Control struct {
	Context Context `json:"context" required:"true" title:"Context"`
	Status  string  `json:"status" title:"Status" readonly:"true"`
	Stop    bool    `json:"stop" format:"button" title:"Stop" required:"true"`
	Start   bool    `json:"start" format:"button" title:"Start" required:"true"`
}

func (c Control) PrepareJSONSchema(schema *jsonschema.Schema) error {

	if c.Start {
		delete(schema.Properties, "stop")
	} else {
		delete(schema.Properties, "start")
	}
	return nil
}

var _ jsonschema.Preparer = (*Control)(nil)

func (t *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Ticker",
		Info:        "Periodic emitter. Click Start to begin emitting context on Out. Uses blocking API: waits for Out port to unblock (downstream completes), then waits [delay] ms before next emit. Click Stop to pause. Use for polling or scheduled triggers.",
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
	_ = handler(context.Background(), v1alpha1.ReconcilePort, nil)

	defer func() {
		t.setCancelFunc(nil)
		_ = handler(context.Background(), v1alpha1.ReconcilePort, nil)
	}()

	timer := time.NewTimer(time.Duration(t.settings.Delay) * time.Millisecond)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			_ = handler(trace.ContextWithSpanContext(runCtx, trace.NewSpanContext(trace.SpanContextConfig{})), OutPort, t.settings.Context)
			timer.Reset(time.Duration(t.settings.Delay) * time.Millisecond)

		case <-runCtx.Done():
			err := runCtx.Err()
			if errors.Is(err, context.Canceled) {
				return nil
			}

			return err
		}
	}
}

func (t *Component) Handle(ctx context.Context, handler module.Handler, port string, msg interface{}) any {

	switch port {
	case v1alpha1.SettingsPort:
		in, ok := msg.(Settings)
		if !ok {
			return fmt.Errorf("invalid settings")
		}
		t.settings = in

		return nil

	case v1alpha1.ControlPort:
		if msg == nil {
			break
		}

		if !utils.IsLeader(ctx) {
			return nil
		}
		switch msg.(type) {
		case Control:
			if msg.(Control).Stop {
				return t.stop()
			}
			t.settings.Context = msg.(Control).Context
			return t.emit(ctx, handler)
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
			Name:          v1alpha1.ControlPort,
			Label:         "Control",
			Source:        true,
			Configuration: t.getControl(),
		},
	}

	return ports
}

func (t *Component) getControl() interface{} {
	if t.isRunning() {
		return Control{
			Status:  "Running",
			Context: t.settings.Context,
			Stop:    true,
		}
	}
	return Control{
		Context: t.settings.Context,
		Status:  "Not running",
		Start:   true,
	}
}

var _ module.Component = (*Component)(nil)

func init() {
	registry.Register((&Component{}).Instance())
}
