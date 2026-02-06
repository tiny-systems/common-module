package ticker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/goccy/go-json"
	"github.com/rs/zerolog/log"
	"github.com/swaggest/jsonschema-go"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/pkg/utils"
	"github.com/tiny-systems/module/registry"
	"go.opentelemetry.io/otel/trace"
)

const (
	ComponentName        = "ticker"
	OutPort       string = "out"

	metadataKeyRunning = "ticker-running"
	metadataKeyConfig  = "ticker-config"
)

type Context any

type Settings struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context" description:"Arbitrary message to be send each period of time"`
	Delay   int     `json:"delay" required:"true" title:"Delay (ms)" description:"Delay between signals" minimum:"0" default:"1000"`
}

type Component struct {
	settings Settings

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
		Info:        "Periodic emitter. Click Start to begin emitting context on Out. Waits for Out port to complete, then waits [delay] ms before next emit. Click Stop to pause. Survives pod restarts via metadata persistence.",
		Tags:        []string{"SDK"},
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

	case v1alpha1.ReconcilePort:
		return t.handleReconcile(ctx, handler, msg)

	case v1alpha1.ControlPort:
		return t.handleControl(ctx, handler, msg)
	}

	return fmt.Errorf("invalid port: %s", port)
}

func (t *Component) handleReconcile(ctx context.Context, handler module.Handler, msg interface{}) error {
	node, ok := msg.(v1alpha1.TinyNode)
	if !ok {
		return nil
	}

	if node.Status.Metadata == nil {
		return nil
	}

	// Check if we should be running
	if _, running := node.Status.Metadata[metadataKeyRunning]; !running {
		return nil
	}

	// Already running, skip
	if t.isRunning() {
		return nil
	}

	// Only leader restarts
	if !utils.IsLeader(ctx) {
		return nil
	}

	// Restore config from metadata
	if configStr, ok := node.Status.Metadata[metadataKeyConfig]; ok {
		var cfg Settings
		if err := json.Unmarshal([]byte(configStr), &cfg); err == nil {
			t.settings = cfg
		}
	}

	log.Info().Interface("settings", t.settings).Msg("ticker: restoring from metadata")
	go t.emit(ctx, handler)

	return nil
}

func (t *Component) handleControl(ctx context.Context, handler module.Handler, msg interface{}) error {
	if msg == nil {
		return nil
	}

	if !utils.IsLeader(ctx) {
		return nil
	}

	ctrl, ok := msg.(Control)
	if !ok {
		return nil
	}

	if ctrl.Stop {
		t.clearMetadata(handler)
		return t.stop()
	}

	t.settings.Context = ctrl.Context
	t.persistMetadata(handler)

	if t.isRunning() {
		return nil
	}

	go t.emit(context.Background(), handler)
	return nil
}

func (t *Component) emit(ctx context.Context, handler module.Handler) error {
	t.runLock.Lock()
	defer t.runLock.Unlock()

	// Use Background context - emit is long-running and shouldn't inherit caller's deadline
	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()

	// Bridge: cancel emit when parent context is done (e.g., runner shutdown)
	go func() {
		select {
		case <-ctx.Done():
			runCancel()
		case <-runCtx.Done():
		}
	}()

	t.setCancelFunc(runCancel)
	// Update control port to show Running
	_ = handler(context.Background(), v1alpha1.ControlPort, t.getControl())

	defer func() {
		t.setCancelFunc(nil)
		// Update control port to show Not running
		_ = handler(context.Background(), v1alpha1.ControlPort, t.getControl())
	}()

	timer := time.NewTimer(0) // first tick fires immediately
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

func (t *Component) persistMetadata(handler module.Handler) {
	configBytes, _ := json.Marshal(t.settings)
	_ = handler(context.Background(), v1alpha1.ReconcilePort, func(n *v1alpha1.TinyNode) error {
		if n.Status.Metadata == nil {
			n.Status.Metadata = make(map[string]string)
		}
		n.Status.Metadata[metadataKeyRunning] = "true"
		n.Status.Metadata[metadataKeyConfig] = string(configBytes)
		return nil
	})
}

func (t *Component) clearMetadata(handler module.Handler) {
	_ = handler(context.Background(), v1alpha1.ReconcilePort, func(n *v1alpha1.TinyNode) error {
		if n.Status.Metadata == nil {
			return nil
		}
		delete(n.Status.Metadata, metadataKeyRunning)
		delete(n.Status.Metadata, metadataKeyConfig)
		return nil
	})
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
	return []module.Port{
		{
			Name:          v1alpha1.SettingsPort,
			Label:         "Settings",
			Configuration: t.settings,
		},
		{
			Name:          v1alpha1.ReconcilePort,
			Label:         "Reconcile",
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
