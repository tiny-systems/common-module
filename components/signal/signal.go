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
	"sync"
	"time"
)

const (
	ComponentName        = "signal"
	OutPort       string = "out"
	RunningMetadata      = "signal-running"
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
	handleLock     *sync.Mutex // Serialize control port handling to prevent races
	isRunning      bool        // Synced from metadata for all pods
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
		handleLock:     &sync.Mutex{},
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

		// Serialize control port handling to prevent race conditions
		// when multiple signals arrive concurrently
		t.handleLock.Lock()

		// Always preserve context data (don't clear on reset)
		// This allows Reset to cancel the operation while keeping data for next Send
		t.controlContext = in.Context

		t.cancelFuncLock.Lock()
		if t.cancelFunc != nil {
			t.cancelFunc()
			t.cancelFunc = nil
		}
		t.cancelFuncLock.Unlock()

		if in.Reset {
			log.Info().Msg("signal component: reset requested, updating metadata")
			// Update metadata to indicate not running
			_ = handler(context.Background(), v1alpha1.ReconcilePort, func(n *v1alpha1.TinyNode) error {
				if n.Status.Metadata == nil {
					n.Status.Metadata = map[string]string{}
				}
				n.Status.Metadata[RunningMetadata] = "false"
				return nil
			})
			t.handleLock.Unlock()

			log.Info().Msg("signal component: reset blocking until context done")
			// so signal controller do not try bomb us all the time we stay put
			<-ctx.Done()
			log.Info().Interface("ctxErr", ctx.Err()).Msg("signal component: context done after reset")
			// we block signal
			return ctx.Err()
		}

		t.cancelFuncLock.Lock()
		ctx, t.cancelFunc = context.WithCancel(ctx)
		t.cancelFuncLock.Unlock()

		// Update metadata to indicate running
		_ = handler(context.Background(), v1alpha1.ReconcilePort, func(n *v1alpha1.TinyNode) error {
			if n.Status.Metadata == nil {
				n.Status.Metadata = map[string]string{}
			}
			n.Status.Metadata[RunningMetadata] = "true"
			return nil
		})
		t.handleLock.Unlock()

		log.Info().
			Interface("ctxErrBefore", ctx.Err()).
			Msg("signal component: calling OutPort handler")

		outStart := time.Now()
		// we blocked by requested resource
		outResult := handler(ctx, OutPort, in.Context)
		outDuration := time.Since(outStart)

		log.Info().
			Dur("duration", outDuration).
			Interface("ctxErrAfter", ctx.Err()).
			Interface("result", outResult).
			Msg("signal component: OutPort handler returned")

		return ctx.Err()

	case v1alpha1.SettingsPort:
		in, ok := msg.(Settings)
		if !ok {
			return fmt.Errorf("invalid settings")
		}
		t.settings = in

	case v1alpha1.ReconcilePort:
		// Sync running state from metadata for all pods
		if node, ok := msg.(v1alpha1.TinyNode); ok {
			t.isRunning = node.Status.Metadata[RunningMetadata] == "true"
			log.Info().Bool("isRunning", t.isRunning).Msg("signal component: synced running state from metadata")
		}
	}
	return nil
}

func (t *Component) Ports() []module.Port {

	t.cancelFuncLock.Lock()
	defer t.cancelFuncLock.Unlock()

	// Use isRunning from metadata (synced across all pods) as primary source
	// Fall back to cancelFunc for the leader pod that hasn't synced yet
	resetEnable := t.isRunning || t.cancelFunc != nil

	log.Info().
		Bool("isRunning", t.isRunning).
		Bool("cancelFuncIsNil", t.cancelFunc == nil).
		Bool("resetEnable", resetEnable).
		Msg("signal component: Ports() called")

	return []module.Port{
		{
			Name: v1alpha1.ReconcilePort, // to receive TinyNode for metadata sync
		},
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
				ResetEnable: resetEnable,
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
