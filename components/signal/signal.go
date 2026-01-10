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
	"sync/atomic"
	"time"
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
	handleLock     *sync.Mutex // Serialize control port handling to prevent races
	blocking       int32       // Atomic counter: >0 means handler is blocking
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

// isBlocking returns true if any handler is currently blocking
func (t *Component) isBlocking() bool {
	return atomic.LoadInt32(&t.blocking) > 0
}

func (t *Component) Handle(ctx context.Context, handler module.Handler, port string, msg interface{}) any {

	switch port {
	case v1alpha1.ControlPort:
		in, ok := msg.(Control)
		if !ok {
			return fmt.Errorf("invalid input msg")
		}

		// ALL pods update context
		t.controlContext = in.Context

		// Non-leaders block to avoid requeue spam
		if !utils.IsLeader(ctx) {
			// Only count as blocking if Send (flow running), not Reset
			if in.Send {
				atomic.AddInt32(&t.blocking, 1)
				defer atomic.AddInt32(&t.blocking, -1)
			}
			<-ctx.Done()
			return ctx.Err()
		}

		// Leader: serialize control port handling
		t.handleLock.Lock()

		t.cancelFuncLock.Lock()
		if t.cancelFunc != nil {
			t.cancelFunc()
			t.cancelFunc = nil
		}
		t.cancelFuncLock.Unlock()

		if in.Reset {
			log.Info().Msg("signal component: reset requested")
			t.handleLock.Unlock()

			// Don't increment blocking - Reset means NOT running
			// Just block to avoid controller requeue spam
			log.Info().Msg("signal component: reset blocking until context done")
			<-ctx.Done()
			log.Info().Interface("ctxErr", ctx.Err()).Msg("signal component: context done after reset")
			return ctx.Err()
		}

		t.cancelFuncLock.Lock()
		ctx, t.cancelFunc = context.WithCancel(ctx)
		t.cancelFuncLock.Unlock()

		t.handleLock.Unlock()

		atomic.AddInt32(&t.blocking, 1)
		defer atomic.AddInt32(&t.blocking, -1)

		log.Info().
			Interface("ctxErrBefore", ctx.Err()).
			Msg("signal component: calling OutPort handler")

		outStart := time.Now()
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
	}
	return nil
}

func (t *Component) Ports() []module.Port {
	// Real check: is any handler currently blocking?
	resetEnable := t.isBlocking()

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
