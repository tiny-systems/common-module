package signal

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

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

// State represents the Signal's runtime state stored in TinyState
// Presence of TinyState means running, absence means not running
type State struct {
	Context Context `json:"context,omitempty"`
}

type Component struct {
	settings Settings
	nodeName string

	// State synced from TinyState - source of truth for multi-pod
	stateLock *sync.RWMutex
	state     State
	isRunning bool // true when TinyState exists

	// Active handler goroutine management
	activeHandlerLock   *sync.Mutex
	activeHandlerCtx    context.Context
	activeHandlerCancel context.CancelFunc

	// Handler reference for goroutine
	handler module.Handler
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
		stateLock:         &sync.RWMutex{},
		activeHandlerLock: &sync.Mutex{},
		settings:          Settings{},
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
	// Store handler for goroutine use
	t.handler = handler

	switch port {
	case v1alpha1.ReconcilePort:
		// Receive TinyNode for node name
		if node, ok := msg.(v1alpha1.TinyNode); ok {
			t.nodeName = node.Name
		}
		return nil

	case v1alpha1.StatePort:
		// Receive state data as []byte - source of truth for multi-pod coordination
		// nil means state was deleted (reset)
		data, ok := msg.([]byte)
		if !ok {
			return fmt.Errorf("invalid state message: expected []byte")
		}

		// nil data means state was deleted - treat as reset/stop
		if data == nil {
			log.Info().
				Bool("isLeader", utils.IsLeader(ctx)).
				Msg("signal component: state deleted, resetting")

			// Reset local state
			t.stateLock.Lock()
			t.state = State{}
			t.isRunning = false
			t.stateLock.Unlock()

			// Only leader cancels the handler
			if utils.IsLeader(ctx) {
				t.cancelActiveHandler()
			}
			return nil
		}

		// Parse the state data - presence of data means running
		var signalState State
		if err := json.Unmarshal(data, &signalState); err != nil {
			log.Error().Err(err).Msg("signal component: failed to unmarshal state")
			return err
		}

		log.Info().
			Bool("isLeader", utils.IsLeader(ctx)).
			Msg("signal component: received state, now running")

		// Update local state
		t.stateLock.Lock()
		t.state = signalState
		t.isRunning = true
		t.stateLock.Unlock()

		// Only leader manages the active handler
		if !utils.IsLeader(ctx) {
			return nil
		}

		// Check if we need to start the handler
		t.activeHandlerLock.Lock()
		hasActiveHandler := t.activeHandlerCancel != nil
		t.activeHandlerLock.Unlock()

		if !hasActiveHandler {
			// State exists but no active handler - start one
			log.Info().Msg("signal component: state exists, starting handler goroutine")
			go t.runBlockingHandler(context.Background(), signalState.Context)
		}

		return nil

	case v1alpha1.ControlPort:
		// Only leader processes control commands
		if !utils.IsLeader(ctx) {
			return nil
		}

		in, ok := msg.(Control)
		if !ok {
			return fmt.Errorf("invalid input msg")
		}

		if in.Reset {
			log.Info().Msg("signal component: reset requested, deleting state")

			// Cancel active handler first
			t.cancelActiveHandler()

			// Delete TinyState by sending nil Data
			if err := t.deleteState(handler); err != nil {
				log.Error().Err(err).Msg("signal component: failed to delete state")
				return err
			}

			// Trigger reconcile to update UI
			_ = handler(context.Background(), v1alpha1.ReconcilePort, nil)

			log.Info().Msg("signal component: reset complete, returning immediately")
			return nil // Don't block! TinySignal will be deleted after this returns
		}

		if in.Send {
			log.Info().Msg("signal component: send requested, writing state")

			// Write TinyState - presence means running
			newState := State{Context: in.Context}
			if err := t.writeState(handler, newState); err != nil {
				log.Error().Err(err).Msg("signal component: failed to write send state")
				return err
			}

			// Trigger reconcile to update UI
			_ = handler(context.Background(), v1alpha1.ReconcilePort, nil)

			log.Info().Msg("signal component: send state written, returning immediately")
			return nil // Don't block! TinySignal will be deleted after this returns
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

// runBlockingHandler runs the handler to OutPort in a goroutine.
// This is called when TinyState says running=true and we don't have an active handler.
func (t *Component) runBlockingHandler(parentCtx context.Context, signalContext Context) {
	t.activeHandlerLock.Lock()
	if t.activeHandlerCancel != nil {
		// Already running
		t.activeHandlerLock.Unlock()
		return
	}

	ctx, cancel := context.WithCancel(parentCtx)
	t.activeHandlerCtx = ctx
	t.activeHandlerCancel = cancel
	t.activeHandlerLock.Unlock()

	defer func() {
		t.activeHandlerLock.Lock()
		t.activeHandlerCancel = nil
		t.activeHandlerCtx = nil
		t.activeHandlerLock.Unlock()
	}()

	log.Info().Msg("signal component: starting blocking handler to OutPort")

	// This blocks until context is cancelled or downstream completes
	result := t.handler(ctx, OutPort, signalContext)

	log.Info().
		Interface("result", result).
		Interface("ctxErr", ctx.Err()).
		Msg("signal component: blocking handler returned")

	// If context was cancelled (Reset or leader change), state already deleted
	// If downstream completed naturally, we should delete state
	if ctx.Err() == nil {
		log.Info().Msg("signal component: handler completed naturally, deleting state")
		if err := t.deleteState(t.handler); err != nil {
			log.Error().Err(err).Msg("signal component: failed to delete completion state")
		}
		_ = t.handler(context.Background(), v1alpha1.ReconcilePort, nil)
	}
}

// cancelActiveHandler cancels the active handler goroutine if one exists
func (t *Component) cancelActiveHandler() {
	t.activeHandlerLock.Lock()
	defer t.activeHandlerLock.Unlock()

	if t.activeHandlerCancel != nil {
		log.Info().Msg("signal component: cancelling active handler")
		t.activeHandlerCancel()
		t.activeHandlerCancel = nil
		t.activeHandlerCtx = nil
	}
}

// writeState writes the signal state to TinyState CRD
func (t *Component) writeState(handler module.Handler, state State) error {
	stateData, err := json.Marshal(state)
	if err != nil {
		return err
	}

	// Use ReconcilePort with a state update function
	// The scheduler will create/update TinyState for this node
	result := handler(context.Background(), v1alpha1.ReconcilePort, v1alpha1.StateUpdate{
		Data: stateData,
	})

	return utils.CheckForError(result)
}

// deleteState deletes the TinyState CRD (nil Data signals deletion)
func (t *Component) deleteState(handler module.Handler) error {
	result := handler(context.Background(), v1alpha1.ReconcilePort, v1alpha1.StateUpdate{
		Data: nil,
	})
	return utils.CheckForError(result)
}

// getState returns the current state and running status
func (t *Component) getState() (State, bool) {
	t.stateLock.RLock()
	defer t.stateLock.RUnlock()
	return t.state, t.isRunning
}

func (t *Component) Ports() []module.Port {
	state, isRunning := t.getState()

	log.Info().
		Bool("isRunning", isRunning).
		Msg("signal component: Ports() called")

	// Get context from state, fallback to settings
	controlContext := state.Context
	if controlContext == nil {
		controlContext = t.settings.Context
	}

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
				Context:     controlContext,
				ResetEnable: isRunning, // Show Reset when running, Send when not
			},
		},
		{
			Name: v1alpha1.StatePort,
			// Hidden port - receives TinyState for state sync
		},
	}
}

var _ module.Component = (*Component)(nil)

func init() {
	registry.Register((&Component{}).Instance())
}
