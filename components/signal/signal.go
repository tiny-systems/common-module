package signal

import (
	"context"
	"fmt"
	"sync"

	"github.com/goccy/go-json"
	"github.com/rs/zerolog/log"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/pkg/utils"
	"github.com/tiny-systems/module/registry"
)

const (
	metadataKeyRunning = "signal-running"
	metadataKeyContext = "signal-context"
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
	module.Base

	settings         Settings
	settingsFromPort bool // prevents reconcile from restoring stale metadata
	nodeName         string
	mu               sync.Mutex // protects isRunning, sentContext, cancelFunc
	isRunning        bool
	sentContext      Context
	cancelFunc       context.CancelFunc
}

// ControlStopped is the _control port schema when the signal is not running
type ControlStopped struct {
	Context Context `json:"context" required:"true" title:"Context"`
	Send    bool    `json:"send" format:"button" title:"Send" required:"true"`
}

// ControlRunning is the _control port schema when the signal is running
type ControlRunning struct {
	Context Context `json:"context" required:"true" title:"Context" readonly:"true"`
	Reset   bool    `json:"reset" format:"button" title:"Reset" required:"true"`
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

// OnSettings receives Settings from the SettingsPort and marks
// settingsFromPort to suppress later reconciles from restoring stale data.
func (t *Component) OnSettings(_ context.Context, msg any) error {
	in, ok := msg.(Settings)
	if !ok {
		return fmt.Errorf("invalid settings")
	}
	t.settings = in
	t.settingsFromPort = true
	t.mu.Lock()
	if !t.isRunning {
		t.sentContext = nil
	}
	t.mu.Unlock()
	return nil
}

// OnReconcile restores send context from metadata and resumes any orphaned
// blocking call (pod restart while OutPort was still pending).
func (t *Component) OnReconcile(ctx context.Context, node v1alpha1.TinyNode) error {
	t.nodeName = node.Name
	t.restoreContextFromMetadata(node.Status.Metadata)
	t.handleOrphanedRunningState(ctx, node.Status.Metadata)
	return nil
}

// OnControl handles Send/Reset dashboard clicks.
func (t *Component) OnControl(ctx context.Context, msg any) error {
	log.Info().
		Bool("isLeader", utils.IsLeader(ctx)).
		Interface("msg", msg).
		Str("msgType", fmt.Sprintf("%T", msg)).
		Msg("signal component: ControlPort received")

	if !utils.IsLeader(ctx) {
		return nil
	}

	switch ctrl := msg.(type) {
	case ControlRunning:
		log.Info().Bool("reset", ctrl.Reset).Msg("signal component: ControlRunning parsed")
		if ctrl.Reset {
			return t.handleReset()
		}
	case ControlStopped:
		log.Info().Bool("send", ctrl.Send).Msg("signal component: ControlStopped parsed")
		if ctrl.Send {
			return t.handleSend(ctrl.Context)
		}
	default:
		log.Error().Str("msgType", fmt.Sprintf("%T", msg)).Msg("signal component: type assertion failed")
		return fmt.Errorf("invalid input msg: expected ControlRunning or ControlStopped, got %T", msg)
	}
	return nil
}

// Handle is unreachable: OutPort is source-only, no business input.
func (t *Component) Handle(_ context.Context, _ module.Handler, port string, _ any) any {
	return fmt.Errorf("signal has no business-port input: got %q", port)
}

func (t *Component) restoreContextFromMetadata(metadata map[string]string) {
	if metadata == nil {
		return
	}
	if t.settingsFromPort {
		return // settings are fresher than metadata
	}

	ctxStr, ok := metadata[metadataKeyContext]
	if !ok || ctxStr == "" {
		return
	}

	var savedCtx Context
	if err := json.Unmarshal([]byte(ctxStr), &savedCtx); err != nil {
		return
	}
	t.mu.Lock()
	t.sentContext = savedCtx
	t.mu.Unlock()
}

func (t *Component) handleOrphanedRunningState(ctx context.Context, metadata map[string]string) {
	if metadata == nil {
		return
	}

	if _, hasRunning := metadata[metadataKeyRunning]; !hasRunning {
		return
	}

	t.mu.Lock()
	if t.cancelFunc != nil {
		t.isRunning = true
		t.mu.Unlock()
		return
	}

	if !utils.IsLeader(ctx) {
		t.mu.Unlock()
		return
	}

	// Resume the flow instead of clearing - pod restarted while running
	if t.sentContext == nil {
		t.mu.Unlock()
		log.Warn().Msg("signal component: cannot resume - no saved context, clearing orphaned state")
		t.clearRunningMetadata()
		return
	}
	t.mu.Unlock()

	log.Info().Msg("signal component: resuming flow after pod restart")
	t.resumeBlockingCall()
}

func (t *Component) resumeBlockingCall() {
	t.mu.Lock()
	t.isRunning = true
	sendCtx, cancel := context.WithCancel(context.Background())
	t.cancelFunc = cancel
	sentCtx := t.sentContext
	t.mu.Unlock()

	go t.runBlockingCall(sendCtx, sentCtx)
}

func (t *Component) handleReset() error {
	log.Info().Msg("signal component: reset requested")

	t.mu.Lock()
	if t.cancelFunc != nil {
		log.Info().Msg("signal component: cancelling blocking call")
		t.cancelFunc()
		t.cancelFunc = nil
	}
	t.isRunning = false
	t.mu.Unlock()

	t.clearRunningMetadata()
	return nil
}

func (t *Component) handleSend(sendContext Context) error {
	log.Info().Msg("signal component: send requested, calling OutPort")

	t.mu.Lock()
	t.isRunning = true
	t.sentContext = sendContext
	sendCtx, cancel := context.WithCancel(context.Background())
	t.cancelFunc = cancel
	t.mu.Unlock()

	t.persistRunningState(sendContext)

	go t.runBlockingCall(sendCtx, sendContext)

	return nil
}

func (t *Component) runBlockingCall(ctx context.Context, sendContext Context) {
	result := t.Emit(ctx, OutPort, sendContext)

	log.Info().Interface("result", result).Msg("signal component: OutPort returned, send complete")

	t.mu.Lock()
	t.isRunning = false
	t.cancelFunc = nil
	t.mu.Unlock()

	t.clearRunningMetadata()
	_ = t.Emit(context.Background(), v1alpha1.ReconcilePort, nil)
}

func (t *Component) persistRunningState(sendContext Context) {
	ctxBytes, _ := json.Marshal(sendContext)
	_ = t.Emit(context.Background(), v1alpha1.ReconcilePort, func(n *v1alpha1.TinyNode) error {
		if n.Status.Metadata == nil {
			n.Status.Metadata = make(map[string]string)
		}
		n.Status.Metadata[metadataKeyRunning] = "true"
		n.Status.Metadata[metadataKeyContext] = string(ctxBytes)
		return nil
	})
}

func (t *Component) clearRunningMetadata() {
	_ = t.Emit(context.Background(), v1alpha1.ReconcilePort, func(n *v1alpha1.TinyNode) error {
		if n.Status.Metadata != nil {
			delete(n.Status.Metadata, metadataKeyRunning)
		}
		return nil
	})
}

func (t *Component) Ports() []module.Port {
	t.mu.Lock()
	isRunning := t.isRunning
	sentContext := t.sentContext
	t.mu.Unlock()

	log.Info().Bool("isRunning", isRunning).Msg("signal component: Ports() called")

	controlContext := t.settings.Context
	if sentContext != nil {
		controlContext = sentContext
	}

	var controlConfig interface{}
	if isRunning {
		controlConfig = ControlRunning{
			Context: controlContext,
			Reset:   true,
		}
	} else {
		controlConfig = ControlStopped{
			Context: controlContext,
		}
	}

	return []module.Port{
		{
			Name: v1alpha1.ReconcilePort,
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
			Name:          v1alpha1.ControlPort,
			Label:         "Control",
			Source:        true,
			Configuration: controlConfig,
		},
	}
}

var (
	_ module.Component        = (*Component)(nil)
	_ module.SettingsHandler  = (*Component)(nil)
	_ module.ReconcileHandler = (*Component)(nil)
	_ module.ControlHandler   = (*Component)(nil)
)

func init() {
	registry.Register((&Component{}).Instance())
}
