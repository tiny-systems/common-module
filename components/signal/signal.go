package signal

import (
	"context"
	"fmt"

	"github.com/goccy/go-json"
	"github.com/rs/zerolog/log"
	"github.com/swaggest/jsonschema-go"
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
	settings    Settings
	nodeName    string
	handler     module.Handler
	isRunning   bool
	sentContext Context
	cancelFunc  context.CancelFunc
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

func (t *Component) Handle(ctx context.Context, handler module.Handler, port string, msg interface{}) any {
	t.handler = handler

	switch port {
	case v1alpha1.ReconcilePort:
		return t.handleReconcile(ctx, handler, msg)
	case v1alpha1.ControlPort:
		return t.handleControl(ctx, handler, msg)
	case v1alpha1.SettingsPort:
		return t.handleSettings(msg)
	}
	return nil
}

func (t *Component) handleReconcile(ctx context.Context, handler module.Handler, msg interface{}) error {
	node, ok := msg.(v1alpha1.TinyNode)
	if !ok {
		return nil
	}

	t.nodeName = node.Name
	t.restoreContextFromMetadata(node.Status.Metadata)
	t.handleOrphanedRunningState(ctx, handler, node.Status.Metadata)
	return nil
}

func (t *Component) restoreContextFromMetadata(metadata map[string]string) {
	if metadata == nil {
		return
	}

	ctxStr, ok := metadata[metadataKeyContext]
	if !ok || ctxStr == "" {
		return
	}

	var savedCtx Context
	if err := json.Unmarshal([]byte(ctxStr), &savedCtx); err != nil {
		return
	}
	t.sentContext = savedCtx
}

func (t *Component) handleOrphanedRunningState(ctx context.Context, handler module.Handler, metadata map[string]string) {
	if metadata == nil {
		return
	}

	if _, hasRunning := metadata[metadataKeyRunning]; !hasRunning {
		return
	}

	if t.cancelFunc != nil {
		t.isRunning = true
		return
	}

	if !utils.IsLeader(ctx) {
		return
	}

	// Resume the flow instead of clearing - pod restarted while running
	if t.sentContext == nil {
		log.Warn().Msg("signal component: cannot resume - no saved context, clearing orphaned state")
		t.clearRunningMetadata(handler)
		return
	}

	log.Info().Msg("signal component: resuming flow after pod restart")
	t.resumeBlockingCall(handler)
}

func (t *Component) resumeBlockingCall(handler module.Handler) {
	t.isRunning = true

	sendCtx, cancel := context.WithCancel(context.Background())
	t.cancelFunc = cancel

	go t.runBlockingCall(handler, sendCtx, t.sentContext)
}

func (t *Component) handleControl(ctx context.Context, handler module.Handler, msg interface{}) error {
	log.Info().
		Bool("isLeader", utils.IsLeader(ctx)).
		Interface("msg", msg).
		Str("msgType", fmt.Sprintf("%T", msg)).
		Msg("signal component: ControlPort received")

	if !utils.IsLeader(ctx) {
		return nil
	}

	in, ok := msg.(Control)
	if !ok {
		log.Error().Str("msgType", fmt.Sprintf("%T", msg)).Msg("signal component: type assertion failed")
		return fmt.Errorf("invalid input msg: expected Control, got %T", msg)
	}

	log.Info().Bool("send", in.Send).Bool("reset", in.Reset).Msg("signal component: Control parsed")

	if in.Reset {
		return t.handleReset(handler)
	}

	if in.Send {
		return t.handleSend(handler, in.Context)
	}

	return nil
}

func (t *Component) handleReset(handler module.Handler) error {
	log.Info().Msg("signal component: reset requested")

	if t.cancelFunc != nil {
		log.Info().Msg("signal component: cancelling blocking call")
		t.cancelFunc()
		t.cancelFunc = nil
	}

	t.isRunning = false
	t.clearRunningMetadata(handler)
	return nil
}

func (t *Component) handleSend(handler module.Handler, sendContext Context) error {
	log.Info().Msg("signal component: send requested, calling OutPort")

	t.isRunning = true
	t.sentContext = sendContext

	sendCtx, cancel := context.WithCancel(context.Background())
	t.cancelFunc = cancel

	t.persistRunningState(handler, sendContext)

	go t.runBlockingCall(handler, sendCtx, sendContext)

	return nil
}

func (t *Component) runBlockingCall(handler module.Handler, ctx context.Context, sendContext Context) {
	result := handler(ctx, OutPort, sendContext)

	log.Info().Interface("result", result).Msg("signal component: OutPort returned, send complete")

	t.isRunning = false
	t.cancelFunc = nil

	t.clearRunningMetadata(handler)
	_ = handler(context.Background(), v1alpha1.ReconcilePort, nil)
}

func (t *Component) persistRunningState(handler module.Handler, sendContext Context) {
	ctxBytes, _ := json.Marshal(sendContext)
	_ = handler(context.Background(), v1alpha1.ReconcilePort, func(n *v1alpha1.TinyNode) error {
		if n.Status.Metadata == nil {
			n.Status.Metadata = make(map[string]string)
		}
		n.Status.Metadata[metadataKeyRunning] = "true"
		n.Status.Metadata[metadataKeyContext] = string(ctxBytes)
		return nil
	})
}

func (t *Component) clearRunningMetadata(handler module.Handler) {
	_ = handler(context.Background(), v1alpha1.ReconcilePort, func(n *v1alpha1.TinyNode) error {
		if n.Status.Metadata != nil {
			delete(n.Status.Metadata, metadataKeyRunning)
		}
		return nil
	})
}

func (t *Component) handleSettings(msg interface{}) error {
	in, ok := msg.(Settings)
	if !ok {
		return fmt.Errorf("invalid settings")
	}
	t.settings = in
	return nil
}

func (t *Component) Ports() []module.Port {
	log.Info().Bool("isRunning", t.isRunning).Msg("signal component: Ports() called")

	controlContext := t.settings.Context
	if t.sentContext != nil {
		controlContext = t.sentContext
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
			Blocking:      true,
		},
		{
			Name:   v1alpha1.ControlPort,
			Label:  "Control",
			Source: true,
			Configuration: Control{
				Context:     controlContext,
				ResetEnable: t.isRunning,
			},
		},
	}
}

var _ module.Component = (*Component)(nil)

func init() {
	registry.Register((&Component{}).Instance())
}
