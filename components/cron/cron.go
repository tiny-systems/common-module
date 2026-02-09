package cron

import (
	"context"
	"crypto/rand"
	"fmt"
	"sync"
	"time"

	"github.com/goccy/go-json"
	"github.com/robfig/cron/v3"
	"github.com/rs/zerolog/log"
	"github.com/swaggest/jsonschema-go"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/pkg/utils"
	"github.com/tiny-systems/module/registry"
	"go.opentelemetry.io/otel/trace"
)

const (
	ComponentName = "cron"
	OutPort       = "out"
)

const (
	metadataKeyRunning  = "cron-running"
	metadataKeySchedule = "cron-schedule"
	metadataKeyContext  = "cron-context"
	metadataKeyError    = "cron-error"
)

type Context any

type Settings struct {
	Context  Context `json:"context" configurable:"true" title:"Context" description:"Arbitrary message to send on each scheduled execution"`
	Schedule string  `json:"schedule" required:"true" title:"Schedule" description:"Cron expression (e.g., '*/5 * * * *' for every 5 minutes, '0 9 * * 1-5' for 9 AM on weekdays)" default:"*/1 * * * *"`
}

type Control struct {
	Context  Context `json:"context" required:"true" title:"Context"`
	Schedule string  `json:"schedule" required:"true" title:"Schedule" description:"Cron expression"`
	NextRun  string  `json:"nextRun" title:"Next Run" readonly:"true"`
	Status   string  `json:"status" title:"Status" readonly:"true"`
	Stop     bool    `json:"stop" format:"button" title:"Stop" required:"true"`
	Start    bool    `json:"start" format:"button" title:"Start" required:"true"`
}

func (ctrl Control) PrepareJSONSchema(schema *jsonschema.Schema) error {
	if ctrl.Start {
		delete(schema.Properties, "stop")
	} else {
		delete(schema.Properties, "start")
	}
	return nil
}

type Component struct {
	mu       sync.Mutex
	settings Settings
	cancel   context.CancelFunc
	nextTick time.Time
	handler  module.Handler

	// settingsFromPort is set when _settings or _control port provides values.
	// When true, _reconcile skips metadata restore to avoid overwriting fresh values.
	settingsFromPort bool

	lastError string
	runMu     sync.Mutex
}

func (c *Component) Instance() module.Component {
	return &Component{
		settings: Settings{Schedule: "*/1 * * * *"},
	}
}

func (c *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Cron",
		Info:        "Scheduled emitter using cron expressions. Click Start to begin emitting context on Out port according to the schedule. Supports standard cron syntax (minute hour day-of-month month day-of-week). Examples: '*/5 * * * *' (every 5 min), '0 */2 * * *' (every 2 hours), '0 9 * * 1-5' (9 AM weekdays). Click Stop to pause. Cron survives pod restarts and leadership changes.",
		Tags:        []string{"SDK"},
	}
}

func (c *Component) Handle(ctx context.Context, handler module.Handler, port string, msg any) any {
	c.handler = handler

	switch port {
	case v1alpha1.ReconcilePort:
		return c.handleReconcile(ctx, handler, msg)

	case v1alpha1.SettingsPort:
		in, ok := msg.(Settings)
		if !ok {
			return fmt.Errorf("invalid settings")
		}
		c.mu.Lock()
		c.settings = in
		c.settingsFromPort = true
		isRunning := c.cancel != nil
		c.mu.Unlock()
		if isRunning {
			c.persistRunningState(handler)
		}
		return nil

	case v1alpha1.ControlPort:
		if msg == nil {
			return nil
		}
		if !utils.IsLeader(ctx) {
			return nil
		}
		ctrl, ok := msg.(Control)
		if !ok {
			return fmt.Errorf("invalid control message")
		}
		if ctrl.Stop {
			return c.stop(handler)
		}

		// Validate schedule before starting
		parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
		if _, err := parser.Parse(ctrl.Schedule); err != nil {
			errMsg := fmt.Sprintf("invalid schedule %q: %v", ctrl.Schedule, err)
			c.mu.Lock()
			c.lastError = errMsg
			c.mu.Unlock()
			c.persistError(handler, errMsg)
			return nil
		}

		c.mu.Lock()
		c.settings.Context = ctrl.Context
		c.settings.Schedule = ctrl.Schedule
		c.settingsFromPort = true
		c.lastError = ""
		c.mu.Unlock()
		c.clearError(handler)

		c.persistRunningState(handler)
		go c.run(context.Background(), handler)
		return nil

	default:
		return fmt.Errorf("unknown port: %s", port)
	}
}

func (c *Component) handleReconcile(ctx context.Context, handler module.Handler, msg interface{}) error {
	node, ok := msg.(v1alpha1.TinyNode)
	if !ok {
		return nil
	}

	c.restoreSettingsFromMetadata(node.Status.Metadata)
	c.handleOrphanedRunningState(ctx, handler, node.Status.Metadata)
	return nil
}

func (c *Component) restoreSettingsFromMetadata(metadata map[string]string) {
	if metadata == nil {
		return
	}

	c.mu.Lock()
	if c.settingsFromPort {
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()

	if schedule, ok := metadata[metadataKeySchedule]; ok && schedule != "" {
		c.mu.Lock()
		c.settings.Schedule = schedule
		c.mu.Unlock()
	}

	if ctxStr, ok := metadata[metadataKeyContext]; ok && ctxStr != "" {
		var savedCtx Context
		if err := json.Unmarshal([]byte(ctxStr), &savedCtx); err == nil {
			c.mu.Lock()
			c.settings.Context = savedCtx
			c.mu.Unlock()
		}
	}

	if errMsg, ok := metadata[metadataKeyError]; ok {
		c.mu.Lock()
		c.lastError = errMsg
		c.mu.Unlock()
	}
}

func (c *Component) handleOrphanedRunningState(ctx context.Context, handler module.Handler, metadata map[string]string) {
	if metadata == nil {
		return
	}

	if _, hasRunning := metadata[metadataKeyRunning]; !hasRunning {
		return
	}

	// Already running locally
	c.mu.Lock()
	if c.cancel != nil {
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()

	if !utils.IsLeader(ctx) {
		return
	}

	log.Info().Msg("cron component: resuming after pod restart or leadership change")
	go c.run(context.Background(), handler)
}

func (c *Component) run(ctx context.Context, handler module.Handler) error {
	c.runMu.Lock()
	defer c.runMu.Unlock()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	c.mu.Lock()
	c.cancel = cancel
	schedule := c.settings.Schedule
	c.mu.Unlock()

	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	sched, err := parser.Parse(schedule)
	if err != nil {
		c.mu.Lock()
		c.cancel = nil
		c.mu.Unlock()
		c.clearRunningMetadata(handler)
		return fmt.Errorf("invalid cron expression %q: %w", schedule, err)
	}

	c.mu.Lock()
	c.nextTick = sched.Next(time.Now())
	c.mu.Unlock()

	handler(context.Background(), v1alpha1.ReconcilePort, nil)

	defer func() {
		c.mu.Lock()
		c.cancel = nil
		c.nextTick = time.Time{}
		c.mu.Unlock()
		handler(context.Background(), v1alpha1.ReconcilePort, nil)
	}()

	log.Info().Str("schedule", schedule).Time("nextTick", c.nextTick).Msg("cron: started")

	for {
		c.mu.Lock()
		nextTick := c.nextTick
		c.mu.Unlock()

		if err := c.waitUntil(ctx, nextTick); err != nil {
			return nil
		}

		c.mu.Lock()
		data := c.settings.Context
		c.mu.Unlock()

		var traceID trace.TraceID
		var spanID trace.SpanID
		rand.Read(traceID[:])
		rand.Read(spanID[:])
		tickCtx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
			TraceID:    traceID,
			SpanID:     spanID,
			TraceFlags: trace.FlagsSampled,
		}))
		handler(tickCtx, OutPort, data)

		if ctx.Err() != nil {
			return nil
		}

		c.mu.Lock()
		c.nextTick = sched.Next(time.Now())
		c.mu.Unlock()

		handler(context.Background(), v1alpha1.ReconcilePort, nil)

		log.Debug().Time("nextTick", c.nextTick).Msg("cron: scheduled next tick")
	}
}

func (c *Component) waitUntil(ctx context.Context, t time.Time) error {
	wait := time.Until(t)
	if wait <= 0 {
		return nil
	}

	timer := time.NewTimer(wait)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Component) stop(handler module.Handler) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.cancel != nil {
		c.cancel()
	}
	c.clearRunningMetadata(handler)
	return nil
}

func (c *Component) persistRunningState(handler module.Handler) {
	c.mu.Lock()
	schedule := c.settings.Schedule
	cronCtx := c.settings.Context
	c.mu.Unlock()

	ctxBytes, _ := json.Marshal(cronCtx)
	_ = handler(context.Background(), v1alpha1.ReconcilePort, func(n *v1alpha1.TinyNode) error {
		if n.Status.Metadata == nil {
			n.Status.Metadata = make(map[string]string)
		}
		n.Status.Metadata[metadataKeyRunning] = "true"
		n.Status.Metadata[metadataKeySchedule] = schedule
		n.Status.Metadata[metadataKeyContext] = string(ctxBytes)
		return nil
	})
}

func (c *Component) clearRunningMetadata(handler module.Handler) {
	_ = handler(context.Background(), v1alpha1.ReconcilePort, func(n *v1alpha1.TinyNode) error {
		if n.Status.Metadata != nil {
			delete(n.Status.Metadata, metadataKeyRunning)
			delete(n.Status.Metadata, metadataKeySchedule)
			delete(n.Status.Metadata, metadataKeyContext)
			delete(n.Status.Metadata, metadataKeyError)
		}
		return nil
	})
}

func (c *Component) persistError(handler module.Handler, errMsg string) {
	_ = handler(context.Background(), v1alpha1.ReconcilePort, func(n *v1alpha1.TinyNode) error {
		if n.Status.Metadata == nil {
			n.Status.Metadata = make(map[string]string)
		}
		n.Status.Metadata[metadataKeyError] = errMsg
		return nil
	})
}

func (c *Component) clearError(handler module.Handler) {
	_ = handler(context.Background(), v1alpha1.ReconcilePort, func(n *v1alpha1.TinyNode) error {
		if n.Status.Metadata != nil {
			delete(n.Status.Metadata, metadataKeyError)
		}
		return nil
	})
}

func (c *Component) Ports() []module.Port {
	c.mu.Lock()
	defer c.mu.Unlock()

	return []module.Port{
		{Name: v1alpha1.ReconcilePort},
		{Name: v1alpha1.SettingsPort, Label: "Settings", Configuration: c.settings},
		{Name: OutPort, Label: "Out", Source: true, Position: module.Right, Configuration: new(Context)},
		{Name: v1alpha1.ControlPort, Label: "Control", Source: true, Configuration: c.control()},
	}
}

func (c *Component) control() Control {
	if c.cancel != nil {
		nextRun := ""
		if !c.nextTick.IsZero() {
			nextRun = c.nextTick.Format(time.RFC3339)
		}
		return Control{
			Status:   "Running",
			Context:  c.settings.Context,
			Schedule: c.settings.Schedule,
			NextRun:  nextRun,
			Stop:     true,
		}
	}

	status := "Not running"
	if c.lastError != "" {
		status = c.lastError
	}

	return Control{
		Context:  c.settings.Context,
		Schedule: c.settings.Schedule,
		Status:   status,
		Start:    true,
	}
}

var (
	_ module.Component    = (*Component)(nil)
	_ jsonschema.Preparer = (*Control)(nil)
)

func init() {
	registry.Register((&Component{}).Instance())
}
