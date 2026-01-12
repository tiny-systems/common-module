package cron

import (
	"context"
	"fmt"
	"sync"
	"time"

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
	ComponentName    = "cron"
	OutPort          = "out"
	NextTickMetadata = "cron-next-tick"
)

type Context any

type Settings struct {
	Context  Context `json:"context,omitempty" configurable:"true" title:"Context" description:"Arbitrary message to send on each scheduled execution"`
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

	runMu sync.Mutex // serializes run/stop
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
		Info:        "Scheduled emitter using cron expressions. Click Start to begin emitting context on Out port according to the schedule. Supports standard cron syntax (minute hour day-of-month month day-of-week). Examples: '*/5 * * * *' (every 5 min), '0 */2 * * *' (every 2 hours), '0 9 * * 1-5' (9 AM weekdays). Click Stop to pause. Persists next execution time - if restarted after scheduled time, executes immediately.",
		Tags:        []string{"SDK"},
	}
}

func (c *Component) Handle(ctx context.Context, handler module.Handler, port string, msg any) any {
	switch port {
	case v1alpha1.ReconcilePort:
		return c.handleReconcile(msg)

	case v1alpha1.SettingsPort:
		return c.handleSettings(msg)

	case v1alpha1.ControlPort:
		return c.handleControl(ctx, handler, msg)

	default:
		return fmt.Errorf("unknown port: %s", port)
	}
}

func (c *Component) handleReconcile(msg interface{}) error {
	node, ok := msg.(v1alpha1.TinyNode)
	if !ok {
		return nil
	}

	tickStr := node.Status.Metadata[NextTickMetadata]
	if tickStr == "" {
		return nil
	}

	t, err := time.Parse(time.RFC3339Nano, tickStr)
	if err != nil {
		return nil
	}

	c.mu.Lock()
	c.nextTick = t
	c.mu.Unlock()

	log.Info().Time("nextTick", t).Msg("cron: restored next tick from metadata")
	return nil
}

func (c *Component) handleSettings(msg interface{}) error {
	in, ok := msg.(Settings)
	if !ok {
		return fmt.Errorf("invalid settings")
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.settings = in
	return nil
}

func (c *Component) handleControl(ctx context.Context, handler module.Handler, msg interface{}) error {
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
		return c.stop()
	}

	c.mu.Lock()
	c.settings.Context = ctrl.Context
	c.settings.Schedule = ctrl.Schedule
	c.mu.Unlock()

	return c.run(ctx, handler)
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
		return fmt.Errorf("invalid cron expression %q: %w", schedule, err)
	}

	c.mu.Lock()
	nextTick := c.nextTick
	if nextTick.IsZero() {
		nextTick = sched.Next(time.Now())
		c.nextTick = nextTick
	}
	c.mu.Unlock()

	c.saveNextTick(handler, nextTick)
	handler(context.Background(), v1alpha1.ReconcilePort, nil)

	defer func() {
		c.mu.Lock()
		c.cancel = nil
		c.nextTick = time.Time{}
		c.mu.Unlock()
		c.clearNextTick(handler)
		handler(context.Background(), v1alpha1.ReconcilePort, nil)
	}()

	log.Info().Str("schedule", schedule).Time("nextTick", nextTick).Msg("cron: started")

	for {
		if err := c.waitUntil(ctx, nextTick); err != nil {
			return nil // cancelled
		}

		c.clearNextTick(handler)

		c.mu.Lock()
		data := c.settings.Context
		c.mu.Unlock()

		handler(trace.ContextWithSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{})), OutPort, data)

		if ctx.Err() != nil {
			return nil
		}

		nextTick = sched.Next(time.Now())
		c.mu.Lock()
		c.nextTick = nextTick
		c.mu.Unlock()
		c.saveNextTick(handler, nextTick)

		log.Debug().Time("nextTick", nextTick).Msg("cron: scheduled next tick")
	}
}

func (c *Component) waitUntil(ctx context.Context, t time.Time) error {
	now := time.Now()
	if !now.Before(t) {
		log.Info().Time("nextTick", t).Time("now", now).Msg("cron: missed tick, executing immediately")
		return nil
	}

	timer := time.NewTimer(t.Sub(now))
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Component) stop() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.cancel != nil {
		c.cancel()
	}
	return nil
}

func (c *Component) saveNextTick(handler module.Handler, t time.Time) {
	handler(context.Background(), v1alpha1.ReconcilePort, func(n *v1alpha1.TinyNode) error {
		if n.Status.Metadata == nil {
			n.Status.Metadata = make(map[string]string)
		}
		n.Status.Metadata[NextTickMetadata] = t.Format(time.RFC3339Nano)
		return nil
	})
}

func (c *Component) clearNextTick(handler module.Handler) {
	handler(context.Background(), v1alpha1.ReconcilePort, func(n *v1alpha1.TinyNode) error {
		delete(n.Status.Metadata, NextTickMetadata)
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

	return Control{
		Context:  c.settings.Context,
		Schedule: c.settings.Schedule,
		Status:   "Not running",
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
