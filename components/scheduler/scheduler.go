package scheduler

import (
	"context"
	"fmt"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
	"go.opentelemetry.io/otel/trace"
	"sync"
	"time"
)

const (
	ComponentName        = "scheduler"
	OutPort       string = "out"
	InPort        string = "in"
	StartPort     string = "start"
	AckPort       string = "ack"
	StopPort      string = "stop"
)

type Settings struct {
	EnableAckPort  bool `json:"enableAckPort" title:"Enable task acknowledge port" description:"Port gives information if incoming task was scheduled properly"`
	EnableStopPort bool `json:"enableStopPort" required:"true" title:"Enable stop port" description:"Stop port allows you to stop scheduler"`
}

type StartControl struct {
	Start  bool   `json:"start" format:"button" title:"Start" required:"true" description:"Run"`
	Status string `json:"status" title:"Status" readonly:"true"`
}

type StopControl struct {
	Stop   bool   `json:"stop" format:"button" title:"Stop" required:"true" description:"Stop"`
	Status string `json:"status" title:"Status" readonly:"true"`
}

type Stop struct {
}

type Start struct {
}

type Context any

type InMessage struct {
	Context Context `json:"context" title:"Context" configurable:"true" description:"Arbitrary message to be send further"`
	Task    Task    `json:"task" title:"Task" required:"true"`
}

type Task struct {
	ID       string    `json:"id" required:"true" title:"Unique task ID"`
	DateTime time.Time `json:"dateTime" required:"true" title:"Date and time" description:"Format examples: 2012-10-01T09:45:00.000+02:00"`
	Schedule bool      `json:"schedule" required:"true" title:"Schedule" description:"You can unschedule existing task by settings schedule equals false. Default: true"`
}

type OutMessage struct {
	Task    Task    `json:"task"`
	Context Context `json:"context"`
}

type TaskAck struct {
	Task        Task    `json:"task"`
	Context     Context `json:"context"`
	ScheduledIn int64   `json:"scheduledIn"`
	Error       *string `json:"error"`
}

type task struct {
	timer *time.Timer
	call  func(ctx context.Context)
	id    string
}

type Component struct {
	settings       Settings
	cancelFunc     context.CancelFunc
	cancelFuncLock *sync.Mutex

	//
	runCtx  context.Context
	runLock *sync.Mutex
	tasks   cmap.ConcurrentMap[string, *task]
}

func (s *Component) Instance() module.Component {
	return &Component{
		cancelFuncLock: &sync.Mutex{},
		runLock:        &sync.Mutex{},
		tasks:          cmap.New[*task](),
	}
}

func (s *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Scheduler",
		Info:        "Collects tasks messages. When its running sends messages further when scheduled date and time come. Tasks with same IDs are updating scheduled date and task itself. If scheduled date is already passed - sends message as soon as being started.",
		Tags:        []string{"SDK"},
	}
}

func (s *Component) run(ctx context.Context, handler module.Handler) error {

	s.runLock.Lock()
	defer s.runLock.Unlock()

	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()

	s.runCtx = runCtx

	s.setCancelFunc(runCancel)
	// reconcile so show we are listening
	_ = handler(context.Background(), module.ReconcilePort, nil)

	defer func() {
		s.setCancelFunc(nil)
		_ = handler(context.Background(), module.ReconcilePort, nil)
	}()

	<-s.runCtx.Done()
	return nil
}

func (s *Component) setCancelFunc(f func()) {
	s.cancelFuncLock.Lock()
	defer s.cancelFuncLock.Unlock()
	s.cancelFunc = f
}

func (s *Component) isRunning() bool {
	s.cancelFuncLock.Lock()
	defer s.cancelFuncLock.Unlock()
	return s.cancelFunc != nil
}

func (s *Component) stop() error {
	s.cancelFuncLock.Lock()
	defer s.cancelFuncLock.Unlock()
	if s.cancelFunc == nil {
		return nil
	}
	s.cancelFunc()
	return nil
}

func (s *Component) Handle(ctx context.Context, handler module.Handler, port string, msg interface{}) error {

	switch port {
	case module.SettingsPort:
		in, ok := msg.(Settings)
		if !ok {
			return fmt.Errorf("invalid settings")
		}
		s.settings = in
		return nil

	case module.ControlPort:
		if msg == nil {
			break
		}
		switch msg.(type) {
		case StartControl:
			return s.run(ctx, handler)
		case StopControl:
			return s.stop()
		}

	case StartPort:
		return s.run(ctx, handler)

	case StopPort:
		return s.stop()

	case InPort:
		in, ok := msg.(InMessage)
		if !ok {
			return fmt.Errorf("invalid input task message")
		}
		var (
			t           = in.Task
			scheduledIn int64
		)

		if in.Task.Schedule {
			scheduledIn = int64(t.DateTime.Sub(time.Now()).Seconds())
		}

		ackErr := s.addOrUpdateTask(t.ID, t.Schedule, t.DateTime.Sub(time.Now()), func(ctx context.Context) {
			_ = handler(ctx, OutPort, OutMessage{
				Task:    in.Task,
				Context: in.Context,
			})
		})

		if s.settings.EnableAckPort {
			ack := TaskAck{
				Task:        in.Task,
				Context:     in.Context,
				ScheduledIn: scheduledIn,
			}
			if ackErr != nil {
				ackErrStr := ackErr.Error()
				ack.Error = &ackErrStr
			}

			if err := handler(ctx, AckPort, ack); err != nil {
				return err
			}
		} else if ackErr != nil {
			return ackErr
		}

		//

	default:
		return fmt.Errorf("invalid port: %s", port)
	}
	return nil
}

func (s *Component) addOrUpdateTask(id string, schedule bool, duration time.Duration, f func(ctx context.Context)) error {

	if !s.isRunning() {
		return fmt.Errorf("scheduler is not running")
	}
	if duration.Seconds() < 0 {
		return fmt.Errorf("scheduled time is past")
	}

	if d, ok := s.tasks.Get(id); ok {
		// stop and remove it
		d.timer.Stop()
		s.tasks.Remove(id)
	}
	// not found and don't ask to schedule
	if !schedule {
		return nil
	}

	// schedule a new task
	tt := &task{
		timer: time.NewTimer(duration),
		id:    id,
		call:  f,
	}

	s.tasks.Set(id, tt)
	go s.waitTask(tt)
	return nil
}

func (s *Component) waitTask(d *task) {

	defer s.tasks.Remove(d.id)
	select {
	case <-d.timer.C:
		// new trace
		d.call(trace.ContextWithSpanContext(s.runCtx, trace.NewSpanContext(trace.SpanContextConfig{})))
	case <-s.runCtx.Done():
	}
}

func (s *Component) getControl() interface{} {
	if s.isRunning() {
		return StopControl{
			Status: "Running",
		}
	}
	return StartControl{
		Status: "Not running",
	}
}

func (s *Component) Ports() []module.Port {
	ports := []module.Port{
		{
			Name:          module.SettingsPort,
			Label:         "Settings",
			Source:        true,
			Configuration: Settings{},
		},
		{
			Name:          StartPort,
			Label:         "Start",
			Source:        true,
			Configuration: Start{},
			Position:      module.Left,
		},
		{
			Name:          module.ControlPort,
			Label:         "Dashboard",
			Configuration: s.getControl(),
		},
		{
			Name:   InPort,
			Label:  "Tasks",
			Source: true,
			Configuration: InMessage{
				Task: Task{
					ID:       "someUniqueID",
					DateTime: time.Now(),
					Schedule: true,
				},
			},
			Position: module.Left,
		},
		{
			Name:          OutPort,
			Label:         "Scheduled",
			Source:        false,
			Configuration: OutMessage{},
			Position:      module.Right,
		},
	}

	// programmatically stop server
	if s.settings.EnableStopPort {
		ports = append(ports, module.Port{
			Position:      module.Bottom,
			Name:          StopPort,
			Label:         "Stop",
			Source:        true,
			Configuration: Stop{},
		})
	}

	if !s.settings.EnableAckPort {
		return ports
	}

	return append(ports, module.Port{
		Name:          AckPort,
		Label:         "Ack",
		Source:        false,
		Configuration: TaskAck{},
		Position:      module.Right,
	})
}

var scheduler = (*Component)(nil)

var _ module.Component = scheduler

func init() {
	registry.Register(&Component{})
}
