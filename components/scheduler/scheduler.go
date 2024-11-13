package scheduler

import (
	"context"
	"fmt"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
	"time"
)

const (
	ComponentName        = "scheduler"
	OutPort       string = "out"
	InPort        string = "in"
	StartPort     string = "start"
	AckPort       string = "ack"
)

type Settings struct {
	EnableAckPort bool `json:"enableAckPort" title:"Enable task acknowledge port" description:"Port gives information if incoming task was scheduled properly"`
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
	call  func()
	id    string
}

type Component struct {
	settings Settings
	tasks    cmap.ConcurrentMap[string, *task]
}

func (s *Component) Instance() module.Component {
	return &Component{
		tasks: cmap.New[*task](),
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

func (s *Component) emit(ctx context.Context) error {
	for _, k := range s.tasks.Keys() {
		v, _ := s.tasks.Get(k)
		go s.waitTask(ctx, v)
	}
	<-ctx.Done()
	return nil
}

func (s *Component) Handle(ctx context.Context, handler module.Handler, port string, msg interface{}) error {

	//emit
	if port == module.SettingsPort {
		in, ok := msg.(Settings)
		if !ok {
			return fmt.Errorf("invalid settings")
		}
		s.settings = in
		return nil
	}

	if port == StartPort {
		return s.emit(ctx)
	}

	if port != InPort {
		return fmt.Errorf("invalid port: %s", port)
	}

	in, ok := msg.(InMessage)
	if !ok {
		return fmt.Errorf("invalid message")
	}

	var (
		t           = in.Task
		scheduledIn int64
	)
	if in.Task.Schedule {
		scheduledIn = int64(t.DateTime.Sub(time.Now()).Seconds())
	}

	if s.settings.EnableAckPort {
		if err := handler(ctx, AckPort, TaskAck{
			Task:        in.Task,
			Context:     in.Context,
			ScheduledIn: scheduledIn,
		}); err != nil {
			return err
		}
	}

	s.addOrUpdateTask(t.ID, t.Schedule, t.DateTime.Sub(time.Now()), func() {
		_ = handler(ctx, OutPort, OutMessage{
			Task:    in.Task,
			Context: in.Context,
		})
	})
	return nil
}

func (s *Component) addOrUpdateTask(id string, start bool, duration time.Duration, f func()) {
	if d, ok := s.tasks.Get(id); ok {
		// job is registered
		// tasks it
		d.timer.Stop()
		if start {
			d.timer.Reset(duration)
		} else {
			s.tasks.Remove(id)
		}
		return
	}
	if !start {
		return
	}
	tt := &task{
		timer: time.NewTimer(duration),
		id:    id,
		call:  f,
	}
	s.tasks.Set(id, tt)
}

func (s *Component) waitTask(ctx context.Context, d *task) {
	select {
	case <-d.timer.C:
		s.tasks.Remove(d.id)
		d.call()
	case <-ctx.Done():
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
			Name:   InPort,
			Label:  "Tasks",
			Source: true,
			Configuration: InMessage{
				Task: Task{
					ID:       "someID2323",
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

	if !s.settings.EnableAckPort {
		return ports
	}

	return append(ports, module.Port{
		Name:          AckPort,
		Label:         "Ack",
		Source:        false,
		Configuration: TaskAck{},
		Position:      module.Bottom,
	})
}

var scheduler = (*Component)(nil)

var _ module.Component = scheduler

func init() {
	registry.Register(&Component{})
}
