package runstart

import (
	"context"
	"fmt"

	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName = "run_start"
	InPort        = "in"
	OutPort       = "out"
	StartedPort   = "started"
)

type Context any

type Request struct {
	Context Context `json:"context" configurable:"true" title:"Context" description:"Payload to send into the durable chain"`
}

type Started struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context"`
	RunID   string  `json:"runID" title:"Run ID" description:"Durable run id — poll it with run_status"`
}

// Component is the front door into durable execution. It starts a durable
// run, fires the chain on Out WITHOUT waiting for it (the emit returns as
// soon as the first hop is durably stored), and then emits {runID} on
// Started — which, wired back to an http_server response, gives the HTTP
// caller an immediate handle to poll while the run continues in the
// background, surviving pod restarts and migrating across replicas.
//
// Place it on a CLASSIC (non-durable) node between the trigger and the
// durable chain. Downstream nodes need no labels: every hop emitted under
// the run identity rides the durable path.
type Component struct{}

func (t *Component) Instance() module.Component {
	return &Component{}
}

func (t *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Durable Run Start",
		Info:        "Front door into durable execution. Starts a durable run, emits the payload on Out fire-and-forget (returns once the first hop is durably stored — does NOT wait for the chain), then emits {context, runID} on Started for the synchronous reply. Wire Started back to http_server's Response to give HTTP callers a run handle they can poll with run_status. The run survives pod death and migrates across replicas.",
		Tags:        []string{"SDK", "durable"},
	}
}

func (t *Component) Handle(ctx context.Context, handler module.Handler, port string, msg interface{}) module.Result {
	if port != InPort {
		return module.Fail(fmt.Errorf("port %s is not supported", port))
	}
	in, ok := msg.(Request)
	if !ok {
		return module.Fail(fmt.Errorf("invalid message"))
	}

	// Fire the chain under a fresh run identity: this emit is durable
	// (fire-and-forget) and returns once the hop is stored.
	runCtx, runID := module.BeginRun(ctx)
	if res := handler(runCtx, OutPort, in.Context); res.Err() != nil {
		return res
	}

	// Synchronous reply on the ORIGINAL context — classic blocking path back
	// to the caller (e.g. http_server's response).
	return handler(ctx, StartedPort, Started{Context: in.Context, RunID: runID})
}

func (t *Component) Ports() []module.Port {
	return []module.Port{
		{
			Name:          InPort,
			Label:         "In",
			Configuration: Request{},
			Position:      module.Left,
		},
		{
			Name:          OutPort,
			Label:         "Out",
			Source:        true,
			Configuration: new(Context),
			Position:      module.Right,
		},
		{
			Name:          StartedPort,
			Label:         "Started",
			Source:        true,
			Configuration: Started{},
			Position:      module.Bottom,
		},
	}
}

var _ module.Component = (*Component)(nil)

func init() {
	registry.Register((&Component{}).Instance())
}
