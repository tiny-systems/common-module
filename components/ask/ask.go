package ask

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/pkg/utils"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName = "ask"
	RequestPort   = "request"
	OutPort       = "out"
)

// Context is the passthrough payload — whatever the flow was carrying when it
// reached the gate. It rides through the human's decision unchanged so the
// downstream action knows what it is acting on.
type Context any

// defaultForm is published when the node has no authored form. Approve/Deny is
// the overwhelmingly common case, so an unconfigured ask is still useful: it
// asks the human to confirm, and the incoming context is displayed above the
// buttons by the readonly `context` field.
const defaultForm = `{
  "type": "object",
  "properties": {
    "context": {"type": "object", "title": "Under review", "readonly": true, "propertyOrder": 1},
    "approve": {"type": "boolean", "title": "Approve", "format": "button", "propertyOrder": 2},
    "deny":    {"type": "boolean", "title": "Deny",    "format": "button", "propertyOrder": 3}
  }
}`

type Settings struct {
	// Form is a JSON Schema rendered as the question put to the human. It is
	// authored here rather than derived from a Go type, which is the whole
	// point: the same component asks "approve this restart?" or "how many
	// replicas?" depending on what is written here.
	Form string `json:"form,omitempty" title:"Form" format:"code" language:"json" description:"JSON Schema of the form shown to the human. Fields with format:\"button\" are the answers (e.g. approve/deny). Leave empty for a default Approve/Deny form."`
}

// Request is what the flow sends to open the gate. The form is fixed by
// Settings; this carries the thing being decided on.
type Request struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context" description:"Payload under review — shown to the human and passed through to the answer."`
}

// Reply is emitted once a human submits the form.
type Reply struct {
	Values  map[string]interface{} `json:"values" title:"Values" description:"What the human submitted, keyed by the form's field names."`
	Context Context                `json:"context,omitempty" title:"Context" description:"The request payload, unchanged."`
}

type Component struct {
	module.Base
	settings Settings

	mu sync.Mutex
	// pending is the data half of the published form: the values the widget
	// renders into the schema. Never nil — the runtime decodes an incoming
	// control submission into reflect.TypeOf(port.Configuration), and a nil map
	// has no type to reflect.
	pending map[string]interface{}
	// askContext is the payload this question is about, held so it can ride
	// out with the answer.
	askContext Context
	// open is false until a request arrives and after an answer is emitted, so
	// a resubmitted or replayed form cannot fire the action twice.
	open bool
}

func (c *Component) Instance() module.Component {
	return &Component{
		settings: Settings{},
		pending:  map[string]interface{}{},
	}
}

func (c *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Ask a human",
		Info:        "Human-in-the-loop gate. A message on Request publishes a form on the Control port; the flow does NOT block. A human fills the form in the editor and submits, which emits {values, context} on Out — wire Out to the action being gated (e.g. workload_restart) and branch on the submitted values with a router. The form is a JSON Schema authored in settings; fields with format:\"button\" are the answers. Defaults to Approve/Deny. Use to put a person in front of anything destructive.",
		Tags:        []string{"SDK", "Human"},
	}
}

func (c *Component) OnSettings(_ context.Context, msg any) error {
	in, ok := msg.(Settings)
	if !ok {
		return fmt.Errorf("invalid settings")
	}
	if f := in.Form; f != "" && !json.Valid([]byte(f)) {
		// Refuse a broken form at configuration time. Publishing invalid bytes
		// would leave the node with an unrenderable port and no clue why.
		return fmt.Errorf("form is not valid JSON")
	}
	c.settings = in
	return nil
}

// form returns the schema to publish: the authored one, or Approve/Deny.
func (c *Component) form() json.RawMessage {
	if c.settings.Form != "" {
		return json.RawMessage(c.settings.Form)
	}
	return json.RawMessage(defaultForm)
}

// Handle opens the gate. It returns immediately: the run does not stay parked
// waiting for a human. Continuity lives in this node's published form, and the
// answer arrives later as a separate control message that starts the
// downstream hop itself.
func (c *Component) Handle(ctx context.Context, _ module.Handler, port string, msg any) module.Result {
	if port != RequestPort {
		return module.Fail(fmt.Errorf("port %s is not supported", port))
	}
	in, ok := msg.(Request)
	if !ok {
		return module.Fail(fmt.Errorf("invalid message"))
	}

	c.mu.Lock()
	c.askContext = in.Context
	c.open = true
	// Seed the form's data so readonly fields render the payload under review.
	c.pending = map[string]interface{}{"context": in.Context}
	c.mu.Unlock()

	// Re-publish the control port so the widget picks up the new question.
	return c.Emit(ctx, v1alpha1.ControlPort, c.control())
}

// OnControl receives the human's submission. The form has no Go type — its
// shape is only known at runtime — so the runtime decodes it into the map this
// port advertises as its configuration, and the submitted values arrive here
// as that map rather than a typed struct.
func (c *Component) OnControl(ctx context.Context, msg any) error {
	if !utils.IsLeader(ctx) {
		return nil
	}
	values, ok := msg.(map[string]interface{})
	if !ok {
		return fmt.Errorf("invalid control msg: expected map, got %T", msg)
	}

	c.mu.Lock()
	if !c.open {
		// No question outstanding — a re-render or a replayed submission.
		c.mu.Unlock()
		return nil
	}
	// A form is submitted whole, so the presence of values proves nothing; an
	// answer is a button press. The editor only sends on a button click, but a
	// direct API call could send anything, so require one to be true.
	if !hasPressedButton(values) {
		c.mu.Unlock()
		return nil
	}
	askContext := c.askContext
	c.open = false
	c.pending = map[string]interface{}{"context": askContext}
	c.mu.Unlock()

	// Detached, like signal: nothing here waits on the downstream action, and
	// the control message must not stay open for the length of the flow.
	go c.Emit(context.Background(), OutPort, Reply{
		Values:  values,
		Context: askContext,
	})
	return nil
}

// hasPressedButton reports whether any boolean in the submission is true. The
// form's answer fields are format:"button" booleans; the readonly context
// object is not a bool, so it can never look like an answer.
func hasPressedButton(values map[string]interface{}) bool {
	for _, v := range values {
		if b, ok := v.(bool); ok && b {
			return true
		}
	}
	return false
}

func (c *Component) control() map[string]interface{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Copy: the runtime marshals this outside the lock.
	out := make(map[string]interface{}, len(c.pending))
	for k, v := range c.pending {
		out[k] = v
	}
	return out
}

func (c *Component) Ports() []module.Port {
	return []module.Port{
		{
			Name:          v1alpha1.SettingsPort,
			Label:         "Settings",
			Configuration: c.settings,
		},
		{
			Name:          RequestPort,
			Label:         "Request",
			Position:      module.Left,
			Configuration: Request{},
		},
		{
			Name:          OutPort,
			Label:         "Out",
			Source:        true,
			Position:      module.Right,
			Configuration: Reply{},
		},
		{
			Name:     v1alpha1.ControlPort,
			Label:    "Control",
			Source:   true,
			Position: module.Top,
			// The data is a map so an untyped submission has something to
			// decode into; the schema is the authored form, published verbatim
			// because no Go type describes it.
			Configuration: c.control(),
			Schema:        c.form(),
		},
	}
}

var (
	_ module.Component       = (*Component)(nil)
	_ module.SettingsHandler = (*Component)(nil)
	_ module.ControlHandler  = (*Component)(nil)
)

func init() {
	registry.Register((&Component{}).Instance())
}
