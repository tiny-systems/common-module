// Package prompt is the dashboard's request→response widget: a person fills a
// form, submits, sees a working state, and the answer returns to the SAME
// widget. It is the human-initiated mirror of `ask` (which is system asks
// human); here a human asks the flow and waits for it to answer.
//
// The round trip is a loop the author wires: the widget emits the submission
// on Out, the flow processes it, and the result is wired back into the
// widget's In port. A request id rides the whole way so a returning answer is
// matched to the submission that asked for it — a late answer to a
// superseded question is ignored rather than shown against the wrong prompt.
//
// The three states a person sees, all in one tile:
//   - idle: the form and a Submit button (plus the previous answer, if any)
//   - working: the form hidden, a "working…" line — the feedback whose
//     absence made a bare signal widget feel dead
//   - answered: the answer rendered as markdown, the form ready to ask again
package prompt

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/pkg/utils"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName = "prompt"
	OutPort       = "out"
	InPort        = "in"

	// sessionKey holds the one in-flight request plus the last answer. One
	// prompt handles one question at a time, so this is a single object, not
	// a queue.
	sessionKey = "session"

	// ridField round-trips the request id through the flow so a returning
	// answer proves which submission it belongs to. Declared (not hidden)
	// because the editor prunes form keys absent from the schema; hidden from
	// the person by a gate, since it is bookkeeping.
	ridField = "_rid"
	// statusField carries the "working…" line while a request is outstanding.
	// Its PRESENCE is the discriminator the form fields gate on.
	statusField = "_status"
	// answerField shows the last response, rendered as markdown.
	answerField = "_answer"
	// submitField is the button that fires a submission.
	submitField = "submit"

	workingMessage = "Working on it…"
)

// defaultForm is used when the author configures none: a single question box.
// The submit button, status and answer fields are injected around it.
const defaultForm = `{
  "type": "object",
  "properties": {
    "question": {"type": "string", "title": "Question", "format": "textarea", "propertyOrder": 1}
  }
}`

// session is the persisted state: the outstanding request (if any) and the
// last answer shown.
type session struct {
	RequestID string          `json:"requestId,omitempty"`
	Pending   bool            `json:"pending"`
	Values    json.RawMessage `json:"values,omitempty"` // the submitted form, echoed back on Out
	Answer    string          `json:"answer,omitempty"`
}

// Out is what a submission emits: the form values, the request id to carry
// back, and a passthrough context.
type Out struct {
	RequestID string          `json:"requestId" title:"Request ID" description:"Carry this back unchanged on the answer so the widget matches it."`
	Values    json.RawMessage `json:"values" title:"Values" description:"The submitted form."`
	Context   any             `json:"context,omitempty" configurable:"true" title:"Context"`
}

// In is the answer coming back: the request id it answers and the text to
// show. Anything not matching the outstanding request is ignored.
type In struct {
	RequestID string `json:"requestId" title:"Request ID" description:"The id from Out, unchanged."`
	Answer    string `json:"answer" title:"Answer" description:"Text to show in the widget. Markdown is rendered."`
	Context   any    `json:"context,omitempty" configurable:"true" title:"Context"`
}

type Settings struct {
	Form string `json:"form,omitempty" title:"Form" format:"code" language:"json" description:"JSON Schema of the input form. Leave empty for a single Question box. A Submit button, a working indicator and the answer panel are added automatically."`
}

type Component struct {
	module.Base
	settings Settings
}

func (c *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Prompt",
		Info: "Dashboard request→response widget. A person fills a form, submits, sees a working state, then the answer appears in the SAME widget. " +
			"Wire Out into the flow and wire the flow's result back into In, carrying requestId unchanged so the answer matches the question. " +
			"Enable it as a dashboard widget. The human-initiated mirror of `ask`.",
		Tags: []string{"SDK", "dashboard"},
	}
}

func (c *Component) OnSettings(_ context.Context, msg any) error {
	in, ok := msg.(Settings)
	if !ok {
		return fmt.Errorf("invalid settings")
	}
	c.settings = in
	return nil
}

func (c *Component) form() json.RawMessage {
	if c.settings.Form != "" {
		return json.RawMessage(c.settings.Form)
	}
	return json.RawMessage(defaultForm)
}

func (c *Component) load(ctx context.Context) session {
	st := c.State()
	if st == nil {
		return session{}
	}
	raw, found, err := st.Get(ctx, sessionKey)
	if err != nil || !found {
		return session{}
	}
	var s session
	if json.Unmarshal(raw, &s) != nil {
		return session{}
	}
	return s
}

func (c *Component) save(ctx context.Context, s session) error {
	st := c.State()
	if st == nil {
		return fmt.Errorf("state backend not available")
	}
	// Nothing outstanding and nothing to show → carry no state.
	if !s.Pending && s.Answer == "" {
		return st.Delete(ctx, sessionKey)
	}
	data, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return st.Set(ctx, sessionKey, data)
}

// Handle receives the answer coming back on In. It also handles the Out/In
// ports the runtime dispatches here; the submission itself arrives via
// OnControl (the widget), not Handle.
func (c *Component) Handle(ctx context.Context, _ module.Handler, port string, msg any) module.Result {
	if port != InPort {
		return module.Fail(fmt.Errorf("unknown port: %s", port))
	}
	if !utils.IsLeader(ctx) {
		return module.Result{}
	}
	in, ok := msg.(In)
	if !ok {
		return module.Fail(fmt.Errorf("invalid message on In"))
	}

	s := c.load(ctx)
	// Only the outstanding request may be answered; a late reply to a
	// superseded question is dropped rather than shown against the wrong one.
	if !s.Pending || (in.RequestID != "" && in.RequestID != s.RequestID) {
		return module.Result{}
	}
	// Stored as given. Redaction is by field NAME and this field is "answer";
	// a flow that pipes a secret into a person-facing panel is the thing to
	// fix, not this. Publishing redacts credential-shaped fields on the way out.
	s = session{Pending: false, Answer: in.Answer}
	if err := c.save(ctx, s); err != nil {
		return module.Fail(err)
	}
	return c.Emit(ctx, v1alpha1.ControlPort, c.control(s))
}

// OnControl receives a submission from the widget: the filled form with the
// submit button true. It emits the values on Out and flips the widget to
// working.
func (c *Component) OnControl(ctx context.Context, msg any) error {
	if !utils.IsLeader(ctx) {
		return nil
	}
	form, ok := msg.(map[string]interface{})
	if !ok {
		return fmt.Errorf("invalid control submission")
	}
	// Fire only on an actual submit press; ignore incidental republishes.
	if b, _ := form[submitField].(bool); !b {
		return nil
	}

	// Everything the person entered, minus the control machinery, is the
	// submission.
	values := map[string]interface{}{}
	for k, v := range form {
		switch k {
		case submitField, ridField, statusField, answerField:
			continue
		}
		values[k] = v
	}
	valuesRaw, _ := json.Marshal(values)

	rid, err := newRequestID()
	if err != nil {
		return err
	}
	s := session{RequestID: rid, Pending: true, Values: valuesRaw}
	if err := c.save(ctx, s); err != nil {
		return err
	}

	// Emit the submission for the flow, and flip the widget to working.
	c.Emit(context.Background(), OutPort, Out{RequestID: rid, Values: valuesRaw})
	c.Emit(context.Background(), v1alpha1.ControlPort, c.control(s))
	return nil
}

// OnReconcile rehydrates the widget after a restart: re-publish whatever the
// persisted session says, so a page reload shows the working state or the
// last answer rather than a blank form.
func (c *Component) OnReconcile(ctx context.Context, _ v1alpha1.TinyNode) error {
	if !utils.IsLeader(ctx) {
		return nil
	}
	if c.State() == nil {
		return nil
	}
	s := c.load(ctx)
	c.Emit(ctx, v1alpha1.ControlPort, c.control(s))
	return nil
}

// control is the data half of the widget for the current session.
func (c *Component) control(s session) map[string]interface{} {
	out := map[string]interface{}{}
	if s.Pending {
		out[statusField] = workingMessage
		out[ridField] = s.RequestID
	}
	if s.Answer != "" {
		out[answerField] = s.Answer
	}
	return out
}

// controlSchema is the widget's form. The authored input fields and the submit
// button are shown only while idle (gated on the working line being absent);
// the working line shows while pending; the answer shows whenever there is
// one.
func (c *Component) controlSchema(s session) json.RawMessage {
	var doc map[string]interface{}
	if json.Unmarshal(c.form(), &doc) != nil {
		doc = map[string]interface{}{"type": "object", "properties": map[string]interface{}{}}
	}
	props, ok := doc["properties"].(map[string]interface{})
	if !ok {
		props = map[string]interface{}{}
	}

	// Input fields and Submit are hidden while a request is outstanding — the
	// person cannot fire a second one over an in-flight one.
	gateIdle := []interface{}{statusField, "isUndefined"}
	for name, raw := range props {
		if field, ok := raw.(map[string]interface{}); ok {
			field["requiredWhen"] = gateIdle
			props[name] = field
		}
	}
	props[submitField] = map[string]interface{}{
		"type": "boolean", "title": "Submit", "format": "button",
		"propertyOrder": 100, "requiredWhen": gateIdle,
	}
	props[statusField] = map[string]interface{}{
		"type": "string", "title": "", "readonly": true, "format": "markdown",
		"propertyOrder": 0,
	}
	props[answerField] = map[string]interface{}{
		"type": "string", "title": "Answer", "readonly": true, "format": "markdown",
		"propertyOrder": 200,
	}
	props[ridField] = map[string]interface{}{
		"type": "string", "readonly": true, "title": "",
		"requiredWhen": []interface{}{statusField, "isUndefined"},
	}
	doc["properties"] = props
	out, err := json.Marshal(doc)
	if err != nil {
		return c.form()
	}
	return out
}

func (c *Component) Ports() []module.Port {
	s := c.load(context.Background())
	return []module.Port{
		{
			Name:          InPort,
			Label:         "In",
			Configuration: In{},
			Position:      module.Left,
		},
		{
			Name:          OutPort,
			Label:         "Out",
			Source:        true,
			Position:      module.Right,
			Configuration: Out{},
		},
		{
			Name:          v1alpha1.ControlPort,
			Label:         "Control",
			Source:        true,
			Position:      module.Top,
			Configuration: c.control(s),
			Schema:        c.controlSchema(s),
		},
		{
			Name:          v1alpha1.SettingsPort,
			Label:         "Settings",
			Configuration: c.settings,
		},
	}
}

func (c *Component) Instance() module.Component { return &Component{} }

func newRequestID() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

var (
	_ module.Component        = (*Component)(nil)
	_ module.SettingsHandler  = (*Component)(nil)
	_ module.ControlHandler   = (*Component)(nil)
	_ module.ReconcileHandler = (*Component)(nil)
)

func init() {
	registry.Register((&Component{}).Instance())
}
