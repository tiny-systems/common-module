package ask

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/pkg/state"
	"github.com/tiny-systems/module/pkg/utils"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName = "ask"
	RequestPort   = "request"
	OutPort       = "out"
	ErrorPort     = "error"

	// queueStateKey is the single State entry holding the FIFO of pending
	// questions. One key (not one per question) because the queue's ORDER is
	// itself state: a slice under one key persists it for free.
	queueStateKey = "queue"

	// qidField is injected into the published control schema and data as a
	// readonly string, so a submission carries WHICH question it answers.
	// The editor's form prunes keys that are not in the schema and round-trips
	// the ones that are, which is why it must be a declared (visible) field
	// rather than hidden data. A submission whose qid does not match the head
	// of the queue is stale — the form it came from is no longer the current
	// question — and is ignored.
	qidField = "_qid"
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
	// TimeoutSeconds bounds how long a question may wait for a human. Zero
	// keeps the original behavior: wait forever.
	TimeoutSeconds int `json:"timeoutSeconds" title:"Timeout Seconds" default:"0" description:"How long a question may stay unanswered before it expires onto the Error port (0 = wait forever). Expiry is passive — checked when messages arrive and on reconcile ticks — so an idle node holds an expired question until the next event."`
	// EnableErrorPort follows the standard pattern (see collect): without it
	// expired questions are dropped with only a log line.
	EnableErrorPort bool `json:"enableErrorPort" required:"true" title:"Enable Error Port" description:"Emit timed-out questions on the Error port as {context, error}. Without it expired questions are dropped silently (a warning is logged) — enable this when a timeout is set."`
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

// ErrorMessage reports a question nobody answered in time.
type ErrorMessage struct {
	Context Context `json:"context,omitempty" title:"Context" description:"The request payload of the expired question, unchanged."`
	Error   string  `json:"error" title:"Error"`
}

// pendingQuestion is one persisted entry of the FIFO. The form is snapshotted
// at ask time so a Settings change cannot silently swap the question under an
// outstanding request — the human answers the form the flow actually asked.
type pendingQuestion struct {
	ID      string          `json:"id"`
	Context Context         `json:"context"`
	Form    json.RawMessage `json:"form"`
	AskedAt time.Time       `json:"askedAt"`
}

// Component is a human-in-the-loop gate. Pending questions live in the node's
// State (same backing as kv/collect: TinyNode status metadata), so they
// survive pod restarts and are visible to every replica. The Control port can
// only render ONE form per node, so questions queue FIFO: the oldest is
// presented first and answering it (or its expiry) reveals the next.
type Component struct {
	module.Base

	mu       sync.Mutex
	settings Settings

	// stateMu serializes the read-modify-write of the queue within this
	// replica so a concurrent request and answer cannot lose updates.
	stateMu sync.Mutex

	// rehydrated flips on the first leader reconcile of this pod: a restart
	// must republish the outstanding question to the widget exactly once;
	// after that the form is only re-pushed when the queue actually changes,
	// so reconcile ticks do not re-render a form a human may be filling in.
	rehydrated bool

	// now is the clock; a seam so tests can drive timeout expiry without
	// sleeping. Nil-safe via clock().
	now func() time.Time
}

func (c *Component) Instance() module.Component {
	return &Component{
		settings: Settings{},
		now:      time.Now,
	}
}

func (c *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Ask a human",
		Info:        "Human-in-the-loop gate. A message on Request publishes a form on the Control port; the flow does NOT block. A human fills the form in the editor and submits, which emits {values, context} on Out — wire Out to the action being gated (e.g. workload_restart) and branch on the submitted values with a router. The form is a JSON Schema authored in settings; fields with format:\"button\" are the answers. Defaults to Approve/Deny. Concurrent requests queue FIFO: the Control port shows the OLDEST pending question first and answering it reveals the next. Pending questions persist in the node's State, so they survive pod restarts and are multi-replica safe. Questions older than timeoutSeconds (0 = wait forever) expire onto the Error port as {context, error}; expiry is passive — checked when messages arrive and on reconcile ticks — so an idle node holds an expired question until the next event. Use to put a person in front of anything destructive.",
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
	if in.TimeoutSeconds < 0 {
		in.TimeoutSeconds = 0
	}
	c.mu.Lock()
	c.settings = in
	c.mu.Unlock()
	return nil
}

// form returns the schema to ask with NOW: the authored one, or Approve/Deny.
// Questions already in the queue keep the snapshot taken when they were asked.
func (c *Component) form() json.RawMessage {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.settings.Form != "" {
		return json.RawMessage(c.settings.Form)
	}
	return json.RawMessage(defaultForm)
}

func (c *Component) timeout() (time.Duration, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return time.Duration(c.settings.TimeoutSeconds) * time.Second, c.settings.EnableErrorPort
}

func (c *Component) clock() time.Time {
	if c.now == nil {
		return time.Now()
	}
	return c.now()
}

// newQuestionID generates the queue key for one request. Request carries
// nothing usable as an identity (only the passthrough context), so one is
// minted here: time-ordered for humans reading state, random suffix for
// uniqueness within a nanosecond.
func newQuestionID(now time.Time) string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return fmt.Sprintf("q-%d-%x", now.UnixNano(), b)
}

// loadQueue reads the persisted FIFO. Nil-safe on a missing State backend
// (registration-time Ports() calls) — that just means "no pending questions".
func (c *Component) loadQueue(ctx context.Context) ([]pendingQuestion, error) {
	st := c.State()
	if st == nil {
		return nil, nil
	}
	raw, found, err := st.Get(ctx, queueStateKey)
	if err != nil {
		return nil, fmt.Errorf("state.Get: %v", err)
	}
	if !found {
		return nil, nil
	}
	var q []pendingQuestion
	if err := json.Unmarshal(raw, &q); err != nil {
		// An unreadable queue can never be answered; start over rather than
		// wedging the node.
		return nil, nil
	}
	return q, nil
}

// saveQueue persists the FIFO, deleting the key when the queue drains so an
// idle node carries no state at all.
func (c *Component) saveQueue(ctx context.Context, q []pendingQuestion) error {
	st := c.State()
	if st == nil {
		return fmt.Errorf("state backend not available")
	}
	if len(q) == 0 {
		return st.Delete(ctx, queueStateKey)
	}
	data, err := json.Marshal(q)
	if err != nil {
		return fmt.Errorf("failed to marshal question queue: %v", err)
	}
	if err := st.Set(ctx, queueStateKey, data); err != nil {
		if errors.Is(err, state.ErrStateTooLarge) {
			return fmt.Errorf("state budget exhausted: the node's total state is capped at ~%dKB and the pending-question queue no longer fits; answer or expire outstanding questions, or carry smaller request context (IDs, not blobs): %v",
				state.MaxStateBytes/1024, err)
		}
		return fmt.Errorf("state.Set: %v", err)
	}
	return nil
}

// sweepExpired splits the queue into kept and expired questions. Pure —
// callers persist and emit. A timeout of zero means wait forever.
func sweepExpired(q []pendingQuestion, now time.Time, timeout time.Duration) (kept []pendingQuestion, expired []ErrorMessage) {
	if timeout <= 0 {
		return q, nil
	}
	for _, p := range q {
		if now.Sub(p.AskedAt) > timeout {
			expired = append(expired, ErrorMessage{
				Context: p.Context,
				Error:   fmt.Sprintf("ask timeout after %ds", int(timeout/time.Second)),
			})
			continue
		}
		kept = append(kept, p)
	}
	return kept, expired
}

// emitExpired reports timed-out questions. Like collect: onto the Error port
// when enabled, otherwise a log line — a silent drop would be the exact
// failure mode this component exists to prevent.
func (c *Component) emitExpired(ctx context.Context, expired []ErrorMessage, errPort bool) {
	for _, e := range expired {
		if errPort {
			c.Emit(ctx, ErrorPort, e)
		} else {
			log.Warn().Str("component", ComponentName).
				Msg(e.Error + " (error port disabled; question dropped)")
		}
	}
}

// Handle opens the gate. It returns immediately: the run does not stay parked
// waiting for a human. Continuity lives in this node's persisted queue and
// published form, and the answer arrives later as a separate control message
// that starts the downstream hop itself.
func (c *Component) Handle(ctx context.Context, _ module.Handler, port string, msg any) module.Result {
	if port != RequestPort {
		return module.Fail(fmt.Errorf("port %s is not supported", port))
	}
	in, ok := msg.(Request)
	if !ok {
		return module.Fail(fmt.Errorf("invalid message"))
	}
	if c.State() == nil {
		return module.Fail(fmt.Errorf("state backend not available"))
	}
	timeout, errPort := c.timeout()
	now := c.clock()

	c.stateMu.Lock()
	q, err := c.loadQueue(ctx)
	if err != nil {
		c.stateMu.Unlock()
		return module.Fail(err)
	}
	// New traffic is the expiry heartbeat: questions past the deadline free
	// their queue slot before the new one joins.
	kept, expired := sweepExpired(q, now, timeout)
	kept = append(kept, pendingQuestion{
		ID:      newQuestionID(now),
		Context: in.Context,
		Form:    c.form(),
		AskedAt: now,
	})
	if err := c.saveQueue(ctx, kept); err != nil {
		c.stateMu.Unlock()
		return module.Fail(err)
	}
	c.stateMu.Unlock()

	c.emitExpired(ctx, expired, errPort)

	// Re-publish the control port so the widget picks up the head of the
	// queue — the new question when it is alone, an older one otherwise.
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
	timeout, errPort := c.timeout()
	now := c.clock()

	c.stateMu.Lock()
	q, err := c.loadQueue(ctx)
	if err != nil {
		c.stateMu.Unlock()
		return err
	}
	if len(q) == 0 {
		// No question outstanding — a re-render or a replayed submission.
		c.stateMu.Unlock()
		return nil
	}

	// A form is submitted whole, so the presence of values proves nothing; an
	// answer is a button press. And it must answer the question currently at
	// the head of the queue: the published form carries the head's ID in a
	// readonly field that rides back with the submission, so a replayed form
	// for an already-answered question cannot consume the NEXT one. An answer
	// beats the deadline race — a human answering the still-published head
	// just past its timeout is a success, not a timeout — so the head is
	// popped before the expiry sweep runs.
	var answered *pendingQuestion
	if hasPressedButton(values) && submissionMatches(values, q[0].ID) {
		head := q[0]
		answered = &head
		q = q[1:]
	}
	kept, expired := sweepExpired(q, now, timeout)
	if answered != nil || len(expired) > 0 {
		if err := c.saveQueue(ctx, kept); err != nil {
			c.stateMu.Unlock()
			return err
		}
	}
	c.stateMu.Unlock()

	c.emitExpired(ctx, expired, errPort)
	if answered == nil && len(expired) == 0 {
		return nil
	}

	if answered != nil {
		// The injected question ID is bookkeeping, not an answer field.
		out := make(map[string]interface{}, len(values))
		for k, v := range values {
			if k == qidField {
				continue
			}
			out[k] = v
		}
		// Detached, like signal: nothing here waits on the downstream action,
		// and the control message must not stay open for the length of the flow.
		go c.Emit(context.Background(), OutPort, Reply{
			Values:  out,
			Context: answered.Context,
		})
	}

	// Reveal the next question (or clear the form when the queue drained).
	c.Emit(context.Background(), v1alpha1.ControlPort, c.control())
	return nil
}

// OnReconcile is the rehydration hook and the idle expiry tick. The queue
// itself needs no restoring — State reads through the cluster cache — but
// after a pod restart the widget must be re-shown the outstanding question,
// and reconcile ticks are the only traffic an otherwise idle node ever sees.
func (c *Component) OnReconcile(ctx context.Context, _ v1alpha1.TinyNode) error {
	if !utils.IsLeader(ctx) {
		return nil
	}
	if c.State() == nil {
		return nil
	}
	timeout, errPort := c.timeout()
	now := c.clock()

	c.stateMu.Lock()
	q, err := c.loadQueue(ctx)
	if err != nil {
		c.stateMu.Unlock()
		return err
	}
	kept, expired := sweepExpired(q, now, timeout)
	if len(expired) > 0 {
		if err := c.saveQueue(ctx, kept); err != nil {
			c.stateMu.Unlock()
			return err
		}
	}
	// Republish when the queue changed, or once per pod lifetime when a
	// question was outstanding across a restart. Every other tick stays
	// silent so a form a human is filling in is not re-rendered under them.
	republish := len(expired) > 0 || (!c.rehydrated && len(kept) > 0)
	c.rehydrated = true
	c.stateMu.Unlock()

	c.emitExpired(ctx, expired, errPort)
	if republish {
		c.Emit(ctx, v1alpha1.ControlPort, c.control())
	}
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

// submissionMatches reports whether a submission answers the question with the
// given ID. A missing or empty qid is accepted — an older editor or a direct
// API call may not round-trip the injected field, and for them the head is the
// only question they can mean — but a PRESENT mismatching qid is proof the
// form was rendered for a question that is no longer current.
func submissionMatches(values map[string]interface{}, id string) bool {
	v, ok := values[qidField]
	if !ok {
		return true
	}
	s, ok := v.(string)
	if !ok || s == "" {
		return true
	}
	return s == id
}

// sampleValues builds an example submission from the authored form: one entry
// per form field, holding the zero value of its declared type. Without it the
// Out port would advertise a nil Values map, and an edge reading
// `$.values.approve` could not be validated when the flow is built — the form's
// field names are only knowable from the form itself.
func sampleValues(form json.RawMessage) map[string]interface{} {
	out := map[string]interface{}{}

	var parsed struct {
		Properties map[string]struct {
			Type string `json:"type"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(form, &parsed); err != nil {
		return out
	}
	for name, prop := range parsed.Properties {
		switch prop.Type {
		case "boolean":
			out[name] = false
		case "string":
			out[name] = ""
		case "number", "integer":
			out[name] = 0
		case "array":
			out[name] = []interface{}{}
		default:
			out[name] = map[string]interface{}{}
		}
	}
	return out
}

// head returns the current head of the queue and the queue length, nil-safe
// everywhere (missing State backend, empty queue). Ports() has no caller
// context, so callers read with Background like kv does.
func (c *Component) head(ctx context.Context) (*pendingQuestion, int) {
	q, err := c.loadQueue(ctx)
	if err != nil || len(q) == 0 {
		return nil, 0
	}
	return &q[0], len(q)
}

// control is the data half of the published form: the values the widget
// renders into the schema. Never nil — the runtime decodes an incoming
// control submission into reflect.TypeOf(port.Configuration), and a nil map
// has no type to reflect.
func (c *Component) control() map[string]interface{} {
	head, _ := c.head(context.Background())
	if head == nil {
		return map[string]interface{}{statusField: idleMessage}
	}
	return map[string]interface{}{
		"context": head.Context,
		qidField:  head.ID,
	}
}

// controlSchema is the schema half: the head question's form snapshot with the
// question-ID field injected, or the plain authored form when nothing is
// pending (so an idle node's port advertises exactly what will be asked).
func (c *Component) controlSchema() json.RawMessage {
	head, n := c.head(context.Background())
	if head == nil {
		// Idle: publish the authored form, but gated so its fields hide
		// themselves. The form's shape stays advertised on the port (useful
		// when authoring), while the widget shows only the notice — a person
		// reading the dashboard can tell a waiting decision from an idle gate,
		// which is the one thing this component exists to communicate.
		//
		// The gate is `requiredWhen` on each answer field, keyed to the
		// question-id field: the editor already treats a false condition as
		// "hidden", so this needs no rendering support of its own.
		return gateFormOnPendingQuestion(c.form())
	}
	base := head.Form
	if len(base) == 0 {
		base = c.form()
	}
	return injectQuestionMeta(base, n)
}

// injectQuestionMeta adds the readonly question-ID field to a form schema so
// submissions round-trip which question they answer, and a queue note when
// more questions wait behind the presented one. On any parse trouble the base
// form is published verbatim — a renderable form beats replay protection.
func injectQuestionMeta(base json.RawMessage, queueLen int) json.RawMessage {
	var m map[string]interface{}
	if err := json.Unmarshal(base, &m); err != nil {
		return base
	}
	props, _ := m["properties"].(map[string]interface{})
	if props == nil {
		props = map[string]interface{}{}
		m["properties"] = props
	}
	props[qidField] = map[string]interface{}{
		"type":          "string",
		"title":         "Question ID",
		"readonly":      true,
		"propertyOrder": 9999,
	}
	if queueLen > 1 {
		m["description"] = fmt.Sprintf("%d more question(s) waiting behind this one — answering reveals the next.", queueLen-1)
	}
	out, err := json.Marshal(m)
	if err != nil {
		return base
	}
	return out
}

func (c *Component) Ports() []module.Port {
	c.mu.Lock()
	settings := c.settings
	c.mu.Unlock()

	ports := []module.Port{
		{Name: v1alpha1.ReconcilePort},
		{
			Name:          v1alpha1.SettingsPort,
			Label:         "Settings",
			Configuration: settings,
		},
		{
			Name:          RequestPort,
			Label:         "Request",
			Position:      module.Left,
			Configuration: Request{},
		},
		{
			Name:     OutPort,
			Label:    "Out",
			Source:   true,
			Position: module.Right,
			// Advertise the form's own fields as the example submission so a
			// downstream edge reading `$.values.<field>` validates at build
			// time instead of resolving to null at runtime.
			Configuration: Reply{Values: sampleValues(c.form())},
		},
		{
			Name:     v1alpha1.ControlPort,
			Label:    "Control",
			Source:   true,
			Position: module.Top,
			// The data is a map so an untyped submission has something to
			// decode into; the schema is the asked form, published verbatim
			// because no Go type describes it. Both derive from the persisted
			// queue, so a freshly restarted pod advertises the outstanding
			// question without any in-memory handoff.
			Configuration: c.control(),
			Schema:        c.controlSchema(),
		},
	}

	if settings.EnableErrorPort {
		ports = append(ports, module.Port{
			Name:          ErrorPort,
			Label:         "Error",
			Source:        true,
			Position:      module.Bottom,
			Configuration: ErrorMessage{},
		})
	}

	return ports
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

// statusField carries the idle notice. Named so it cannot collide with an
// authored form field: those are the answers.
const statusField = "_status"

const idleMessage = "Nothing to approve right now."

// gateFormOnPendingQuestion returns the form with every field hidden until a
// question exists, plus the idle notice.
//
// Hiding is expressed as requiredWhen [qidField, "isUndefined"] inverted: a
// field is shown only when the question id is PRESENT. The editor evaluates
// the condition against sibling values and treats false as hidden, so an idle
// widget renders the notice alone. When a question arrives the form is
// published with its id set and the fields appear.
func gateFormOnPendingQuestion(base json.RawMessage) json.RawMessage {
	var doc map[string]interface{}
	if err := json.Unmarshal(base, &doc); err != nil {
		return base // an unrenderable gate is worse than an ungated form
	}
	props, ok := doc["properties"].(map[string]interface{})
	if !ok {
		return base
	}
	for name, raw := range props {
		field, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		// Shown only when the idle notice is absent. The notice exists
		// exactly while nothing is pending, so "no notice" is the same
		// statement as "a question is waiting" — expressed with the one
		// operator the vocabulary has for presence.
		field["requiredWhen"] = []interface{}{statusField, "isUndefined"}
		props[name] = field
	}
	props[statusField] = map[string]interface{}{
		"type":          "string",
		"title":         "",
		"readonly":      true,
		"format":        "markdown",
		"propertyOrder": 0,
		"requiredWhen":  []interface{}{qidField, "isUndefined"},
	}
	// The question id rides along so a submission says which question it
	// answers, but it is bookkeeping — hidden with the answers it belongs to,
	// rather than shown as an empty box on an idle dashboard.
	props[qidField] = map[string]interface{}{
		"type":         "string",
		"readonly":     true,
		"title":        "",
		"requiredWhen": []interface{}{statusField, "isUndefined"},
	}
	doc["properties"] = props
	out, err := json.Marshal(doc)
	if err != nil {
		return base
	}
	return out
}
