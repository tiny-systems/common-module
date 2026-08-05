package ask

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/tiny-systems/common-module/internal/testharness"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/pkg/utils"
)

// OnControl ignores non-leader replicas, so tests must present as leader.
func leaderCtx() context.Context {
	return utils.WithLeader(context.Background(), true)
}

// newAsk returns a harness plus the concrete component so tests can drive the
// clock seam and read the presented question without sleeping.
func newAsk(t *testing.T, settings *Settings) (*testharness.Harness, *Component) {
	t.Helper()
	comp, ok := (&Component{}).Instance().(*Component)
	if !ok {
		t.Fatal("Instance() did not return *Component")
	}
	h := testharness.New(comp)
	if settings != nil {
		if r := h.Handle(context.Background(), v1alpha1.SettingsPort, *settings); r.Err() != nil {
			t.Fatalf("settings failed: %v", r.Err())
		}
	}
	return h, comp
}

func ask(t *testing.T, h *testharness.Harness, ctx any) {
	t.Helper()
	if r := h.Handle(context.Background(), RequestPort, Request{Context: ctx}); r.Err() != nil {
		t.Fatalf("request failed: %v", r.Err())
	}
}

// answer submits the currently presented form: a pressed button plus the
// question ID the widget would round-trip from the published data.
func answer(t *testing.T, h *testharness.Harness, comp *Component, extra map[string]interface{}) {
	t.Helper()
	values := map[string]interface{}{"approve": true}
	if qid, ok := comp.control()[qidField]; ok {
		values[qidField] = qid
	}
	for k, v := range extra {
		values[k] = v
	}
	if r := h.Handle(leaderCtx(), v1alpha1.ControlPort, values); r.Err() != nil {
		t.Fatalf("control submission failed: %v", r.Err())
	}
}

// waitOutputs polls for n outputs on a port — the Out emission is detached
// (goroutine), like signal's.
func waitOutputs(t *testing.T, h *testharness.Harness, port string, n int) []any {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		outs := h.PortOutputs(port)
		if len(outs) >= n {
			return outs
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d outputs on %q, have %d", n, port, len(outs))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// settleOutputs waits long enough for a stray detached emission to land, then
// returns what arrived — for asserting something did NOT emit.
func settleOutputs(h *testharness.Harness, port string) []any {
	time.Sleep(50 * time.Millisecond)
	return h.PortOutputs(port)
}

// stateKeys counts leftover _state/ entries in harness metadata — a drained
// queue must not leak state.
func stateKeys(h *testharness.Harness) int {
	n := 0
	for k := range h.Metadata {
		if strings.HasPrefix(k, "_state/") {
			n++
		}
	}
	return n
}

func TestOnSettingsRejectsInvalidForm(t *testing.T) {
	c := &Component{}
	if err := c.OnSettings(nil, Settings{Form: `{"type":`}); err == nil {
		t.Fatal("expected invalid JSON form to be rejected at settings time")
	}
	if err := c.OnSettings(nil, Settings{Form: `{"type":"object"}`}); err != nil {
		t.Fatalf("valid form rejected: %v", err)
	}
	// A negative timeout is meaningless; it must clamp to "wait forever".
	if err := c.OnSettings(nil, Settings{TimeoutSeconds: -5}); err != nil {
		t.Fatalf("negative timeout rejected instead of clamped: %v", err)
	}
	if c.settings.TimeoutSeconds != 0 {
		t.Errorf("negative timeout not clamped to 0, got %d", c.settings.TimeoutSeconds)
	}
}

func TestFormFallsBackToApproveDeny(t *testing.T) {
	c := &Component{}
	raw := c.form()
	if !json.Valid(raw) {
		t.Fatalf("default form is not valid JSON: %s", raw)
	}
	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	props, ok := m["properties"].(map[string]interface{})
	if !ok {
		t.Fatal("default form has no properties")
	}
	for _, want := range []string{"approve", "deny", "context"} {
		if _, ok := props[want]; !ok {
			t.Errorf("default form missing %q", want)
		}
	}

	// An authored form must win over the default.
	c.settings.Form = `{"type":"object","properties":{"replicas":{"type":"number"}}}`
	if string(c.form()) != c.settings.Form {
		t.Error("authored form did not override the default")
	}
}

func TestControlPortPublishesFormAsSchema(t *testing.T) {
	// No pending question: the port must advertise the authored form verbatim
	// (no injected fields) so an idle node shows exactly what will be asked.
	c := &Component{}
	c.settings.Form = `{"type":"object","properties":{"go":{"type":"boolean","format":"button"}}}`

	for _, p := range c.Ports() {
		if p.Name != v1alpha1.ControlPort {
			continue
		}
		if string(p.Schema) != c.settings.Form {
			t.Errorf("control schema = %s, want the authored form", p.Schema)
		}
		// The data half must be a non-nil map: the runtime decodes an incoming
		// submission into reflect.TypeOf(Configuration), and a nil map has no
		// type to reflect.
		if _, ok := p.Configuration.(map[string]interface{}); !ok {
			t.Errorf("control configuration is %T, want map[string]interface{}", p.Configuration)
		}
		return
	}
	t.Fatal("no _control port published")
}

func TestHasPressedButton(t *testing.T) {
	cases := []struct {
		name   string
		values map[string]interface{}
		want   bool
	}{
		{"approve pressed", map[string]interface{}{"approve": true, "deny": false}, true},
		{"deny pressed", map[string]interface{}{"approve": false, "deny": true}, true},
		{"nothing pressed", map[string]interface{}{"approve": false, "deny": false}, false},
		{"only context, no answer", map[string]interface{}{"context": map[string]interface{}{"pod": "x"}}, false},
		{"empty", map[string]interface{}{}, false},
		// A non-bool truthy value is not an answer: only buttons count.
		{"string value is not a button", map[string]interface{}{"note": "yes"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := hasPressedButton(tc.values); got != tc.want {
				t.Errorf("hasPressedButton(%v) = %v, want %v", tc.values, got, tc.want)
			}
		})
	}
}

// A gate with no pending question must ignore submissions, so a replayed or
// resubmitted form cannot fire a destructive action twice.
func TestClosedGateIgnoresSubmission(t *testing.T) {
	h, _ := newAsk(t, nil)
	if r := h.Handle(leaderCtx(), v1alpha1.ControlPort, map[string]interface{}{"approve": true}); r.Err() != nil {
		t.Fatalf("closed gate returned an error instead of ignoring: %v", r.Err())
	}
	if outs := settleOutputs(h, OutPort); len(outs) != 0 {
		t.Errorf("closed gate emitted %d replies on a stray submission", len(outs))
	}
}

// The Out port must advertise the form's field names, otherwise an edge that
// reads a submitted value cannot be validated when the flow is built.
func TestOutSampleMirrorsTheForm(t *testing.T) {
	c := &Component{}
	c.settings.Form = `{"type":"object","properties":{
		"approve":{"type":"boolean"},
		"note":{"type":"string"},
		"replicas":{"type":"number"}}}`

	for _, p := range c.Ports() {
		if p.Name != OutPort {
			continue
		}
		reply, ok := p.Configuration.(Reply)
		if !ok {
			t.Fatalf("out configuration is %T, want Reply", p.Configuration)
		}
		for field, want := range map[string]interface{}{
			"approve": false, "note": "", "replicas": 0,
		} {
			got, present := reply.Values[field]
			if !present {
				t.Errorf("out sample missing form field %q", field)
				continue
			}
			if got != want {
				t.Errorf("out sample %q = %v (%T), want %v (%T)", field, got, got, want, want)
			}
		}
		return
	}
	t.Fatal("no out port published")
}

// Two concurrent questions must BOTH be answerable: FIFO — the oldest is
// presented first, answering it reveals the next, and each answer carries its
// own question's context.
func TestTwoConcurrentQuestionsBothAnswerable(t *testing.T) {
	h, comp := newAsk(t, nil)

	ask(t, h, "first")
	ask(t, h, "second")

	// The presented question is the OLDEST; the schema must say more wait.
	ctl := comp.control()
	if ctl["context"] != "first" {
		t.Fatalf("head question context = %v, want %q", ctl["context"], "first")
	}
	schema := string(comp.controlSchema())
	if !strings.Contains(schema, qidField) {
		t.Errorf("published schema must carry the question-ID field: %s", schema)
	}
	if !strings.Contains(schema, "1 more question") {
		t.Errorf("published schema should note the queued question: %s", schema)
	}

	answer(t, h, comp, map[string]interface{}{"note": "ok"})
	outs := waitOutputs(t, h, OutPort, 1)
	first := outs[0].(Reply)
	if first.Context != "first" {
		t.Errorf("first answer context = %v, want %q", first.Context, "first")
	}
	if first.Values["approve"] != true || first.Values["note"] != "ok" {
		t.Errorf("first answer values: %+v", first.Values)
	}
	if _, leaked := first.Values[qidField]; leaked {
		t.Errorf("internal question ID leaked into Out values: %+v", first.Values)
	}

	// Answering revealed the second question.
	if ctl := comp.control(); ctl["context"] != "second" {
		t.Fatalf("after first answer, head context = %v, want %q", ctl["context"], "second")
	}
	answer(t, h, comp, nil)
	outs = waitOutputs(t, h, OutPort, 2)
	second := outs[1].(Reply)
	if second.Context != "second" {
		t.Errorf("second answer context = %v, want %q", second.Context, "second")
	}

	// Both answered: the queue must drain and leave no state behind.
	if n := stateKeys(h); n != 0 {
		t.Errorf("drained queue must not leak state, %d keys left: %+v", n, h.Metadata)
	}
	if ctl := comp.control(); len(ctl) != 0 {
		t.Errorf("drained queue should publish an empty control map, got %+v", ctl)
	}
}

// A replayed submission for an already-answered question must not consume the
// next queued question: its round-tripped question ID no longer matches.
func TestStaleSubmissionCannotConsumeNextQuestion(t *testing.T) {
	h, comp := newAsk(t, nil)

	ask(t, h, "a")
	ask(t, h, "b")

	staleQid := comp.control()[qidField]
	answer(t, h, comp, nil)
	waitOutputs(t, h, OutPort, 1)

	// Replay the exact submission that answered "a".
	replay := map[string]interface{}{"approve": true, qidField: staleQid}
	if r := h.Handle(leaderCtx(), v1alpha1.ControlPort, replay); r.Err() != nil {
		t.Fatalf("replay errored instead of being ignored: %v", r.Err())
	}
	if outs := settleOutputs(h, OutPort); len(outs) != 1 {
		t.Fatalf("replay consumed a queued question: %d replies", len(outs))
	}
	// "b" is still pending and still answerable.
	if ctl := comp.control(); ctl["context"] != "b" {
		t.Fatalf("queued question lost after replay, head = %v", ctl["context"])
	}
	answer(t, h, comp, nil)
	outs := waitOutputs(t, h, OutPort, 2)
	if outs[1].(Reply).Context != "b" {
		t.Errorf("second answer context = %v, want %q", outs[1].(Reply).Context, "b")
	}
}

// A pod restart must keep the outstanding question: the queue lives in node
// State, and the first leader reconcile republishes the Control form.
func TestPodRestartKeepsPendingQuestion(t *testing.T) {
	pod1, _ := newAsk(t, nil)
	ask(t, pod1, "survives")

	pod2 := pod1.NewPod()
	if r := pod2.ReconcileAsLeader(context.Background()); r.Err() != nil {
		t.Fatalf("reconcile on new pod failed: %v", r.Err())
	}

	// Rehydration republished the form with the persisted question.
	ctls := pod2.PortOutputs(v1alpha1.ControlPort)
	if len(ctls) != 1 {
		t.Fatalf("expected 1 control republish after restart, got %d", len(ctls))
	}
	ctl := ctls[0].(map[string]interface{})
	if ctl["context"] != "survives" {
		t.Errorf("rehydrated form context = %v, want %q", ctl["context"], "survives")
	}
	qid, _ := ctl[qidField].(string)
	if qid == "" {
		t.Fatalf("rehydrated form carries no question ID: %+v", ctl)
	}

	// Republish happens once, not on every tick — a later reconcile with an
	// unchanged queue must stay silent.
	if r := pod2.ReconcileAsLeader(context.Background()); r.Err() != nil {
		t.Fatalf("second reconcile failed: %v", r.Err())
	}
	if n := len(pod2.PortOutputs(v1alpha1.ControlPort)); n != 1 {
		t.Errorf("unchanged queue re-published the form on a reconcile tick: %d emits", n)
	}

	// And the question is answerable on the new pod.
	submit := map[string]interface{}{"approve": true, qidField: qid}
	if r := pod2.Handle(leaderCtx(), v1alpha1.ControlPort, submit); r.Err() != nil {
		t.Fatalf("answer after restart failed: %v", r.Err())
	}
	outs := waitOutputs(t, pod2, OutPort, 1)
	if outs[0].(Reply).Context != "survives" {
		t.Errorf("answer after restart context = %v, want %q", outs[0].(Reply).Context, "survives")
	}
	if n := stateKeys(pod2); n != 0 {
		t.Errorf("answered question must not leak state, %d keys left", n)
	}
}

// Questions past the deadline expire when new traffic arrives.
func TestTimeoutExpiryOnTraffic(t *testing.T) {
	h, comp := newAsk(t, &Settings{TimeoutSeconds: 10, EnableErrorPort: true})

	base := time.Now()
	comp.now = func() time.Time { return base }
	ask(t, h, "too-slow")

	comp.now = func() time.Time { return base.Add(11 * time.Second) }
	ask(t, h, "fresh")

	errs := h.PortOutputs(ErrorPort)
	if len(errs) != 1 {
		t.Fatalf("expected 1 timeout error, got %d: %+v", len(errs), errs)
	}
	e := errs[0].(ErrorMessage)
	if e.Context != "too-slow" {
		t.Errorf("timeout context = %v, want %q", e.Context, "too-slow")
	}
	if e.Error != "ask timeout after 10s" {
		t.Errorf("timeout text: %q", e.Error)
	}
	// The fresh question took the expired one's place at the head.
	if ctl := comp.control(); ctl["context"] != "fresh" {
		t.Errorf("head after expiry = %v, want %q", ctl["context"], "fresh")
	}
}

// Reconcile ticks are the expiry heartbeat for an idle node: no flow traffic
// is needed for a question to time out.
func TestTimeoutExpiryOnReconcile(t *testing.T) {
	h, comp := newAsk(t, &Settings{TimeoutSeconds: 10, EnableErrorPort: true})

	base := time.Now()
	comp.now = func() time.Time { return base }
	ask(t, h, "abandoned")
	h.Reset()

	comp.now = func() time.Time { return base.Add(11 * time.Second) }
	if r := h.ReconcileAsLeader(context.Background()); r.Err() != nil {
		t.Fatalf("reconcile failed: %v", r.Err())
	}

	errs := h.PortOutputs(ErrorPort)
	if len(errs) != 1 {
		t.Fatalf("expected 1 timeout error on reconcile, got %d", len(errs))
	}
	if e := errs[0].(ErrorMessage); e.Context != "abandoned" || e.Error != "ask timeout after 10s" {
		t.Errorf("timeout error: %+v", e)
	}
	// The drained queue cleared the widget and left no state behind.
	ctls := h.PortOutputs(v1alpha1.ControlPort)
	if len(ctls) != 1 {
		t.Fatalf("expected the cleared form to be republished, got %d control emits", len(ctls))
	}
	if m := ctls[0].(map[string]interface{}); len(m) != 0 {
		t.Errorf("cleared form should be empty, got %+v", m)
	}
	if n := stateKeys(h); n != 0 {
		t.Errorf("expired question must be deleted from state, %d keys left", n)
	}
}

// Timeout zero is the default and means wait forever — the original contract.
func TestZeroTimeoutWaitsForever(t *testing.T) {
	h, comp := newAsk(t, &Settings{EnableErrorPort: true})

	base := time.Now()
	comp.now = func() time.Time { return base }
	ask(t, h, "patient")

	comp.now = func() time.Time { return base.Add(1000 * time.Hour) }
	if r := h.ReconcileAsLeader(context.Background()); r.Err() != nil {
		t.Fatalf("reconcile failed: %v", r.Err())
	}
	if errs := h.PortOutputs(ErrorPort); len(errs) != 0 {
		t.Fatalf("zero timeout must never expire, got %+v", errs)
	}
	if ctl := comp.control(); ctl["context"] != "patient" {
		t.Errorf("question lost without a timeout configured: %+v", ctl)
	}
}

// A human answering the still-published head just past its deadline wins:
// late success beats failure, like collect's straggler rule.
func TestLateAnswerBeatsDeadline(t *testing.T) {
	h, comp := newAsk(t, &Settings{TimeoutSeconds: 10, EnableErrorPort: true})

	base := time.Now()
	comp.now = func() time.Time { return base }
	ask(t, h, "slow-human")

	comp.now = func() time.Time { return base.Add(11 * time.Second) }
	answer(t, h, comp, nil)

	outs := waitOutputs(t, h, OutPort, 1)
	if outs[0].(Reply).Context != "slow-human" {
		t.Errorf("late answer context = %v", outs[0].(Reply).Context)
	}
	if errs := settleOutputs(h, ErrorPort); len(errs) != 0 {
		t.Errorf("an answered question must not also time out: %+v", errs)
	}
}

// Without the error port an expired question is dropped with only a log line,
// mirroring collect's contract.
func TestTimeoutWithoutErrorPortDropsSilently(t *testing.T) {
	h, comp := newAsk(t, &Settings{TimeoutSeconds: 10})

	base := time.Now()
	comp.now = func() time.Time { return base }
	ask(t, h, "dropped")

	comp.now = func() time.Time { return base.Add(11 * time.Second) }
	if r := h.ReconcileAsLeader(context.Background()); r.Err() != nil {
		t.Fatalf("reconcile failed: %v", r.Err())
	}
	if errs := h.PortOutputs(ErrorPort); len(errs) != 0 {
		t.Fatalf("error port disabled but errors emitted: %+v", errs)
	}
	if n := stateKeys(h); n != 0 {
		t.Errorf("expired question must still be removed, %d keys left", n)
	}
}

// The node's total state is capped (~900KB); a request whose context does not
// fit must fail loudly rather than silently truncate the queue.
func TestStateTooLargeMessage(t *testing.T) {
	h, _ := newAsk(t, nil)
	big := strings.Repeat("x", 900*1024)
	r := h.Handle(context.Background(), RequestPort, Request{Context: big})
	if r.Err() == nil {
		t.Fatal("expected oversized request context to fail")
	}
	if !strings.Contains(r.Err().Error(), "state budget exhausted") {
		t.Errorf("error should name the state budget, got: %v", r.Err())
	}
	if !strings.Contains(r.Err().Error(), "900KB") {
		t.Errorf("error should name the ~900KB cap, got: %v", r.Err())
	}
}

func TestErrorPortHiddenByDefault(t *testing.T) {
	h, _ := newAsk(t, nil)
	for _, p := range h.Ports() {
		if p.Name == ErrorPort {
			t.Fatal("error port should not be visible unless enabled")
		}
	}

	h2, _ := newAsk(t, &Settings{EnableErrorPort: true})
	for _, p := range h2.Ports() {
		if p.Name == ErrorPort {
			return
		}
	}
	t.Fatal("error port missing despite EnableErrorPort")
}
