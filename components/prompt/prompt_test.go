package prompt

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

func leaderCtx() context.Context { return utils.WithLeader(context.Background(), true) }

func newPrompt(t *testing.T) (*testharness.Harness, *Component) {
	t.Helper()
	comp, ok := (&Component{}).Instance().(*Component)
	if !ok {
		t.Fatal("Instance() did not return *Component")
	}
	return testharness.New(comp), comp
}

func waitOut(t *testing.T, h *testharness.Harness, n int) []any {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		outs := h.PortOutputs(OutPort)
		if len(outs) >= n {
			return outs
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d Out emissions, have %d", n, len(outs))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// The whole point: submit → working → answer, all keyed to one request id,
// and a late answer to a superseded request is ignored.
func TestPromptRoundTrip(t *testing.T) {
	h, comp := newPrompt(t)

	// Idle: form shown, no working line, no answer.
	if ctl := comp.control(comp.load(leaderCtx())); ctl[statusField] != nil || ctl[answerField] != nil {
		t.Fatalf("idle control should be empty, got %+v", ctl)
	}

	// Submit a question.
	if r := h.Handle(leaderCtx(), v1alpha1.ControlPort, map[string]interface{}{
		submitField: true,
		"question":  "is anything unhealthy?",
	}); r.Err() != nil {
		t.Fatalf("submit failed: %v", r.Err())
	}

	// It emitted the submission on Out, with a request id and the values.
	outs := waitOut(t, h, 1)
	sub := outs[0].(Out)
	if sub.RequestID == "" {
		t.Fatal("Out carried no request id to correlate the answer")
	}
	var vals map[string]interface{}
	_ = json.Unmarshal(sub.Values, &vals)
	if vals["question"] != "is anything unhealthy?" {
		t.Errorf("submitted values = %+v, want the question", vals)
	}
	if _, leaked := vals[submitField]; leaked {
		t.Error("the submit button leaked into the emitted values")
	}

	// Widget is now WORKING: the status line is present, which is the
	// feedback a bare signal widget never gave.
	s := comp.load(leaderCtx())
	if !s.Pending {
		t.Fatal("session is not pending after submit")
	}
	if comp.control(s)[statusField] != workingMessage {
		t.Errorf("working control has no status line: %+v", comp.control(s))
	}
	// And the form fields gate off while working.
	sch := string(comp.controlSchema(s))
	if !strings.Contains(sch, `"requiredWhen":["_status","isUndefined"]`) {
		t.Errorf("form fields are not gated on idle: %s", sch)
	}

	// A stale answer (wrong id) is ignored.
	h.Handle(leaderCtx(), InPort, In{RequestID: "not-the-one", Answer: "stale"})
	if a := comp.load(leaderCtx()).Answer; a != "" {
		t.Errorf("a mismatched answer was accepted: %q", a)
	}

	// The real answer lands and is shown; pending clears.
	if r := h.Handle(leaderCtx(), InPort, In{RequestID: sub.RequestID, Answer: "**All healthy.**"}); r.Err() != nil {
		t.Fatalf("answer failed: %v", r.Err())
	}
	s = comp.load(leaderCtx())
	if s.Pending {
		t.Error("still pending after the answer arrived")
	}
	if comp.control(s)[answerField] != "**All healthy.**" {
		t.Errorf("answer not shown in widget: %+v", comp.control(s))
	}
	if comp.control(s)[statusField] != nil {
		t.Error("working line still present after the answer")
	}
}

// The answer is stored exactly as the flow sent it — the panel shows what it
// was told to show.
func TestPromptStoresAnswerVerbatim(t *testing.T) {
	h, comp := newPrompt(t)
	h.Handle(leaderCtx(), v1alpha1.ControlPort, map[string]interface{}{submitField: true, "question": "q"})
	rid := waitOut(t, h, 1)[0].(Out).RequestID
	h.Handle(leaderCtx(), InPort, In{RequestID: rid, Answer: "All 8 pods healthy"})
	if a := comp.load(leaderCtx()).Answer; a != "All 8 pods healthy" {
		t.Errorf("answer altered: %q", a)
	}
}
