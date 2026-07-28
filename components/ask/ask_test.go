package ask

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/tiny-systems/module/pkg/utils"
)

// OnControl ignores non-leader replicas, so tests must present as leader.
func leaderCtx() context.Context {
	return utils.WithLeader(context.Background(), true)
}

func TestOnSettingsRejectsInvalidForm(t *testing.T) {
	c := &Component{pending: map[string]interface{}{}}
	if err := c.OnSettings(nil, Settings{Form: `{"type":`}); err == nil {
		t.Fatal("expected invalid JSON form to be rejected at settings time")
	}
	if err := c.OnSettings(nil, Settings{Form: `{"type":"object"}`}); err != nil {
		t.Fatalf("valid form rejected: %v", err)
	}
}

func TestFormFallsBackToApproveDeny(t *testing.T) {
	c := &Component{pending: map[string]interface{}{}}
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
	c := &Component{pending: map[string]interface{}{}}
	c.settings.Form = `{"type":"object","properties":{"go":{"type":"boolean","format":"button"}}}`

	for _, p := range c.Ports() {
		if p.Name != "_control" {
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

// A gate that is not open must ignore submissions, so a replayed or resubmitted
// form cannot fire a destructive action twice.
func TestClosedGateIgnoresSubmission(t *testing.T) {
	c := &Component{pending: map[string]interface{}{}, open: false}
	if err := c.OnControl(leaderCtx(), map[string]interface{}{"approve": true}); err != nil {
		t.Fatalf("closed gate returned an error instead of ignoring: %v", err)
	}
	// Still closed.
	if c.open {
		t.Error("closed gate became open on a stray submission")
	}
}
