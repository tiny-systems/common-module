package display

import (
	"context"
	"encoding/json"
	"github.com/tiny-systems/common-module/internal/testharness"
	"strings"
	"testing"

	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/pkg/schema"
)

// The control port must tell the editor to RENDER the text. Without the
// format the same string lands in a single-line input, which is how a
// paragraph of model output became unreadable on the dashboard.
func TestControlPortAsksForMarkdown(t *testing.T) {
	c := &Component{}
	for _, p := range c.Ports() {
		if p.Name != v1alpha1.ControlPort {
			continue
		}
		s, err := schema.CreateSchema(p.Configuration)
		if err != nil {
			t.Fatalf("schema: %v", err)
		}
		b, _ := json.Marshal(s)
		if !strings.Contains(string(b), `"format":"markdown"`) {
			t.Errorf("control schema does not request markdown: %s", b)
		}
		if !strings.Contains(string(b), `"readonly":true`) {
			t.Errorf("control schema does not mark the field read-only: %s", b)
		}
		return
	}
	t.Fatal("no control port")
}

// A display stores exactly what it was handed: the value is what a person
// asked to see.
func TestDisplayStoresTextVerbatim(t *testing.T) {
	c, ok := (&Component{}).Instance().(*Component)
	if !ok {
		t.Fatal("Instance() did not return *Component")
	}
	h := testharness.New(c)
	if r := h.Handle(context.Background(), InPort, InMessage{Text: "All 8 pods healthy"}); r.Err() != nil {
		t.Fatalf("handle failed: %v", r.Err())
	}
	if got := c.text(context.Background()); got != "All 8 pods healthy" {
		t.Errorf("text altered: %q", got)
	}
}

// The text outlives the pod that received it: a panel written once must not
// blank on the next restart or redeploy.
func TestDisplayTextSurvivesInstanceRecreate(t *testing.T) {
	c, _ := (&Component{}).Instance().(*Component)
	h := testharness.New(c)
	if r := h.Handle(context.Background(), InPort, InMessage{Text: "readme"}); r.Err() != nil {
		t.Fatalf("handle failed: %v", r.Err())
	}
	// A fresh instance sharing the same state backend is what the runtime
	// builds after a restart.
	fresh, _ := (&Component{}).Instance().(*Component)
	fresh.OnState(c.State())
	if got := fresh.text(context.Background()); got != "readme" {
		t.Fatalf("text after recreate = %q, want it restored from state", got)
	}
}
