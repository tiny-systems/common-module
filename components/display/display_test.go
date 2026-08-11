package display

import (
	"context"
	"encoding/json"
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
	c := &Component{}
	c.Handle(context.Background(), nil, InPort, InMessage{Text: "All 8 pods healthy"})
	if c.text != "All 8 pods healthy" {
		t.Errorf("text altered: %q", c.text)
	}
}
