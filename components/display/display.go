package display

import (
	"context"
	"fmt"

	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName        = "display"
	InPort        string = "in"
)

// Settings holds nothing the user configures: what a display shows is
// whatever arrives, and how it renders is fixed by the component.
type Settings struct{}

// InMessage is what to show. A single named field, deliberately: a display
// panel exists to answer one question, and a flow that wants to surface three
// things should say which three rather than pushing its whole state at a
// person.
type InMessage struct {
	Text string `json:"text" required:"true" title:"Text" description:"What to show. Markdown is rendered."`
}

// Control is the dashboard surface: the text, rendered as prose and not
// offered for editing.
//
// format:"markdown" is what makes the editor render it instead of putting it
// in a single-line input, where a paragraph of model output is unreadable and
// pretends to be typeable. readonly says the obvious thing out loud.
type Control struct {
	Text string `json:"text" readonly:"true" format:"markdown" title:"" description:""`
}

type Component struct {
	module.Base
	settings Settings
	text     string
}

func (t *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Display",
		Info: "Shows one piece of text on the dashboard, rendered as markdown. " +
			"Use it as a flow's answer panel: wire the value a person actually reads into Text " +
			"(e.g. text: \"{{$.outputData.messages[0].content}}\") rather than passing the whole message, " +
			"which renders as a wall of form fields. Has no output ports — it is a sink. " +
			"Enable it as a dashboard widget to give a flow a readable result.",
		Tags: []string{"SDK", "dashboard"},
	}
}

func (t *Component) OnSettings(_ context.Context, msg any) error {
	in, ok := msg.(Settings)
	if !ok {
		return fmt.Errorf("invalid settings")
	}
	t.settings = in
	return nil
}

func (t *Component) Handle(ctx context.Context, _ module.Handler, port string, msg interface{}) module.Result {
	if port != InPort {
		return module.Fail(fmt.Errorf("unknown port: %s", port))
	}
	in, ok := msg.(InMessage)
	if !ok {
		return module.Fail(fmt.Errorf("invalid message in"))
	}
	// Mask credential-shaped values before they become node state: what is
	// stored here is rendered on the dashboard, written to the node resource,
	// and carried into any export of the project.
	// Deliberately stored as given. Redaction works by field NAME, and a
	// display's field is called "text" — so a flow that pipes a secret in
	// here is the thing to fix, not this component. Publishing redacts
	// credential-shaped fields on the way out.
	t.text = in.Text
	t.Emit(ctx, v1alpha1.ReconcilePort, nil)
	return module.Result{}
}

func (t *Component) Ports() []module.Port {
	return []module.Port{
		{
			Name:          InPort,
			Label:         "In",
			Configuration: InMessage{},
			Position:      module.Left,
		},
		{
			Name:   v1alpha1.ControlPort,
			Label:  "Control",
			Source: true,
			Configuration: Control{
				Text: t.text,
			},
		},
		{
			Name:          v1alpha1.SettingsPort,
			Label:         "Settings",
			Configuration: t.settings,
		},
	}
}

func (t *Component) Instance() module.Component {
	return &Component{}
}

var (
	_ module.Component       = (*Component)(nil)
	_ module.SettingsHandler = (*Component)(nil)
)

func init() {
	registry.Register(&Component{})
}
