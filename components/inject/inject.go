package inject

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName     = "inject"
	ConfigPort        = "config"
	MessagePort       = "message"
	OutputPort        = "output"
	ErrorPort         = "error"
	metadataKeyConfig = "inject-config"
)

type Context any
type Data any

type Settings struct {
	ConfigRequired bool `json:"configRequired" title:"Config Required" description:"When enabled, messages arriving before config is set are sent to the error port instead of output"`
}

// Config is stored in metadata and injected into messages
type Config struct {
	Data Data `json:"data" configurable:"true" required:"true" title:"Data" description:"Configuration data to inject into messages"`
}

// Message passes through with config injected
type Message struct {
	Context Context `json:"context" configurable:"true" title:"Context" description:"Passthrough context for correlation"`
}

// Output contains original context plus injected config
type Output struct {
	Context Context `json:"context" configurable:"true" title:"Context"`
	Config  Data    `json:"config" title:"Config" description:"Injected configuration from metadata"`
}

// ErrorOutput is sent when config is required but not set
type ErrorOutput struct {
	Context Context `json:"context" configurable:"true" title:"Context"`
	Error   string  `json:"error" title:"Error"`
}

// Component implements config injection with metadata persistence
type Component struct {
	settings         Settings
	config           any
	settingsFromPort bool // set when config port provides data; prevents _reconcile from overwriting with stale metadata
}

func (c *Component) Instance() module.Component {
	return &Component{}
}

func (c *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Inject",
		Info:        "Injects stored configuration into passing messages. Send config once, then every message passing through gets it attached. Config persists across pod restarts via metadata.",
		Tags:        []string{"Data", "Config", "Enrich"},
	}
}

func (c *Component) Handle(ctx context.Context, handler module.Handler, port string, msg any) any {
	switch port {
	case v1alpha1.ReconcilePort:
		return c.handleReconcile(msg)

	case v1alpha1.SettingsPort:
		in, ok := msg.(Settings)
		if !ok {
			return fmt.Errorf("invalid settings")
		}
		c.settings = in
		return nil

	case ConfigPort:
		return c.handleConfig(handler, msg)

	case MessagePort:
		return c.handleMessage(ctx, handler, msg)
	}

	return fmt.Errorf("unknown port: %s", port)
}

func (c *Component) handleReconcile(msg any) error {
	node, ok := msg.(v1alpha1.TinyNode)
	if !ok {
		return nil
	}

	if node.Status.Metadata == nil {
		return nil
	}

	configStr, ok := node.Status.Metadata[metadataKeyConfig]
	if !ok {
		return nil
	}

	if c.settingsFromPort {
		return nil
	}

	var config any
	if err := json.Unmarshal([]byte(configStr), &config); err != nil {
		return nil
	}

	c.config = config
	return nil
}

func (c *Component) handleConfig(handler module.Handler, msg any) any {
	in, ok := msg.(Config)
	if !ok {
		return fmt.Errorf("invalid config")
	}

	c.config = in.Data
	c.settingsFromPort = true
	c.persistConfig(handler)
	return nil
}

func (c *Component) handleMessage(ctx context.Context, handler module.Handler, msg any) any {
	in, ok := msg.(Message)
	if !ok {
		return fmt.Errorf("invalid message")
	}

	if c.settings.ConfigRequired && c.config == nil {
		return handler(ctx, ErrorPort, ErrorOutput{
			Context: in.Context,
			Error:   "config not set",
		})
	}

	return handler(ctx, OutputPort, Output{
		Context: in.Context,
		Config:  c.config,
	})
}

func (c *Component) persistConfig(handler module.Handler) {
	configBytes, _ := json.Marshal(c.config)
	_ = handler(context.Background(), v1alpha1.ReconcilePort, func(n *v1alpha1.TinyNode) error {
		if n.Status.Metadata == nil {
			n.Status.Metadata = make(map[string]string)
		}
		n.Status.Metadata[metadataKeyConfig] = string(configBytes)
		return nil
	})
}

func (c *Component) Ports() []module.Port {
	ports := []module.Port{
		{Name: v1alpha1.ReconcilePort},
		{Name: v1alpha1.SettingsPort, Label: "Settings", Configuration: c.settings},
		{
			Name:          ConfigPort,
			Label:         "Config",
			Configuration: Config{},
			Position:      module.Top,
		},
		{
			Name:          MessagePort,
			Label:         "Message",
			Configuration: Message{},
			Position:      module.Left,
		},
		{
			Name:          OutputPort,
			Label:         "Output",
			Source:        true,
			Configuration: Output{},
			Position:      module.Right,
		},
	}
	if c.settings.ConfigRequired {
		ports = append(ports, module.Port{
			Name:          ErrorPort,
			Label:         "Error",
			Source:        true,
			Configuration: ErrorOutput{},
			Position:      module.Bottom,
		})
	}
	return ports
}

var _ module.Component = (*Component)(nil)

func init() {
	registry.Register(&Component{})
}
