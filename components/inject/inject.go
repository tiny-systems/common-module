package inject

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName = "inject"
	ConfigPort    = "config"
	MessagePort   = "message"
	OutputPort    = "output"
	ErrorPort     = "error"
)

// Settings - only port flags
type Settings struct {
	EnableErrorPort bool `json:"enableErrorPort" title:"Enable Error Port" description:"Output errors to error port instead of failing"`
}

// Config is stored and injected into messages
type Config struct {
	Data any `json:"data" configurable:"true" required:"true" title:"Data" description:"Configuration data to inject into messages"`
}

// Message passes through with config injected
type Message struct {
	Context any `json:"context,omitempty" configurable:"true" title:"Context" description:"Passthrough context for correlation"`
	Data    any `json:"data,omitempty" configurable:"true" title:"Data" description:"Message data"`
}

// Output contains original message plus injected config
type Output struct {
	Context any `json:"context,omitempty" title:"Context"`
	Data    any `json:"data,omitempty" title:"Data" description:"Original message data"`
	Config  any `json:"config,omitempty" title:"Config" description:"Injected configuration"`
}

// Error output
type Error struct {
	Context any     `json:"context,omitempty" title:"Context"`
	Error   string  `json:"error" title:"Error"`
	Message Message `json:"message" title:"Message"`
}

// Component implements config injection
type Component struct {
	settings   Settings
	config     any
	configLock sync.RWMutex
}

func (c *Component) Instance() module.Component {
	return &Component{}
}

func (c *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Inject",
		Info:        "Injects stored configuration into passing messages. Config port receives settings (stored), message port receives high-throughput messages that get config injected.",
		Tags:        []string{"Data", "Config", "Enrich"},
	}
}

func (c *Component) Handle(ctx context.Context, handler module.Handler, port string, msg any) any {
	switch port {
	case v1alpha1.SettingsPort:
		in, ok := msg.(Settings)
		if !ok {
			return fmt.Errorf("invalid settings")
		}
		c.settings = in
		return nil

	case ConfigPort:
		in, ok := msg.(Config)
		if !ok {
			return fmt.Errorf("invalid config")
		}
		c.configLock.Lock()
		c.config = in.Data
		c.configLock.Unlock()
		return nil

	case MessagePort:
		in, ok := msg.(Message)
		if !ok {
			return fmt.Errorf("invalid message")
		}
		return c.handleMessage(ctx, handler, in)
	}

	return fmt.Errorf("unknown port: %s", port)
}

func (c *Component) handleMessage(ctx context.Context, handler module.Handler, msg Message) any {
	c.configLock.RLock()
	config := c.config
	c.configLock.RUnlock()

	if config == nil {
		return c.handleError(ctx, handler, msg, "no config set - send config first")
	}

	output := Output{
		Context: msg.Context,
		Data:    msg.Data,
		Config:  config,
	}

	return handler(ctx, OutputPort, output)
}

func (c *Component) handleError(ctx context.Context, handler module.Handler, msg Message, errMsg string) any {
	if c.settings.EnableErrorPort {
		// Return handler result to propagate responses back through the call chain
		// (critical for blocking I/O patterns like HTTP Server)
		return handler(ctx, ErrorPort, Error{
			Context: msg.Context,
			Error:   errMsg,
			Message: msg,
		})
	}
	return errors.New(errMsg)
}

func (c *Component) Ports() []module.Port {
	ports := []module.Port{
		{
			Name:          v1alpha1.SettingsPort,
			Label:         "Settings",
			Configuration: Settings{},
		},
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
			Name:   OutputPort,
			Label:  "Output",
			Source: true,
			Configuration: Output{
				Config: map[string]any{"example": "config"},
			},
			Position: module.Right,
		},
	}

	if c.settings.EnableErrorPort {
		ports = append(ports, module.Port{
			Name:          ErrorPort,
			Label:         "Error",
			Source:        true,
			Configuration: Error{},
			Position:      module.Bottom,
		})
	}

	return ports
}

var _ module.Component = (*Component)(nil)

func init() {
	registry.Register(&Component{})
}
