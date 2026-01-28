package groupby

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName = "group_by"
	InPort        = "in"
	OutPort       = "out"
)

// Context type alias for schema generation
type Context any

// Settings configures the component
type Settings struct {
	GroupByPath string `json:"groupByPath" required:"true" title:"Group By Path" description:"JSON path to group by (e.g., 'labels.app', 'namespace', 'kind')"`
}

// InMessage is the input
type InMessage struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context"`
	Items   []any   `json:"items" required:"true" title:"Items" description:"Array of items to group"`
}

// Group represents a single group
type Group struct {
	Key   string `json:"key" title:"Key" description:"The group key value"`
	Items []any  `json:"items" title:"Items" description:"Items in this group"`
	Count int    `json:"count" title:"Count" description:"Number of items in group"`
}

// OutMessage is the output
type OutMessage struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context"`
	Groups  []Group `json:"groups" title:"Groups" description:"Grouped items"`
	Total   int     `json:"total" title:"Total" description:"Total number of items"`
}

// Component implements the group_by logic
type Component struct {
	settings Settings
}

func (c *Component) Instance() module.Component {
	return &Component{}
}

func (c *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Group By",
		Info:        "Groups an array of items by a specified field path. Input: array of objects + groupByPath (e.g., 'labels.app'). Output: array of groups, each with key and items.",
		Tags:        []string{"SDK", "Array", "Aggregate"},
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

	case InPort:
		in, ok := msg.(InMessage)
		if !ok {
			return fmt.Errorf("invalid message")
		}
		return c.handleGroupBy(ctx, handler, in)
	}

	return fmt.Errorf("unknown port: %s", port)
}

func (c *Component) handleGroupBy(ctx context.Context, handler module.Handler, in InMessage) any {
	if c.settings.GroupByPath == "" {
		return fmt.Errorf("groupByPath is required in settings")
	}

	// Group items by the path
	groups := make(map[string][]any)
	pathParts := strings.Split(c.settings.GroupByPath, ".")

	for _, item := range in.Items {
		key := extractPath(item, pathParts)
		groups[key] = append(groups[key], item)
	}

	// Convert to output format
	result := make([]Group, 0, len(groups))
	for key, items := range groups {
		result = append(result, Group{
			Key:   key,
			Items: items,
			Count: len(items),
		})
	}

	return handler(ctx, OutPort, OutMessage{
		Context: in.Context,
		Groups:  result,
		Total:   len(in.Items),
	})
}

// extractPath extracts a value from a nested structure by path
func extractPath(item any, pathParts []string) string {
	current := item

	for _, part := range pathParts {
		if current == nil {
			return ""
		}

		switch v := current.(type) {
		case map[string]any:
			current = v[part]
		case map[string]string:
			if val, ok := v[part]; ok {
				return val
			}
			return ""
		default:
			// Try reflection for struct fields
			rv := reflect.ValueOf(current)
			if rv.Kind() == reflect.Ptr {
				rv = rv.Elem()
			}
			if rv.Kind() == reflect.Struct {
				field := rv.FieldByNameFunc(func(name string) bool {
					return strings.EqualFold(name, part)
				})
				if field.IsValid() {
					current = field.Interface()
					continue
				}
			}
			return ""
		}
	}

	// Convert final value to string
	if current == nil {
		return ""
	}
	return fmt.Sprintf("%v", current)
}

func (c *Component) Ports() []module.Port {
	return []module.Port{
		{
			Name:          v1alpha1.SettingsPort,
			Label:         "Settings",
			Configuration: Settings{GroupByPath: "labels.app"},
		},
		{
			Name:          InPort,
			Label:         "In",
			Configuration: InMessage{},
			Position:      module.Left,
		},
		{
			Name:   OutPort,
			Label:  "Out",
			Source: true,
			Configuration: OutMessage{
				Groups: []Group{
					{Key: "myapp", Count: 2, Items: []any{}},
					{Key: "other", Count: 1, Items: []any{}},
				},
				Total: 3,
			},
			Position: module.Right,
		},
	}
}

var _ module.Component = (*Component)(nil)

func init() {
	registry.Register(&Component{})
}
