package kv

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/swaggest/jsonschema-go"
	"github.com/tiny-systems/ajson"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName = "kv"

	StorePort       = "store"
	QueryPort       = "query"
	QueryResultPort = "query_result"
	StoreAckPort    = "store_ack"

	OpStore  = "store"
	OpDelete = "delete"

	metadataPrefix     = "kv-"
	defaultMaxRecords  = 100
	maxRecordSizeBytes = 32 * 1024 // 32KB per record
)

// Separate context types for schema propagation — platform matches by type name.
// StoreContext flows through store → store_ack, QueryContext through query → query_result.
type StoreContext any
type QueryContext any

// Document is the user-defined value structure
type Document map[string]interface{}

func (d Document) PrepareJSONSchema(schema *jsonschema.Schema) error {
	if len(schema.Properties) == 0 {
		id := jsonschema.Schema{}
		id.AddType(jsonschema.String)
		id.WithTitle("ID")
		schema.WithRequired("id")
		schema.Properties = map[string]jsonschema.SchemaOrBool{
			"id": id.ToSchemaOrBool(),
		}
	}
	return nil
}

// Settings configures the KV store
type Settings struct {
	Document       Document `json:"document,omitempty" type:"object" required:"true" title:"Document" description:"Structure of stored documents. Values are arbitrary. Make sure the document has the primary key defined below." configurable:"true"`
	PrimaryKey     string   `json:"primaryKey" title:"Primary Key" required:"true" default:"id"`
	MaxRecords     int      `json:"maxRecords" title:"Max Records" description:"Maximum number of records (0 = default 100)" default:"100"`
	EnableStoreAck bool     `json:"enableStoreAckPort" required:"true" title:"Enable Store Ack Port" description:"Emits acknowledgment after store/delete operations"`
}

// StoreRequest is the input for store/delete operations
type StoreRequest struct {
	Context   StoreContext `json:"context,omitempty" configurable:"true" title:"Context"`
	Operation string       `json:"operation" required:"true" enum:"store,delete" enumTitles:"Store,Delete" default:"store" title:"Operation"`
	Document  Document     `json:"document" required:"true" title:"Document" description:"Document to store or delete"`
}

// StoreAck acknowledges a store/delete operation
type StoreAck struct {
	Context StoreContext `json:"context,omitempty" title:"Context"`
	Request StoreRequest `json:"request" title:"Request"`
}

// QueryRequest is the input for querying records
type QueryRequest struct {
	Context QueryContext `json:"context,omitempty" configurable:"true" title:"Context"`
	Query   string       `json:"query,omitempty" required:"true" title:"Query" description:"JSONPath expression evaluated against each record (e.g. $.status == 'DOWN')"`
}

// QueryResultItem is a single matched record
type QueryResultItem struct {
	Key      string   `json:"key" title:"Key"`
	Document Document `json:"document" title:"Document"`
}

// QueryResult returns all matching records
type QueryResult struct {
	Context QueryContext      `json:"context,omitempty" title:"Context"`
	Results []QueryResultItem `json:"results" title:"Results"`
	Count   int               `json:"count" title:"Count"`
	Query   string            `json:"query" title:"Query"`
}

// Control exposes the reset button on the _control port
type Control struct {
	Records int  `json:"records" title:"Records" readonly:"true"`
	Reset   bool `json:"reset" format:"button" title:"Reset" required:"true"`
}

// Component implements a metadata-backed key-value store
type Component struct {
	mu        sync.RWMutex
	settings  Settings
	records   map[string][]byte
	storeUsed bool
}

func (c *Component) Instance() module.Component {
	return &Component{
		settings: Settings{
			PrimaryKey: "id",
			MaxRecords: defaultMaxRecords,
			Document:   Document{"id": "ID"},
		},
		records: make(map[string][]byte),
	}
}

func (c *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Key-Value Store",
		Info:        "Key-value store backed by TinyNode metadata. Stores documents with a configurable schema and primary key. Supports JSONPath queries. Persistence is best-effort: writes are debounced (1s) before patching K8s, so data may be lost on pod crash within that window. Multi-pod safe via leader-reader pattern but eventually consistent. Best suited for state that gets periodically refreshed.",
		Tags:        []string{"KV", "Storage", "Data"},
	}
}

func (c *Component) getControl() Control {
	return Control{
		Records: len(c.records),
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
		return c.handleSettings(in)

	case v1alpha1.ControlPort:
		in, ok := msg.(Control)
		if !ok {
			return fmt.Errorf("invalid control")
		}
		return c.handleControl(handler, in)

	case StorePort:
		in, ok := msg.(StoreRequest)
		if !ok {
			return fmt.Errorf("invalid store request")
		}
		return c.handleStore(ctx, handler, in)

	case QueryPort:
		in, ok := msg.(QueryRequest)
		if !ok {
			return fmt.Errorf("invalid query request")
		}
		return c.handleQuery(ctx, handler, in)
	}

	return fmt.Errorf("unknown port: %s", port)
}

func (c *Component) handleControl(handler module.Handler, in Control) error {
	if !in.Reset {
		return nil
	}

	c.mu.Lock()
	c.records = make(map[string][]byte)
	c.storeUsed = false
	c.mu.Unlock()

	// Remove all kv-* metadata keys
	_ = handler(context.Background(), v1alpha1.ReconcilePort, func(n *v1alpha1.TinyNode) error {
		if n.Status.Metadata == nil {
			return nil
		}
		for k := range n.Status.Metadata {
			if strings.HasPrefix(k, metadataPrefix) {
				delete(n.Status.Metadata, k)
			}
		}
		return nil
	})

	// Update control port with new record count
	_ = handler(context.Background(), v1alpha1.ControlPort, c.getControl())
	return nil
}

func (c *Component) handleSettings(in Settings) error {
	if len(in.Document) == 0 {
		return fmt.Errorf("document must have at least one field")
	}
	if in.PrimaryKey == "" {
		return fmt.Errorf("primary key cannot be empty")
	}
	if _, ok := in.Document[in.PrimaryKey]; !ok {
		return fmt.Errorf("primary key %q not found in document", in.PrimaryKey)
	}
	if in.MaxRecords <= 0 {
		in.MaxRecords = defaultMaxRecords
	}

	c.mu.Lock()
	c.settings = in
	c.mu.Unlock()
	return nil
}

func (c *Component) handleReconcile(msg any) error {
	node, ok := msg.(v1alpha1.TinyNode)
	if !ok {
		return nil
	}
	if node.Status.Metadata == nil {
		return nil
	}

	c.mu.RLock()
	skip := c.storeUsed
	c.mu.RUnlock()

	if skip {
		return nil
	}

	for k, v := range node.Status.Metadata {
		if !strings.HasPrefix(k, metadataPrefix) {
			continue
		}
		key := k[len(metadataPrefix):]
		c.records[key] = []byte(v)
	}
	return nil
}

func (c *Component) handleStore(ctx context.Context, handler module.Handler, req StoreRequest) any {
	c.mu.RLock()
	pk := c.settings.PrimaryKey
	maxRecords := c.settings.MaxRecords
	enableAck := c.settings.EnableStoreAck
	c.mu.RUnlock()

	pkVal, ok := req.Document[pk]
	if !ok {
		return fmt.Errorf("primary key %q not found in document", pk)
	}
	pkStr, ok := pkVal.(string)
	if !ok {
		return fmt.Errorf("primary key must be a string, got %T", pkVal)
	}
	if pkStr == "" {
		return fmt.Errorf("primary key cannot be empty")
	}

	c.mu.Lock()
	c.storeUsed = true
	c.mu.Unlock()

	metaKey := metadataPrefix + pkStr

	switch req.Operation {
	case OpStore:
		data, err := json.Marshal(req.Document)
		if err != nil {
			return fmt.Errorf("failed to marshal document: %v", err)
		}
		if len(data) > maxRecordSizeBytes {
			return fmt.Errorf("document too large: %d bytes (max %d)", len(data), maxRecordSizeBytes)
		}
		// Check capacity (allow update of existing key)
		if _, exists := c.records[pkStr]; !exists && len(c.records) >= maxRecords {
			return fmt.Errorf("store full: %d records (max %d)", len(c.records), maxRecords)
		}

		c.records[pkStr] = data
		_ = handler(context.Background(), v1alpha1.ReconcilePort, func(n *v1alpha1.TinyNode) error {
			if n.Status.Metadata == nil {
				n.Status.Metadata = make(map[string]string)
			}
			n.Status.Metadata[metaKey] = string(data)
			return nil
		})

	case OpDelete:
		delete(c.records, pkStr)
		_ = handler(context.Background(), v1alpha1.ReconcilePort, func(n *v1alpha1.TinyNode) error {
			if n.Status.Metadata != nil {
				delete(n.Status.Metadata, metaKey)
			}
			return nil
		})

	default:
		return fmt.Errorf("unknown operation: %s", req.Operation)
	}

	// Update control port with current record count
	_ = handler(context.Background(), v1alpha1.ControlPort, c.getControl())

	if enableAck {
		return handler(ctx, StoreAckPort, StoreAck{
			Context: req.Context,
			Request: req,
		})
	}
	return nil
}

func (c *Component) sortedKeys() []string {
	keys := make([]string, 0, len(c.records))
	for k := range c.records {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (c *Component) handleQuery(ctx context.Context, handler module.Handler, req QueryRequest) any {
	var results []QueryResultItem

	if req.Query == "" {
		// Empty query: return all records
		for _, key := range c.sortedKeys() {
			doc := Document{}
			if err := json.Unmarshal(c.records[key], &doc); err != nil {
				continue
			}
			results = append(results, QueryResultItem{Key: key, Document: doc})
		}
	} else {
		// Evaluate JSONPath query against each record
		for _, key := range c.sortedKeys() {
			data := c.records[key]
			node, err := ajson.Unmarshal(data)
			if err != nil {
				continue
			}
			result, err := ajson.Eval(node, req.Query)
			if err != nil {
				continue
			}
			v, err := result.Unpack()
			if err != nil {
				continue
			}
			if v == true {
				doc := Document{}
				if err = json.Unmarshal(data, &doc); err != nil {
					continue
				}
				results = append(results, QueryResultItem{Key: key, Document: doc})
			}
		}
	}

	if results == nil {
		results = []QueryResultItem{}
	}

	return handler(ctx, QueryResultPort, QueryResult{
		Context: req.Context,
		Results: results,
		Count:   len(results),
		Query:   req.Query,
	})
}

func (c *Component) Ports() []module.Port {
	c.mu.RLock()
	settings := c.settings
	c.mu.RUnlock()

	ports := []module.Port{
		{Name: v1alpha1.ReconcilePort},
		{
			Name:          v1alpha1.SettingsPort,
			Label:         "Settings",
			Configuration: settings,
		},
		{
			Name:          v1alpha1.ControlPort,
			Label:         "Control",
			Source:        true,
			Configuration: c.getControl(),
		},
		{
			Name:  StorePort,
			Label: "Store",
			Configuration: StoreRequest{
				Operation: OpStore,
				Document:  settings.Document,
			},
			Position: module.Left,
		},
		{
			Name:  QueryPort,
			Label: "Query",
			Configuration: QueryRequest{
				Query: "$.status == 'DOWN'",
			},
			Position: module.Left,
		},
		{
			Name:   QueryResultPort,
			Label:  "Query Result",
			Source: true,
			Configuration: QueryResult{
				Results: []QueryResultItem{
					{Document: settings.Document},
				},
			},
			Position: module.Right,
		},
	}

	if settings.EnableStoreAck {
		ports = append(ports, module.Port{
			Name:   StoreAckPort,
			Label:  "Store Ack",
			Source: true,
			Configuration: StoreAck{
				Request: StoreRequest{
					Document: settings.Document,
				},
			},
			Position: module.Right,
		})
	}

	return ports
}

var (
	_ module.Component    = (*Component)(nil)
	_ jsonschema.Preparer = (*Document)(nil)
)

func init() {
	registry.Register((&Component{}).Instance())
}
