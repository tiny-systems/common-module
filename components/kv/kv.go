package kv

import (
	"context"
	"encoding/json"
	"fmt"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/spyzhov/ajson"
	"github.com/swaggest/jsonschema-go"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
)

type KeyValueQueryRequestContext any
type KeyValueStoreRequestContext any

const (
	OpStore   = "store"
	OptDelete = "delete"
)

const (
	PortStore       = "store"
	PortQuery       = "query"
	PortQueryResult = "query_result"
	PortStoreAck    = "store_ack"
)

type KeyValueStoreDocument map[string]interface{}

func (k KeyValueStoreDocument) PrepareJSONSchema(schema *jsonschema.Schema) error {
	if len(schema.Properties) == 0 {
		id := jsonschema.Schema{}
		id.AddType(jsonschema.String)
		id.WithTitle("ID")
		schema.WithRequired("id")
		schema.Properties = map[string]jsonschema.SchemaOrBool{"id": id.ToSchemaOrBool()}
	}
	return nil
}

type KeyValueStoreSettings struct {
	Document           KeyValueStoreDocument `json:"document,omitempty" type:"object" required:"true" title:"Document" description:"Structure of the object will be used to store incoming messages. Values are arbitrary. Make sure the document has primary key defined below." configurable:"true"`
	PrimaryKey         string                `json:"primaryKey" title:"Primary key" required:"true" default:"id"`
	EnableStoreAckPort bool                  `json:"enableStoreResultPort" required:"true" title:"Enable Store Ack Port" default:"false" description:"Emits information if message was stored or not"`
}

type KeyValueStore struct {
	records  cmap.ConcurrentMap[string, []byte]
	settings KeyValueStoreSettings
}

type KeyValueQueryRequest struct {
	Context KeyValueQueryRequestContext `json:"context,omitempty" configurable:"true" title:"Context"`
	Query   string                      `json:"query,omitempty" required:"true" title:"Query"`
}

type KeyValueQueryResult struct {
	Context  KeyValueQueryRequestContext `json:"context"`
	Document KeyValueStoreDocument       `json:"document"`
	Found    bool                        `json:"found"`
	Query    string                      `json:"query"`
}

type KeyValueStoreRequest struct {
	Context   KeyValueStoreRequestContext `json:"context,omitempty" title:"Context" configurable:"true"`
	Operation string                      `json:"operation" required:"true" enum:"store,delete" enumTitles:"Store,Delete" default:"store" title:"Operation"`
	Document  KeyValueStoreDocument       `json:"document" required:"true" title:"Document" description:"Document to be stored"`
}

type KeyValueStoreResult struct {
	Request KeyValueStoreRequest `json:"request"`
}

func (k *KeyValueStore) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        "in_memory_kv",
		Description: "Key-value Store",
		Info:        "In memory key value store. Requires incoming message to be an object with non empty field ID.",
		Tags:        []string{"kv", "db", "storage"},
	}
}

func (k *KeyValueStore) Handle(ctx context.Context, output module.Handler, port string, msg interface{}) error {
	if port == module.SettingsPort {
		in, ok := msg.(KeyValueStoreSettings)
		if !ok {
			return fmt.Errorf("invalid settings")
		}
		if len(in.Document) == 0 {
			return fmt.Errorf("please define atleast one key")
		}
		if in.PrimaryKey == "" {
			return fmt.Errorf("primary key can not be empty")
		}
		if _, ok := in.Document[in.PrimaryKey]; !ok {
			return fmt.Errorf("primary key is missing in the document")
		}
		k.settings = in
		return nil
	}

	if port == PortStore {
		in, ok := msg.(KeyValueStoreRequest)
		if !ok {
			return fmt.Errorf("invalid store message")
		}
		pkVal, ok := in.Document[k.settings.PrimaryKey]
		if !ok {
			return fmt.Errorf("no primary key defined")
		}
		pkValStr, ok := pkVal.(string)
		if !ok {
			return fmt.Errorf("invalid pk type")
		}
		data, err := json.Marshal(in.Document)
		if err != nil {
			return fmt.Errorf("unable to encode message to store: %v", err)
		}

		if in.Operation == OpStore {
			k.records.Set(pkValStr, data)
		} else if in.Operation == OptDelete {
			k.records.Remove(pkValStr)
		} else {
			return fmt.Errorf("unknown operation: %s", in.Operation)
		}

		if k.settings.EnableStoreAckPort {
			return output(ctx, PortStoreAck, KeyValueStoreResult{
				Request: in,
			})
		}
		return nil
	}

	if port != PortQuery {
		return fmt.Errorf("unknown port")
	}

	in, ok := msg.(KeyValueQueryRequest)
	if !ok {
		return fmt.Errorf("invalid query message")
	}
	if in.Query == "" {
		return fmt.Errorf("empty query")
	}

	for _, key := range k.records.Keys() {
		data, _ := k.records.Get(key)
		node, err := ajson.Unmarshal(data)
		if err != nil {
			return fmt.Errorf("unable to encode stored message")
		}
		jsonPathResult, err := ajson.Eval(node, in.Query)
		if err != nil {
			return fmt.Errorf("unable to eval query: %v", err)
		}
		v, err := jsonPathResult.Unpack()
		if err != nil {
			return fmt.Errorf("unable to get result: %v", err)
		}
		if v == true {
			// found it
			result := KeyValueStoreDocument{}
			if err = json.Unmarshal(data, &result); err != nil {
				return fmt.Errorf("unable to decode result: %v", err)
			}
			return output(ctx, PortQueryResult, KeyValueQueryResult{
				Query:    in.Query,
				Context:  in.Context,
				Document: result,
				Found:    true,
			})
		}
	}

	return output(ctx, PortQueryResult, KeyValueQueryResult{
		Query:   in.Query,
		Context: in.Context,
		Found:   false,
	})
}

func (k *KeyValueStore) Ports() []module.Port {
	ports := []module.Port{
		{
			Name:   PortQuery,
			Label:  "Query",
			Source: true,
			Configuration: KeyValueQueryRequest{
				Query: "$.documentProperty == 1",
			},
			Position: module.Left,
		},

		{
			Name:   PortStore,
			Label:  "Store",
			Source: true,
			Configuration: KeyValueStoreRequest{
				Operation: PortStore,
			},
			Position: module.Left,
		},
		{
			Name:          PortQueryResult,
			Label:         "Query result",
			Source:        false,
			Configuration: KeyValueQueryResult{},
			Position:      module.Right,
		},
		{
			Name:   module.SettingsPort,
			Label:  "Settings",
			Source: true,
			Configuration: KeyValueStoreSettings{
				PrimaryKey: "id",
				Document: KeyValueStoreDocument{
					"id": "ID",
				},
			},
		},
	}
	if k.settings.EnableStoreAckPort {
		ports = append(ports, module.Port{
			Name:          PortStoreAck,
			Label:         "Store ack",
			Source:        false,
			Configuration: KeyValueStoreResult{},
			Position:      module.Right,
		})
	}
	return ports
}

func (k *KeyValueStore) Instance() module.Component {
	return &KeyValueStore{
		settings: KeyValueStoreSettings{}, // default settings
		records:  cmap.New[[]byte](),
	}
}

var _ module.Component = (*KeyValueStore)(nil)

func init() {
	registry.Register(&KeyValueStore{})
}
