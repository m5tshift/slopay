package kafka

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/m5tshift/slopay/contracts"
)

// ErrInvalidEvent marks an event that does not satisfy its schema or is not valid JSON.
var ErrInvalidEvent = errors.New("kafka: invalid event")

// Codec encodes and decodes events, validating them against the schemas of contracts/schemas/.
type Codec struct {
	schemas map[string]*jsonschema.Schema // by $id
}

// NewCodec compiles every schema of the contracts. A schema file is registered under its $id.
func NewCodec() (*Codec, error) {
	return newCodec(contracts.Schemas, "schemas")
}

func newCodec(fsys fs.FS, dir string) (*Codec, error) {
	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return nil, fmt.Errorf("kafka: read schemas: %w", err)
	}
	c := jsonschema.NewCompiler()
	c.DefaultDraft(jsonschema.Draft7)
	var ids []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		raw, err := fs.ReadFile(fsys, dir+"/"+e.Name())
		if err != nil {
			return nil, fmt.Errorf("kafka: read %s: %w", e.Name(), err)
		}
		doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
		if err != nil {
			return nil, fmt.Errorf("kafka: parse %s: %w", e.Name(), err)
		}
		m, ok := doc.(map[string]any)
		id, _ := m["$id"].(string)
		if !ok || id == "" {
			return nil, fmt.Errorf("kafka: schema %s has no $id", e.Name())
		}
		if err := c.AddResource(id, doc); err != nil {
			return nil, fmt.Errorf("kafka: add %s: %w", e.Name(), err)
		}
		ids = append(ids, id)
	}
	k := &Codec{schemas: make(map[string]*jsonschema.Schema, len(ids))}
	for _, id := range ids {
		s, err := c.Compile(id)
		if err != nil {
			return nil, fmt.Errorf("kafka: compile %s: %w", id, err)
		}
		k.schemas[id] = s
	}
	return k, nil
}

// Encode validates the event and returns the Kafka value.
func (c *Codec) Encode(ev Event) ([]byte, error) {
	if len(ev.Data) == 0 {
		return nil, fmt.Errorf("%w: data is empty", ErrInvalidEvent)
	}
	raw, err := json.Marshal(ev)
	if err != nil {
		return nil, fmt.Errorf("%w: marshal: %w", ErrInvalidEvent, err)
	}
	if err := c.validate(ev.DataSchema, raw); err != nil {
		return nil, err
	}
	return raw, nil
}

// Decode parses and validates a Kafka value.
func (c *Codec) Decode(raw []byte) (Event, error) {
	var head struct {
		DataSchema string `json:"dataschema"`
	}
	if err := json.Unmarshal(raw, &head); err != nil {
		return Event{}, fmt.Errorf("%w: %w", ErrInvalidEvent, err)
	}
	if err := c.validate(head.DataSchema, raw); err != nil {
		return Event{}, err
	}
	var ev Event
	if err := json.Unmarshal(raw, &ev); err != nil {
		return Event{}, fmt.Errorf("%w: %w", ErrInvalidEvent, err)
	}
	return ev, nil
}

func (c *Codec) validate(schemaID string, raw []byte) error {
	s, ok := c.schemas[schemaID]
	if !ok || schemaID == "" {
		return fmt.Errorf("%w: unknown dataschema %q", ErrInvalidEvent, schemaID)
	}
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidEvent, err)
	}
	if err := s.Validate(doc); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidEvent, err)
	}
	return nil
}
