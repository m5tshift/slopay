package kafka

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

const (
	// ContentType is the Kafka content-type header of a structured-mode CloudEvent.
	ContentType = "application/cloudevents+json; charset=UTF-8"
	// SchemaBaseURL is the prefix of every dataschema; the file name follows (<event>.v<N>.json).
	SchemaBaseURL = "https://raw.githubusercontent.com/m5tshift/weigate/main/contracts/schemas/"

	specVersion         = "1.0"
	dataContentTypeJSON = "application/json"
)

// Event is the CloudEvents envelope of contracts/schemas/cloudevent.v1.json.
type Event struct {
	SpecVersion     string          `json:"specversion"`
	ID              string          `json:"id"`
	Source          string          `json:"source"`
	Type            string          `json:"type"`
	Time            time.Time       `json:"time"`
	Subject         string          `json:"subject"`
	DataSchema      string          `json:"dataschema"`
	DataContentType string          `json:"datacontenttype"`
	Data            json.RawMessage `json:"data"`
}

// SchemaURL returns the dataschema of the schema file of an event type and version,
// for example SchemaURL("invoice.paid", 1).
func SchemaURL(eventType string, version int) string {
	return fmt.Sprintf("%s%s.v%d.json", SchemaBaseURL, eventType, version)
}

// NewEvent builds an event with a fresh ID and the current time. data is marshalled to JSON.
// source is /weigate/<service>; subject is the aggregate and becomes the Kafka key.
// To republish the same fact (outbox retry) build the event once and keep its ID.
func NewEvent(source, eventType, subject string, schemaVersion int, data any) (Event, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return Event{}, fmt.Errorf("kafka: marshal data: %w", err)
	}
	if len(raw) == 0 || raw[0] != '{' {
		return Event{}, errors.New("kafka: event data must be a JSON object")
	}
	return Event{
		SpecVersion:     specVersion,
		ID:              uuid.NewString(),
		Source:          source,
		Type:            eventType,
		Time:            time.Now().UTC(),
		Subject:         subject,
		DataSchema:      SchemaURL(eventType, schemaVersion),
		DataContentType: dataContentTypeJSON,
		Data:            raw,
	}, nil
}
