package merchant

import (
	"errors"

	"github.com/m5tshift/weigate/pkg/kafka"
)

// Event constants of contracts/asyncapi/merchant-service.yaml.
const (
	Topic         = "merchant.events"
	EventSource   = "/weigate/merchant-service"
	EventType     = "merchant.updated"
	schemaVersion = 1
)

// UpdatedData is the data of merchant.updated (contracts/schemas/merchant.updated.v1.json): the whole
// published state. The webhook secret is never part of it, only its version.
type UpdatedData struct {
	MerchantID           string  `json:"merchantId"`
	Version              int64   `json:"version"`
	Xpub                 string  `json:"xpub"`
	WebhookURL           *string `json:"webhookUrl"`
	ToleranceBps         int     `json:"toleranceBps"`
	WebhookSecretVersion int     `json:"webhookSecretVersion"`
}

// ErrNotPublishable is returned for a merchant without an xpub: the event schema requires one, and
// consumers cannot issue invoices for such a merchant anyway.
var ErrNotPublishable = errors.New("merchant: no xpub, merchant.updated is not published")

// Publishable reports whether the state of m is published to merchant.events.
func Publishable(m Merchant) bool { return m.Xpub != nil }

// UpdatedEvent builds merchant.updated for the state of m. The Kafka key (subject) is the merchant id.
func UpdatedEvent(m Merchant) (kafka.Event, error) {
	if !Publishable(m) {
		return kafka.Event{}, ErrNotPublishable
	}
	id := m.ID.String()
	return kafka.NewEvent(EventSource, EventType, id, schemaVersion, UpdatedData{
		MerchantID:           id,
		Version:              m.Version,
		Xpub:                 *m.Xpub,
		WebhookURL:           m.WebhookURL,
		ToleranceBps:         m.ToleranceBps,
		WebhookSecretVersion: m.WebhookSecretVersion,
	})
}
