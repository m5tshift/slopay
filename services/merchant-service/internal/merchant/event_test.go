package merchant

import (
	"encoding/json"
	"errors"
	"os"
	"testing"

	"github.com/google/uuid"
	"go.yaml.in/yaml/v3"

	"github.com/m5tshift/slopay/pkg/kafka"
	"github.com/m5tshift/slopay/services/merchant-service/internal/xpubtest"
)

// TestUpdatedEventContract checks merchant.updated against contracts/schemas/merchant.updated.v1.json and
// the channel of contracts/asyncapi/merchant-service.yaml.
func TestUpdatedEventContract(t *testing.T) {
	codec, err := kafka.NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	xpub, url := xpubtest.Xpub, "https://merchant.example.com/hooks/slopay"
	for name, m := range map[string]Merchant{
		"without webhook": {ID: uuid.New(), Xpub: &xpub, ToleranceBps: 100, WebhookSecretVersion: 1, Version: 2},
		"full":            {ID: uuid.New(), Xpub: &xpub, WebhookURL: &url, ToleranceBps: 500, WebhookSecretVersion: 3, Version: 9},
		"zero tolerance":  {ID: uuid.New(), Xpub: &xpub, ToleranceBps: 0, WebhookSecretVersion: 1, Version: 1},
	} {
		t.Run(name, func(t *testing.T) {
			ev, err := UpdatedEvent(m)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := codec.Encode(ev)
			if err != nil {
				t.Fatalf("event does not match the schema: %v", err)
			}
			back, err := codec.Decode(raw)
			if err != nil || back.Subject != m.ID.String() || back.Type != "merchant.updated" || back.Source != "/slopay/merchant-service" {
				t.Fatalf("decoded %+v, err %v", back, err)
			}
			var data map[string]any
			if err := json.Unmarshal(back.Data, &data); err != nil {
				t.Fatal(err)
			}
			// The webhook secret itself is never published.
			for k := range data {
				if k == "webhookSecret" || k == "secret" {
					t.Errorf("secret published: %s", k)
				}
			}
			if _, ok := data["webhookUrl"]; !ok {
				t.Error("webhookUrl must be present (null until set)")
			}
		})
	}
	if _, err := UpdatedEvent(Merchant{ID: uuid.New(), ToleranceBps: 100, WebhookSecretVersion: 1, Version: 1}); !errors.Is(err, ErrNotPublishable) {
		t.Errorf("without xpub: %v", err)
	}
}

func TestTopicMatchesAsyncAPI(t *testing.T) {
	raw, err := os.ReadFile("../../../../contracts/asyncapi/merchant-service.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Channels map[string]struct {
			Address string `yaml:"address"`
		} `yaml:"channels"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	if doc.Channels["merchantEvents"].Address != Topic {
		t.Errorf("topic %q, contract %q", Topic, doc.Channels["merchantEvents"].Address)
	}
}
