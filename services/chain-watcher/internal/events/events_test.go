package events

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/m5tshift/slopay/pkg/kafka"
	"github.com/m5tshift/slopay/services/chain-watcher/internal/chain"
)

var head = chain.Header{
	Number:     101,
	Hash:       "0xb2c3d4e5b2c3d4e5b2c3d4e5b2c3d4e5b2c3d4e5b2c3d4e5b2c3d4e5b2c3d4e5",
	ParentHash: "0xa1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4a1b2c3d4",
	Timestamp:  1794736932,
}

func codec(t *testing.T) *kafka.Codec {
	t.Helper()
	c, err := kafka.NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// TestNewHeadMatchesContract encodes the event through the codec, which validates it against
// contracts/schemas/chain.new_head.v1.json.
func TestNewHeadMatchesContract(t *testing.T) {
	ev, err := NewHead(31337, head)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := codec(t).Encode(ev)
	if err != nil {
		t.Fatalf("contract: %v", err)
	}
	if ev.ID != "31337:"+head.Hash || ev.Subject != "31337" || ev.Source != EventSource || ev.Type != TypeNewHead {
		t.Fatalf("envelope = %+v", ev)
	}
	var got struct {
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"chainId": 31337.0, "number": 101.0, "hash": head.Hash, "parentHash": head.ParentHash,
		"timestamp": 1794736932.0}
	for k, v := range want {
		if got.Data[k] != v {
			t.Errorf("data.%s = %v, want %v", k, got.Data[k], v)
		}
	}
	again, _ := NewHead(31337, head)
	if again.ID != ev.ID {
		t.Error("the ID of a block is not deterministic")
	}
}

func TestReorgMatchesContract(t *testing.T) {
	ev, err := Reorg(31337, 100, head.Hash)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := codec(t).Encode(ev)
	if err != nil {
		t.Fatalf("contract: %v", err)
	}
	if ev.ID != "31337:reorg:100:"+head.Hash || ev.Subject != "31337" || ev.Type != TypeReorg {
		t.Fatalf("envelope = %+v", ev)
	}
	var got struct {
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got.Data["chainId"] != 31337.0 || got.Data["fromBlock"] != 100.0 || len(got.Data) != 2 {
		t.Fatalf("data = %v", got.Data)
	}
}

func TestReorgFromGenesisViolatesContract(t *testing.T) {
	ev, err := Reorg(31337, 0, head.Hash)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := codec(t).Encode(ev); err == nil {
		t.Fatal("fromBlock 0 passed the schema")
	}
}

type recorder struct {
	topic string
	evs   []kafka.Event
	err   error
}

func (r *recorder) Publish(_ context.Context, topic string, ev kafka.Event) error {
	r.topic = topic
	r.evs = append(r.evs, ev)
	return r.err
}

func TestPublisher(t *testing.T) {
	r := &recorder{}
	p := NewPublisher(r, TopicBlocks)
	if err := p.PublishNewHead(context.Background(), 31337, head); err != nil {
		t.Fatal(err)
	}
	if err := p.PublishReorg(context.Background(), 31337, 100, head.Hash); err != nil {
		t.Fatal(err)
	}
	if r.topic != TopicBlocks || len(r.evs) != 2 || r.evs[0].Type != TypeNewHead || r.evs[1].Type != TypeReorg {
		t.Fatalf("published %s %+v", r.topic, r.evs)
	}
	r.err = errors.New("broker down")
	if err := p.PublishNewHead(context.Background(), 31337, head); !errors.Is(err, r.err) {
		t.Fatalf("err = %v", err)
	}
	if err := p.PublishReorg(context.Background(), 31337, 100, head.Hash); !errors.Is(err, r.err) {
		t.Fatalf("err = %v", err)
	}
}
