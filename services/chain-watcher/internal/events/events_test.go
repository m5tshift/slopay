package events

import (
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"testing"

	"github.com/m5tshift/weigate/pkg/kafka"
	"github.com/m5tshift/weigate/services/chain-watcher/internal/chain"
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

var transfer = chain.Transfer{
	Block: head,
	Tx: chain.Tx{
		Hash:  "0xd4e5f6a7d4e5f6a7d4e5f6a7d4e5f6a7d4e5f6a7d4e5f6a7d4e5f6a7d4e5f6a7",
		From:  "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266",
		To:    "0x8ba1f109551bd432803012645ac136ddd64dba72",
		Value: new(big.Int).Mul(big.NewInt(6), big.NewInt(1e15)),
	},
}

// TestTransferMatchesContract validates chain.transfer against contracts/schemas/chain.transfer.v1.json.
func TestTransferMatchesContract(t *testing.T) {
	ev, err := Transfer(31337, transfer)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := codec(t).Encode(ev)
	if err != nil {
		t.Fatalf("contract: %v", err)
	}
	if ev.ID != head.Hash+":"+transfer.Tx.Hash+":" || ev.Subject != transfer.Tx.To || ev.Type != TypeTransfer ||
		ev.Source != EventSource {
		t.Fatalf("envelope = %+v", ev)
	}
	var got struct {
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"chainId": 31337.0, "blockNumber": 101.0, "blockHash": head.Hash,
		"blockTimestamp": 1794736932.0, "txHash": transfer.Tx.Hash, "logIndex": nil, "from": transfer.Tx.From,
		"to": transfer.Tx.To, "amount": "6000000000000000"}
	for k, v := range want {
		if got.Data[k] != v {
			t.Errorf("data.%s = %v, want %v", k, got.Data[k], v)
		}
	}
	if _, ok := got.Data["logIndex"]; !ok {
		t.Error("logIndex is missing, the contract wants null")
	}
	if a, _ := got.Data["asset"].(map[string]any); a["kind"] != "ETH" || a["decimals"] != 18.0 || len(a) != 2 {
		t.Errorf("asset = %v", got.Data["asset"])
	}
	again, _ := Transfer(31337, transfer)
	if again.ID != ev.ID {
		t.Error("the ID of a transfer is not deterministic")
	}
	if _, err := Transfer(31337, chain.Transfer{Block: head, Tx: chain.Tx{Hash: transfer.Tx.Hash}}); err == nil {
		t.Error("transfer without a value accepted")
	}
}

type recorder struct {
	topics []string
	evs    []kafka.Event
	err    error
}

func (r *recorder) Publish(_ context.Context, topic string, ev kafka.Event) error {
	r.topics = append(r.topics, topic)
	r.evs = append(r.evs, ev)
	return r.err
}

func TestPublisher(t *testing.T) {
	r := &recorder{}
	p := NewPublisher(r, TopicBlocks, TopicTransfers)
	if err := p.PublishNewHead(context.Background(), 31337, head); err != nil {
		t.Fatal(err)
	}
	if err := p.PublishReorg(context.Background(), 31337, 100, head.Hash); err != nil {
		t.Fatal(err)
	}
	if err := p.PublishTransfer(context.Background(), 31337, transfer); err != nil {
		t.Fatal(err)
	}
	if len(r.evs) != 3 || r.evs[0].Type != TypeNewHead || r.evs[1].Type != TypeReorg || r.evs[2].Type != TypeTransfer {
		t.Fatalf("published %+v", r.evs)
	}
	if r.topics[0] != TopicBlocks || r.topics[1] != TopicBlocks || r.topics[2] != TopicTransfers {
		t.Fatalf("topics %v", r.topics)
	}
	r.err = errors.New("broker down")
	if err := p.PublishNewHead(context.Background(), 31337, head); !errors.Is(err, r.err) {
		t.Fatalf("err = %v", err)
	}
	if err := p.PublishReorg(context.Background(), 31337, 100, head.Hash); !errors.Is(err, r.err) {
		t.Fatalf("err = %v", err)
	}
	if err := p.PublishTransfer(context.Background(), 31337, transfer); !errors.Is(err, r.err) {
		t.Fatalf("err = %v", err)
	}
	if err := p.PublishTransfer(context.Background(), 31337, chain.Transfer{}); err == nil {
		t.Fatal("transfer without a value published")
	}
}
