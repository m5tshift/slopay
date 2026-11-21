// Package events maps a reconciliation report to the events of contracts/asyncapi/reconciliation-service.yaml on
// reconciliation.events (key and subject runId): one reconciliation.mismatch per discrepancy, then
// reconciliation.completed.
package events

import (
	"context"
	"fmt"

	"github.com/m5tshift/weigate/pkg/kafka"
	"github.com/m5tshift/weigate/services/reconciliation-service/internal/recon"
)

// Event source, types and topic of the contract.
const (
	EventSource   = "/weigate/reconciliation-service"
	TypeMismatch  = "reconciliation.mismatch"
	TypeCompleted = "reconciliation.completed"
	Topic         = "reconciliation.events"
)

type assetData struct {
	Kind     string `json:"kind"`
	Contract string `json:"contract,omitempty"`
	Decimals int    `json:"decimals"`
}

type mismatchData struct {
	RunID         string    `json:"runId"`
	MerchantID    string    `json:"merchantId"`
	Address       string    `json:"address"`
	Asset         assetData `json:"asset"`
	Kind          string    `json:"kind"`
	ChainBalance  string    `json:"chainBalance"`
	InvoicesTotal string    `json:"invoicesTotal"`
	LedgerTotal   string    `json:"ledgerTotal"`
}

type completedData struct {
	RunID            string `json:"runId"`
	ChainID          uint64 `json:"chainId"`
	BlockNumber      uint64 `json:"blockNumber"`
	StartedAt        string `json:"startedAt"`
	FinishedAt       string `json:"finishedAt"`
	AddressesChecked int    `json:"addressesChecked"`
	Mismatches       int    `json:"mismatches"`
	Result           string `json:"result"`
}

const timeFormat = "2006-01-02T15:04:05.000Z07:00"

// Mismatch builds reconciliation.mismatch of a discrepancy row. The ID is <runId>:<address>:<ETH or contract>.
func Mismatch(r recon.Report, row recon.Row) (kafka.Event, error) {
	ev, err := kafka.NewEvent(EventSource, TypeMismatch, r.RunID, 1, mismatchData{
		RunID: r.RunID, MerchantID: row.MerchantID, Address: row.Address,
		Asset: assetData{Kind: row.Asset.Kind, Contract: row.Asset.Contract, Decimals: row.Asset.Decimals},
		Kind:  row.Kind, ChainBalance: row.ChainBalance.String(), InvoicesTotal: row.InvoicesTotal.String(),
		LedgerTotal: row.LedgerTotal.String(),
	})
	ev.ID = r.RunID + ":" + row.Address + ":" + row.Asset.Key()
	ev.Time = r.FinishedAt.UTC()
	return ev, err
}

// Completed builds reconciliation.completed of a report. The ID is <runId>:completed.
func Completed(r recon.Report) (kafka.Event, error) {
	ev, err := kafka.NewEvent(EventSource, TypeCompleted, r.RunID, 1, completedData{
		RunID: r.RunID, ChainID: r.ChainID, BlockNumber: r.BlockNumber,
		StartedAt: r.StartedAt.UTC().Format(timeFormat), FinishedAt: r.FinishedAt.UTC().Format(timeFormat),
		AddressesChecked: r.AddressesChecked, Mismatches: r.Mismatches, Result: r.Result(),
	})
	ev.ID = r.RunID + ":completed"
	ev.Time = r.FinishedAt.UTC()
	return ev, err
}

// Producer is the part of kafka.Producer the publisher needs.
type Producer interface {
	Publish(ctx context.Context, topic string, ev kafka.Event) error
}

// Publisher publishes the events of reports.
type Publisher struct {
	producer Producer
}

var _ recon.Publisher = (*Publisher)(nil)

// NewPublisher returns a publisher on p.
func NewPublisher(p Producer) *Publisher { return &Publisher{producer: p} }

// Publish publishes the mismatches of r in the order of its rows, then completed. The IDs depend only on the
// report, so a report published again yields the same events, which consumers deduplicate.
func (p *Publisher) Publish(ctx context.Context, r recon.Report) error {
	for _, row := range r.Rows {
		if !row.Mismatch() {
			continue
		}
		ev, err := Mismatch(r, row)
		if err != nil {
			return err
		}
		if err := p.producer.Publish(ctx, Topic, ev); err != nil {
			return fmt.Errorf("events: %s %s: %w", ev.Type, ev.ID, err)
		}
	}
	ev, err := Completed(r)
	if err != nil {
		return err
	}
	if err := p.producer.Publish(ctx, Topic, ev); err != nil {
		return fmt.Errorf("events: %s %s: %w", ev.Type, ev.ID, err)
	}
	return nil
}
