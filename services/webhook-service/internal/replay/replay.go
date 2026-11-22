// Package replay resends webhooks from webhook.dlq on the operator's request (SRS 4.1.3, FR-09). A record cannot be
// deleted from a Kafka topic, so a delivery "leaves the DLQ" when the journal holds a delivered replay of it
// (<event id>:dlq); such records are skipped by the next call. A replay that fails changes nothing: the record stays.
package replay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/m5tshift/weigate/pkg/kafka"
)

const (
	defaultLimit = 100
	maxLimit     = 1000
)

// DLQ returns all records of webhook.dlq, oldest first.
type DLQ interface {
	Events(ctx context.Context) ([]kafka.Event, error)
}

// Deliverer makes one new attempt of a dead-lettered delivery.
type Deliverer interface {
	Replay(ctx context.Context, ev kafka.Event) (bool, error)
}

// Journal tells whether a dead-lettered delivery was already replayed successfully.
type Journal interface {
	Delivered(ctx context.Context, dlqEventID string) (bool, error)
}

// Request is the body of POST /internal/dlq/replay.
type Request struct {
	InvoiceID  string `json:"invoiceId"`
	MerchantID string `json:"merchantId"`
	Limit      int    `json:"limit"`
}

// Result is the answer of POST /internal/dlq/replay.
type Result struct {
	Selected  int `json:"selected"`
	Delivered int `json:"delivered"`
	Failed    int `json:"failed"`
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Handler serves POST /internal/dlq/replay.
type Handler struct {
	DLQ       DLQ
	Deliverer Deliverer
	Journal   Journal
	Log       *slog.Logger
}

// Register adds the endpoint to mux.
func (h *Handler) Register(mux *http.ServeMux) { mux.HandleFunc("POST /internal/dlq/replay", h.serve) }

func (h *Handler) serve(w http.ResponseWriter, r *http.Request) {
	req, err := parse(r.Body)
	if err != nil {
		write(w, http.StatusBadRequest, apiError{"validation_failed", err.Error()})
		return
	}
	res, err := h.run(r.Context(), req)
	if err != nil {
		h.Log.ErrorContext(r.Context(), "dlq replay", "error", err)
		w.Header().Set("Retry-After", "30")
		write(w, http.StatusServiceUnavailable, apiError{"dlq_unavailable", "The DLQ is temporarily unavailable"})
		return
	}
	write(w, http.StatusOK, res)
}

func parse(body io.Reader) (Request, error) {
	var req Request
	dec := json.NewDecoder(body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		return req, errors.New("invalid JSON body")
	}
	for name, v := range map[string]string{"invoiceId": req.InvoiceID, "merchantId": req.MerchantID} {
		if _, err := uuid.Parse(v); v != "" && err != nil {
			return req, fmt.Errorf("%s must be a UUID", name)
		}
	}
	switch {
	case req.Limit == 0:
		req.Limit = defaultLimit
	case req.Limit < 1 || req.Limit > maxLimit:
		return req, fmt.Errorf("limit must be between 1 and %d", maxLimit)
	}
	return req, nil
}

func (h *Handler) run(ctx context.Context, req Request) (Result, error) {
	events, err := h.DLQ.Events(ctx)
	if err != nil {
		return Result{}, err
	}
	sort.SliceStable(events, func(i, j int) bool { return events[i].Time.Before(events[j].Time) })
	var res Result
	for _, ev := range events {
		if res.Selected >= req.Limit {
			break
		}
		var d struct {
			InvoiceID  string `json:"invoiceId"`
			MerchantID string `json:"merchantId"`
		}
		if err := json.Unmarshal(ev.Data, &d); err != nil ||
			(req.InvoiceID != "" && d.InvoiceID != req.InvoiceID) || (req.MerchantID != "" && d.MerchantID != req.MerchantID) {
			continue
		}
		done, err := h.Journal.Delivered(ctx, ev.ID)
		if err != nil {
			return res, err
		}
		if done {
			continue
		}
		res.Selected++
		ok, err := h.Deliverer.Replay(ctx, ev)
		switch {
		case err != nil:
			// The journal or the projection failed: nothing was decided about this delivery; count it as failed.
			h.Log.ErrorContext(ctx, "dlq replay of one delivery", "event_id", ev.ID, "error", err)
			res.Failed++
		case ok:
			res.Delivered++
		default:
			res.Failed++
		}
	}
	return res, nil
}

func write(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// Reader reads webhook.dlq from Kafka without a consumer group.
type Reader struct {
	Brokers []string
	Topic   string
	Codec   *kafka.Codec
	Log     *slog.Logger
}

// Events reads every record that is in the topic when the call starts. A record that is not a valid event is
// skipped.
func (r *Reader) Events(ctx context.Context) ([]kafka.Event, error) {
	admin, err := kgo.NewClient(kgo.SeedBrokers(r.Brokers...))
	if err != nil {
		return nil, fmt.Errorf("replay: kafka: %w", err)
	}
	defer admin.Close()
	ends, err := endOffsets(ctx, admin, r.Topic)
	if err != nil {
		return nil, err
	}
	start := map[int32]kgo.Offset{}
	left := 0
	for p, end := range ends {
		start[p] = kgo.NewOffset().AtStart()
		if end > 0 {
			left++
		}
	}
	if left == 0 {
		return nil, nil
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(r.Brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{r.Topic: start}))
	if err != nil {
		return nil, fmt.Errorf("replay: kafka: %w", err)
	}
	defer cl.Close()
	var out []kafka.Event
	for left > 0 {
		fs := cl.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := fs.Err0(); err != nil {
			return nil, fmt.Errorf("replay: read %s: %w", r.Topic, err)
		}
		fs.EachRecord(func(rec *kgo.Record) {
			if rec.Offset >= ends[rec.Partition] {
				return
			}
			if rec.Offset == ends[rec.Partition]-1 {
				left--
			}
			ev, err := r.Codec.Decode(rec.Value)
			if err != nil {
				r.Log.Warn("skipping invalid dlq record", "partition", rec.Partition, "offset", rec.Offset, "error", err)
				return
			}
			out = append(out, ev)
		})
	}
	return out, nil
}

// endOffsets returns the end offset of every partition of the topic.
func endOffsets(ctx context.Context, cl *kgo.Client, topic string) (map[int32]int64, error) {
	meta := kmsg.NewPtrMetadataRequest()
	mt := kmsg.NewMetadataRequestTopic()
	mt.Topic = &topic
	meta.Topics = append(meta.Topics, mt)
	mresp, err := meta.RequestWith(ctx, cl)
	if err != nil {
		return nil, fmt.Errorf("replay: metadata: %w", err)
	}
	req := kmsg.NewPtrListOffsetsRequest()
	req.ReplicaID = -1
	for _, t := range mresp.Topics {
		if t.ErrorCode != 0 {
			return nil, fmt.Errorf("replay: metadata of %s: error code %d", topic, t.ErrorCode)
		}
		lt := kmsg.NewListOffsetsRequestTopic()
		lt.Topic = topic
		for _, p := range t.Partitions {
			lp := kmsg.NewListOffsetsRequestTopicPartition()
			lp.Partition, lp.Timestamp = p.Partition, -1
			lt.Partitions = append(lt.Partitions, lp)
		}
		req.Topics = append(req.Topics, lt)
	}
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return nil, fmt.Errorf("replay: list offsets: %w", err)
	}
	ends := map[int32]int64{}
	for _, t := range resp.Topics {
		for _, p := range t.Partitions {
			if p.ErrorCode != 0 {
				return nil, fmt.Errorf("replay: end offset of %s/%d: error code %d", topic, p.Partition, p.ErrorCode)
			}
			ends[p.Partition] = p.Offset
		}
	}
	return ends, nil
}
