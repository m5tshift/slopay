package leader

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"k8s.io/client-go/kubernetes/fake"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func eventually(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestNewValidation(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Error("empty name accepted")
	}
	if _, err := New(Config{Name: "x", Client: fake.NewSimpleClientset()}); err == nil {
		t.Error("missing namespace/identity accepted")
	}
	l, err := New(Config{Name: "x", Namespace: "ns", Identity: "a", Client: fake.NewSimpleClientset()})
	if err != nil {
		t.Fatal(err)
	}
	if l.cfg.LeaseDuration != DefaultLeaseDuration || l.cfg.RenewDeadline != DefaultRenewDeadline || l.cfg.RetryPeriod != DefaultRetryPeriod {
		t.Error("defaults not applied")
	}
}

func TestAlwaysLeader(t *testing.T) {
	l, err := New(Config{Name: "x", Logger: quiet()})
	if err != nil {
		t.Fatal(err)
	}
	if l.IsLeader() {
		t.Fatal("leader before Run")
	}
	ran := false
	if err := l.Run(context.Background(), func(context.Context) {
		ran = true
		if !l.IsLeader() {
			t.Error("not leader while working")
		}
	}); err != nil {
		t.Fatal(err)
	}
	if !ran {
		t.Error("work not run")
	}
	if l.IsLeader() {
		t.Error("still leader after Run")
	}
}

func fast(_, id string, c *fake.Clientset) Config {
	return Config{Name: "lease", Namespace: "apps", Identity: id, Client: c, Logger: quiet(),
		LeaseDuration: 2 * time.Second, RenewDeadline: time.Second, RetryPeriod: 100 * time.Millisecond}
}

func TestLeadershipTransfer(t *testing.T) {
	client := fake.NewSimpleClientset()
	a, err := New(fast("lease", "a", client))
	if err != nil {
		t.Fatal(err)
	}
	b, err := New(fast("lease", "b", client))
	if err != nil {
		t.Fatal(err)
	}

	aCtx, aCancel := context.WithCancel(context.Background())
	aStopped := make(chan struct{})
	aWorkDone := make(chan struct{})
	go func() {
		defer close(aStopped)
		_ = a.Run(aCtx, func(c context.Context) { <-c.Done(); close(aWorkDone) })
	}()
	eventually(t, a.IsLeader)

	bCtx, bCancel := context.WithCancel(context.Background())
	bStopped := make(chan struct{})
	bWorking := make(chan struct{})
	bWorkDone := make(chan struct{})
	go func() {
		defer close(bStopped)
		_ = b.Run(bCtx, func(c context.Context) { close(bWorking); <-c.Done(); close(bWorkDone) })
	}()
	time.Sleep(700 * time.Millisecond)
	if b.IsLeader() {
		t.Fatal("b leads while a holds the lease")
	}

	aCancel() // a releases the lease
	<-aStopped
	<-aWorkDone
	if a.IsLeader() {
		t.Error("a still leader after stop")
	}
	select {
	case <-bWorking:
	case <-time.After(10 * time.Second):
		t.Fatal("b did not take over")
	}
	if !b.IsLeader() {
		t.Error("b not leader after takeover")
	}
	bCancel()
	<-bStopped
	<-bWorkDone
	if b.IsLeader() {
		t.Error("b still leader after stop")
	}
}

func TestGauge(t *testing.T) {
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	l, err := New(Config{Name: "x", Logger: quiet(), Meter: mp.Meter("test")})
	if err != nil {
		t.Fatal(err)
	}
	read := func() int64 {
		var rm metricdata.ResourceMetrics
		if err := reader.Collect(context.Background(), &rm); err != nil {
			t.Fatal(err)
		}
		g := rm.ScopeMetrics[0].Metrics[0].Data.(metricdata.Gauge[int64])
		return g.DataPoints[0].Value
	}
	if read() != 0 {
		t.Error("gauge not 0 before leading")
	}
	l.setLeader(true)
	if read() != 1 {
		t.Error("gauge not 1 while leading")
	}
}
