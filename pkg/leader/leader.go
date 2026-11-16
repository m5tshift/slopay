// Package leader runs a piece of work on exactly one replica, chosen through a
// Kubernetes Lease (SRS 4.1.3). Without a Kubernetes client the replica is always
// the leader, which is meant for tests and local runs.
package leader

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// Default timings of client-go's leader election examples. The Lease stores whole
// seconds, so LeaseDuration must be at least 1s.
const (
	DefaultLeaseDuration = 15 * time.Second
	DefaultRenewDeadline = 10 * time.Second
	DefaultRetryPeriod   = 2 * time.Second
)

// Config configures a Leader.
type Config struct {
	// Name is the Lease object name, one per component.
	Name string
	// Namespace is the Lease namespace.
	Namespace string
	// Identity is unique per replica, usually the pod name.
	Identity string
	// Client is the Kubernetes client. Nil selects the always-leader mode.
	Client kubernetes.Interface
	// LeaseDuration, RenewDeadline and RetryPeriod default to the Default* values.
	LeaseDuration time.Duration
	RenewDeadline time.Duration
	RetryPeriod   time.Duration
	// Logger defaults to slog.Default().
	Logger *slog.Logger
	// Meter registers the slopay_leader_is_leader gauge; defaults to the global OTel meter.
	Meter metric.Meter
}

// Leader reports and drives leadership of one replica.
type Leader struct {
	cfg      Config
	log      *slog.Logger
	isLeader atomic.Bool
}

// New validates the configuration and registers the "is leader" gauge (1 while leading, else 0).
func New(cfg Config) (*Leader, error) {
	if cfg.Name == "" {
		return nil, errors.New("leader: name is required")
	}
	if cfg.Client != nil {
		if cfg.Namespace == "" || cfg.Identity == "" {
			return nil, errors.New("leader: namespace and identity are required with a Kubernetes client")
		}
		if cfg.LeaseDuration == 0 {
			cfg.LeaseDuration = DefaultLeaseDuration
		}
		if cfg.RenewDeadline == 0 {
			cfg.RenewDeadline = DefaultRenewDeadline
		}
		if cfg.RetryPeriod == 0 {
			cfg.RetryPeriod = DefaultRetryPeriod
		}
	}
	l := &Leader{cfg: cfg, log: cfg.Logger}
	if l.log == nil {
		l.log = slog.Default()
	}
	m := cfg.Meter
	if m == nil {
		m = otel.Meter("github.com/m5tshift/slopay/pkg/leader")
	}
	_, err := m.Int64ObservableGauge("slopay_leader_is_leader",
		metric.WithDescription("1 if this replica currently holds the lease, else 0"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			var v int64
			if l.IsLeader() {
				v = 1
			}
			o.Observe(v, metric.WithAttributes(attrComponent(cfg.Name)))
			return nil
		}))
	if err != nil {
		return nil, fmt.Errorf("leader: gauge: %w", err)
	}
	return l, nil
}

// IsLeader reports whether this replica currently leads.
func (l *Leader) IsLeader() bool { return l.isLeader.Load() }

// Run blocks until ctx is cancelled. Each time this replica becomes leader it calls
// work with a context that is cancelled when leadership is lost; Run then waits for
// work to return before competing again. In the always-leader mode work runs once with ctx.
func (l *Leader) Run(ctx context.Context, work func(ctx context.Context)) error {
	if l.cfg.Client == nil {
		l.setLeader(true)
		defer l.setLeader(false)
		work(ctx)
		return nil
	}
	lock := &resourcelock.LeaseLock{
		LeaseMeta:  metav1.ObjectMeta{Name: l.cfg.Name, Namespace: l.cfg.Namespace},
		Client:     l.cfg.Client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{Identity: l.cfg.Identity},
	}
	elector, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:            lock,
		LeaseDuration:   l.cfg.LeaseDuration,
		RenewDeadline:   l.cfg.RenewDeadline,
		RetryPeriod:     l.cfg.RetryPeriod,
		ReleaseOnCancel: true,
		Name:            l.cfg.Name,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(c context.Context) {
				l.setLeader(true)
				l.log.Info("leadership acquired", "lease", l.cfg.Name, "identity", l.cfg.Identity)
				work(c)
			},
			OnStoppedLeading: func() {
				l.setLeader(false)
				l.log.Info("leadership lost", "lease", l.cfg.Name, "identity", l.cfg.Identity)
			},
		},
	})
	if err != nil {
		return fmt.Errorf("leader: elector: %w", err)
	}
	for ctx.Err() == nil {
		elector.Run(ctx) // returns when leadership is lost or ctx is done
	}
	return nil
}

func (l *Leader) setLeader(v bool) { l.isLeader.Store(v) }
