package tracked

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"maps"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/m5tshift/slopay/services/chain-watcher/internal/projection"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// memStore is the projection: address -> searched, with the end of the window.
type memStore struct {
	searched map[string]bool
	until    map[string]time.Time
	err      error
	lookups  int
	marked   [][]string
	keep     map[string]bool // Delete leaves them (window extended meanwhile)
}

func newStore() *memStore {
	return &memStore{searched: map[string]bool{}, until: map[string]time.Time{}, keep: map[string]bool{}}
}

func (s *memStore) Lookup(_ context.Context, addrs []string) (map[string]bool, error) {
	s.lookups++
	if s.err != nil {
		return nil, s.err
	}
	out := map[string]bool{}
	for _, a := range addrs {
		if s.searched[a] {
			out[a] = true
		}
	}
	return out, nil
}

func (s *memStore) Pending(_ context.Context, now time.Time, limit int) ([]projection.Pending, error) {
	if s.err != nil {
		return nil, s.err
	}
	var out []projection.Pending
	for a, ok := range s.searched {
		if !ok && !s.until[a].Before(now) && len(out) < limit {
			out = append(out, projection.Pending{Address: a})
		}
	}
	return out, nil
}

func (s *memStore) MarkRescanned(_ context.Context, addrs []string) error {
	if s.err != nil {
		return s.err
	}
	s.marked = append(s.marked, addrs)
	for _, a := range addrs {
		if _, ok := s.searched[a]; ok {
			s.searched[a] = true
		}
	}
	return nil
}

func (s *memStore) Expired(_ context.Context, now time.Time, limit int) ([]string, error) {
	if s.err != nil {
		return nil, s.err
	}
	var out []string
	for _, a := range slices.Sorted(maps.Keys(s.until)) {
		if s.until[a].Before(now) && !s.keep[a] && len(out) < limit {
			out = append(out, a)
		}
	}
	for a := range s.keep {
		if len(out) < limit && !slices.Contains(out, a) {
			out = append(out, a)
		}
	}
	return out, nil
}

func (s *memStore) Delete(_ context.Context, addrs []string, _ time.Time) error {
	if s.err != nil {
		return s.err
	}
	for _, a := range addrs {
		if !s.keep[a] {
			delete(s.searched, a)
			delete(s.until, a)
		}
	}
	return nil
}

func (s *memStore) Searched(_ context.Context, batch int, fn func([]string) error) error {
	if s.err != nil {
		return s.err
	}
	var all []string
	for a, ok := range s.searched {
		if ok {
			all = append(all, a)
		}
	}
	sort.Strings(all)
	for i := 0; i < len(all); i += batch {
		if err := fn(all[i:min(i+batch, len(all))]); err != nil {
			return err
		}
	}
	return nil
}

func (s *memStore) add(a string, searched bool, until time.Time) {
	s.searched[a], s.until[a] = searched, until
}

// memCache is a Valkey set; down makes every call fail, lost drops the content as a restart does.
type memCache struct {
	set     map[string]bool
	down    bool
	loads   int
	members int
}

func (c *memCache) Members(_ context.Context, token string, addrs []string) (map[string]bool, error) {
	if c.down {
		return nil, ErrNoClient
	}
	c.members++
	if !c.set[tokenPrefix+token] {
		return nil, ErrStale
	}
	out := map[string]bool{}
	for _, a := range addrs {
		if c.set[a] {
			out[a] = true
		}
	}
	return out, nil
}

func (c *memCache) Load(_ context.Context, token string, fill func(func([]string) error) error) error {
	if c.down {
		return ErrNoClient
	}
	c.loads++
	c.set = map[string]bool{}
	if err := fill(func(a []string) error { return c.Add(context.Background(), a) }); err != nil {
		return err
	}
	return c.Add(context.Background(), []string{tokenPrefix + token})
}

func (c *memCache) Add(_ context.Context, addrs []string) error {
	if c.down {
		return ErrNoClient
	}
	for _, a := range addrs {
		c.set[a] = true
	}
	return nil
}

func (c *memCache) Remove(_ context.Context, addrs []string) error {
	if c.down {
		return ErrNoClient
	}
	for _, a := range addrs {
		delete(c.set, a)
	}
	return nil
}

type fixture struct {
	b     *Book
	store *memStore
	cache *memCache
	now   time.Time
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{store: newStore(), cache: &memCache{set: map[string]bool{}}, now: time.Unix(1_700_000_000, 0)}
	b, err := NewBook(f.store, f.cache, 5*time.Second, nil, quiet)
	if err != nil {
		t.Fatal(err)
	}
	b.now = func() time.Time { return f.now }
	f.b = b
	return f
}

var (
	ctx   = context.Background()
	chain = time.Unix(1_800_000_000, 0)
)

func (f *fixture) tracked(t *testing.T, addrs ...string) []string {
	t.Helper()
	m, err := f.b.Tracked(ctx, addrs)
	if err != nil {
		t.Fatal(err)
	}
	return slices.Sorted(maps.Keys(m))
}

func TestTrackedFromCache(t *testing.T) {
	f := newFixture(t)
	f.store.add("0xa", true, chain)
	f.store.add("0xb", false, chain) // not searched yet
	if got := f.tracked(t, "0xa", "0xb", "0xc"); !slices.Equal(got, []string{"0xa"}) {
		t.Fatalf("tracked %v", got)
	}
	if f.cache.loads != 1 || f.store.lookups != 0 {
		t.Fatalf("loads %d, lookups %d", f.cache.loads, f.store.lookups)
	}
	f.tracked(t, "0xa")
	if f.cache.loads != 1 || f.cache.members != 2 {
		t.Fatalf("loads %d, members %d", f.cache.loads, f.cache.members)
	}
	if got, err := f.b.Tracked(ctx, nil); err != nil || len(got) != 0 || f.cache.members != 2 {
		t.Fatalf("empty: %v %v", got, err)
	}
}

// Without Valkey the projection answers; the cache is loaded again at most once per retry.
func TestTrackedWithoutValkey(t *testing.T) {
	f := newFixture(t)
	f.store.add("0xa", true, chain)
	f.cache.down = true
	for range 3 {
		if got := f.tracked(t, "0xa", "0xc"); !slices.Equal(got, []string{"0xa"}) {
			t.Fatalf("tracked %v", got)
		}
	}
	if f.store.lookups != 3 {
		t.Fatalf("lookups %d", f.store.lookups)
	}
	f.cache.down = false
	f.tracked(t, "0xa")
	if f.cache.loads != 0 || f.store.lookups != 4 {
		t.Fatalf("loaded before the retry: loads %d", f.cache.loads)
	}
	f.now = f.now.Add(5 * time.Second)
	f.tracked(t, "0xa")
	if f.cache.loads != 1 || f.store.lookups != 4 {
		t.Fatalf("loads %d, lookups %d", f.cache.loads, f.store.lookups)
	}
}

// A Valkey that restarted has lost the token: the next check loads the cache again.
func TestTrackedAfterValkeyLostData(t *testing.T) {
	f := newFixture(t)
	f.store.add("0xa", true, chain)
	f.tracked(t, "0xa")
	f.cache.set = map[string]bool{}
	if got := f.tracked(t, "0xa"); !slices.Equal(got, []string{"0xa"}) || f.store.lookups != 1 {
		t.Fatalf("stale cache: %v, lookups %d", got, f.store.lookups)
	}
	if got := f.tracked(t, "0xa"); !slices.Equal(got, []string{"0xa"}) || f.cache.loads != 2 {
		t.Fatalf("reload: %v, loads %d", got, f.cache.loads)
	}
	f.store.err = errors.New("mongodb down")
	f.cache.down = true
	if _, err := f.b.Tracked(ctx, []string{"0xa"}); err == nil {
		t.Fatal("no error without both stores")
	}
}

func TestRescannedAddsToCache(t *testing.T) {
	f := newFixture(t)
	f.store.add("0xa", false, chain)
	f.tracked(t, "0xa") // loads the cache
	if err := f.b.Rescanned(ctx, []string{"0xa"}); err != nil {
		t.Fatal(err)
	}
	if !f.cache.set["0xa"] || !f.store.searched["0xa"] {
		t.Fatalf("cache %v, store %v", f.cache.set, f.store.searched)
	}
	f.cache.down = true
	f.store.add("0xb", false, chain)
	if err := f.b.Rescanned(ctx, []string{"0xb"}); err != nil {
		t.Fatal(err)
	}
	if f.b.loaded != "" {
		t.Fatal("cache kept after a failed write")
	}
	f.store.err = errors.New("mongodb down")
	if err := f.b.Rescanned(ctx, []string{"0xb"}); err == nil {
		t.Fatal("no error")
	}
}

func TestExpireRemovesAddresses(t *testing.T) {
	f := newFixture(t)
	f.store.add("0xa", true, chain.Add(-time.Second))
	f.store.add("0xb", true, chain.Add(time.Hour))
	f.store.add("0xc", true, chain.Add(-time.Hour))
	f.store.keep["0xc"] = true // extended between the query and the delete
	f.tracked(t, "0xa")
	if err := f.b.Expire(ctx, chain); err != nil {
		t.Fatal(err)
	}
	if f.cache.set["0xa"] || !f.cache.set["0xb"] || !f.cache.set["0xc"] {
		t.Fatalf("cache %v", f.cache.set)
	}
	if _, ok := f.store.until["0xa"]; ok {
		t.Fatal("0xa kept in the projection")
	}
	f.store.add("0xd", true, chain.Add(-time.Second))
	f.cache.down = true
	if err := f.b.Expire(ctx, chain); err != nil || f.b.loaded != "" {
		t.Fatalf("err = %v, loaded %q", err, f.b.loaded)
	}
	f.store.err = errors.New("mongodb down")
	if err := f.b.Expire(ctx, chain); err == nil {
		t.Fatal("no error")
	}
}

func TestExpireInBatches(t *testing.T) {
	f := newFixture(t)
	for i := range batch + 5 {
		f.store.add(time.Duration(i).String(), true, chain.Add(-time.Second))
	}
	if err := f.b.Expire(ctx, chain); err != nil {
		t.Fatal(err)
	}
	if len(f.store.until) != 0 {
		t.Fatalf("%d left", len(f.store.until))
	}
}

func TestExpireFailures(t *testing.T) {
	f := newFixture(t)
	f.store.add("0xa", true, chain.Add(-time.Second))
	del := &failingDelete{memStore: f.store}
	f.b.store = del
	if err := f.b.Expire(ctx, chain); err == nil {
		t.Fatal("delete: no error")
	}
	del.lookupErr, del.deleteOK = errors.New("mongodb down"), true
	if err := f.b.Expire(ctx, chain); err == nil {
		t.Fatal("lookup: no error")
	}
}

type failingDelete struct {
	*memStore
	deleteOK  bool
	lookupErr error
}

func (d *failingDelete) Delete(ctx context.Context, addrs []string, now time.Time) error {
	if !d.deleteOK {
		return errors.New("mongodb down")
	}
	return d.memStore.Delete(ctx, addrs, now)
}

func (d *failingDelete) Lookup(ctx context.Context, addrs []string) (map[string]bool, error) {
	if d.lookupErr != nil {
		return nil, d.lookupErr
	}
	return d.memStore.Lookup(ctx, addrs)
}

func TestPending(t *testing.T) {
	f := newFixture(t)
	f.store.add("0xa", false, chain)
	f.store.add("0xb", false, chain.Add(-time.Second))
	f.store.add("0xc", true, chain)
	p, err := f.b.Pending(ctx, chain)
	if err != nil || len(p) != 1 || p[0].Address != "0xa" {
		t.Fatalf("pending %v %v", p, err)
	}
}

func TestLoadFailureFallsBack(t *testing.T) {
	f := newFixture(t)
	f.store.add("0xa", true, chain)
	f.store.err = errors.New("mongodb down")
	if _, err := f.b.Tracked(ctx, []string{"0xa"}); err == nil {
		t.Fatal("no error")
	}
	if f.b.loaded != "" {
		t.Fatal("cache used after a failed load")
	}
}
