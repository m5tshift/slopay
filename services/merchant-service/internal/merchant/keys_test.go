package merchant

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestGenerateKey(t *testing.T) {
	k1, h1 := GenerateKey()
	k2, _ := GenerateKey()
	if k1 == k2 || !strings.HasPrefix(k1, KeyPrefix) || len(k1) > 256 {
		t.Fatalf("keys %d chars, equal=%v", len(k1), k1 == k2)
	}
	if !bytes.Equal(h1, HashKey(k1)) || len(h1) != 32 || bytes.Contains(h1, []byte(k1)) {
		t.Error("hash is not SHA-256 of the key")
	}
	if !wellFormedKey(k1) {
		t.Error("generated key is not well-formed")
	}
	for _, bad := range []string{"", "slp_live_", "other_" + k1[len(KeyPrefix):], k1 + "A", KeyPrefix + "!!!"} {
		if wellFormedKey(bad) {
			t.Errorf("%q accepted", bad)
		}
	}
}

func key(created time.Time, expires *time.Time) APIKey {
	return APIKey{ID: uuid.New(), CreatedAt: created, ExpiresAt: expires}
}

func TestPlanIssue(t *testing.T) {
	now := time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC)
	grace := 24 * time.Hour
	nk := NewKey{APIKey: key(now, nil)}

	t.Run("first rotation expires the old key", func(t *testing.T) {
		old := key(now.Add(-time.Hour), nil)
		ch, err := PlanIssue([]APIKey{old}, nk, now, grace, 2)
		if err != nil {
			t.Fatal(err)
		}
		if ch.Insert == nil || ch.Insert.ID != nk.ID || len(ch.Revoke) != 0 {
			t.Errorf("changes = %+v", ch)
		}
		if exp := ch.SetExpiry[old.ID]; exp == nil || !exp.Equal(now.Add(grace)) {
			t.Errorf("old key expiry = %v", exp)
		}
	})
	t.Run("already expiring keys keep their expiry", func(t *testing.T) {
		soon := now.Add(time.Hour)
		ch, err := PlanIssue([]APIKey{key(now, &soon)}, nk, now, grace, 3)
		if err != nil || len(ch.SetExpiry) != 0 {
			t.Errorf("changes = %+v, err %v", ch, err)
		}
	})
	t.Run("no active keys", func(t *testing.T) {
		ch, err := PlanIssue(nil, nk, now, grace, 2)
		if err != nil || ch.Insert == nil || len(ch.SetExpiry) != 0 {
			t.Errorf("changes = %+v, err %v", ch, err)
		}
	})
	t.Run("limit reached", func(t *testing.T) {
		soon := now.Add(time.Hour)
		_, err := PlanIssue([]APIKey{key(now, &soon), key(now, nil)}, nk, now, grace, 2)
		if !errors.Is(err, ErrTooManyKeys) {
			t.Errorf("err = %v", err)
		}
	})
}

func TestPlanRevoke(t *testing.T) {
	now := time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC)
	soon := now.Add(time.Hour)

	t.Run("unknown key", func(t *testing.T) {
		if _, err := PlanRevoke([]APIKey{key(now, nil)}, uuid.New()); !errors.Is(err, ErrKeyNotFound) {
			t.Errorf("err = %v", err)
		}
	})
	t.Run("last key", func(t *testing.T) {
		k := key(now, nil)
		if _, err := PlanRevoke([]APIKey{k}, k.ID); !errors.Is(err, ErrLastKey) {
			t.Errorf("err = %v", err)
		}
	})
	t.Run("revoke the expiring old key", func(t *testing.T) {
		old, cur := key(now.Add(-time.Hour), &soon), key(now, nil)
		ch, err := PlanRevoke([]APIKey{old, cur}, old.ID)
		if err != nil || len(ch.Revoke) != 1 || ch.Revoke[0] != old.ID || len(ch.SetExpiry) != 0 || ch.Insert != nil {
			t.Errorf("changes = %+v, err %v", ch, err)
		}
	})
	t.Run("revoke the new key keeps the newest remaining key", func(t *testing.T) {
		older, old, cur := key(now.Add(-2*time.Hour), &soon), key(now.Add(-time.Hour), &soon), key(now, nil)
		ch, err := PlanRevoke([]APIKey{old, older, cur}, cur.ID)
		if err != nil {
			t.Fatal(err)
		}
		exp, ok := ch.SetExpiry[old.ID]
		if !ok || exp != nil || len(ch.SetExpiry) != 1 {
			t.Errorf("expiry changes = %v, want old key without expiry", ch.SetExpiry)
		}
	})
}
