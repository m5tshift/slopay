package merchant

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"strings"
	"time"

	"github.com/google/uuid"
)

// KeyPrefix starts every API key; it lets secret scanners recognise leaked keys.
const KeyPrefix = "slp_live_"

// keyBytes is the entropy of a key. 256 random bits make a fast hash (SHA-256) safe for storage:
// there is nothing to brute-force, unlike a password.
const keyBytes = 32

// GenerateKey returns a new API key and its hash.
func GenerateKey() (key string, hash []byte) {
	b := make([]byte, keyBytes)
	_, _ = rand.Read(b) // crypto/rand.Read never returns an error
	key = KeyPrefix + base64.RawURLEncoding.EncodeToString(b)
	return key, HashKey(key)
}

// HashKey is the stored form of an API key.
func HashKey(key string) []byte {
	sum := sha256.Sum256([]byte(key))
	return sum[:]
}

// wellFormedKey rejects values that cannot be a key before touching the database.
func wellFormedKey(key string) bool {
	rest, ok := strings.CutPrefix(key, KeyPrefix)
	if !ok {
		return false
	}
	raw, err := base64.RawURLEncoding.DecodeString(rest)
	return err == nil && len(raw) == keyBytes
}

// PlanIssue decides the changes for issuing a key (SRS 4.1.10): at most maxActive keys are active, and
// every key that has no expiry yet is deactivated grace after the new one is issued.
func PlanIssue(active []APIKey, newKey NewKey, now time.Time, grace time.Duration, maxActive int) (KeyChanges, error) {
	if len(active) >= maxActive {
		return KeyChanges{}, ErrTooManyKeys
	}
	ch := KeyChanges{Insert: &newKey, SetExpiry: map[uuid.UUID]*time.Time{}}
	expires := now.Add(grace)
	for _, k := range active {
		if k.ExpiresAt == nil {
			ch.SetExpiry[k.ID] = &expires
		}
	}
	return ch, nil
}

// PlanRevoke decides the changes for an emergency revoke: the key stops working at once. The last active
// key cannot be revoked. If only expiring keys remain, the newest of them stays active without expiry,
// so the merchant is never locked out by a planned deactivation.
func PlanRevoke(active []APIKey, keyID uuid.UUID) (KeyChanges, error) {
	found := false
	var rest []APIKey
	for _, k := range active {
		if k.ID == keyID {
			found = true
			continue
		}
		rest = append(rest, k)
	}
	switch {
	case !found:
		return KeyChanges{}, ErrKeyNotFound
	case len(rest) == 0:
		return KeyChanges{}, ErrLastKey
	}
	ch := KeyChanges{Revoke: []uuid.UUID{keyID}}
	newest := rest[0]
	for _, k := range rest {
		if k.ExpiresAt == nil {
			return ch, nil
		}
		if k.CreatedAt.After(newest.CreatedAt) {
			newest = k
		}
	}
	ch.SetExpiry = map[uuid.UUID]*time.Time{newest.ID: nil}
	return ch, nil
}
