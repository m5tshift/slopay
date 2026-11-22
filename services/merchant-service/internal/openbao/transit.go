package openbao

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// TransitSign signs input with the ECDSA P-256 key `key` of the Transit mount (SHA-256, JWS marshaling:
// the signature is R || S, 64 bytes) and returns the key version that signed. The private key never leaves
// OpenBao.
func (c *Client) TransitSign(ctx context.Context, mount, key string, input []byte) (version int, sig []byte, err error) {
	body, err := json.Marshal(map[string]any{
		"input":                base64.StdEncoding.EncodeToString(input),
		"hash_algorithm":       "sha2-256",
		"marshaling_algorithm": "jws",
	})
	if err != nil {
		return 0, nil, err
	}
	out, err := c.do(ctx, http.MethodPost, transitPath(mount, "sign", key), body)
	if err != nil {
		return 0, nil, err
	}
	var resp struct {
		Data struct {
			Signature string `json:"signature"`
		} `json:"data"`
	}
	if err := json.Unmarshal(out, &resp); err != nil {
		return 0, nil, fmt.Errorf("%w: bad sign response", ErrUnavailable)
	}
	// vault:v<version>:<base64url signature>
	parts := strings.Split(resp.Data.Signature, ":")
	if len(parts) != 3 || parts[0] != "vault" || !strings.HasPrefix(parts[1], "v") {
		return 0, nil, fmt.Errorf("%w: bad signature format", ErrUnavailable)
	}
	version, err = strconv.Atoi(parts[1][1:])
	if err != nil || version < 1 {
		return 0, nil, fmt.Errorf("%w: bad key version", ErrUnavailable)
	}
	sig, err = base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || len(sig) != 64 {
		return 0, nil, fmt.Errorf("%w: bad signature encoding", ErrUnavailable)
	}
	return version, sig, nil
}

// TransitKeys returns the public keys of the Transit key, PEM by version, and the latest version.
func (c *Client) TransitKeys(ctx context.Context, mount, key string) (latest int, pems map[int]string, err error) {
	out, err := c.do(ctx, http.MethodGet, transitPath(mount, "keys", key), nil)
	if err != nil {
		return 0, nil, err
	}
	var resp struct {
		Data struct {
			Type          string                    `json:"type"`
			LatestVersion int                       `json:"latest_version"`
			Keys          map[string]map[string]any `json:"keys"`
		} `json:"data"`
	}
	if err := json.Unmarshal(out, &resp); err != nil {
		return 0, nil, fmt.Errorf("%w: bad keys response", ErrUnavailable)
	}
	if resp.Data.Type != "ecdsa-p256" {
		return 0, nil, fmt.Errorf("%w: transit key is not ecdsa-p256", ErrUnavailable)
	}
	pems = make(map[int]string, len(resp.Data.Keys))
	for v, k := range resp.Data.Keys {
		n, err := strconv.Atoi(v)
		pem, _ := k["public_key"].(string)
		if err != nil || pem == "" {
			return 0, nil, fmt.Errorf("%w: bad key entry", ErrUnavailable)
		}
		pems[n] = pem
	}
	if _, ok := pems[resp.Data.LatestVersion]; !ok {
		return 0, nil, fmt.Errorf("%w: no latest key version", ErrUnavailable)
	}
	return resp.Data.LatestVersion, pems, nil
}

func transitPath(mount, op, key string) string {
	return "/v1/" + strings.Trim(mount, "/") + "/" + op + "/" + key
}

// TransitKey is one Transit key of a Client; it implements jwt.Transit.
type TransitKey struct {
	c          *Client
	mount, key string
}

// Transit binds the Transit mount and key name.
func (c *Client) Transit(mount, key string) *TransitKey {
	return &TransitKey{c: c, mount: mount, key: key}
}

// Sign implements jwt.Transit.
func (k *TransitKey) Sign(ctx context.Context, input []byte) (int, []byte, error) {
	return k.c.TransitSign(ctx, k.mount, k.key, input)
}

// Keys implements jwt.Transit.
func (k *TransitKey) Keys(ctx context.Context) (int, map[int]string, error) {
	return k.c.TransitKeys(ctx, k.mount, k.key)
}
