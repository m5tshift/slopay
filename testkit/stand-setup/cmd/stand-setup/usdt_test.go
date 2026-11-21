package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

func TestDeployUSDTArgumentErrors(t *testing.T) {
	pk, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	testKey := common.Bytes2Hex(crypto.FromECDSA(pk))
	dir := t.TempDir()
	kf := filepath.Join(dir, "deployer")
	_ = os.WriteFile(kf, []byte(testKey+"\n"), 0o600)
	bad := filepath.Join(dir, "bad")
	_ = os.WriteFile(bad, []byte("nope\n"), 0o600)
	inRepo := filepath.Join(dir, "repo")
	_ = os.MkdirAll(filepath.Join(inRepo, ".git"), 0o750)
	repoKey := filepath.Join(inRepo, "k")
	_ = os.WriteFile(repoKey, []byte(testKey), 0o600)
	base := []string{"deploy-usdt", "-rpc-url", "http://127.0.0.1:1", "-deployer-key-file", kf}
	with := func(extra ...string) []string { return append(append([]string{}, base...), extra...) }
	cases := map[string]struct {
		args []string
		want string
	}{
		"bad flag":       {[]string{"deploy-usdt", "-nope"}, "flag provided"},
		"missing":        {[]string{"deploy-usdt"}, "are required"},
		"bad amount":     {with("-amount", "x"), "-amount"},
		"zero amount":    {with("-amount", "0"), "-amount"},
		"bad recipient":  {with("-recipients", "0x12"), "not an address"},
		"deployer repo":  {[]string{"deploy-usdt", "-rpc-url", "x", "-deployer-key-file", repoKey}, "outside a git repository"},
		"payer in repo":  {with("-payer-keys-file", repoKey), "outside a git repository"},
		"no deployer":    {[]string{"deploy-usdt", "-rpc-url", "x", "-deployer-key-file", filepath.Join(dir, "none")}, "read key file"},
		"bad deployer":   {[]string{"deploy-usdt", "-rpc-url", "x", "-deployer-key-file", bad}, "not a private key"},
		"bad payer keys": {with("-payer-keys-file", bad), "not a private key"},
		"bad rpc url":    {[]string{"deploy-usdt", "-rpc-url", "ftp://x", "-deployer-key-file", kf}, "dial anvil"},
		"unreachable":    {with(), "chain id"},
	}
	for name, tc := range cases {
		err := run(tc.args)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err %v", name, err)
		}
	}
}

func TestDedupe(t *testing.T) {
	a := common.HexToAddress("0x00000000000000000000000000000000000000a1")
	b := common.HexToAddress("0x00000000000000000000000000000000000000b2")
	got := dedupe([]common.Address{a, b, a})
	if len(got) != 2 || got[0] != a || got[1] != b {
		t.Fatalf("got %v", got)
	}
}
