package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunArgumentErrors(t *testing.T) {
	dir := t.TempDir()
	mn := filepath.Join(dir, "mn")
	_ = os.WriteFile(mn, []byte("one two"), 0o600)
	inRepo := filepath.Join(dir, "repo")
	_ = os.MkdirAll(filepath.Join(inRepo, ".git"), 0o750)
	full := func(extra ...string) []string {
		return append([]string{"-base-url", "http://127.0.0.1:1", "-mnemonic-file", mn,
			"-keys-file", filepath.Join(dir, "keys.json"), "-webhook-url", "https://h"}, extra...)
	}
	cases := map[string]struct {
		args []string
		want string
	}{
		"bad flag":     {[]string{"-nope"}, "flag provided"},
		"missing":      {[]string{"-base-url", "x"}, "is required"},
		"keys in repo": {append(full(), "-keys-file", filepath.Join(inRepo, "k.json")), "outside a git repository"},
		"no mnemonic":  {append(full(), "-mnemonic-file", filepath.Join(dir, "none")), "read mnemonic file"},
		"short phrase": {full(), "mnemonic must have"},
	}
	for name, tc := range cases {
		err := run(tc.args)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err %v", name, err)
		}
	}
}

func TestRunValidatesConfig(t *testing.T) {
	dir := t.TempDir()
	mn := filepath.Join(dir, "mn")
	_ = os.WriteFile(mn, []byte(strings.Repeat("w ", 12)), 0o600)
	err := run([]string{"-base-url", "http://127.0.0.1:1", "-mnemonic-file", mn,
		"-keys-file", filepath.Join(dir, "k.json"), "-webhook-url", "https://h", "-tolerance-bps", "900"})
	if err == nil || !strings.Contains(err.Error(), "tolerance") {
		t.Fatalf("err %v", err)
	}
}
