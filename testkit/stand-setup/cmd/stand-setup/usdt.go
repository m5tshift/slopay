package main

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"os"
	"os/signal"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/m5tshift/slopay/testkit/stand-setup/internal/setup"
	"github.com/m5tshift/slopay/testkit/stand-setup/internal/usdt"
)

// runDeployUSDT deploys the test USDT (testkit/erc20) to Anvil, mints it to the
// payer-sim accounts and prints the contract address for the configuration.
func runDeployUSDT(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("stand-setup deploy-usdt", flag.ContinueOnError)
	var (
		rpcURL      = fs.String("rpc-url", "", "Anvil JSON-RPC URL (required)")
		deployer    = fs.String("deployer-key-file", "", "file with the deployer hex private key (first key is used), outside git (required)")
		payerKeys   = fs.String("payer-keys-file", "", "payer-sim keys file (one hex key per line), outside git: tokens are minted to its accounts")
		recipients  = fs.String("recipients", "", "comma-separated extra addresses to mint to")
		amountUnits = fs.String("amount", "1000000000000", "minimal units minted to each account (10^-6 USDT; default 1 000 000 USDT)")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *rpcURL == "" || *deployer == "" {
		return errors.New("-rpc-url and -deployer-key-file are required")
	}
	amount, ok := new(big.Int).SetString(*amountUnits, 10)
	if !ok || amount.Sign() <= 0 {
		return errors.New("-amount must be a positive integer")
	}
	to, err := usdt.ParseAddresses(*recipients)
	if err != nil {
		return err
	}
	files := []string{*deployer}
	if *payerKeys != "" {
		files = append(files, *payerKeys)
	}
	for _, p := range files {
		in, err := setup.InsideGitRepo(p)
		if err != nil {
			return err
		}
		if in {
			return errors.New("secret files must be outside a git repository: " + p)
		}
	}
	dk, err := readKeys(*deployer)
	if err != nil {
		return err
	}
	if *payerKeys != "" {
		pk, err := readKeys(*payerKeys)
		if err != nil {
			return err
		}
		to = append(to, usdt.Addresses(pk)...)
	}
	to = dedupe(to)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ec, err := ethclient.DialContext(ctx, *rpcURL)
	if err != nil {
		return fmt.Errorf("dial anvil: %w", err)
	}
	defer ec.Close()
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	res, err := usdt.Deploy(ctx, usdt.Config{Chain: ec, Key: dk[0], Recipients: to, Amount: amount}, log)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "USDT_CONTRACT_ADDRESS=%s\n", res.Address.Hex())
	return err
}

func readKeys(path string) ([]*ecdsa.PrivateKey, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // operator-supplied path
	if err != nil {
		return nil, fmt.Errorf("read key file: %w", err)
	}
	return usdt.ParseKeys(string(raw))
}

func dedupe(in []common.Address) []common.Address {
	seen := map[common.Address]bool{}
	var out []common.Address
	for _, a := range in {
		if !seen[a] {
			seen[a] = true
			out = append(out, a)
		}
	}
	return out
}
