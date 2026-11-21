//go:build e2e

package e2e_test

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/m5tshift/slopay/pkg/storage"
)

// The images of the integration tests of the services.
const (
	kafkaImage    = "confluentinc/confluent-local:7.5.0"
	postgresImage = "postgres:16-alpine"
	mongoImage    = "mongo:7"
	valkeyImage   = "valkey/valkey:8.0"
	anvilImage    = "ghcr.io/foundry-rs/foundry:v1.10.0"

	anvilChainID = 31337
	startTimeout = 3 * time.Minute
	mongoDB      = "chain_watcher"
)

// components are the binaries the test builds: name -> package.
var components = map[string]string{
	"merchant-service": "./services/merchant-service/cmd/merchant-service",
	"invoice-service":  "./services/invoice-service/cmd/invoice-service",
	"chain-watcher":    "./services/chain-watcher/cmd/chain-watcher",
	"rate-service":     "./services/rate-service/cmd/rate-service",
	"rate-mock":        "./testkit/rate-mock/cmd/rate-mock",
	"payer-sim":        "./testkit/payer-sim/cmd/payer-sim",
}

// topics are the slice topics with the partitions of contracts/asyncapi; rate.latest is compacted.
var topics = map[string]int32{
	"merchant.events": 3,
	"invoice.events":  6,
	"chain.blocks":    1,
	"chain.transfers": 6,
	"rate.ticks":      3,
	"rate.latest":     1,
}

// Payers: one payer-sim per behaviour, each with its own funded account, so the nonces do not interfere.
const (
	payerFull     = "pay"
	payerUnder    = "underpay"
	payerReplayer = "reorg"
)

// env is the running slice.
var env *stack

type stack struct {
	dir string

	mu         sync.Mutex
	containers []testcontainers.Container
	procs      []*proc

	brokers     []string
	pgURL       string // without the database
	mongoURI    string
	valkeyAddr  string
	anvilURL    string
	pgPassword  string
	mongoPWFile string
	valkeyPW    string

	merchantURL string
	invoiceURL  string
	payerURLs   map[string]string

	mongo  *storage.Mongo
	events *collector
}

type proc struct {
	name string
	cmd  *exec.Cmd
	log  string
	done chan struct{}
}

func TestMain(m *testing.M) { os.Exit(run(m)) }

func run(m *testing.M) int {
	dir, err := os.MkdirTemp("", "slopay-e2e-")
	if err != nil {
		fmt.Fprintln(os.Stderr, "e2e:", err)
		return 1
	}
	defer func() { _ = os.RemoveAll(dir) }()
	env = &stack{dir: dir, payerURLs: map[string]string{}}
	defer env.stop()

	ctx, cancel := context.WithTimeout(context.Background(), startTimeout*2)
	defer cancel()
	if err := env.start(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "e2e: start:", err)
		env.dumpLogs()
		return 1
	}
	code := m.Run()
	if code != 0 {
		env.dumpLogs()
	}
	return code
}

// start runs the containers and the build in parallel, then the components in dependency order.
func (s *stack) start(ctx context.Context) error {
	pgPW, mongoPW, valkeyPW := randomSecret(), randomSecret(), randomSecret()
	s.pgPassword, s.valkeyPW = pgPW, valkeyPW
	files := map[string]string{"postgres": pgPW, "mongodb": mongoPW, "valkey": valkeyPW}
	for name, v := range files {
		if err := os.WriteFile(s.path(name), []byte(v), 0o600); err != nil {
			return err
		}
	}
	s.mongoPWFile = s.path("mongodb")
	if err := s.writeSigningKey(); err != nil {
		return err
	}

	if err := parallel(
		func() error { return s.build(ctx) },
		func() error { return s.startKafka(ctx) },
		func() error { return s.startPostgres(ctx, pgPW) },
		func() error { return s.startMongo(ctx, mongoPW) },
		func() error { return s.startValkey(ctx, valkeyPW) },
		func() error { return s.startAnvil(ctx) },
	); err != nil {
		return err
	}
	if err := createTopics(ctx, s.brokers); err != nil {
		return err
	}
	var err error
	if s.events, err = newCollector(s.brokers); err != nil {
		return err
	}
	if s.mongo, err = storage.OpenMongo(ctx, storage.MongoConfig{
		URI: s.mongoURI, Username: "app", PasswordFile: storage.SecretFile(s.mongoPWFile), AuthSource: "admin",
		Database: mongoDB,
	}); err != nil {
		return err
	}
	return s.startComponents(ctx)
}

func (s *stack) startComponents(ctx context.Context) error {
	kafka := s.brokerList()
	valkey := map[string]string{"VALKEY_ADDRS": s.valkeyAddr, "VALKEY_PASSWORD_FILE": s.path("valkey")}

	rateMock, err := s.spawn(ctx, "rate-mock", "rate-mock", nil)
	if err != nil {
		return err
	}
	if _, err := s.spawn(ctx, "rate-service", "rate-service", merge(valkey, map[string]string{
		"KAFKA_BROKERS": kafka, "RATE_SOURCE_MODE": "mock", "RATE_MOCK_URL": rateMock, "POLL_INTERVAL": "1s",
	})); err != nil {
		return err
	}
	rateService := s.procURL("rate-service")
	if s.merchantURL, err = s.spawn(ctx, "merchant-service", "merchant-service", map[string]string{
		"KAFKA_BROKERS":          kafka,
		"POSTGRES_URL":           s.pgURL + "/merchant?sslmode=disable",
		"POSTGRES_PASSWORD_FILE": s.path("postgres"),
		"JWT_SIGNING_KEY_FILE":   s.path("jwt-signing-key"),
	}); err != nil {
		return err
	}
	if s.invoiceURL, err = s.spawn(ctx, "invoice-service", "invoice-service", merge(valkey, map[string]string{
		"KAFKA_BROKERS":          kafka,
		"POSTGRES_URL":           s.pgURL + "/invoice?sslmode=disable",
		"POSTGRES_PASSWORD_FILE": s.path("postgres"),
		"RATE_SERVICE_URL":       rateService,
		"JWKS_URL":               s.merchantURL + "/.well-known/jwks.json",
		"CHAIN_ID":               fmt.Sprint(anvilChainID),
	})); err != nil {
		return err
	}
	if _, err := s.spawn(ctx, "chain-watcher", "chain-watcher", merge(valkey, map[string]string{
		"KAFKA_BROKERS":         kafka,
		"MONGODB_URI":           s.mongoURI,
		"MONGODB_USERNAME":      "app",
		"MONGODB_PASSWORD_FILE": s.mongoPWFile,
		"MONGODB_AUTH_SOURCE":   "admin",
		"MONGODB_DATABASE":      mongoDB,
		"NODE_URL":              s.anvilURL,
		"CHAIN_ID":              fmt.Sprint(anvilChainID),
		"POLL_INTERVAL":         "200ms",
	})); err != nil {
		return err
	}
	for name, shares := range map[string][3]int{
		payerFull: {100, 0, 0}, payerUnder: {0, 100, 0}, payerReplayer: {100, 0, 0},
	} {
		keys, err := s.fundPayer(ctx, name)
		if err != nil {
			return err
		}
		if s.payerURLs[name], err = s.spawn(ctx, "payer-sim-"+name, "payer-sim", map[string]string{
			"INVOICE_SERVICE_URL": s.invoiceURL,
			"ANVIL_RPC_URL":       s.anvilURL,
			"PAYER_KEYS_FILE":     keys,
			"PAY_PERCENT":         fmt.Sprint(shares[0]),
			"UNDERPAY_PERCENT":    fmt.Sprint(shares[1]),
			"SKIP_PERCENT":        fmt.Sprint(shares[2]),
			"UNDERPAY_BPS":        "5000",
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *stack) path(name string) string { return filepath.Join(s.dir, name) }

func (s *stack) brokerList() string { return strings.Join(s.brokers, ",") }

// build compiles every component into dir/bin.
func (s *stack) build(ctx context.Context) error {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		return fmt.Errorf("repository root not found: %w", err)
	}
	args := []string{"build", "-o", s.path("bin") + string(filepath.Separator)}
	for _, pkg := range components {
		args = append(args, pkg)
	}
	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Dir = root
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("go build: %w\n%s", err, out)
	}
	return nil
}

func (s *stack) track(c testcontainers.Container) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c != nil {
		s.containers = append(s.containers, c)
	}
}

func (s *stack) startKafka(ctx context.Context) error {
	kc, err := tckafka.Run(ctx, kafkaImage)
	s.track(kc)
	if err != nil {
		return fmt.Errorf("kafka: %w", err)
	}
	s.brokers, err = kc.Brokers(ctx)
	return err
}

// startPostgres runs one server with a database per service: merchant and invoice.
func (s *stack) startPostgres(ctx context.Context, password string) error {
	pc, err := postgres.Run(ctx, postgresImage,
		postgres.WithDatabase("merchant"), postgres.WithUsername("app"), postgres.WithPassword(password),
		postgres.BasicWaitStrategies())
	s.track(pc)
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	ep, err := pc.PortEndpoint(ctx, "5432/tcp", "")
	if err != nil {
		return err
	}
	s.pgURL = "postgres://app@" + ep
	pg, err := storage.OpenPostgres(ctx, storage.PostgresConfig{
		URL: s.pgURL + "/merchant?sslmode=disable", PasswordFile: storage.SecretFile(s.path("postgres")),
	})
	if err != nil {
		return err
	}
	defer pg.Close()
	_, err = pg.Pool().Exec(ctx, "CREATE DATABASE invoice")
	return err
}

func (s *stack) startMongo(ctx context.Context, password string) error {
	mc, err := mongodb.Run(ctx, mongoImage, mongodb.WithUsername("app"), mongodb.WithPassword(password))
	s.track(mc)
	if err != nil {
		return fmt.Errorf("mongodb: %w", err)
	}
	s.mongoURI, err = mc.PortEndpoint(ctx, "27017/tcp", "mongodb")
	return err
}

func (s *stack) startValkey(ctx context.Context, password string) error {
	vc, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        valkeyImage,
			ExposedPorts: []string{"6379/tcp"},
			Cmd:          []string{"valkey-server", "--requirepass", password},
			WaitingFor:   wait.ForLog("Ready to accept connections").WithStartupTimeout(startTimeout),
		},
		Started: true,
	})
	s.track(vc)
	if err != nil {
		return fmt.Errorf("valkey: %w", err)
	}
	s.valkeyAddr, err = vc.PortEndpoint(ctx, "6379/tcp", "")
	return err
}

// startAnvil runs Anvil with automine: a transaction is mined at once, other blocks only on anvil_mine.
func (s *stack) startAnvil(ctx context.Context) error {
	ac, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        anvilImage,
			Entrypoint:   []string{"/bin/sh", "-c"},
			Cmd:          []string{"exec anvil --host 0.0.0.0"},
			ExposedPorts: []string{"8545/tcp"},
			WaitingFor:   wait.ForLog("Listening on").WithStartupTimeout(startTimeout),
		},
		Started: true,
	})
	s.track(ac)
	if err != nil {
		return fmt.Errorf("anvil: %w", err)
	}
	s.anvilURL, err = ac.PortEndpoint(ctx, "8545/tcp", "http")
	return err
}

// writeSigningKey generates the ECDSA P-256 JWT signing key of merchant-service for this run.
func (s *stack) writeSigningKey() error {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return err
	}
	return os.WriteFile(s.path("jwt-signing-key"), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o600)
}

// fundPayer generates an account, funds it in Anvil and writes the keys file of a payer-sim.
func (s *stack) fundPayer(ctx context.Context, name string) (string, error) {
	key, err := crypto.GenerateKey()
	if err != nil {
		return "", err
	}
	rc, err := rpc.DialContext(ctx, s.anvilURL)
	if err != nil {
		return "", err
	}
	defer rc.Close()
	balance := new(big.Int).Mul(big.NewInt(1000), big.NewInt(1e18))
	if err := rc.CallContext(ctx, nil, "anvil_setBalance", crypto.PubkeyToAddress(key.PublicKey), hexutil.EncodeBig(balance)); err != nil {
		return "", fmt.Errorf("fund payer %s: %w", name, err)
	}
	file := s.path("payer-keys-" + name)
	return file, os.WriteFile(file, []byte(hex.EncodeToString(crypto.FromECDSA(key))+"\n"), 0o600)
}

// spawn starts a component on a free local port and waits for its readiness; it returns the base URL.
func (s *stack) spawn(ctx context.Context, name, bin string, vars map[string]string) (string, error) {
	port, err := freePort()
	if err != nil {
		return "", err
	}
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	logFile := s.path(name + ".log")
	f, err := os.Create(logFile)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	// The process outlives ctx, the start deadline: stop terminates it with SIGTERM.
	cmd := exec.CommandContext(context.WithoutCancel(ctx), filepath.Join(s.path("bin"), bin)) //nolint:gosec // a binary built by this test
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"), "LOG_LEVEL=info",
		fmt.Sprintf("HTTP_ADDR=127.0.0.1:%d", port), "OTEL_SERVICE_NAME=" + name}
	for k, v := range vars {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	cmd.Stdout, cmd.Stderr = f, f
	if err := cmd.Start(); err != nil {
		return "", fmt.Errorf("start %s: %w", name, err)
	}
	p := &proc{name: name, cmd: cmd, log: logFile, done: make(chan struct{})}
	go func() { _ = cmd.Wait(); close(p.done) }()
	s.mu.Lock()
	s.procs = append(s.procs, p)
	s.mu.Unlock()

	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(startTimeout)
	for time.Now().Before(deadline) {
		select {
		case <-p.done:
			return "", fmt.Errorf("%s exited: %s", name, cmd.ProcessState)
		case <-ctx.Done():
			return "", ctx.Err()
		default:
		}
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, base+"/health/ready", nil)
		if resp, err := client.Do(req); err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return base, nil
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return "", fmt.Errorf("%s is not ready after %s", name, startTimeout)
}

func (s *stack) procURL(name string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, p := range s.procs {
		if p.name == name {
			for _, e := range p.cmd.Env {
				if addr, ok := strings.CutPrefix(e, "HTTP_ADDR="); ok {
					return "http://" + addr
				}
			}
		}
	}
	return ""
}

// stop terminates the components with SIGTERM, then the containers.
func (s *stack) stop() {
	if s.events != nil {
		s.events.close()
	}
	if s.mongo != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = s.mongo.Close(ctx)
		cancel()
	}
	s.mu.Lock()
	procs, ctrs := s.procs, s.containers
	s.mu.Unlock()
	for i := len(procs) - 1; i >= 0; i-- {
		p := procs[i]
		_ = p.cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-p.done:
		case <-time.After(15 * time.Second):
			_ = p.cmd.Process.Kill()
			<-p.done
		}
	}
	for _, c := range ctrs {
		_ = testcontainers.TerminateContainer(c)
	}
}

// dumpLogs prints the tail of every component log, for a failed run.
func (s *stack) dumpLogs() {
	s.mu.Lock()
	procs := s.procs
	s.mu.Unlock()
	for _, p := range procs {
		f, err := os.Open(p.log)
		if err != nil {
			continue
		}
		var lines []string
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
		for sc.Scan() {
			lines = append(lines, sc.Text())
		}
		_ = f.Close()
		if len(lines) > 80 {
			lines = lines[len(lines)-80:]
		}
		fmt.Fprintf(os.Stderr, "===== %s (last %d lines) =====\n%s\n", p.name, len(lines), strings.Join(lines, "\n"))
	}
}

func createTopics(ctx context.Context, brokers []string) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return err
	}
	defer cl.Close()
	req := kmsg.NewPtrCreateTopicsRequest()
	for name, partitions := range topics {
		rt := kmsg.NewCreateTopicsRequestTopic()
		rt.Topic, rt.NumPartitions, rt.ReplicationFactor = name, partitions, 1
		if name == "rate.latest" {
			c := kmsg.NewCreateTopicsRequestTopicConfig()
			c.Name, c.Value = "cleanup.policy", kmsg.StringPtr("compact")
			rt.Configs = append(rt.Configs, c)
		}
		req.Topics = append(req.Topics, rt)
	}
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return err
	}
	for _, tr := range resp.Topics {
		if err := kerr.ErrorForCode(tr.ErrorCode); err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("create topic %s: %w", tr.Topic, err)
		}
	}
	return nil
}

func parallel(fns ...func() error) error {
	errs := make([]error, len(fns))
	var wg sync.WaitGroup
	for i, fn := range fns {
		wg.Go(func() { errs[i] = fn() })
	}
	wg.Wait()
	return errors.Join(errs...)
}

func merge(a, b map[string]string) map[string]string {
	out := make(map[string]string, len(a)+len(b))
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}

func freePort() (int, error) {
	var lc net.ListenConfig
	l, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func randomSecret() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
