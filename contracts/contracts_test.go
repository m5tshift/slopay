// Package contracts holds the HTTP and Kafka contracts of Weigate. The tests lint the AsyncAPI documents,
// check the topics against SRS 4.1.5 and validate every event example against its JSON Schema.
package contracts

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	specjsonschemas "github.com/asyncapi/spec-json-schemas/v6"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"go.yaml.in/yaml/v3"
)

const (
	schemaBaseURL = "https://raw.githubusercontent.com/m5tshift/weigate/main/contracts/schemas/"
	asyncAPIID    = "http://asyncapi.com/definitions/3.0.0/asyncapi.json"
	// replicas is the replication factor of every topic (SRS 4.1.5: RF = 3).
	replicas = 3
)

type topicSpec struct {
	partitions int
	key        string   // data field that is the Kafka key and the CloudEvent subject
	keyParts   []string // data fields joined with ":" when the key is composite; key is then empty
	producer   string
	consumers  []string
	types      []string
}

// srsTopics mirrors SRS 4.1.5 (key, partitions) and SRS 4.1.3 (producer, consumers);
// clickhouse is the Kafka table engine of the cold storage (SRS 4.1.5).
var srsTopics = map[string]topicSpec{
	"invoice.events": {
		partitions: 6,
		key:        "invoiceId",
		producer:   "invoice-service",
		consumers:  []string{"chain-watcher", "webhook-service", "ledger-service", "reconciliation-service", "clickhouse"},
		types: []string{
			"invoice.created", "invoice.detected", "invoice.confirming", "invoice.paid", "invoice.underpaid",
			"invoice.expired", "invoice.late_paid", "invoice.reverted", "invoice.late_transfer",
		},
	},
	"chain.blocks": {
		partitions: 1,
		key:        "chainId",
		producer:   "chain-watcher",
		consumers:  []string{"invoice-service"},
		types:      []string{"chain.new_head", "chain.reorg"},
	},
	"chain.transfers": {
		partitions: 6,
		key:        "to",
		producer:   "chain-watcher",
		consumers:  []string{"invoice-service", "clickhouse"},
		types:      []string{"chain.transfer"},
	},
	"merchant.events": {
		partitions: 3,
		key:        "merchantId",
		producer:   "merchant-service",
		consumers:  []string{"invoice-service", "webhook-service"},
		types:      []string{"merchant.updated"},
	},
	"rate.ticks": {
		partitions: 3,
		keyParts:   []string{"source", "pair"},
		producer:   "rate-service",
		consumers:  []string{"clickhouse"},
		types:      []string{"rate.tick"},
	},
	"rate.latest": {
		partitions: 1,
		key:        "pair",
		producer:   "rate-service",
		consumers:  []string{"rate-service"},
		types:      []string{"rate.latest"},
	},
	"ledger.events": {
		partitions: 3,
		key:        "merchantId",
		producer:   "ledger-service",
		consumers:  []string{},
		types:      []string{"ledger.receipt_posted", "ledger.fee_accrued"},
	},
	"reconciliation.events": {
		partitions: 1,
		key:        "runId",
		producer:   "reconciliation-service",
		consumers:  []string{},
		types:      []string{"reconciliation.completed", "reconciliation.mismatch"},
	},
	"webhook.retry.1m":  webhookRetry,
	"webhook.retry.5m":  webhookRetry,
	"webhook.retry.30m": webhookRetry,
	"webhook.dlq": {
		partitions: 1,
		key:        "invoiceId",
		producer:   "webhook-service",
		consumers:  []string{},
		types:      []string{"webhook.dead_lettered"},
	},
}

// webhookRetry is the spec of the three retry topics: the same message, the delay is set by the topic.
var webhookRetry = topicSpec{
	partitions: 3,
	key:        "invoiceId",
	producer:   "webhook-service",
	consumers:  []string{"webhook-service"},
	types:      []string{"webhook.retry"},
}

// compactedTopics are the topics SRS 4.1.5 marks as compacted.
var compactedTopics = []string{"merchant.events", "rate.latest"}

// loader reads YAML and JSON documents as JSON values and resolves $ref between them.
type loader struct {
	docs map[string]any
}

func newLoader() *loader {
	return &loader{docs: map[string]any{}}
}

func (l *loader) load(path string) (any, error) {
	path = filepath.Clean(path)
	if doc, ok := l.docs[path]; ok {
		return doc, nil
	}
	raw, err := os.ReadFile(path) // #nosec G304 -- paths come from the repository tree
	if err != nil {
		return nil, err
	}
	if strings.HasSuffix(path, ".yaml") {
		var v any
		if err := yaml.Unmarshal(raw, &v); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		if raw, err = json.Marshal(v); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
	}
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	l.docs[path] = doc
	return doc, nil
}

// resolve returns the file and the node a $ref found in file points to.
func (l *loader) resolve(file, ref string) (string, any, error) {
	target, pointer, _ := strings.Cut(ref, "#")
	if target != "" {
		file = filepath.Join(filepath.Dir(file), target)
	}
	node, err := l.load(file)
	if err != nil {
		return "", nil, err
	}
	if pointer == "" {
		return file, node, nil
	}
	for _, tok := range strings.Split(strings.TrimPrefix(pointer, "/"), "/") {
		tok = strings.ReplaceAll(strings.ReplaceAll(tok, "~1", "/"), "~0", "~")
		switch n := node.(type) {
		case map[string]any:
			v, ok := n[tok]
			if !ok {
				return "", nil, fmt.Errorf("%s: $ref %q: no %q", file, ref, tok)
			}
			node = v
		case []any:
			i, err := strconv.Atoi(tok)
			if err != nil || i < 0 || i >= len(n) {
				return "", nil, fmt.Errorf("%s: $ref %q: bad index %q", file, ref, tok)
			}
			node = n[i]
		default:
			return "", nil, fmt.Errorf("%s: $ref %q: %q is not a container", file, ref, tok)
		}
	}
	return file, node, nil
}

// deref follows a chain of $ref until a node without $ref.
func (l *loader) deref(file string, node any) (string, any, error) {
	for range 16 {
		ref, ok := obj(node)["$ref"].(string)
		if !ok {
			return file, node, nil
		}
		var err error
		if file, node, err = l.resolve(file, ref); err != nil {
			return "", nil, err
		}
	}
	return "", nil, fmt.Errorf("%s: $ref chain is too long", file)
}

func obj(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

func str(v any) string {
	switch s := v.(type) {
	case string:
		return s
	case json.Number:
		return s.String()
	}
	return ""
}

func refs(node any, visit func(ref string)) {
	switch n := node.(type) {
	case map[string]any:
		for k, v := range n {
			if s, ok := v.(string); ok && k == "$ref" {
				visit(s)
				continue
			}
			refs(v, visit)
		}
	case []any:
		for _, v := range n {
			refs(v, visit)
		}
	}
}

func asyncAPIFiles(t *testing.T) []string {
	t.Helper()
	files, err := filepath.Glob("asyncapi/*.yaml")
	if err != nil || len(files) == 0 {
		t.Fatalf("no asyncapi/*.yaml: %v", err)
	}
	return files
}

func service(file string) string {
	return strings.TrimSuffix(filepath.Base(file), ".yaml")
}

// contractSet compiles every JSON Schema in schemas/ under its $id.
type contractSet struct {
	l   *loader
	c   *jsonschema.Compiler
	ids map[string]string // schema file -> $id
}

func newContractSet(t *testing.T) *contractSet {
	t.Helper()
	k := &contractSet{l: newLoader(), c: jsonschema.NewCompiler(), ids: map[string]string{}}
	k.c.DefaultDraft(jsonschema.Draft7)
	k.c.AssertFormat()
	files, err := filepath.Glob("schemas/*.json")
	if err != nil || len(files) == 0 {
		t.Fatalf("no schemas/*.json: %v", err)
	}
	for _, f := range files {
		doc, err := k.l.load(f)
		if err != nil {
			t.Fatal(err)
		}
		id := str(obj(doc)["$id"])
		if want := schemaBaseURL + filepath.Base(f); id != want {
			t.Fatalf("%s: $id %q, want %q", f, id, want)
		}
		if err := k.c.AddResource(id, doc); err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		k.ids[filepath.Clean(f)] = id
	}
	for f, id := range k.ids {
		if _, err := k.c.Compile(id); err != nil {
			t.Fatalf("%s: %v", f, err)
		}
	}
	return k
}

// schemaAt compiles the schema a $ref in an AsyncAPI document points to.
func (k *contractSet) schemaAt(file, ref string) (*jsonschema.Schema, string, error) {
	target, pointer, _ := strings.Cut(ref, "#")
	path := filepath.Clean(filepath.Join(filepath.Dir(file), target))
	id, ok := k.ids[path]
	if !ok {
		return nil, "", fmt.Errorf("%s: $ref %q is not a file in schemas/", file, ref)
	}
	loc := id
	if pointer != "" {
		loc += "#" + pointer
	}
	s, err := k.c.Compile(loc)
	return s, path, err
}

type example struct {
	name    string
	headers any
	payload any
}

type message struct {
	file       string
	topic      string
	name       string
	schemaFile string
	payload    *jsonschema.Schema
	headers    *jsonschema.Schema
	examples   []example
}

// headersRef returns the $ref of the message headers, declared in the message or in its traits.
func (k *contractSet) headersRef(file string, msg map[string]any) (string, string, error) {
	if ref := str(obj(msg["headers"])["$ref"]); ref != "" {
		return file, ref, nil
	}
	traits, _ := msg["traits"].([]any)
	for _, tr := range traits {
		tf, trait, err := k.l.deref(file, tr)
		if err != nil {
			return "", "", err
		}
		if ref := str(obj(obj(trait)["headers"])["$ref"]); ref != "" {
			return tf, ref, nil
		}
	}
	return "", "", errors.New("message has no headers schema")
}

// messages returns the messages of the channels each AsyncAPI document owns.
func (k *contractSet) messages(t *testing.T) []message {
	t.Helper()
	var out []message
	for _, file := range asyncAPIFiles(t) {
		doc, err := k.l.load(file)
		if err != nil {
			t.Fatal(err)
		}
		for chName, ch := range obj(obj(doc)["channels"]) {
			if _, isRef := obj(ch)["$ref"]; isRef {
				continue
			}
			topic := str(obj(ch)["address"])
			for msgName, m := range obj(obj(ch)["messages"]) {
				mf, node, err := k.l.deref(file, m)
				if err != nil {
					t.Fatalf("%s: channel %s message %s: %v", file, chName, msgName, err)
				}
				msg := obj(node)
				payload, schemaFile, err := k.schemaAt(mf, str(obj(msg["payload"])["$ref"]))
				if err != nil {
					t.Fatalf("%s: message %s payload: %v", mf, msgName, err)
				}
				hf, href, err := k.headersRef(mf, msg)
				if err != nil {
					t.Fatalf("%s: message %s: %v", mf, msgName, err)
				}
				headers, _, err := k.schemaAt(hf, href)
				if err != nil {
					t.Fatalf("%s: message %s headers: %v", mf, msgName, err)
				}
				m := message{file: mf, topic: topic, name: str(msg["name"]), schemaFile: schemaFile, payload: payload, headers: headers}
				exs, _ := msg["examples"].([]any)
				for _, e := range exs {
					m.examples = append(m.examples, example{name: str(obj(e)["name"]), headers: obj(e)["headers"], payload: obj(e)["payload"]})
				}
				out = append(out, m)
			}
		}
	}
	return out
}

// TestAsyncAPIDocuments lints the AsyncAPI documents: valid against the official AsyncAPI 3.0.0 schema,
// every $ref resolves.
func TestAsyncAPIDocuments(t *testing.T) {
	raw, err := specjsonschemas.Get("3.0.0")
	if err != nil || raw == nil {
		t.Fatalf("AsyncAPI 3.0.0 schema: %v", err)
	}
	meta, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	c := jsonschema.NewCompiler()
	c.AssertFormat()
	if err := c.AddResource(asyncAPIID, meta); err != nil {
		t.Fatal(err)
	}
	spec, err := c.Compile(asyncAPIID)
	if err != nil {
		t.Fatal(err)
	}
	l := newLoader()
	for _, file := range asyncAPIFiles(t) {
		t.Run(service(file), func(t *testing.T) {
			doc, err := l.load(file)
			if err != nil {
				t.Fatal(err)
			}
			if err := spec.Validate(doc); err != nil {
				t.Errorf("not a valid AsyncAPI 3.0.0 document: %v", err)
			}
			refs(doc, func(ref string) {
				if _, _, err := l.resolve(file, ref); err != nil {
					t.Errorf("unresolved $ref: %v", err)
				}
			})
		})
	}
}

// TestTopicsMatchSRS checks topics, partitions, replicas, producers, consumers and event types
// against SRS 4.1.3 and 4.1.5.
func TestTopicsMatchSRS(t *testing.T) {
	l := newLoader()
	type channel struct {
		owner string
		node  map[string]any
	}
	channels := map[string]channel{}
	ops := map[string][]string{} // service -> "send:topic", "receive:topic"
	for _, file := range asyncAPIFiles(t) {
		doc, err := l.load(file)
		if err != nil {
			t.Fatal(err)
		}
		for name, ch := range obj(obj(doc)["channels"]) {
			if _, isRef := obj(ch)["$ref"]; isRef {
				continue
			}
			topic := str(obj(ch)["address"])
			if prev, dup := channels[topic]; dup {
				t.Errorf("topic %s is owned by %s and %s", topic, prev.owner, service(file))
			}
			if _, known := srsTopics[topic]; !known {
				t.Errorf("%s: channel %s: topic %q is not covered by this test", file, name, topic)
			}
			channels[topic] = channel{owner: service(file), node: obj(ch)}
		}
		for name, op := range obj(obj(doc)["operations"]) {
			_, ch, err := l.deref(file, obj(op)["channel"])
			if err != nil {
				t.Fatalf("%s: operation %s: %v", file, name, err)
			}
			ops[service(file)] = append(ops[service(file)], str(obj(op)["action"])+":"+str(obj(ch)["address"]))
		}
	}

	for topic, want := range srsTopics {
		t.Run(topic, func(t *testing.T) {
			ch, ok := channels[topic]
			if !ok {
				t.Fatal("no channel")
			}
			if ch.owner != want.producer {
				t.Errorf("defined in %s.yaml, want %s.yaml (producer)", ch.owner, want.producer)
			}
			kafka := obj(obj(ch.node["bindings"])["kafka"])
			if got := str(kafka["partitions"]); got != strconv.Itoa(want.partitions) {
				t.Errorf("partitions %s, want %d", got, want.partitions)
			}
			if got := str(kafka["replicas"]); got != strconv.Itoa(replicas) {
				t.Errorf("replicas %s, want %d", got, replicas)
			}
			if got := str(ch.node["x-producer"]); got != want.producer {
				t.Errorf("x-producer %q, want %q", got, want.producer)
			}
			policy, _ := obj(kafka["topicConfiguration"])["cleanup.policy"].([]any)
			if compacted := len(policy) == 1 && str(policy[0]) == "compact"; compacted != slices.Contains(compactedTopics, topic) {
				t.Errorf("cleanup.policy %v, compacted must be %t", policy, !compacted)
			}
			consumers := []string{}
			for _, c := range ch.node["x-consumers"].([]any) {
				consumers = append(consumers, str(c))
			}
			if !sameSet(consumers, want.consumers) {
				t.Errorf("x-consumers %v, want %v", consumers, want.consumers)
			}
			var types []string
			for _, m := range obj(ch.node["messages"]) {
				_, msg, err := l.deref(filepath.Join("asyncapi", ch.owner+".yaml"), m)
				if err != nil {
					t.Fatal(err)
				}
				types = append(types, str(obj(msg)["name"]))
			}
			if !sameSet(types, want.types) {
				t.Errorf("messages %v, want %v", types, want.types)
			}
			if !slices.Contains(ops[want.producer], "send:"+topic) {
				t.Errorf("%s has no send operation", want.producer)
			}
			for _, c := range want.consumers {
				if _, documented := ops[c]; documented && !slices.Contains(ops[c], "receive:"+topic) {
					t.Errorf("%s has no receive operation", c)
				}
			}
		})
	}
}

func sameSet(a, b []string) bool {
	a, b = slices.Clone(a), slices.Clone(b)
	slices.Sort(a)
	slices.Sort(b)
	return slices.Equal(a, b)
}

// TestExamplesMatchSchemas validates every message example against its payload and headers schemas
// and checks that every event schema is published by exactly one message.
func TestExamplesMatchSchemas(t *testing.T) {
	k := newContractSet(t)
	published := map[string]string{} // schema file -> message name
	for _, m := range k.messages(t) {
		spec, ok := srsTopics[m.topic]
		if !ok {
			t.Fatalf("%s: unknown topic %q", m.file, m.topic)
		}
		// One message may be published on several topics (the webhook retry tiers).
		if prev, dup := published[m.schemaFile]; dup && prev != m.name {
			t.Errorf("%s is the payload of %s and %s", m.schemaFile, prev, m.name)
		}
		published[m.schemaFile] = m.name

		doc, err := k.l.load(m.schemaFile)
		if err != nil {
			t.Fatal(err)
		}
		props := obj(obj(doc)["properties"])
		if got := str(obj(props["type"])["const"]); got != m.name {
			t.Errorf("%s: type %q, want message name %q", m.schemaFile, got, m.name)
		}
		if got := str(obj(props["dataschema"])["const"]); got != k.ids[m.schemaFile] {
			t.Errorf("%s: dataschema %q, want its $id", m.schemaFile, got)
		}
		if !strings.HasPrefix(filepath.Base(m.schemaFile), m.name+".v") {
			t.Errorf("%s: file name must be %s.v<N>.json", m.schemaFile, m.name)
		}
		if len(m.examples) == 0 {
			t.Errorf("%s: message %s has no examples", m.file, m.name)
		}

		for _, ex := range m.examples {
			t.Run(m.name+"/"+ex.name, func(t *testing.T) {
				if err := m.payload.Validate(ex.payload); err != nil {
					t.Errorf("payload: %v", err)
				}
				if err := m.headers.Validate(ex.headers); err != nil {
					t.Errorf("headers: %v", err)
				}
				p := obj(ex.payload)
				if got := str(p["type"]); got != m.name {
					t.Errorf("type %q, want %q", got, m.name)
				}
				fields := spec.keyParts
				if spec.key != "" {
					fields = []string{spec.key}
				}
				var parts []string
				for _, f := range fields {
					parts = append(parts, str(obj(p["data"])[f]))
				}
				if subject, key := str(p["subject"]), strings.Join(parts, ":"); subject != key {
					t.Errorf("subject %q, want data.%v %q (the Kafka key)", subject, fields, key)
				}
			})
		}
	}

	for file := range k.ids {
		base := filepath.Base(file)
		if strings.HasPrefix(base, "defs.") || strings.HasPrefix(base, "cloudevent.") {
			continue
		}
		if _, ok := published[file]; !ok {
			t.Errorf("%s is not the payload of any message", file)
		}
	}
}

type removed struct{}

// edit sets the value at a dotted path ("data.transfers.0.logIndex"); removed{} deletes the key.
func edit(t *testing.T, v any, path string, val any) {
	t.Helper()
	keys := strings.Split(path, ".")
	for i, key := range keys {
		final := i == len(keys)-1
		switch n := v.(type) {
		case map[string]any:
			if final {
				if _, del := val.(removed); del {
					delete(n, key)
				} else {
					n[key] = val
				}
				return
			}
			v = n[key]
		case []any:
			idx, err := strconv.Atoi(key)
			if err != nil || idx >= len(n) {
				t.Fatalf("bad index %q in %s", key, path)
			}
			if final {
				n[idx] = val
				return
			}
			v = n[idx]
		default:
			t.Fatalf("%s: %q is not a container", path, key)
		}
	}
}

func clone(t *testing.T, v any) any {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	out, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// TestSchemasRejectInvalidEvents checks that the schemas enforce the money, asset, transfer
// and envelope rules: each case breaks one rule in a valid example.
func TestSchemasRejectInvalidEvents(t *testing.T) {
	k := newContractSet(t)
	byName := map[string]message{}
	for _, m := range k.messages(t) {
		byName[m.name] = m
	}
	cases := []struct {
		name    string
		event   string
		example string
		path    string
		value   any
	}{
		{"float amount", "invoice.paid", "eth-two-transfers", "data.amount", json.Number("0.01")},
		{"fractional amount string", "invoice.paid", "eth-two-transfers", "data.received", "1.5"},
		{"negative amount", "invoice.paid", "eth-two-transfers", "data.transfers.0.amount", "-1"},
		{"amount with leading zero", "invoice.detected", "eth", "data.received", "01"},
		{"amount longer than NUMERIC(78,0)", "chain.transfer", "eth", "data.amount", strings.Repeat("9", 79)},
		{"zero invoice amount", "invoice.created", "eth", "data.amount", "0"},
		{"usd as float", "invoice.created", "eth", "data.amountUsdCents", json.Number("25.5")},
		{"rate as number", "invoice.created", "eth", "data.rate", json.Number("2500")},
		{"missing sequence", "invoice.paid", "eth-two-transfers", "data.sequence", removed{}},
		{"created with sequence 2", "invoice.created", "eth", "data.sequence", json.Number("2")},
		{"zero sequence", "invoice.expired", "eth", "data.sequence", json.Number("0")},
		{"wrong status", "invoice.paid", "eth-two-transfers", "data.status", "UNDERPAID"},
		{"missing transfers", "invoice.paid", "eth-two-transfers", "data.transfers", removed{}},
		{"empty transfers", "invoice.late_paid", "eth", "data.transfers", []any{}},
		{"transfer without blockHash", "invoice.late_transfer", "erc20", "data.transfers.0.blockHash", removed{}},
		{"transfer without txHash", "invoice.underpaid", "erc20", "data.transfers.0.txHash", removed{}},
		{"transfer without asset", "invoice.paid", "eth-two-transfers", "data.transfers.1.asset", removed{}},
		{"ETH transfer with logIndex", "invoice.paid", "eth-two-transfers", "data.transfers.0.logIndex", json.Number("0")},
		{"ERC-20 transfer without logIndex", "invoice.underpaid", "erc20", "data.transfers.0.logIndex", nil},
		{"ERC-20 without decimals", "invoice.created", "erc20", "data.asset.decimals", removed{}},
		{"ERC-20 without contract", "invoice.created", "erc20", "data.asset.contract", removed{}},
		{"ETH with 6 decimals", "invoice.created", "eth", "data.asset.decimals", json.Number("6")},
		{"unknown asset kind", "chain.transfer", "eth", "data.asset.kind", "BTC"},
		{"upper-case address", "invoice.created", "eth", "data.address", "0x8BA1F109551BD432803012645AC136DDD64DBA72"},
		{"unknown data field", "invoice.paid", "eth-two-transfers", "data.extra", "x"},
		{"reverted to a final status", "invoice.reverted", "eth-back-to-new", "data.status", "PAID"},
		{"late transfer to an EXPIRED invoice", "invoice.late_transfer", "erc20", "data.status", "EXPIRED"},
		{"chain.transfer ETH with logIndex", "chain.transfer", "eth", "data.logIndex", json.Number("1")},
		{"chain.transfer ERC-20 without logIndex", "chain.transfer", "erc20", "data.logIndex", nil},
		{"chain.transfer without blockHash", "chain.transfer", "eth", "data.blockHash", removed{}},
		{"chain.transfer timestamp as string", "chain.transfer", "eth", "data.blockTimestamp", "1794736920"},
		{"new_head without parentHash", "chain.new_head", "anvil", "data.parentHash", removed{}},
		{"new_head without timestamp", "chain.new_head", "anvil", "data.timestamp", removed{}},
		{"reorg without fromBlock", "chain.reorg", "depth-2", "data.fromBlock", removed{}},
		{"other schema version", "invoice.paid", "eth-two-transfers", "dataschema", schemaBaseURL + "invoice.paid.v2.json"},
		{"missing dataschema", "invoice.paid", "eth-two-transfers", "dataschema", removed{}},
		{"missing subject", "invoice.paid", "eth-two-transfers", "subject", removed{}},
		{"missing id", "chain.transfer", "eth", "id", removed{}},
		{"missing source", "invoice.paid", "eth-two-transfers", "source", removed{}},
		{"wrong source", "invoice.paid", "eth-two-transfers", "source", "/weigate/ledger-service"},
		{"time not RFC 3339", "invoice.created", "eth", "time", "15.11.2026 10:00"},
		{"wrong specversion", "chain.reorg", "depth-2", "specversion", "0.3"},
		{"merchant with a webhook secret", "merchant.updated", "registered", "data.webhookSecret", "s3cret"},
		{"merchant without secret version", "merchant.updated", "registered", "data.webhookSecretVersion", removed{}},
		{"merchant without xpub", "merchant.updated", "registered", "data.xpub", removed{}},
		{"merchant with a private key", "merchant.updated", "registered", "data.xpub", "xprv9s21ZrQH143K3QTDL4LXw2F7HEK3wJUD2nW2nRk4stbPy6cq3jPPqjiChkVvvNKmPGJxWUtg6LnF5kejMRNNU3TGtRBeJgk33yuGBxrMPHi"},
		{"merchant tolerance above 5 percent", "merchant.updated", "registered", "data.toleranceBps", json.Number("501")},
		{"merchant tolerance as float", "merchant.updated", "registered", "data.toleranceBps", json.Number("1.5")},
		{"merchant webhook URL over ftp", "merchant.updated", "webhook-secret-rotated", "data.webhookUrl", "ftp://merchant.example.com"},
		{"merchant version zero", "merchant.updated", "registered", "data.version", json.Number("0")},
		{"rate tick with a number price", "rate.tick", "coinbase-eth-usd", "data.price", json.Number("2501.35")},
		{"okx quote of ETH-USD", "rate.tick", "okx-eth-usdt", "data.pair", "ETH-USD"},
		{"unknown rate source", "rate.tick", "coinbase-eth-usd", "data.source", "binance"},
		{"rate tick subject is not the key", "rate.tick", "coinbase-eth-usd", "subject", "ETH-USD"},
		{"latest rate without sources", "rate.latest", "eth-usd", "data.sources", []any{}},
		{"latest rate with a duplicate source", "rate.latest", "eth-usd", "data.sources", []any{"okx", "okx"}},
		{"latest rate as number", "rate.latest", "eth-usd", "data.price", json.Number("2501.075")},
		{"receipt without logIndex of ERC-20", "ledger.receipt_posted", "erc20", "data.transfer.logIndex", nil},
		{"receipt ID is not the idempotency key", "ledger.receipt_posted", "eth", "id", "9f3b2c1d-8e7a-4b6c-9d5e-1f2a3b4c5d01"},
		{"receipt debit is not an address", "ledger.receipt_posted", "eth", "data.debitAccount", "platform:fee_income"},
		{"fee as float", "ledger.fee_accrued", "eth", "data.amount", json.Number("5e13")},
		{"fee rate above 100 percent", "ledger.fee_accrued", "eth", "data.feeBps", json.Number("10001")},
		{"fee credited to a merchant", "ledger.fee_accrued", "eth", "data.creditAccount", "merchant:0b9f4e2a-7c3d-4e5f-8a1b-6c2d3e4f5a6b:received"},
		{"fee without transfer", "ledger.fee_accrued", "eth", "data.transfer", removed{}},
		{"clean run with mismatches", "reconciliation.completed", "ok", "data.mismatches", json.Number("2")},
		{"run with mismatches reported OK", "reconciliation.completed", "with-mismatches", "data.result", "OK"},
		{"run without runId", "reconciliation.completed", "ok", "data.runId", removed{}},
		{"mismatch with a fractional sum", "reconciliation.mismatch", "eth-chain-vs-invoices", "data.ledgerTotal", "1.5"},
		{"mismatch without ledger sum", "reconciliation.mismatch", "eth-chain-vs-invoices", "data.ledgerTotal", removed{}},
		{"unknown mismatch kind", "reconciliation.mismatch", "eth-chain-vs-invoices", "data.kind", "other"},
		{"retry without attempt", "webhook.retry", "first-failure", "data.attempt", removed{}},
		{"retry attempt zero", "webhook.retry", "first-failure", "data.attempt", json.Number("0")},
		{"retry without the event", "webhook.retry", "first-failure", "data.event", removed{}},
		{"retry of a non-invoice event", "webhook.retry", "first-failure", "data.event.type", "chain.transfer"},
		{"retry with a secret", "webhook.retry", "first-failure", "data.secret", "s3cret"},
		{"retry HTTP status out of range", "webhook.retry", "first-failure", "data.lastError.statusCode", json.Number("42")},
		{"dead letter without attempts", "webhook.dead_lettered", "exhausted", "data.attempts", removed{}},
		{"dead letter without the error", "webhook.dead_lettered", "exhausted", "data.lastError", removed{}},
		{"unknown envelope attribute", "chain.reorg", "depth-2", "extra", "x"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, ok := byName[tc.event]
			if !ok {
				t.Fatalf("no message %s", tc.event)
			}
			i := slices.IndexFunc(m.examples, func(e example) bool { return e.name == tc.example })
			if i < 0 {
				t.Fatalf("%s has no example %s", tc.event, tc.example)
			}
			payload := clone(t, m.examples[i].payload)
			if err := m.payload.Validate(payload); err != nil {
				t.Fatalf("example is invalid before the change: %v", err)
			}
			edit(t, payload, tc.path, tc.value)
			if err := m.payload.Validate(payload); err == nil {
				t.Errorf("%s = %v accepted", tc.path, tc.value)
			}
		})
	}
}
