// Package architecture checks that the diagrams and tables in docs/architecture match the code: the event
// catalog against the AsyncAPI contracts, the ER diagrams against the migrations, the MongoDB collections
// against the services, and the events on the sequence diagrams against the catalog.
package architecture

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

const root = "../.."

func read(t *testing.T, path ...string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(path...)) // #nosec G304 -- paths come from the repository tree
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// row is a line of the event catalog or its contract-derived counterpart.
type row struct {
	topic, typ, schema, producer, consumers string
}

func asMap(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

func ref(v any) string {
	r, _ := asMap(v)["$ref"].(string)
	return r
}

// contractRows reads every channel of the AsyncAPI documents with its messages.
func contractRows(t *testing.T) map[string]row {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(root, "contracts/asyncapi/*.yaml"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no AsyncAPI documents: %v", err)
	}
	rows := map[string]row{}
	for _, f := range files {
		var doc map[string]any
		if err := yaml.Unmarshal([]byte(read(t, f)), &doc); err != nil {
			t.Fatal(err)
		}
		messages := asMap(asMap(doc["components"])["messages"])
		for _, c := range asMap(doc["channels"]) {
			ch := asMap(c)
			if ch["$ref"] != nil {
				continue // the channel is defined in the document of its producer
			}
			var consumers []string
			for _, x := range ch["x-consumers"].([]any) {
				consumers = append(consumers, x.(string))
			}
			sort.Strings(consumers)
			for _, m := range asMap(ch["messages"]) {
				msg := asMap(messages[strings.TrimPrefix(ref(m), "#/components/messages/")])
				schema := filepath.Base(ref(asMap(msg["payload"])))
				r := row{
					topic:     ch["address"].(string),
					typ:       msg["name"].(string),
					schema:    schema,
					producer:  ch["x-producer"].(string),
					consumers: strings.Join(consumers, ", "),
				}
				rows[r.topic+"/"+r.typ] = r
			}
		}
	}
	return rows
}

func cells(line string) []string {
	parts := strings.Split(strings.Trim(strings.TrimSpace(line), "|"), "|")
	for i := range parts {
		parts[i] = strings.Trim(strings.TrimSpace(parts[i]), "`")
	}
	return parts
}

// catalogRows reads the main table of events.md: a row has 7 cells.
func catalogRows(t *testing.T) map[string]row {
	t.Helper()
	rows := map[string]row{}
	for _, line := range strings.Split(read(t, "events.md"), "\n") {
		if !strings.HasPrefix(line, "| `") {
			continue
		}
		c := cells(line)
		if len(c) != 7 {
			continue
		}
		consumers := strings.Split(c[6], ", ")
		if c[6] == "—" {
			consumers = nil
		}
		sort.Strings(consumers)
		r := row{topic: c[0], typ: c[3], schema: c[4], producer: c[5], consumers: strings.Join(consumers, ", ")}
		if _, dup := rows[r.topic+"/"+r.typ]; dup {
			t.Errorf("catalog: %s %s is listed twice", r.topic, r.typ)
		}
		rows[r.topic+"/"+r.typ] = r
	}
	return rows
}

func TestEventCatalogMatchesContracts(t *testing.T) {
	want, got := contractRows(t), catalogRows(t)
	for k, w := range want {
		g, ok := got[k]
		if !ok {
			t.Errorf("events.md: missing %s", k)
			continue
		}
		if g != w {
			t.Errorf("events.md: %s is %+v, contract says %+v", k, g, w)
		}
	}
	for k := range got {
		if _, ok := want[k]; !ok {
			t.Errorf("events.md: %s is not in the contracts", k)
		}
	}
	// Every schema file is in the catalog, except the shared ones.
	schemas, _ := filepath.Glob(filepath.Join(root, "contracts/schemas/*.json"))
	text := read(t, "events.md")
	for _, s := range schemas {
		name := filepath.Base(s)
		if !strings.Contains(text, "`"+name+"`") {
			t.Errorf("events.md: schema %s is not mentioned", name)
		}
	}
}

var (
	reTable  = regexp.MustCompile(`(?is)CREATE TABLE (\w+) \((.*?)\n\);`)
	reAlter  = regexp.MustCompile(`(?is)ALTER TABLE (\w+)(.*?);`)
	reAdd    = regexp.MustCompile(`(?i)ADD COLUMN (\w+)`)
	reColumn = regexp.MustCompile(`^\s{4}(\w+)\s+[A-Za-z]`)
	reERBody = regexp.MustCompile(`(?m)^    (\w+) \{\n((?:        .*\n)+?)    \}`)
	reERCol  = regexp.MustCompile(`(?m)^        \w+ (\w+)`)
)

// migrationTables returns table -> columns of the migrations of a service.
func migrationTables(t *testing.T, service string) map[string][]string {
	t.Helper()
	files, _ := filepath.Glob(filepath.Join(root, "services", service, "migrations/*.sql"))
	if len(files) == 0 {
		t.Fatalf("%s: no migrations", service)
	}
	tables := map[string][]string{}
	for _, f := range files {
		up := strings.SplitN(read(t, f), "-- +goose Down", 2)[0]
		for _, m := range reTable.FindAllStringSubmatch(up, -1) {
			for _, line := range strings.Split(m[2], "\n") {
				if c := reColumn.FindStringSubmatch(line); c != nil &&
					!slices.Contains([]string{"PRIMARY", "CHECK", "UNIQUE", "FOREIGN", "CONSTRAINT"}, strings.ToUpper(c[1])) {
					tables[m[1]] = append(tables[m[1]], c[1])
				}
			}
		}
		for _, m := range reAlter.FindAllStringSubmatch(up, -1) {
			for _, c := range reAdd.FindAllStringSubmatch(m[2], -1) {
				tables[m[1]] = append(tables[m[1]], c[1])
			}
		}
	}
	return tables
}

// section returns the part of a markdown file under the "## <title>" heading.
func section(t *testing.T, file, title string) string {
	t.Helper()
	parts := strings.Split(read(t, file), "\n## ")
	for _, p := range parts {
		if strings.HasPrefix(p, title+"\n") {
			return p
		}
	}
	t.Fatalf("%s: no section %q", file, title)
	return ""
}

func TestERMatchesMigrations(t *testing.T) {
	for _, service := range []string{"merchant-service", "invoice-service", "ledger-service"} {
		t.Run(service, func(t *testing.T) {
			want := migrationTables(t, service)
			got := map[string][]string{}
			for _, m := range reERBody.FindAllStringSubmatch(section(t, "er.md", service), -1) {
				for _, c := range reERCol.FindAllStringSubmatch(m[2], -1) {
					got[m[1]] = append(got[m[1]], c[1])
				}
			}
			for table, cols := range want {
				g, ok := got[table]
				if !ok {
					t.Errorf("er.md: table %s is missing", table)
					continue
				}
				sort.Strings(cols)
				sort.Strings(g)
				if !slices.Equal(cols, g) {
					t.Errorf("er.md: columns of %s are %v, migrations say %v", table, g, cols)
				}
			}
			for table := range got {
				if _, ok := want[table]; !ok {
					t.Errorf("er.md: table %s is not in the migrations", table)
				}
			}
		})
	}
}

func TestMongoCollectionsMatchCode(t *testing.T) {
	cases := []struct {
		file, collection string
	}{
		{"services/chain-watcher/internal/store/store.go", "chain_state"},
		{"services/chain-watcher/internal/projection/store.go", "tracked_addresses"},
		{"services/chain-watcher/internal/projection/store.go", "projection_state"},
		{"services/webhook-service/internal/merchants/merchants.go", "merchants"},
		{"services/webhook-service/internal/journal/journal.go", "deliveries"},
		{"services/reconciliation-service/internal/projection/store.go", "invoices"},
		{"services/reconciliation-service/internal/report/store.go", "reports"},
	}
	doc := read(t, "mongodb.md")
	for _, c := range cases {
		if !strings.Contains(read(t, root, c.file), `"`+c.collection+`"`) {
			t.Errorf("%s: collection %s is not in the code", c.file, c.collection)
		}
		if !strings.Contains(doc, "| `"+c.collection+"` |") {
			t.Errorf("mongodb.md: collection %s is missing", c.collection)
		}
	}
	// Fields of the documents are the bson tags of the code.
	fields := map[string][]string{
		"services/chain-watcher/internal/projection/store.go":          {"invoiceId", "createdAt", "expiresAt", "trackUntil", "rescanned"},
		"services/webhook-service/internal/journal/journal.go":         {"eventId", "eventType", "invoiceId", "merchantId", "attempt", "result", "statusCode", "reason", "secretVersion", "at", "durationMs"},
		"services/reconciliation-service/internal/report/store.go":     {"chainId", "blockNumber", "blockTime", "startedAt", "finishedAt", "addressesChecked", "mismatches", "afterWindow", "result", "published", "chainBalance", "invoicesTotal", "ledgerTotal", "excess", "trackUntil"},
		"services/reconciliation-service/internal/projection/store.go": {"merchantId", "address", "createdAt", "expiresAt", "paidAt", "transfers"},
	}
	for file, names := range fields {
		code := read(t, root, file)
		for _, n := range names {
			if !strings.Contains(code, `bson:"`+n) {
				t.Errorf("%s: no bson field %s", file, n)
			}
			if !strings.Contains(doc, "`"+n+"`") {
				t.Errorf("mongodb.md: field %s is not described", n)
			}
		}
	}
}

func TestSequenceDiagramsUseCatalogEvents(t *testing.T) {
	catalog := catalogRows(t)
	types := map[string]bool{}
	topics := map[string]bool{}
	for _, r := range catalog {
		types[r.typ] = true
		topics[r.topic] = true
	}
	text := read(t, "sequences.md")
	reEvent := regexp.MustCompile(`\b(invoice|chain|ledger|merchant|reconciliation|rate)\.[a-z_]+\b`)
	reTopic := regexp.MustCompile(`\b(webhook\.(?:retry\.(?:1m|5m|30m)|dlq))\b`)
	for _, m := range reEvent.FindAllString(text, -1) {
		if types[m] || topics[m] {
			continue
		}
		t.Errorf("sequences.md: %s is neither an event type nor a topic of the catalog", m)
	}
	for _, m := range reTopic.FindAllString(text, -1) {
		if !topics[m] {
			t.Errorf("sequences.md: topic %s is not in the catalog", m)
		}
	}
	// The five scenarios of SRS 9.
	for _, h := range []string{"Успешная оплата", "Недоплата", "Истечение и поздний платёж", "Реорганизация", "Повторная доставка вебхука"} {
		if !strings.Contains(text, "\n## "+h+"\n") {
			t.Errorf("sequences.md: no scenario %q", h)
		}
	}
	// Every invoice event of the lifecycle appears at least once.
	for k, r := range catalog {
		if strings.HasPrefix(r.typ, "invoice.") && !strings.Contains(text, r.typ) {
			t.Errorf("sequences.md: %s is not shown", k)
		}
	}
}
