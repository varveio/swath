// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azure_test

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/replay/server"
	"github.com/varveio/swath/go/internal/replay/sortfixture"
	"github.com/varveio/swath/go/internal/replay/store"
	"github.com/varveio/swath/go/internal/replay/store/memstore"
)

// TestAzureEvidenceReplay replays every Azure capture of the published
// evidence archive (docs/replay-provider-evidence.tar.gz, data only)
// against the Go server serving the archive's corpus sorted, and compares
// each exchange or walk with the native capture as the reference
// conformance test does. Each outcome must equal the archive's pin in
// azure/replay-expectations.json or be better: a pinned mismatch the Go
// server now matches is an improvement, reported by id and not a failure;
// a pinned match that fails, or a mismatch other than the pinned one, is a
// failure.
//
// It runs with SWATH_REPLAY_FIXTURES=ROOT. The corpus is ROOT's
// fixtures/evidence-azure/capture (the Java-written DuckDB capture of the
// manifest), sorted with a 4,096-byte key limit and checked row for row
// against the manifest; without that capture the corpus is built in
// memory from the manifest. SWATH_REPLAY_PROVIDER_REPORT=DIR writes
// DIR/azure-outcomes.json, the per-capture outcomes in the reference
// report's shape.
func TestAzureEvidenceReplay(t *testing.T) {
	root := os.Getenv("SWATH_REPLAY_FIXTURES")
	if root == "" {
		t.Skip("SWATH_REPLAY_FIXTURES is not set: the evidence replay is a fixture-tier test")
	}
	ev := openEvidence(t, root)
	az, index, ep := ev.dir, ev.index, ev.endpoint

	outcomes := map[string]string{}
	for _, key := range slices.Sorted(mapKeys(index.Captures)) {
		file, sdk := ev.capture(t, key)
		err := catch(func() { ep.verify(t, key, file, sdk) })
		if err != nil {
			outcomes[key] = "mismatch: " + err.Error()
		} else {
			outcomes[key] = "match"
		}
	}
	if out := os.Getenv("SWATH_REPLAY_PROVIDER_REPORT"); out != "" {
		if err := os.MkdirAll(out, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(out, "azure-outcomes.json"), prettyOutcomes(outcomes), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	pins := expectations(t, filepath.Join(az, "replay-expectations.json"),
		filepath.Join("..", "..", "..", "..", "docs", "replay-provider-expectations.json"))
	var improved []string
	matched := 0
	for _, key := range slices.Sorted(mapKeys(outcomes)) {
		got := outcomes[key]
		if got == "match" {
			matched++
		}
		pin, ok := pins[key]
		switch {
		case !ok:
			t.Errorf("%s: no pinned expectation; outcome %s", key, got)
		case pin.match:
			if got != "match" {
				t.Errorf("%s: pinned match, got %s", key, got)
			}
		case got == "match":
			improved = append(improved, key)
		case !atPin(key, got, pin.mismatch):
			t.Errorf("%s: pinned mismatch %q, got %s", key, pin.mismatch, got)
		}
	}
	for key := range pins {
		if _, ok := outcomes[key]; !ok {
			t.Errorf("%s: expectation without a capture", key)
		}
	}
	t.Logf("%d captures: %d match (%d pinned matches), improvements over the pins: %v", len(outcomes), matched,
		matched-len(improved), improved)
}

// evidence is the extracted Azure evidence and a Go server serving its
// corpus on the Azure route alone.
type evidence struct {
	dir   string
	index struct {
		Captures  map[string]json.RawMessage `json:"captures"`
		Cases     []json.RawMessage          `json:"cases"`
		Manifests map[string]struct {
			File string `json:"file"`
		} `json:"manifests"`
	}
	endpoint *endpoint
}

// openEvidence extracts the archive and starts the server: over root's
// sorted capture when root has one, else over the manifest in memory.
func openEvidence(t *testing.T, root string) *evidence {
	t.Helper()
	dir := t.TempDir()
	ev := &evidence{dir: filepath.Join(extractEvidence(t, filepath.Join("..", "..", "..", "..", "docs", "replay-provider-evidence.tar.gz"), dir), "azure")}
	readJSON(t, filepath.Join(ev.dir, "index.json"), &ev.index)
	cases := map[string]caseRecord{}
	for _, raw := range ev.index.Cases {
		var c caseRecord
		if err := json.Unmarshal(raw, &c); err != nil {
			t.Fatal(err)
		}
		cases[c.ID] = c
	}
	manifest := ""
	for _, v := range slices.Sorted(mapKeys(ev.index.Manifests)) {
		if f := ev.index.Manifests[v].File; f != "" {
			manifest = f
			break
		}
	}
	objects := manifestObjects(t, filepath.Join(ev.dir, manifest))
	st, identity := corpusStore(t, root, dir, objects)
	srv, err := server.New(server.Config{Bucket: replayBucket, Host: "127.0.0.1", MetricsPort: -1, MaxConcurrentRequests: 16,
		ResponseBufferBudget: 256 << 20, MaxResponseBytes: 64 << 20, StopTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second,
		WriteTimeout: 30 * time.Second, SlowRequestLog: -1, Store: st, ServingMode: "SORTED", FixtureIdentity: identity,
		Protocols: []string{"azure"}, AzureAccount: replayAccount})
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	t.Cleanup(func() { srv.Shutdown(10 * time.Second) })
	ev.endpoint = &endpoint{port: srv.Port(), cases: cases}
	return ev
}

// captureEntry is one capture's index entry.
func (ev *evidence) capture(t *testing.T, key string) (file string, sdk json.RawMessage) {
	t.Helper()
	var entry struct {
		File       string          `json:"file"`
		SDKOptions json.RawMessage `json:"sdk_options"`
	}
	if err := json.Unmarshal(ev.index.Captures[key], &entry); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(ev.dir, entry.File), entry.SDKOptions
}

// wildcardPins are the pins whose actual= field is deliberately a prefix:
// the replay's own marker follows it and differs on every run. Every other
// mismatch pin must equal the outcome in full, so a pin ending in actual=
// pins an empty actual value, not any value.
var wildcardPins = map[string]bool{"azure-09h~2026-06-06": true, "azure-09j~2026-06-06": true}

// differs splits "<label> differs: expected=<e> actual=<a>".
func differs(s string) (label, expected, actual string, ok bool) {
	label, rest, ok := strings.Cut(s, " differs: expected=")
	if !ok {
		return "", "", "", false
	}
	i := strings.LastIndex(rest, " actual=")
	if i < 0 {
		return "", "", "", false
	}
	return label, rest[:i], rest[i+len(" actual="):], true
}

// atPin reports whether a mismatch outcome is the pinned one: equal in
// full, or, for a wildcard pin, the same label and expected value with an
// actual value that starts with the pinned one.
func atPin(key, outcome, pin string) bool {
	got, ok := strings.CutPrefix(outcome, "mismatch: ")
	if !ok {
		return false
	}
	if got == pin {
		return true
	}
	if !wildcardPins[key] {
		return false
	}
	pl, pe, pa, ok1 := differs(pin)
	gl, ge, ga, ok2 := differs(got)
	return ok1 && ok2 && pl == gl && pe == ge && strings.HasPrefix(ga, pa)
}

const (
	replayBucket         = "replay-bucket"
	replayAccount        = "replay"
	nativeEndpoint       = "https://{account}.blob.core.windows.net/"
	nativeContainer      = "{container}"
	tamperedToken        = "CONFORMANCE-TAMPERED-TOKEN"
	maxWalkPages         = 64
	archivedBOMPinPrefix = "Azure BOM/prolog differs: expected=\ufeff<?xml version=\"1.0\" encoding=\"utf-8\"?> actual=<?xml version=\"1.0\" encoding=\"utf-8\"?> | with the BOM restored: "
)

// duplicateQueryProbes are the rows whose subject is a duplicated query
// field.
var duplicateQueryProbes = map[string]bool{"azure-08": true, "azure-07h": true, "azure-07i": true, "azure-08m": true, "azure-08p": true}

type caseRecord struct {
	ID      string     `json:"id"`
	Query   string     `json:"query"`
	Headers [][]string `json:"headers"`
	Token   *struct {
		CaseID   string  `json:"case_id"`
		Mutation *string `json:"mutation"`
		Page     int     `json:"page"`
	} `json:"token"`
}

func mapKeys[M ~map[string]V, V any](m M) func(func(string) bool) {
	return func(yield func(string) bool) {
		for k := range m {
			if !yield(k) {
				return
			}
		}
	}
}

func readJSON(t *testing.T, path string, v any) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
}

// extractEvidence extracts the archive's regular files under dir, refusing
// any other entry and any path outside dir.
func extractEvidence(t *testing.T, archive, dir string) string {
	t.Helper()
	f, err := os.Open(archive)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(dir, "evidence")
	tr := tar.NewReader(gz)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			return root
		}
		if err != nil {
			t.Fatal(err)
		}
		target := filepath.Join(root, filepath.FromSlash(h.Name))
		if h.Typeflag != tar.TypeReg || !strings.HasPrefix(target, root+string(filepath.Separator)) {
			t.Fatalf("unexpected evidence archive entry %s", h.Name)
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			t.Fatal(err)
		}
		b, err := io.ReadAll(tr)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(target, b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// manifestObjects maps the corpus manifest to listed objects as the
// reference test does: name to key, size, updated (RFC 1123) to
// last-modified; nothing else.
func manifestObjects(t *testing.T, path string) []store.Object {
	t.Helper()
	var m struct {
		Objects []struct {
			Name    string `json:"name"`
			Size    int64  `json:"size"`
			Updated string `json:"updated"`
		} `json:"objects"`
	}
	readJSON(t, path, &m)
	out := make([]store.Object, len(m.Objects))
	for i, o := range m.Objects {
		tm, err := time.Parse(time.RFC1123, o.Updated)
		if err != nil {
			t.Fatal(err)
		}
		tm = tm.UTC()
		out[i] = store.Object{Key: []byte(o.Name), Size: &o.Size, LastModified: &tm}
	}
	slices.SortFunc(out, func(a, b store.Object) int { return bytes.Compare(a.Key, b.Key) })
	return out
}

// corpusStore serves the corpus sorted: the Java-written capture sorted by
// sortfixture (names reach 2,010 bytes, so with a 4,096-byte key limit),
// checked against the manifest, or the manifest in memory.
func corpusStore(t *testing.T, root, dir string, objects []store.Object) (store.Store, string) {
	t.Helper()
	capture := filepath.Join(root, "fixtures", "evidence-azure", "capture")
	if _, err := os.Stat(capture); err != nil {
		t.Logf("no %s: serving the manifest from memory", capture)
		st, err := memstore.New(objects, memstore.Options{Rollup: true})
		if err != nil {
			t.Fatal(err)
		}
		return st, "memory"
	}
	out := filepath.Join(dir, "sorted")
	if _, err := sortfixture.Run(context.Background(), sortfixture.Options{Capture: capture, Output: out, KeyLimit: sortfixture.MaxKeyLimit}, io.Discard); err != nil {
		t.Fatal(err)
	}
	st, meta, err := store.Open(context.Background(), out, store.Options{Readers: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	rows, err := st.Rows(context.Background(), nil, true, nil, len(objects)+1, store.Projection{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != len(objects) {
		t.Fatalf("sorted capture has %d rows, the manifest %d", len(rows), len(objects))
	}
	for i := range rows {
		a, b := rows[i], objects[i]
		if !bytes.Equal(a.Key, b.Key) || *a.Size != *b.Size || !a.LastModified.Equal(*b.LastModified) {
			t.Fatalf("sorted capture row %d differs from the manifest: %q", i, a.Key)
		}
	}
	return st, meta.Identity
}

type pin struct {
	match    bool
	mismatch string
}

// expectations reads the archive's pins with the two adjustments the
// reference test applies: a pin opening with the archived BOM mismatch
// expects what follows it, and an entry in the repository's updates file
// replaces the pin for its capture.
func expectations(t *testing.T, archived, updates string) map[string]pin {
	t.Helper()
	var raw map[string]json.RawMessage
	readJSON(t, archived, &raw)
	var file struct {
		Azure map[string]json.RawMessage `json:"azure"`
	}
	readJSON(t, updates, &file)
	out, err := adjustPins(raw, file.Azure)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// adjustPins applies the reference's two adjustments to the archived
// pins: a pin opening with the archived BOM mismatch expects what follows
// it ("nothing else" is a match), and each update replaces the pin for its
// capture; an update must name a pinned capture and change its pin.
func adjustPins(archived, updates map[string]json.RawMessage) (map[string]pin, error) {
	parse := func(v json.RawMessage) (pin, error) {
		var s string
		if json.Unmarshal(v, &s) == nil {
			return pin{match: s == "match"}, nil
		}
		var m struct {
			Mismatch string `json:"mismatch"`
		}
		err := json.Unmarshal(v, &m)
		return pin{mismatch: m.Mismatch}, err
	}
	out := map[string]pin{}
	for k, v := range archived {
		p, err := parse(v)
		if err != nil {
			return nil, err
		}
		if rest, ok := strings.CutPrefix(p.mismatch, archivedBOMPinPrefix); ok {
			switch rest {
			case "":
				return nil, errors.New("archived BOM pin names no remainder")
			case "nothing else":
				p = pin{match: true}
			default:
				p = pin{mismatch: rest}
			}
		}
		out[k] = p
	}
	for _, k := range slices.Sorted(mapKeys(updates)) {
		old, ok := out[k]
		if !ok {
			return nil, fmt.Errorf("expectation update for %s, which the archive does not pin", k)
		}
		p, err := parse(updates[k])
		if err != nil {
			return nil, err
		}
		if p == old {
			return nil, fmt.Errorf("expectation update for %s repeats the expectation it replaces", k)
		}
		out[k] = p
	}
	return out, nil
}

// prettyOutcomes renders the outcomes as the reference report does
// (sorted keys, two-space indent, " : " between key and value).
func prettyOutcomes(m map[string]string) []byte {
	var b bytes.Buffer
	b.WriteString("{")
	for i, k := range slices.Sorted(mapKeys(m)) {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString("\n  " + jsonString(k) + " : " + jsonString(m[k]))
	}
	b.WriteString("\n}\n")
	return b.Bytes()
}

func jsonString(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch {
		case r == '"' || r == '\\':
			b.WriteByte('\\')
			b.WriteRune(r)
		case r == '\n':
			b.WriteString(`\n`)
		case r == '\r':
			b.WriteString(`\r`)
		case r == '\t':
			b.WriteString(`\t`)
		case r == '\b':
			b.WriteString(`\b`)
		case r == '\f':
			b.WriteString(`\f`)
		case r < 0x20:
			fmt.Fprintf(&b, `\u%04X`, r)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
	return b.String()
}

// request is one request's scope as the receipt checks see it.
type request struct {
	method, path, query string
	headers             map[string][]string
}

func newRequest(method, path, query string, headers [][2]string) request {
	h := map[string][]string{}
	seenCase := map[string]string{}
	for _, kv := range headers {
		lower := strings.ToLower(kv[0])
		if first, ok := seenCase[lower]; ok && first != kv[0] {
			fail("duplicate request header name")
		}
		seenCase[lower] = kv[0]
		h[lower] = append(h[lower], kv[1])
	}
	return request{method, path, query, h}
}

func (r request) header(name string) *string {
	v, ok := r.headers[name]
	if !ok {
		return nil
	}
	require(len(v) == 1, "duplicate request header "+name)
	return &v[0]
}

// step is one captured exchange.
type step struct {
	phase    string
	request  request
	exchange exchange
}

type capture struct {
	kind, probeID string
	steps         []step
}

type wireExchange struct {
	Request struct {
		Headers []struct{ Name, Value string } `json:"headers"`
		Method  string                         `json:"method"`
		Path    string                         `json:"path"`
		Query   string                         `json:"query"`
	} `json:"request"`
	Response struct {
		Headers    []struct{ Name, Value string } `json:"headers"`
		Status     int                            `json:"status"`
		BodyBase64 string                         `json:"body_base64"`
	} `json:"response"`
}

func (w wireExchange) step(phase string) step {
	var rh, sh [][2]string
	for _, h := range w.Request.Headers {
		rh = append(rh, [2]string{h.Name, h.Value})
	}
	for _, h := range w.Response.Headers {
		sh = append(sh, [2]string{h.Name, h.Value})
	}
	body, err := base64.StdEncoding.DecodeString(w.Response.BodyBase64)
	if err != nil {
		fail("capture body is not base64")
	}
	return step{phase, newRequest(w.Request.Method, w.Request.Path, w.Request.Query, rh), newExchange(w.Response.Status, sh, body)}
}

// evidenceBytes are a capture's bytes; a .gz file holds them gzipped.
func evidenceBytes(file string) []byte {
	b, err := os.ReadFile(file)
	if err != nil {
		fail("%v", err)
	}
	if !strings.HasSuffix(file, ".gz") {
		return b
	}
	r, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		fail("%v", err)
	}
	out, err := io.ReadAll(r)
	if err != nil {
		fail("%v", err)
	}
	return out
}

type rawCapture struct {
	Provider      string `json:"provider"`
	SchemaVersion string `json:"schema_version"`
	EvidenceKind  string `json:"evidence_kind"`
	ProbeID       string `json:"probe_id"`
	wireExchange
	Steps []struct {
		Phase    string       `json:"phase"`
		Exchange wireExchange `json:"exchange"`
	} `json:"steps"`
}

func readCapture(file string) (capture, rawCapture) {
	var raw rawCapture
	if err := json.Unmarshal(evidenceBytes(file), &raw); err != nil {
		fail("%v", err)
	}
	require(raw.Provider == "azure", "capture provider differs")
	kind := ""
	switch raw.SchemaVersion {
	case "provider-capture-v1":
		kind = "exchange"
	case "provider-capture-run-v1":
		kind = raw.EvidenceKind
	}
	require(slices.Contains([]string{"exchange", "walk", "sdk_run", "retry_run"}, kind), "capture schema/kind differs")
	c := capture{kind: kind, probeID: raw.ProbeID}
	if kind == "exchange" {
		c.steps = []step{raw.wireExchange.step("exchange")}
	} else {
		for _, s := range raw.Steps {
			c.steps = append(c.steps, s.Exchange.step(s.Phase))
		}
		require(len(c.steps) >= 2 && len(c.steps) <= 1024, "run capture is incomplete/unbounded")
	}
	return c, raw
}

// endpoint issues the replay side of each capture with replay's own
// markers.
type endpoint struct {
	port  int
	cases map[string]caseRecord
	// mutate, when set, corrupts the replay's answers (in page order)
	// before they are compared.
	mutate func([]exchange) []exchange
}

func (e *endpoint) apply(pages []exchange) []exchange {
	if e.mutate == nil {
		return pages
	}
	return e.mutate(slices.Clone(pages))
}

func (e *endpoint) replayPath() string { return "/" + replayAccount + "/" + replayBucket }

func (e *endpoint) replayEndpoint() string {
	return "http://127.0.0.1:" + strconv.Itoa(e.port) + "/" + replayAccount + "/"
}

func (e *endpoint) verify(t *testing.T, key, file string, sdkOptions json.RawMessage) {
	c, raw := readCapture(file)
	caseID, _, _ := strings.Cut(key, "~")
	record, ok := e.cases[caseID]
	require(ok, "capture has no case record")
	switch {
	case raw.SchemaVersion == "provider-capture-v1":
		e.verifyExchange(c, record)
	case raw.EvidenceKind == "sdk_run":
		e.verifySDK(c, sdkOptions)
	case raw.EvidenceKind == "walk":
		e.verifyWalk(c)
	default:
		fail("no replay verifier for %s captures", raw.EvidenceKind)
	}
}

func (e *endpoint) verifyExchange(c capture, record caseRecord) {
	native := c.steps[0]
	query := native.request.query
	tokens := map[string]string{}
	if nativeToken := queryValue(query, "marker"); nativeToken != nil && *nativeToken != "" {
		var mutation *string
		if record.Token != nil {
			mutation = record.Token.Mutation
		}
		var replayToken *string
		if mutation != nil && *mutation == "tampered" {
			replayToken = sp(tamperedToken)
		} else {
			replayToken = e.sourceToken(record, native.request.header("x-ms-version"))
		}
		require(replayToken != nil, "replay source page is terminal")
		if mutation != nil && *mutation != "tampered" {
			replayToken = sp(mutate(*replayToken, *mutation))
		}
		encoded := encode(*replayToken)
		tokens[encoded] = *nativeToken
		query = strings.ReplaceAll(query, "marker="+*nativeToken, "marker="+encoded)
	}
	headers := replayHeaders(native.request, &record)
	response := e.apply([]exchange{e.send(native.request.method, query, headers)})[0]
	replayRequest := newRequest(native.request.method, e.replayPath(), query, headers)

	assertMapped(native.request.path, replayRequest.path, nativeEndpoint, e.replayEndpoint())
	assertRequest(native.request, replayRequest, replayRequest.path, tokens, c.probeID)
	version := native.request.header("x-ms-version")
	clientID := native.request.header("x-ms-client-request-id")
	normalized := normalizedResponse(response, clientID)
	assertAzure(native.exchange, normalized, nativeEndpoint, e.replayEndpoint(), nativeContainer, replayBucket,
		version, clientID, token(native.request.query, "marker"), token(mappedQuery(replayRequest.query, tokens), "marker"),
		native.request.method)
}

type replayStep struct {
	request  request
	exchange exchange
}

func (e *endpoint) verifyWalk(c capture) {
	first := c.steps[0].request
	headers := replayHeaders(first, nil)
	var replay []replayStep
	tokens := map[string]string{}
	var tok *string
	for p := 0; p < maxWalkPages; p++ {
		query := first.query
		if tok != nil {
			query += "&marker=" + encode(*tok)
			if p < len(c.steps) {
				if nt := queryValue(c.steps[p].request.query, "marker"); nt != nil {
					tokens[encode(*tok)] = *nt
				}
			}
		}
		resp := e.send("GET", query, headers)
		replay = append(replay, replayStep{newRequest("GET", e.replayPath(), query, headers), resp})
		tok = nil
		if resp.status == 200 {
			tok = nextMarker(resp.body)
		}
		if tok == nil {
			break
		}
	}
	pages := make([]exchange, len(replay))
	for i := range replay {
		pages[i] = replay[i].exchange
	}
	pages = e.apply(pages)
	walked := make([]replayStep, len(pages))
	for i := range pages {
		walked[i] = replayStep{replay[min(i, len(replay)-1)].request, pages[i]}
	}
	replay = walked
	e.verifyRun(c, replay, tokens)
}

// verifyRun is the run receipt check: the requests carry the captured
// scope, and the walks list the same inventory.
func (e *endpoint) verifyRun(c capture, replay []replayStep, tokens map[string]string) {
	require(c.kind != "exchange", "Azure run receipt needs typed run capture")
	require(len(replay) > 0, "Azure replay run is empty")
	for _, s := range replay {
		assertMapped(c.steps[0].request.path, s.request.path, nativeEndpoint, e.replayEndpoint())
	}
	assertWalkRequests(c, replay, replay[0].request.path)
	version := c.steps[0].request.header("x-ms-version")
	var nativePages, replayPages []capturedPage
	for _, s := range c.steps {
		nativePages = append(nativePages, azurePage(s.request, s.exchange))
	}
	for _, s := range replay {
		id := s.request.header("x-ms-client-request-id")
		if id != nil {
			id = sp(opaqueDigest(*id))
		}
		replayPages = append(replayPages, azurePage(s.request, normalizedResponse(s.exchange, id)))
	}
	assertAzureWalk(nativePages, replayPages, nativeEndpoint, e.replayEndpoint(), nativeContainer, replayBucket, version, 1024, 1_000_000)
}

func assertWalkRequests(c capture, replay []replayStep, replayPath string) {
	nativeFirst := c.steps[0].request
	scope := withoutField(nativeFirst.query, "marker")
	for _, s := range c.steps {
		require(withoutField(s.request.query, "marker") == scope, "native walk scope changed between pages")
		id := s.request.header("x-ms-client-request-id")
		var echo *string
		if v, ok := s.exchange.headers["x-ms-client-request-id"]; ok {
			echo = &v[0]
		}
		require(show(id) == show(echo) && isNil(id) == isNil(echo), "native response client ID differs from captured request")
	}
	require(token(nativeFirst.query, "marker") == nil && token(replay[0].request.query, "marker") == nil,
		"a walk must start without a continuation token")
	for i, s := range replay {
		r := s.request
		require(nativeFirst.method == r.method && r.path == replayPath, "replay request method/path differs from captured probe")
		require(withoutField(r.query, "marker") == scope, "replay request query differs from captured probe")
		require(noUnexpectedDuplicates(r.query, duplicateQueryProbes[c.probeID]), "duplicate query field outside duplicate-parameter probe")
		if c.kind == "sdk_run" {
			assertSDKHeaders(nativeFirst, r)
		}
		require(show(nativeFirst.header("x-ms-version")) == show(r.header("x-ms-version")), "replay request service version differs")
		replayID := r.header("x-ms-client-request-id")
		if replayID != nil {
			replayID = sp(opaqueDigest(*replayID))
		}
		if i < len(c.steps) {
			nativeID := c.steps[i].request.header("x-ms-client-request-id")
			require(show(nativeID) == show(replayID) && isNil(nativeID) == isNil(replayID), "replay request client ID differs")
		}
		normalizedResponse(s.exchange, replayID)
	}
}

func assertSDKHeaders(native, replay request) {
	classified := 0
	for _, n := range []string{"user-agent", "x-goog-api-client"} {
		nv := native.header(n)
		if nv == nil {
			continue
		}
		classified++
		rv := replay.header(n)
		require(rv != nil, "replay SDK header "+n+" is missing")
		replayTokens := strings.Fields(*rv)
		for _, tk := range strings.Fields(*nv) {
			ok := slices.Contains(replayTokens, tk)
			if strings.HasPrefix(tk, "gl-java/") {
				ok = slices.ContainsFunc(replayTokens, func(s string) bool { return strings.HasPrefix(s, "gl-java/") })
			}
			require(ok, "replay SDK product/version token differs")
		}
	}
	require(classified > 0, "native SDK run lacks classified product/version header")
}

func azurePage(r request, e exchange) capturedPage {
	return capturedPage{marker: token(r.query, "marker"), prefix: decodedQuery(r.query, "prefix"),
		delimiter: decodedQuery(r.query, "delimiter"), maxResults: decodedQuery(r.query, "maxresults"), exchange: e}
}

// decodedQuery is a form-decoded query value ('+' is a space).
func decodedQuery(query, name string) *string {
	v := token(query, name)
	if v == nil {
		return nil
	}
	d, err := url.QueryUnescape(*v)
	if err != nil {
		fail("URLDecoder: %v", err)
	}
	d = strings.ToValidUTF8(d, "\ufffd")
	return &d
}

func assertMapped(nativePath, replayPath, nativeEP, replayEP string) {
	if nativePath != "/"+nativeContainer || replayPath != "/"+replayAccount+"/"+replayBucket {
		fail("Azure native-host to replay-account-path mapping differs")
	}
	if nativeEP != nativeEndpoint {
		fail("Azure native ServiceEndpoint shape differs")
	}
	u, err := url.Parse(replayEP)
	if err != nil || u.Scheme != "http" && u.Scheme != "https" || u.Hostname() == "" || u.Path != "/"+replayAccount+"/" ||
		u.RawQuery != "" || u.Fragment != "" {
		fail("Azure replay ServiceEndpoint shape differs")
	}
}

func assertRequest(native, replay request, replayPath string, tokens map[string]string, probeID string) {
	require(native.method == replay.method && replay.path == replayPath, "replay request method/path differs from captured probe")
	require(native.query == mappedQuery(replay.query, tokens), "replay request query differs from captured probe")
	dup := duplicateQueryProbes[probeID]
	require(noUnexpectedDuplicates(native.query, dup) && noUnexpectedDuplicates(replay.query, dup),
		"duplicate query field outside duplicate-parameter probe")
	nv := native.header("x-ms-version")
	require(nv != nil && show(nv) == show(replay.header("x-ms-version")), "replay request service version differs")
	nativeID, replayID := native.header("x-ms-client-request-id"), replay.header("x-ms-client-request-id")
	if replayID != nil {
		replayID = sp(opaqueDigest(*replayID))
	}
	require(show(nativeID) == show(replayID) && isNil(nativeID) == isNil(replayID), "replay request client ID differs")
}

// normalizedResponse checks the replay's client request id echo against
// the captured digest and puts the digest in its place.
func normalizedResponse(e exchange, nativeClientID *string) exchange {
	v, ok := e.headers["x-ms-client-request-id"]
	if !ok {
		require(nativeClientID == nil, "replay response omitted captured client ID")
		return e
	}
	require(len(v) == 1, "duplicate replay client ID response header")
	require(nativeClientID != nil && *nativeClientID == opaqueDigest(v[0]), "replay response client ID differs from captured request")
	h := map[string][]string{}
	for k, vs := range e.headers {
		h[k] = vs
	}
	h["x-ms-client-request-id"] = []string{*nativeClientID}
	return exchange{e.status, h, e.body}
}

func mappedQuery(query string, tokens map[string]string) string {
	parts := strings.Split(query, "&")
	for i, p := range parts {
		if v, ok := strings.CutPrefix(p, "marker="); ok && v != "" {
			m, ok := tokens[v]
			require(ok, "replay continuation token lacks explicit mapping")
			parts[i] = "marker=" + m
		}
	}
	return strings.Join(parts, "&")
}

// javaSplit splits on sep and drops trailing empty fields.
func javaSplit(s, sep string) []string {
	parts := strings.Split(s, sep)
	for len(parts) > 0 && parts[len(parts)-1] == "" {
		parts = parts[:len(parts)-1]
	}
	if len(parts) == 0 {
		return []string{""}
	}
	return parts
}

func token(query, name string) *string {
	var out *string
	for _, p := range javaSplit(query, "&") {
		if v, ok := strings.CutPrefix(p, name+"="); ok {
			require(out == nil, "duplicate query field "+name)
			out = &v
		}
	}
	return out
}

func queryValue(query, name string) *string {
	var out *string
	for _, p := range javaSplit(query, "&") {
		if v, ok := strings.CutPrefix(p, name+"="); ok {
			if out != nil {
				fail("duplicate %s in query", name)
			}
			out = &v
		}
	}
	return out
}

func withoutField(query, field string) string {
	var parts []string
	for _, p := range strings.Split(query, "&") {
		if !strings.HasPrefix(p, field+"=") {
			parts = append(parts, p)
		}
	}
	return strings.Join(parts, "&")
}

func noUnexpectedDuplicates(query string, dup bool) bool {
	if dup {
		return true
	}
	names := map[string]bool{}
	for _, p := range javaSplit(query, "&") {
		n, _, _ := strings.Cut(p, "=")
		if names[n] {
			return false
		}
		names[n] = true
	}
	return true
}

// replayHeaders are the captured request headers with each client
// request id (a digest in the capture) replaced by the case record's raw
// value.
func replayHeaders(r request, record *caseRecord) [][2]string {
	raw := map[string]string{}
	if record != nil {
		for _, h := range record.Headers {
			if len(h) == 2 {
				raw[strings.ToLower(h[0])] = h[1]
			}
		}
	}
	var out [][2]string
	names := make([]string, 0, len(r.headers))
	for n := range r.headers {
		names = append(names, n)
	}
	slices.Sort(names)
	for _, n := range names {
		for _, v := range r.headers[n] {
			if n == "x-ms-client-request-id" {
				rv, ok := raw[n]
				if !ok || opaqueDigest(rv) != v {
					fail("captured client request ID has no raw case value")
				}
				v = rv
			}
			out = append(out, [2]string{n, v})
		}
	}
	return out
}

// sourceToken is replay's marker at the named page of the source case's
// own walk.
func (e *endpoint) sourceToken(record caseRecord, version *string) *string {
	require(record.Token != nil, "replay source page is terminal")
	src, ok := e.cases[record.Token.CaseID]
	require(ok, "capture has no case record")
	base := "restype=container&comp=list"
	if src.Query != "" {
		base += "&" + src.Query
	}
	var headers [][2]string
	if version != nil {
		headers = [][2]string{{"x-ms-version", *version}}
	}
	var tok *string
	for p := 0; p <= record.Token.Page; p++ {
		q := base
		if tok != nil {
			q += "&marker=" + encode(*tok)
		}
		resp := e.send("GET", q, headers)
		if resp.status != 200 {
			return nil
		}
		if tok = nextMarker(resp.body); tok == nil {
			return nil
		}
	}
	return tok
}

func nextMarker(body []byte) *string {
	text := string(body)
	start := strings.Index(text, "<NextMarker>")
	if start < 0 {
		return nil
	}
	rest := text[start+len("<NextMarker>"):]
	m := rest[:strings.Index(rest, "</NextMarker>")]
	if m == "" {
		return nil
	}
	return &m
}

// send writes the request as raw HTTP/1.1 on a new connection (the query
// exactly as given), and reads the answer with lower-case header names,
// the hop-by-hop Connection header left out.
func (e *endpoint) send(method, query string, headers [][2]string) exchange {
	conn, err := net.DialTimeout("tcp", "127.0.0.1:"+strconv.Itoa(e.port), 30*time.Second)
	if err != nil {
		fail("%v", err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(30 * time.Second))
	target := e.replayPath()
	if query != "" {
		target += "?" + query
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%s %s HTTP/1.1\r\nHost: 127.0.0.1:%d\r\nConnection: close\r\n", method, target, e.port)
	for _, h := range headers {
		fmt.Fprintf(&b, "%s: %s\r\n", h[0], h[1])
	}
	b.WriteString("\r\n")
	if _, err := io.WriteString(conn, b.String()); err != nil {
		fail("%v", err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: method})
	if err != nil {
		fail("%v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fail("%v", err)
	}
	var hs [][2]string
	for _, n := range slices.Sorted(mapKeys(resp.Header)) {
		if strings.EqualFold(n, "connection") {
			continue
		}
		for _, v := range resp.Header[n] {
			hs = append(hs, [2]string{strings.ToLower(n), v})
		}
	}
	// ReadResponse moves the framing into fields; put Content-Length back
	// where the raw answer had it.
	if resp.ContentLength >= 0 && resp.Header.Get("Content-Length") == "" && !slices.Contains(resp.TransferEncoding, "chunked") {
		hs = append(hs, [2]string{"content-length", strconv.FormatInt(resp.ContentLength, 10)})
	}
	return newExchange(resp.StatusCode, hs, body)
}

// encode percent-encodes all but the RFC 3986 unreserved bytes.
func encode(v string) string {
	var b strings.Builder
	for i := 0; i < len(v); i++ {
		c := v[i]
		if c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '.' || c == '_' || c == '~' {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

func opaqueDigest(v string) string {
	sum := sha256.Sum256([]byte(v))
	return "token-sha256-" + hex.EncodeToString(sum[:])
}

// mutate applies the native probe runner's deterministic token
// alterations.
func mutate(tok, mutation string) string {
	switch mutation {
	case "flip":
		last := byte('A')
		if tok[len(tok)-1] == 'A' {
			last = 'B'
		}
		return tok[:len(tok)-1] + string(last)
	case "truncate":
		if len(tok) > 4 {
			return tok[:len(tok)-4]
		}
		return tok[:1]
	case "flip-decoded":
		urlSafe := strings.ContainsAny(tok, "-_")
		padded := tok + strings.Repeat("=", (4-len(tok)%4)%4)
		enc := base64.StdEncoding
		if urlSafe {
			enc = base64.URLEncoding
		}
		if strings.ContainsAny(padded, "\r\n") {
			fail("replay token is not base64; the native mutation has no analogue")
		}
		raw, err := enc.DecodeString(padded)
		if err != nil {
			fail("replay token is not base64; the native mutation has no analogue")
		}
		if len(raw) == 0 {
			fail("replay token decodes to no bytes")
		}
		raw[len(raw)-1] ^= 1
		out := enc.EncodeToString(raw)
		if !strings.HasSuffix(tok, "=") {
			out = strings.TrimRight(out, "=")
		}
		return out
	}
	panic(fmt.Sprintf("unknown token mutation %s", mutation))
}
