// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package evidence

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/replay/legacy/legacytest"
	"github.com/varveio/swath/go/internal/replay/server"
	"github.com/varveio/swath/go/internal/replay/sortfixture"
	"github.com/varveio/swath/go/internal/replay/store"
)

// The GCS evidence replay. Every capture in the archive's gcs/ tree is
// replayed against the Go server serving the capture corpus: an exchange
// is re-sent with the bucket placeholder mapped to the served bucket and
// a native page token replaced by the replay's own token for the same
// position; a walk is re-walked with the replay's own tokens; an SDK run
// is re-walked with the run's request scope and client headers. Each
// answer is compared with the provider's under the conformance field
// policy (compare_test.go), and each outcome must equal its pin in
// gcs/replay-expectations.json or better: a pinned mismatch that now
// matches is an improvement, logged, never a failure.
//
// With SWATH_PROVIDER_REPORT=DIR the outcomes are written to
// DIR/gcs-outcomes.json, one "match" or "mismatch: ..." per capture id.

const (
	replayBucket  = "replay-bucket"
	tamperedToken = "CONFORMANCE-TAMPERED-TOKEN"
	maxWalkPages  = 64
	gcsReplayPath = "/storage/v1/b/" + replayBucket + "/o"
	gcsNativePath = "/storage/v1/b/{bucket}/o"
	archivedBOM   = "Azure BOM/prolog differs: expected=\uFEFF<?xml version=\"1.0\" encoding=\"utf-8\"?> actual=<?xml version=\"1.0\" encoding=\"utf-8\"?> | with the BOM restored: "
)

// duplicateQueryProbes are the rows whose subject is a repeated query
// field.
var duplicateQueryProbes = map[string]bool{"gcs-05": true, "azure-08": true, "gcs-05h": true, "gcs-05i": true,
	"gcs-06m": true, "azure-07h": true, "azure-07i": true, "azure-08m": true, "azure-08p": true, "s3-05h": true,
	"s3-05i": true, "s3-06m": true, "s3-12d": true}

var repo = filepath.Join("..", "..", "..", "..")

// fixtureRoot is SWATH_REPLAY_FIXTURES, or skips.
func fixtureRoot(t *testing.T) string {
	root := os.Getenv("SWATH_REPLAY_FIXTURES")
	if root == "" {
		t.Skip("SWATH_REPLAY_FIXTURES is not set: the evidence replay is not run")
	}
	return root
}

// extract unpacks the archive's regular files under dir; any other entry,
// or one that would land outside dir, is refused.
func extract(t *testing.T, dir string) string {
	t.Helper()
	f, err := os.Open(filepath.Join(repo, "docs", "replay-provider-evidence.tar.gz"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(dir, "evidence")
	tr := tar.NewReader(zr)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			return root
		}
		if err != nil {
			t.Fatal(err)
		}
		if !filepath.IsLocal(h.Name) {
			t.Fatalf("unexpected evidence archive entry %s", h.Name)
		}
		target := filepath.Join(root, filepath.FromSlash(h.Name))
		if h.Typeflag == tar.TypeDir {
			continue
		}
		if h.Typeflag != tar.TypeReg {
			t.Fatalf("unexpected evidence archive entry %s", h.Name)
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			t.Fatal(err)
		}
		b, err := io.ReadAll(io.LimitReader(tr, 64<<20))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(target, b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// evidenceBytes reads a capture; a .gz capture is stored compressed.
func evidenceBytes(path string) ([]byte, error) {
	b, err := os.ReadFile(path)
	if err != nil || !strings.HasSuffix(path, ".gz") {
		return b, err
	}
	zr, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	return io.ReadAll(zr)
}

type wireHeader struct{ Name, Value string }

type wireRequest struct {
	Method, Path, Query string
	Headers             []wireHeader
}

type wireResponse struct {
	Status     int
	Headers    []wireHeader
	BodyBase64 string `json:"body_base64"`
}

type wireExchange struct {
	Request  wireRequest
	Response wireResponse
}

type captureDoc struct {
	SchemaVersion string `json:"schema_version"`
	EvidenceKind  string `json:"evidence_kind"`
	Provider      string
	ProbeID       string `json:"probe_id"`
	wireExchange
	Steps []struct {
		Phase    string
		Exchange wireExchange
	}
}

type caseRecord struct {
	ID      string
	Query   string
	Headers [][]string
	Token   *struct {
		CaseID   *string `json:"case_id"`
		Mutation *string
		Page     int
	}
}

type evidenceIndex struct {
	Captures map[string]struct {
		File       string
		Kind       string
		SDKOptions sdkOptions `json:"sdk_options"`
	}
	Cases     []caseRecord
	Manifests map[string]struct{ File string }
}

// step is one captured request and its answer.
type step struct {
	phase    string
	request  request
	exchange exchange
}

// request is a request's scope as the receipt checks compare it.
type request struct {
	method, path, query string
	headers             map[string][]string
}

func (r request) header(name string) *string {
	v := r.headers[name]
	require(len(v) <= 1, "duplicate request header "+name)
	if len(v) == 0 {
		return nil
	}
	return &v[0]
}

func toStep(phase string, w wireExchange) step {
	req := request{method: w.Request.Method, path: w.Request.Path, query: w.Request.Query, headers: map[string][]string{}}
	for _, h := range w.Request.Headers {
		req.headers[strings.ToLower(h.Name)] = append(req.headers[strings.ToLower(h.Name)], h.Value)
	}
	var raw [][2]string
	for _, h := range w.Response.Headers {
		raw = append(raw, [2]string{h.Name, h.Value})
	}
	body, err := base64.StdEncoding.DecodeString(w.Response.BodyBase64)
	require(err == nil, "capture body is not base64")
	return step{phase, req, newExchange(w.Response.Status, raw, body)}
}

type capture struct {
	kind, probeID string
	steps         []step
}

func readCapture(path string) capture {
	b, err := evidenceBytes(path)
	if err != nil {
		fail("%v", err)
	}
	var doc captureDoc
	if err := json.Unmarshal(b, &doc); err != nil {
		fail("%v", err)
	}
	require(doc.Provider == "gcs", "capture provider differs")
	kind := ""
	switch doc.SchemaVersion {
	case "provider-capture-v1":
		kind = "exchange"
	case "provider-capture-run-v1":
		kind = doc.EvidenceKind
	}
	require(slices.Contains([]string{"exchange", "walk", "sdk_run", "retry_run"}, kind), "capture schema/kind differs")
	c := capture{kind: kind, probeID: doc.ProbeID}
	if kind == "exchange" {
		c.steps = []step{toStep("exchange", doc.wireExchange)}
	} else {
		for _, s := range doc.Steps {
			c.steps = append(c.steps, toStep(s.Phase, s.Exchange))
		}
		require(len(c.steps) >= 2 && len(c.steps) <= 1024, "run capture is incomplete/unbounded")
	}
	return c
}

// queryValue is the raw value of name in query; a repeated name is an
// error.
func queryValue(query, name string) *string {
	var found *string
	for _, part := range splitQuery(query) {
		if v, ok := strings.CutPrefix(part, name+"="); ok {
			if found != nil {
				fail("duplicate %s in query", name)
			}
			found = &v
		}
	}
	return found
}

// splitQuery splits on '&' dropping trailing empty parts, as the
// receipt checks' splitting does.
func splitQuery(q string) []string {
	parts := strings.Split(q, "&")
	for len(parts) > 1 && parts[len(parts)-1] == "" {
		parts = parts[:len(parts)-1]
	}
	return parts
}

func withoutField(q, field string) string {
	var keep []string
	for _, part := range strings.Split(q, "&") {
		if !strings.HasPrefix(part, field+"=") {
			keep = append(keep, part)
		}
	}
	return strings.Join(keep, "&")
}

func mappedQuery(q, field string, mapping map[string]string) string {
	var out []string
	for _, part := range strings.Split(q, "&") {
		if v, ok := strings.CutPrefix(part, field+"="); ok && v != "" {
			m, ok := mapping[v]
			require(ok, "replay continuation token lacks explicit mapping")
			out = append(out, field+"="+m)
		} else {
			out = append(out, part)
		}
	}
	return strings.Join(out, "&")
}

func noUnexpectedDuplicates(q string, duplicateProbe bool) bool {
	if duplicateProbe {
		return true
	}
	names := map[string]bool{}
	for _, part := range splitQuery(q) {
		name, _, _ := strings.Cut(part, "=")
		if names[name] {
			return false
		}
		names[name] = true
	}
	return true
}

func receiptToken(q string) *string {
	var found *string
	for _, part := range splitQuery(q) {
		if v, ok := strings.CutPrefix(part, "pageToken="); ok {
			require(found == nil, "duplicate query field pageToken")
			found = &v
		}
	}
	return found
}

// encode percent-encodes all but RFC 3986's unreserved characters, as
// the native probe runner did.
func encode(v string) string {
	var b strings.Builder
	for i := 0; i < len(v); i++ {
		c := v[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.IndexByte("-._~", c) >= 0 {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

// mutate applies the native probe runner's deterministic token
// alteration.
func mutate(token, mutation string) string {
	switch mutation {
	case "flip":
		last := byte('A')
		if token[len(token)-1] == 'A' {
			last = 'B'
		}
		return token[:len(token)-1] + string(last)
	case "truncate":
		if len(token) > 4 {
			return token[:len(token)-4]
		}
		return token[:1]
	case "flip-decoded":
		enc := base64.StdEncoding
		if strings.ContainsAny(token, "-_") {
			enc = base64.URLEncoding
		}
		padded := token + strings.Repeat("=", (4-len(token)%4)%4)
		raw, err := enc.Strict().DecodeString(padded)
		if err != nil || strings.ContainsAny(token, "\r\n") {
			panic(&mismatch{"replay token is not base64; the native mutation has no analogue"})
		}
		require(len(raw) > 0, "replay token decodes to no bytes")
		raw[len(raw)-1] ^= 1
		out := enc.EncodeToString(raw)
		if !strings.HasSuffix(token, "=") {
			out = strings.TrimRight(out, "=")
		}
		return out
	}
	fail("unknown token mutation %s", mutation)
	return ""
}

// mutation corrupts replay answers in page order (the identity in normal
// runs).
type mutation func(pages []exchange) []exchange

// endpoint issues the replay side of the captures.
type endpoint struct {
	port   int
	cases  map[string]caseRecord
	mutate mutation
	index  *evidenceIndex
}

// send is one HTTP/1.1 exchange with the request target as given, byte
// for byte; the Connection header the client asked for is dropped from
// the answer.
func (e *endpoint) send(method, query string, headers [][2]string) exchange {
	conn, err := net.DialTimeout("tcp", "127.0.0.1:"+strconv.Itoa(e.port), 30*time.Second)
	if err != nil {
		fail("%v", err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(30 * time.Second))
	target := gcsReplayPath
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
	var raw [][2]string
	for name, vs := range resp.Header {
		if strings.EqualFold(name, "connection") {
			continue
		}
		for _, v := range vs {
			raw = append(raw, [2]string{name, v})
		}
	}
	return newExchange(resp.StatusCode, raw, body)
}

func nextToken(body []byte) *string {
	n, err := parseNode(body)
	if err != nil {
		fail("%v", err)
	}
	if t := n.get("nextPageToken"); t.kind != 0 && t.kind != 'z' {
		return &t.str
	}
	return nil
}

// verify replays one capture; it returns nil on a match.
func (e *endpoint) verify(id, path string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			m, ok := r.(*mismatch)
			if !ok {
				panic(r)
			}
			err = m
		}
	}()
	c := readCapture(path)
	caseID, _, _ := strings.Cut(id, "~")
	record, ok := e.cases[caseID]
	require(ok, "capture has no case record")
	switch c.kind {
	case "exchange":
		e.verifyExchange(c, record)
	case "walk":
		e.verifyWalk(c)
	case "sdk_run":
		e.verifySDK(c, e.index.Captures[id].SDKOptions)
	default:
		// A retry run is replayed attempt by attempt, not as a fresh
		// walk; there is no such verifier, so it is refused.
		fail("no replay verifier for %s captures", c.kind)
	}
	return nil
}

func (e *endpoint) verifyExchange(c capture, record caseRecord) {
	native := c.steps[0]
	query := native.request.query
	tokens := map[string]string{}
	if nativeToken := queryValue(query, "pageToken"); nativeToken != nil && *nativeToken != "" {
		require(record.Token != nil, "capture has no token source")
		var replayToken *string
		mut := ""
		if record.Token.Mutation != nil {
			mut = *record.Token.Mutation
		}
		if mut == "tampered" {
			replayToken = strp(tamperedToken)
		} else {
			replayToken = e.sourceToken(record.Token.CaseID, record.Token.Page)
		}
		require(replayToken != nil, "replay source page is terminal")
		tok := *replayToken
		if mut != "" && mut != "tampered" {
			tok = mutate(tok, mut)
		}
		encoded := encode(tok)
		tokens[encoded] = *nativeToken
		query = strings.ReplaceAll(query, "pageToken="+*nativeToken, "pageToken="+encoded)
	}
	var headers [][2]string
	for _, h := range sortedHeaders(native.request) {
		headers = append(headers, h)
	}
	resp := e.mutate([]exchange{e.send(native.request.method, query, headers)})[0]
	replay := request{method: native.request.method, path: gcsReplayPath, query: query}
	require(native.request.method == replay.method && replay.path == gcsReplayPath, "replay request method/path differs from captured probe")
	require(native.request.path == gcsNativePath, "native request path differs")
	require(native.request.query == mappedQuery(replay.query, "pageToken", tokens), "replay request query differs from captured probe")
	dup := duplicateQueryProbes[c.probeID]
	require(noUnexpectedDuplicates(native.request.query, dup) && noUnexpectedDuplicates(replay.query, dup),
		"duplicate query field outside duplicate-parameter probe")
	full := false
	if p := queryValue(native.request.query, "projection"); p != nil && *p == "full" {
		full = true
	}
	assertGCS(native.exchange, resp, "{bucket}", replayBucket, full)
}

// sortedHeaders is a request's headers in the order captured (names
// sorted for determinism, values in order).
func sortedHeaders(r request) [][2]string {
	var out [][2]string
	for _, name := range slices.Sorted(func(yield func(string) bool) {
		for k := range r.headers {
			if !yield(k) {
				return
			}
		}
	}) {
		for _, v := range r.headers[name] {
			out = append(out, [2]string{name, v})
		}
	}
	return out
}

// sourceToken walks the source case's own query to the named page and
// returns the replay's token there, nil when the walk ends first.
func (e *endpoint) sourceToken(caseID *string, page int) *string {
	require(caseID != nil, "token source names no case")
	record, ok := e.cases[*caseID]
	require(ok, "token source case is missing")
	var token *string
	for p := 0; p <= page; p++ {
		query := record.Query
		if token != nil {
			query += "&pageToken=" + encode(*token)
		}
		resp := e.send("GET", query, [][2]string{{"Accept", "application/json"}})
		if resp.status != 200 {
			return nil
		}
		if token = nextToken(resp.body); token == nil {
			return nil
		}
	}
	return token
}

// verifyWalk re-walks a walk with the replay's own tokens and compares
// the two complete walks.
func (e *endpoint) verifyWalk(c capture) {
	first := c.steps[0].request
	base := first.query
	headers := sortedHeaders(first)
	var replay []step
	var token *string
	for page := 0; page < maxWalkPages; page++ {
		query := base
		if token != nil {
			query = base + "&pageToken=" + encode(*token)
		}
		resp := e.send("GET", query, headers)
		replay = append(replay, step{"walk", request{method: "GET", path: gcsReplayPath, query: query, headers: headerMap(headers)}, resp})
		token = nil
		if resp.status == 200 {
			token = nextToken(resp.body)
		}
		if token == nil {
			break
		}
	}
	answers := make([]exchange, len(replay))
	for i := range replay {
		answers[i] = replay[i].exchange
	}
	answers = e.mutate(answers)
	walked := make([]step, len(answers))
	for i := range answers {
		walked[i] = step{"walk", replay[min(i, len(replay)-1)].request, answers[i]}
	}
	verifyRun(c, walked)
}

func headerMap(h [][2]string) map[string][]string {
	m := map[string][]string{}
	for _, kv := range h {
		m[strings.ToLower(kv[0])] = append(m[strings.ToLower(kv[0])], kv[1])
	}
	return m
}

// verifyRun is the receipt check of a walk: every replay
// request carries the captured scope, then the walks are compared.
func verifyRun(c capture, replay []step) {
	require(c.kind == "walk", "GCS walk receipt needs a walk capture")
	require(len(replay) > 0, "replay run is empty")
	nativeFirst := c.steps[0].request
	scope := withoutField(nativeFirst.query, "pageToken")
	for _, s := range c.steps {
		require(withoutField(s.request.query, "pageToken") == scope, "native walk scope changed between pages")
	}
	require(receiptToken(nativeFirst.query) == nil && receiptToken(replay[0].request.query) == nil,
		"a walk must start without a continuation token")
	for _, s := range replay {
		require(s.phase == "walk", "replay run phase differs")
		require(nativeFirst.method == s.request.method && s.request.path == gcsReplayPath, "replay request method/path differs from captured probe")
		require(withoutField(s.request.query, "pageToken") == scope, "replay request query differs from captured probe")
		require(noUnexpectedDuplicates(s.request.query, duplicateQueryProbes[c.probeID]), "duplicate query field outside duplicate-parameter probe")
	}
	var nativePages, replayPages []capturedPage
	for _, s := range c.steps {
		nativePages = append(nativePages, capturedPage{receiptToken(s.request.query), s.exchange})
	}
	for _, s := range replay {
		replayPages = append(replayPages, capturedPage{receiptToken(s.request.query), s.exchange})
	}
	full := false
	if p := queryValue(nativeFirst.query, "projection"); p != nil && *p == "full" {
		full = true
	}
	assertGCSWalk(nativePages, replayPages, "{bucket}", replayBucket, full, 1024, 1_000_000)
}

// expectations are the pins replay must meet: the archive's, with an
// archived Azure BOM pin replaced by what it found with the BOM restored,
// then the repository's updates for the provider.
func expectations(t *testing.T, archived, updates map[string]json.RawMessage) map[string]string {
	out := map[string]string{}
	for id, raw := range archived {
		var pin string
		if json.Unmarshal(raw, &pin) == nil {
			out[id] = pin
			continue
		}
		var m struct{ Mismatch string }
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatalf("pin %s: %v", id, err)
		}
		if rest, ok := strings.CutPrefix(m.Mismatch, archivedBOM); ok {
			if rest == "nothing else" {
				out[id] = "match"
			} else {
				out[id] = "mismatch:" + rest
			}
			continue
		}
		out[id] = "mismatch:" + m.Mismatch
	}
	for id, raw := range updates {
		prev, ok := out[id]
		if !ok {
			t.Fatalf("expectation update for %s, which the archive does not pin", id)
		}
		var pin string
		if json.Unmarshal(raw, &pin) != nil {
			var m struct{ Mismatch string }
			json.Unmarshal(raw, &m)
			pin = "mismatch:" + m.Mismatch
		}
		if pin == prev {
			t.Fatalf("expectation update for %s repeats the expectation it replaces", id)
		}
		out[id] = pin
	}
	return out
}

// corpus writes the evidence manifest's objects as a capture and sorts it
// with sort-fixture: the same mapping as the fixture inventory (name,
// size, updated in microseconds; no other metadata).
func corpus(t *testing.T, root, dir string, index *evidenceIndex) string {
	t.Helper()
	var manifest string
	for _, m := range index.Manifests {
		if m.File != "" {
			manifest = m.File
		}
	}
	b, err := os.ReadFile(filepath.Join(root, manifest))
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Objects []struct {
			Name    string
			Size    int64
			Updated string
		}
	}
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatal(err)
	}
	var rows []legacytest.Row
	latest := true
	for _, o := range doc.Objects {
		tm, err := time.Parse(time.RFC3339Nano, o.Updated)
		if err != nil {
			t.Fatal(err)
		}
		size, micros := o.Size, tm.UnixMicro()
		rows = append(rows, legacytest.Row{Key: o.Name, Size: &size, LastModified: &micros, IsLatest: &latest, RowType: "OBJECT"})
	}
	capture := filepath.Join(dir, "capture")
	if err := os.MkdirAll(capture, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := legacytest.Write(filepath.Join(capture, "part.parquet"), legacytest.Part{Rows: rows}); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "sorted")
	if _, err := sortfixture.Run(context.Background(), sortfixture.Options{Capture: capture, Output: out}, io.Discard); err != nil {
		t.Fatal(err)
	}
	return out
}

// serve starts the Go server on fixture with the GCS route.
func serve(t *testing.T, fixture string) int {
	t.Helper()
	st, meta, err := store.Open(context.Background(), fixture, store.Options{Readers: 2})
	if err != nil {
		t.Fatal(err)
	}
	srv, err := server.New(server.Config{Bucket: replayBucket, Host: "127.0.0.1", MetricsPort: -1, MaxConcurrentRequests: 16,
		ResponseBufferBudget: server.DefaultResponseBufferBudget, MaxResponseBytes: server.DefaultMaxResponseBytes,
		StopTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, SlowRequestLog: -1,
		Store: st, ServingMode: "SORTED", FixtureIdentity: meta.Identity, Protocols: []string{server.ProtocolGCS}})
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	t.Cleanup(func() { srv.Shutdown(5 * time.Second) })
	return srv.Port()
}

type evidence struct {
	root  string
	index *evidenceIndex
	cases map[string]caseRecord
}

func loadEvidence(t *testing.T) *evidence {
	root := filepath.Join(extract(t, t.TempDir()), "gcs")
	b, err := os.ReadFile(filepath.Join(root, "index.json"))
	if err != nil {
		t.Skipf("no committed gcs evidence: %v", err)
	}
	var index evidenceIndex
	if err := json.Unmarshal(b, &index); err != nil {
		t.Fatal(err)
	}
	cases := map[string]caseRecord{}
	for _, c := range index.Cases {
		cases[c.ID] = c
	}
	return &evidence{root, &index, cases}
}

// fixtures are the corpora the replay runs against: the sorted
// evidence-gcs fixture under the fixture root (go/bench/replay/fixtures)
// when present, and always the corpus written in the test.
func (ev *evidence) fixtures(t *testing.T, root string) map[string]string {
	out := map[string]string{"generated": corpus(t, ev.root, t.TempDir(), ev.index)}
	if fixture := filepath.Join(root, "fixtures", "evidence-gcs", "sorted"); isDir(fixture) {
		out["fixture"] = fixture
	}
	return out
}

func isDir(p string) bool {
	info, err := os.Stat(p)
	return err == nil && info.IsDir()
}

func TestGCSNativeCapturesMatchReplay(t *testing.T) {
	root := fixtureRoot(t)
	ev := loadEvidence(t)
	var archived, updates map[string]json.RawMessage
	readJSONFile(t, filepath.Join(ev.root, "replay-expectations.json"), &archived)
	var allUpdates map[string]json.RawMessage
	readJSONFile(t, filepath.Join(repo, "docs", "replay-provider-expectations.json"), &allUpdates)
	if raw, ok := allUpdates["gcs"]; ok {
		if err := json.Unmarshal(raw, &updates); err != nil {
			t.Fatal(err)
		}
	}
	pins := expectations(t, archived, updates)
	for name, fixture := range ev.fixtures(t, root) {
		t.Run(name, func(t *testing.T) {
			e := &endpoint{port: serve(t, fixture), cases: ev.cases, index: ev.index, mutate: func(p []exchange) []exchange { return p }}
			outcomes := map[string]string{}
			for id, c := range ev.index.Captures {
				if err := e.verify(id, filepath.Join(ev.root, c.File)); err != nil {
					outcomes[id] = "mismatch: " + err.Error()
				} else {
					outcomes[id] = "match"
				}
			}
			if dir := os.Getenv("SWATH_PROVIDER_REPORT"); dir != "" {
				writeOutcomes(t, filepath.Join(dir, name), outcomes)
			}
			matches, improvements := 0, []string{}
			for _, id := range slices.Sorted(mapsKeys(outcomes)) {
				got, pin := outcomes[id], pins[id]
				if got == "match" {
					matches++
				}
				switch {
				case pin == "":
					t.Errorf("%s: no expectation for this capture: %s", id, got)
				case pin == "match" && got != "match":
					t.Errorf("%s: pinned match, now %s", id, got)
				case strings.HasPrefix(pin, "mismatch:") && got == "match":
					improvements = append(improvements, id)
				case strings.HasPrefix(pin, "mismatch:") && !atPin(strings.TrimPrefix(pin, "mismatch:"), got):
					t.Errorf("%s: pinned %q, now %s", id, strings.TrimPrefix(pin, "mismatch:"), got)
				}
			}
			for id := range pins {
				if _, ok := outcomes[id]; !ok {
					t.Errorf("%s: expectation without a capture", id)
				}
			}
			t.Logf("%s: %d captures, %d match (%d pinned match), improvements: %v", name, len(outcomes), matches, countMatch(pins), improvements)
		})
	}
}

// tokenSpan is the per-run part a pin may leave open: a replay token's
// body after its prefix.
var tokenSpan = regexp.MustCompile(`^[A-Za-z0-9_=-]*`)

// replayTokenPrefixes are the prefixes of the replay's own tokens. A pin
// that ends with one stops where a token issued during the run begins
// (it differs on every run), so that span, and only that span, is a
// wildcard.
var replayTokenPrefixes = []string{"gcs1.", "az1.", "v2:"}

// atPin reports whether outcome is the pinned mismatch: the whole message,
// equal, except that a pin ending at a replay token's prefix accepts the
// token body there and nothing after it. A message that merely contains
// the pin (a longer reason, an extra clause) is not at the pin.
func atPin(pin, outcome string) bool {
	rest, ok := strings.CutPrefix(outcome, "mismatch: ")
	if !ok {
		return false
	}
	if rest == pin {
		return true
	}
	for _, prefix := range replayTokenPrefixes {
		if strings.HasSuffix(pin, prefix) && strings.HasPrefix(rest, pin) {
			tail := rest[len(pin):]
			return len(tokenSpan.FindString(tail)) == len(tail)
		}
	}
	return false
}

func countMatch(pins map[string]string) int {
	n := 0
	for _, p := range pins {
		if p == "match" {
			n++
		}
	}
	return n
}

func mapsKeys[V any](m map[string]V) func(func(string) bool) {
	return func(yield func(string) bool) {
		for k := range m {
			if !yield(k) {
				return
			}
		}
	}
}

func readJSONFile(t *testing.T, path string, v any) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatal(err)
	}
}

// writeOutcomes writes DIR/gcs-outcomes.json in the shape the suite's
// report writes: one member per capture id in id order, two-space indent,
// " : " between name and value, a final newline.
func writeOutcomes(t *testing.T, dir string, outcomes map[string]string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	b.WriteString("{")
	for i, id := range slices.Sorted(mapsKeys(outcomes)) {
		if i > 0 {
			b.WriteString(",")
		}
		k, _ := json.Marshal(id)
		v := jsonText(outcomes[id])
		fmt.Fprintf(&b, "\n  %s : %s", k, v)
	}
	b.WriteString("\n}\n")
	if err := os.WriteFile(filepath.Join(dir, "gcs-outcomes.json"), []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

// jsonText quotes s escaping only what JSON requires (quote, backslash,
// C0 controls), leaving other characters as UTF-8.
func jsonText(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch {
		case r == '"' || r == '\\':
			b.WriteByte('\\')
			b.WriteRune(r)
		case r < 0x20:
			fmt.Fprintf(&b, "\\u%04X", r)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
	return b.String()
}

// TestEvidenceRejectsCorruptedReplayResults corrupts replay answers that
// matched their captures: a lost, repeated or reordered item, a decoded
// name, an early end, a repeated prefix, a wrong status, a wrong error
// reason and an ill-formed UTF-8 byte in a name must each be rejected, so a comparator that tolerates them is
// caught here.
func TestEvidenceRejectsCorruptedReplayResults(t *testing.T) {
	fixtureRoot(t)
	ev := loadEvidence(t)
	port := serve(t, corpus(t, ev.root, t.TempDir(), ev.index))
	for _, c := range [][2]string{
		{"gcs-01a", "drop-first"}, {"gcs-01a", "duplicate-last"}, {"gcs-04a", "swap-order-pair"},
		{"gcs-05o", "double-decode"}, {"gcs-07q", "terminate-early"}, {"gcs-07c", "repeat-prefix"},
		{"gcs-07g", "wrong-status"}, {"gcs-07g", "wrong-error"}, {"gcs-04a", "invalid-utf8"},
		{"gcs-09a", "drop-first"}, {"gcs-09b", "repeat-prefix"}, {"gcs-09c", "terminate-early"},
	} {
		path := filepath.Join(ev.root, ev.index.Captures[c[0]].File)
		clean := &endpoint{port: port, cases: ev.cases, index: ev.index, mutate: func(p []exchange) []exchange { return p }}
		if err := clean.verify(c[0], path); err != nil {
			t.Fatalf("%s: %v", c[0], err)
		}
		bad := &endpoint{port: port, cases: ev.cases, index: ev.index, mutate: corruption(c[1])}
		if err := bad.verify(c[0], path); err == nil {
			t.Errorf("%s with %s was accepted", c[0], c[1])
		}
	}
	// A pinned mismatch whose reason is changed is no longer at its pin,
	// even though the new message contains the pinned one.
	const pin = "GCS error reason differs: expected=invalidParameter actual=invalid"
	path := filepath.Join(ev.root, ev.index.Captures["gcs-06f"].File)
	clean := &endpoint{port: port, cases: ev.cases, index: ev.index, mutate: func(p []exchange) []exchange { return p }}
	if err := clean.verify("gcs-06f", path); err == nil || !atPin(pin, "mismatch: "+err.Error()) {
		t.Fatalf("gcs-06f: %v", err)
	}
	extended := &endpoint{port: port, cases: ev.cases, index: ev.index, mutate: onFirst(func(b string) string {
		return strings.Replace(b, `"reason":"invalid"`, `"reason":"invalidFoo"`, 1)
	})}
	if err := extended.verify("gcs-06f", path); err == nil || atPin(pin, "mismatch: "+err.Error()) {
		t.Errorf("gcs-06f with an extended reason is at its pin: %v", err)
	}
}

func corruption(name string) mutation {
	switch name {
	case "drop-first":
		return onFirst(func(b string) string { return editItems(b, func(l []*node) []*node { return l[1:] }) })
	case "duplicate-last":
		return onFirst(func(b string) string { return editItems(b, func(l []*node) []*node { return append(l, l[len(l)-1]) }) })
	case "swap-order-pair":
		return onFirst(func(b string) string {
			return editItems(b, func(l []*node) []*node {
				n := len(l) - 1
				l[n], l[n-1] = l[n-1], l[n]
				return l
			})
		})
	case "double-decode":
		return onFirst(func(b string) string {
			require(strings.Contains(b, "a%2Bb"), "mutation target a%2Bb is absent")
			return strings.Replace(b, "a%2Bb", "a+b", 1)
		})
	case "terminate-early":
		return func(p []exchange) []exchange {
			short := slices.Clone(p[:len(p)-1])
			last := len(short) - 1
			short[last] = withBody(short[last], regexp.MustCompile(`,?"nextPageToken":"[^"]*"`).ReplaceAllString(string(short[last].body), ""))
			return short
		}
	case "repeat-prefix":
		return func(p []exchange) []exchange {
			m := regexp.MustCompile(`"prefixes":\[("[^"]*")`).FindStringSubmatch(string(p[0].body))
			require(m != nil, "first page has no prefix to repeat")
			out := slices.Clone(p)
			body := string(out[1].body)
			if strings.Contains(body, `"prefixes":[`) {
				body = strings.Replace(body, `"prefixes":[`, `"prefixes":[`+m[1]+",", 1)
			} else {
				body = strings.TrimSuffix(body, "}") + `,"prefixes":[` + m[1] + "]}"
			}
			out[1] = withBody(out[1], body)
			return out
		}
	case "wrong-status":
		return func(p []exchange) []exchange {
			s := 400
			if p[0].status == 400 {
				s = 409
			}
			return []exchange{{s, p[0].headers, p[0].body}}
		}
	case "invalid-utf8":
		// A name's U+FFFD replaced by a raw 0xFF byte: a decoder that
		// repairs ill-formed UTF-8 would read the name back unchanged.
		return onFirst(func(b string) string {
			require(strings.Contains(b, "\uFFFD"), "mutation target U+FFFD is absent")
			return strings.Replace(b, "\uFFFD", "\xff", 1)
		})
	case "wrong-error":
		return onFirst(func(b string) string {
			return regexp.MustCompile(`"reason":"[A-Za-z]+"`).ReplaceAllLiteralString(b, `"reason":"conflict"`)
		})
	}
	panic(errors.New("unknown corruption " + name))
}

func onFirst(edit func(string) string) mutation {
	return func(p []exchange) []exchange {
		out := slices.Clone(p)
		out[0] = withBody(out[0], edit(string(out[0].body)))
		return out
	}
}

func editItems(body string, edit func([]*node) []*node) string {
	page := readJSON([]byte(body))
	page.get("items").elems = edit(page.get("items").elems)
	return string(page.encode(nil))
}

func withBody(e exchange, body string) exchange {
	h := map[string][]string{}
	for k, v := range e.headers {
		h[k] = v
	}
	if _, ok := h["content-length"]; ok {
		h["content-length"] = []string{strconv.Itoa(len(body))}
	}
	return exchange{e.status, h, []byte(body)}
}

func TestExpectationRules(t *testing.T) {
	raw := func(s string) json.RawMessage { return json.RawMessage(s) }
	archived := map[string]json.RawMessage{
		"only-bom":     raw(`{"mismatch":` + jsonText(archivedBOM+"nothing else") + `}`),
		"bom-and-echo": raw(`{"mismatch":` + jsonText(archivedBOM+"Delimiter echo differs: expected=null actual=") + `}`),
		"status":       raw(`{"mismatch":"GCS HTTP status differs: expected=200 actual=400"}`),
		"matched":      raw(`"match"`),
	}
	got := expectations(t, archived, map[string]json.RawMessage{"status": raw(`"match"`)})
	if got["only-bom"] != "match" || got["bom-and-echo"] != "mismatch:Delimiter echo differs: expected=null actual=" ||
		got["status"] != "match" || got["matched"] != "match" {
		t.Errorf("%v", got)
	}
}

// TestComparatorMessages holds the messages the pins are matched against.
func TestComparatorMessages(t *testing.T) {
	catch := func(f func()) (msg string) {
		defer func() {
			if r := recover(); r != nil {
				msg = r.(*mismatch).msg
			}
		}()
		f()
		return ""
	}
	h := [][2]string{{"Content-Type", "application/json; charset=UTF-8"}}
	page := func(items ...string) []byte {
		var b []string
		for _, n := range items {
			b = append(b, `{"kind":"storage#object","bucket":"B","name":"`+n+`","size":"1","updated":"2026-01-01T00:00:00.000000Z","etag":"e","generation":"1","metageneration":"1","storageClass":"STANDARD","contentType":"application/octet-stream"}`)
		}
		return []byte(`{"kind":"storage#objects","items":[` + strings.Join(b, ",") + `]}`)
	}
	native := newExchange(200, h, page("a b", "c"))
	for _, c := range []struct {
		replay exchange
		want   string
	}{
		{newExchange(400, h, nil), "GCS HTTP status differs: expected=200 actual=400"},
		{newExchange(200, h, page("a+b", "c")), "name 0 differs: expected=a b actual=a+b"},
		{newExchange(200, h, page("a b")), "item count differs: expected=2 actual=1"},
		{newExchange(200, append(h, [2]string{"Allow", "GET"}), page("a b", "c")), "unclassified HTTP header allow"},
		{newExchange(200, h, page("a b", "c")), ""},
	} {
		if got := catch(func() { assertGCS(native, c.replay, "B", "B", false) }); got != c.want {
			t.Errorf("got %q, want %q", got, c.want)
		}
	}
	e400 := func(reason string) exchange {
		return newExchange(400, h, []byte(`{"error":{"code":400,"message":"m","errors":[{"message":"m","domain":"global","reason":"`+reason+`"}]}}`))
	}
	if got := catch(func() { assertGCS(e400("invalidParameter"), e400("invalid"), "B", "B", false) }); got != "GCS error reason differs: expected=invalidParameter actual=invalid" {
		t.Errorf("%q", got)
	}
	if got := javaString(time.Date(2026, 1, 2, 3, 4, 5, 296_000_000, time.UTC)); got != "2026-01-02T03:04:05.296Z" {
		t.Errorf("%s", got)
	}
}

func TestPinsMatchWholeMessages(t *testing.T) {
	const pin = "GCS error reason differs: expected=invalidParameter actual=invalid"
	for outcome, want := range map[string]bool{
		"mismatch: " + pin:                 true,
		"mismatch: " + pin + "Foo":         false,
		"mismatch: " + pin + " | and more": false,
		"mismatch: prefix " + pin:          false,
		"match":                            false,
		"mismatch: GCS HTTP status differs: expected=200 actual=400": false,
	} {
		if got := atPin(pin, outcome); got != want {
			t.Errorf("%q: %v", outcome, got)
		}
	}
	const tokenPin = "replay marker echo differs: expected=token-sha256-96 actual=az1."
	for outcome, want := range map[string]bool{
		"mismatch: " + tokenPin + "AbC_-9":        true,
		"mismatch: " + tokenPin:                   true,
		"mismatch: " + tokenPin + "AbC and more":  false,
		"mismatch: " + tokenPin[:len(tokenPin)-1]: false,
	} {
		if got := atPin(tokenPin, outcome); got != want {
			t.Errorf("%q: %v", outcome, got)
		}
	}
}
