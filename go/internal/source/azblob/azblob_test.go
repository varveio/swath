// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblob

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spf13/pflag"

	"github.com/varveio/swath/go/internal/order"
	"github.com/varveio/swath/go/internal/registry"
	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source"
	"github.com/varveio/swath/go/internal/source/azblob/azblobtest"
	"github.com/varveio/swath/go/internal/source/azblob/client"
	"github.com/varveio/swath/go/internal/source/keyrange"
	"github.com/varveio/swath/go/internal/source/objhttp"
	"github.com/varveio/swath/go/internal/source/s3"
)

// highByte reports whether s holds a byte in EE..F4: a lead byte of a
// scalar at U+E000 and above, where Azure's order and byte order can
// disagree.
func highByte(s string) bool {
	return strings.ContainsFunc(s, func(r rune) bool { return r >= 0xE000 })
}

// base is the replay's endpoint for --endpoint-url: without the
// account path segment, which the client adds from the root.
func base(r *azblobtest.Replay) string { return strings.TrimSuffix(r.Endpoint, "/"+r.Account) }

type collect struct {
	mu   sync.Mutex
	rows []Row
}

func (c *collect) Write(_ context.Context, segs [][]Row) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, s := range segs {
		c.rows = append(c.rows, s...)
	}
	return nil
}

// run lists opt with workers through the order stage: the rows emitted
// before the end, the provider's stats and the listing's error.
func run(t *testing.T, opt Options, workers int) ([]Row, Stats, error) {
	t.Helper()
	opt.Workers = workers
	if opt.Client == nil {
		opt.Anonymous = true
	}
	p, err := New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	c := &collect{}
	_, err = order.Run[Range, Object](context.Background(), p, c, &Schema, order.Options{Workers: workers, Budget: 1 << 22})
	return c.rows, p.Stats(), err
}

func list(t *testing.T, opt Options, workers int) ([]Row, Stats) {
	t.Helper()
	rows, st, err := run(t, opt, workers)
	if err != nil {
		t.Fatal(err)
	}
	return rows, st
}

func keysOf(rows []Row) []string {
	keys := make([]string, len(rows))
	for i := range rows {
		keys[i] = rows[i].Key
	}
	return keys
}

// replayOptions lists the replay's container.
func replayOptions(r *azblobtest.Replay) Options {
	return Options{Account: r.Account, Container: r.Container, Endpoint: base(r)}
}

// TestListReplay: the provider lists the replay's Azure route exactly, in
// byte order, every field as the replay serves it (size i and Last-Modified
// i seconds after 2026-01-01 for the i-th key; the route's synthesised
// ETag, type, tier and content type), sequentially and with 64 workers,
// with the seed and structure probes (delimiter listings) engaged, at page
// sizes 1, 2, 7 and 5,000; the default page is 5,000. Where s3 is set, the
// same fixture through the replay's S3 route (the S3 provider) lists the
// same keys in the same order, once each, with the same size and
// Last-Modified at the Azure wire's second resolution (the dual-route
// oracle; ETag, blob type, tier and content type are the Azure route's
// own synthesis, replay/azure/xml.go, not compared).
//
// The corpus is the whole hostile one: names at U+E000 and above come
// through the byte-order adapter, which engages in every configuration.
func TestListReplay(t *testing.T) {
	keys := azblobtest.Keys()
	r := azblobtest.Serve(t, keys)
	const epoch = 1_767_225_600_000_000
	for _, c := range []struct {
		name    string
		pol     keyrange.Policy
		workers int
		s3      bool
	}{
		{"sequential", keyrange.Policy{Sequential: true}, 1, true},
		{"8 workers", keyrange.Policy{}, 8, false},
		{"64 workers, seed, structure", keyrange.Policy{Seed: true, Structure: keyrange.StructureOn}, 64, true},
		{"page size 1", keyrange.Policy{PageSize: 1, Seed: true, Structure: keyrange.StructureOn}, 64, false},
		{"page size 2", keyrange.Policy{PageSize: 2}, 8, false},
		{"page size 7", keyrange.Policy{PageSize: 7, Seed: true, Structure: keyrange.StructureOn}, 64, true},
		{"page size 5000", keyrange.Policy{PageSize: 5000, Sequential: true}, 1, false},
	} {
		opt := replayOptions(r)
		opt.Policy = c.pol
		rows, st := list(t, opt, c.workers)
		if got := keysOf(rows); !slices.Equal(got, keys) {
			t.Fatalf("%s: %d keys, want %d", c.name, len(got), len(keys))
		}
		for i, row := range rows {
			o := row.Data
			if o.Size != int64(i) || o.Mtime != epoch+int64(i)*1_000_000 || o.BlobType != "BlockBlob" || o.AccessTier != "Hot" ||
				o.ETag == "" || o.ContentType != "application/octet-stream" || o.ContentMD5 != "" || o.CreationTime != 0 {
				t.Fatalf("%s: row %d %q: %+v", c.name, i, row.Key, o)
			}
		}
		v := st.Counters.Value
		if v("order_splits") == 0 || v("order_seeks") == 0 {
			t.Fatalf("%s: order_splits %d, order_seeks %d: the adapter did not engage", c.name, v("order_splits"), v("order_seeks"))
		}
		if c.pol.Seed && v("seed.requests") == 0 || c.pol.Structure == keyrange.StructureOn && v("structure.requests") == 0 {
			t.Fatalf("%s: seed requests %d, structure requests %d: the delimiter listings did not engage", c.name, v("seed.requests"), v("structure.requests"))
		}
		if c.pol.Sequential {
			if want := int64((len(keys) + c.pol.PageSize - 1) / max(c.pol.PageSize, 1)); c.pol.PageSize > 0 && v("pages") != want {
				t.Fatalf("%s: %d pages, want %d of %d", c.name, v("pages"), want, c.pol.PageSize)
			}
			if c.pol.PageSize == 0 && v("pages") != 2 {
				t.Fatalf("%s: the default page size: %d pages for %d keys, want 2", c.name, v("pages"), len(keys))
			}
		}
		if !c.s3 {
			continue
		}
		sp, err := s3.New(context.Background(), s3.Options{Bucket: r.Container, Endpoint: r.S3Endpoint, PathStyle: true, Anonymous: true,
			Workers: c.workers, Policy: c.pol})
		if err != nil {
			t.Fatal(err)
		}
		var sc s3collect
		if _, err := order.Run[s3.Range, s3.Object](context.Background(), sp, &sc, &s3.Schema, order.Options{Workers: c.workers, Budget: 1 << 22}); err != nil {
			t.Fatal(err)
		}
		if len(sc.rows) != len(rows) {
			t.Fatalf("%s: azure %d rows, s3 %d", c.name, len(rows), len(sc.rows))
		}
		for i := range rows {
			a, b := rows[i], sc.rows[i]
			if a.Key != b.Key || a.Data.Size != b.Data.Size || a.Data.Mtime/1_000_000 != b.Data.Mtime/1_000_000 {
				t.Fatalf("%s: row %d: azure %q %d %d, s3 %q %d %d", c.name, i, a.Key, a.Data.Size, a.Data.Mtime, b.Key, b.Data.Size, b.Data.Mtime)
			}
		}
	}
	// A prefix scope.
	opt := replayOptions(r)
	opt.Prefix = "f/"
	rows, _ := list(t, opt, 8)
	if len(rows) != 5000 || rows[0].Key != "f/0000" {
		t.Fatalf("prefix f/: %d rows", len(rows))
	}
}

type s3collect struct {
	mu   sync.Mutex
	rows []s3.Row
}

func (c *s3collect) Write(_ context.Context, segs [][]s3.Row) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, s := range segs {
		c.rows = append(c.rows, s...)
	}
	return nil
}

// pages is a supplied client that answers fixed pages in turn and counts
// its List calls.
type pages struct {
	hns   bool
	err   error // AccountInfo's
	out   []client.ListOutput
	lists atomic.Int64
}

func (p *pages) AccountInfo(context.Context, string) (bool, error) { return p.hns, p.err }

func (p *pages) List(_ context.Context, in *client.ListInput) (*client.ListOutput, error) {
	n := int(p.lists.Add(1)) - 1
	if n >= len(p.out) {
		return &client.ListOutput{}, nil
	}
	out := p.out[n]
	return &out, nil
}

// fetchAll drains q through p.fetch, as the engine's chain would.
func fetchAll(t *testing.T, p *Provider, q keyrange.Query) []string {
	t.Helper()
	a := p.adapter()
	defer a.Release()
	ctx := context.WithValue(context.Background(), adapterKey{}, a)
	var got []string
	for {
		pg, err := p.fetch(ctx, &q)
		if err != nil {
			t.Fatalf("%+v: %v", q, err)
		}
		for _, r := range pg.Rows {
			got = append(got, r.Key)
		}
		got = append(got, pg.Prefixes...)
		if !pg.Truncated {
			slices.Sort(got)
			return got
		}
		q.Token = pg.Token
	}
}

// TestHighBounds: a cursor, a bound or a scope at a name at U+E000 and
// above, which the staged refusal used to refuse, lists exactly the keys
// after the cursor and up to the bound, in byte order, through fetch over
// the replay's Azure route; and fetch maps blobs to rows, prefixes to
// Prefixes.
func TestHighBounds(t *testing.T) {
	keys := azblobtest.Keys()
	r := azblobtest.Serve(t, keys)
	opt := replayOptions(r)
	opt.Anonymous, opt.Workers = true, 1
	p, err := New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []keyrange.Query{
		{After: "p\U0001f600", Max: 7},
		{After: "a", Through: "pｚ", Bounded: true, Max: 7},
		{After: "a", Through: "pｚ", Max: 7}, // unbounded: Through ignored
		{Prefix: "r\U00010000", Max: 2},
		{Prefix: "n1/n2/n3/", After: "n1/n2/n3/p\U00010000", Max: 5000},
	} {
		var want []string
		for _, k := range keys {
			if strings.HasPrefix(k, q.Prefix) && k > q.After && (!q.Bounded || k <= q.Through) {
				want = append(want, k)
			}
		}
		if got := fetchAll(t, p, q); !slices.Equal(got, want) {
			t.Fatalf("%+v: %d keys, want %d", q, len(got), len(want))
		}
	}
	c := &pages{out: []client.ListOutput{{Items: []client.Item{{Name: "a", Size: 3, BlobType: "PageBlob"}, {Name: "b/", Prefix: true},
		{Name: "c"}}}}}
	sp, err := New(context.Background(), Options{Account: "acct", Container: "c", Client: c, Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	a := sp.adapter()
	defer a.Release()
	ctx := context.WithValue(context.Background(), adapterKey{}, a)
	pg, err := sp.fetch(ctx, &keyrange.Query{Delimiter: "/", Max: 10})
	if err != nil || len(pg.Rows) != 2 || pg.Rows[0].Data.Size != 3 || pg.Rows[0].Data.BlobType != "PageBlob" ||
		!slices.Equal(pg.Prefixes, []string{"b/"}) || pg.Truncated {
		t.Fatalf("page %+v, %v", pg, err)
	}
}

// TestHighPrefixListed: a scope prefix that carries a byte in EE..F4,
// refused at open by the staged refusal, is listed exactly.
func TestHighPrefixListed(t *testing.T) {
	keys := azblobtest.Keys()
	r := azblobtest.Serve(t, keys)
	for _, prefix := range []string{"r\U00010000", "only-e/", "pｚ"} {
		opt := replayOptions(r)
		opt.Prefix = prefix
		rows, _ := list(t, opt, 8)
		want := slices.DeleteFunc(slices.Clone(keys), func(k string) bool { return !strings.HasPrefix(k, prefix) })
		if got := keysOf(rows); len(want) == 0 || !slices.Equal(got, want) {
			t.Fatalf("prefix %q: %q, want %q", prefix, got, want)
		}
	}
}

// sortedAPI serves keys in byte order, startFrom inclusive and the marker
// a key, recording every StartFrom sent.
type sortedAPI struct {
	keys []string
	mu   sync.Mutex
	from []string
}

func (s *sortedAPI) AccountInfo(context.Context, string) (bool, error) { return false, nil }

func (s *sortedAPI) List(_ context.Context, in *client.ListInput) (*client.ListOutput, error) {
	s.mu.Lock()
	s.from = append(s.from, in.StartFrom)
	s.mu.Unlock()
	start := in.StartFrom
	if in.Marker != "" {
		start = in.Marker
	}
	i, _ := slices.BinarySearch(s.keys, start)
	j := min(i+in.MaxResults, len(s.keys))
	out := &client.ListOutput{}
	for _, k := range s.keys[i:j] {
		out.Items = append(out.Items, client.Item{Name: k})
	}
	if j < len(s.keys) {
		out.NextMarker = s.keys[j]
	}
	return out, nil
}

// TestSynthesizedPivotListed: the engine's pivot alphabet includes every
// scalar from U+E000 up, so over keys that all sit below U+E000 (lead byte
// ED) its frontier splits synthesise bounds at U+E000 and above, which
// the staged refusal used to refuse. Through the adapter they are sent as
// they are and the run completes with exact output.
func TestSynthesizedPivotListed(t *testing.T) {
	var keys []string
	for r := rune(0xD000); r < 0xD7A0; r += 2 {
		keys = append(keys, "k"+string(r))
	}
	slices.Sort(keys)
	f := &sortedAPI{keys: keys}
	rows, _ := list(t, Options{Account: "acct", Container: "c", Client: f, Policy: keyrange.Policy{PageSize: 3}}, 64)
	if got := keysOf(rows); !slices.Equal(got, keys) {
		t.Fatalf("%d keys, want %d", len(got), len(keys))
	}
	if !slices.ContainsFunc(f.from, highByte) {
		t.Fatal("no synthesised bound at U+E000 and above was sent")
	}
}

// TestAdmission: only a positively flat account is listed. A
// hierarchical namespace (the replay's), a refused account-information
// call (403, 401) and --all-versions refuse the run before any listing
// request, as unsupported.
func TestAdmission(t *testing.T) {
	hns := azblobtest.Serve(t, []string{"a", "b"}, azblobtest.HNS())
	_, err := New(context.Background(), Options{Account: hns.Account, Container: hns.Container, Endpoint: base(hns), Anonymous: true, Workers: 1})
	if !errors.Is(err, source.ErrUnsupported) || objhttp.Label(err) != "unsupported" || !strings.Contains(err.Error(), "hierarchical namespace") {
		t.Fatalf("HNS: %v (class %q)", err, objhttp.Label(err))
	}
	var lists atomic.Int64
	for status, code := range map[int]string{403: "AuthorizationFailure", 401: "NoAuthenticationInformation"} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Query().Get("comp") == "list" {
				lists.Add(1)
			}
			w.Header().Set("x-ms-error-code", code)
			w.WriteHeader(status)
		}))
		_, err := New(context.Background(), Options{Account: "acct", Container: "c", Endpoint: srv.URL, Anonymous: true, Workers: 1})
		srv.Close()
		if !errors.Is(err, source.ErrUnsupported) || objhttp.Label(err) != "unsupported" || !strings.Contains(err.Error(), "account acct: cannot establish") {
			t.Fatalf("%d: %v (class %q)", status, err, objhttp.Label(err))
		}
	}
	if lists.Load() != 0 {
		t.Fatalf("%d list requests before the refusal", lists.Load())
	}
	flat := azblobtest.Serve(t, []string{"a"})
	p, err := New(context.Background(), Options{Account: flat.Account, Container: flat.Container, Endpoint: base(flat), Anonymous: true, Workers: 1})
	if err != nil || p.Stats().Namespace != "account information" {
		t.Fatalf("flat replay: %v", err)
	}
	c := &pages{err: errors.New("x-ms-is-hns-enabled missing")}
	if _, err := New(context.Background(), Options{Account: "acct", Container: "c", Client: c}); !errors.Is(err, source.ErrUnsupported) {
		t.Fatalf("unknown namespace: %v", err)
	}
	c = &pages{}
	_, err = New(context.Background(), Options{Account: "acct", Container: "c", Client: c, AllVersions: true})
	if !errors.Is(err, source.ErrUnsupported) || !strings.Contains(err.Error(), "--all-versions") || objhttp.Label(err) != "unsupported" {
		t.Fatalf("all versions: %v", err)
	}
	src, err := open(context.Background(), registry.Request{Root: "az://acct/c", Options: &Options{Client: c, AllVersions: true}, Workers: 1})
	if src != nil || !errors.Is(err, source.ErrUnsupported) || c.lists.Load() != 0 {
		t.Fatalf("all versions through open: %v", err)
	}
}

// hostFake answers each host the client addresses with its own handler:
// the account's blob and Data Lake hosts on the default endpoint.
type hostFake map[string]http.HandlerFunc

func (h hostFake) RoundTrip(req *http.Request) (*http.Response, error) {
	rec := httptest.NewRecorder()
	f, ok := h[req.URL.Host]
	if !ok {
		return nil, fmt.Errorf("no host %s", req.URL.Host)
	}
	f(rec, req)
	return rec.Result(), nil
}

// TestAdmissionFallback: when Get Account Information is refused (401,
// as Azure answers any anonymous caller, or 403) or silent, the Data Lake
// host's Filesystem Get Properties decides: false lists, true refuses; an
// absent header or a refusal there is unknown, which refuses unless
// --azure-assume-flat asserts a flat namespace (frozen in the identity, shown in
// the report, warned about). A hierarchical namespace refuses whatever is
// asserted, and an endpoint that is not ACCOUNT.blob.core.windows.net has
// no Data Lake host to ask.
func TestAdmissionFallback(t *testing.T) {
	blob := func(status int, hns string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if hns != "" {
				w.Header().Set("x-ms-is-hns-enabled", hns)
			}
			w.WriteHeader(status)
		}
	}
	dfs := func(status int, enabled string, asked *int) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			*asked++
			if r.Method != http.MethodHead || r.URL.Query().Get("resource") != "filesystem" || r.URL.Path != "/c" {
				w.WriteHeader(400)
				return
			}
			if enabled != "" {
				w.Header().Set("x-ms-namespace-enabled", enabled)
			}
			w.WriteHeader(status)
		}
	}
	for _, c := range []struct {
		name         string
		blob         http.HandlerFunc
		dfsStatus    int
		dfsEnabled   string
		flat         bool
		admitted     string // Stats.Namespace, "" when refused
		hierarchical bool
		dfsNotAsked  bool
	}{
		{"401, dfs false", blob(401, ""), 200, "false", false, "filesystem properties", false, false},
		{"403, dfs false", blob(403, ""), 200, "false", false, "filesystem properties", false, false},
		{"silent, dfs false", blob(200, ""), 200, "false", false, "filesystem properties", false, false},
		{"401, dfs true", blob(401, ""), 200, "true", false, "", true, false},
		{"401, dfs true, asserted", blob(401, ""), 200, "true", true, "", true, false},
		{"401, dfs absent", blob(401, ""), 200, "", false, "", false, false},
		{"401, dfs absent, asserted", blob(401, ""), 200, "", true, "asserted", false, false},
		{"401, dfs 401, asserted", blob(401, ""), 401, "", true, "asserted", false, false},
		{"blob false", blob(200, "false"), 200, "true", false, "account information", false, true},
		{"blob true, asserted", blob(200, "true"), 200, "false", true, "", true, true},
		{"blob 500, asserted", blob(500, ""), 200, "false", true, "asserted", false, true},
	} {
		asked := 0
		hc := &http.Client{Transport: hostFake{"acct.blob.core.windows.net": c.blob, "acct.dfs.core.windows.net": dfs(c.dfsStatus, c.dfsEnabled, &asked)}}
		cl, err := client.New(context.Background(), client.Config{Account: "acct", Anonymous: true, HTTPClient: hc, Retry: objhttp.Retry{MaxAttempts: 1}})
		if err != nil {
			t.Fatal(err)
		}
		var warned []string
		src, err := open(context.Background(), registry.Request{Root: "az://acct/c", Options: &Options{Client: cl, Flat: c.flat}, Workers: 1,
			Warn: func(s string) { warned = append(warned, s) }})
		switch {
		case c.admitted == "":
			if !errors.Is(err, source.ErrUnsupported) || objhttp.Label(err) != "unsupported" ||
				strings.Contains(err.Error(), "hierarchical namespace") != c.hierarchical {
				t.Fatalf("%s: %v, want a refusal (hierarchical %v)", c.name, err, c.hierarchical)
			}
		case err != nil:
			t.Fatalf("%s: %v", c.name, err)
		default:
			o := src.(*opened)
			if n := o.p.Stats().Namespace; n != c.admitted {
				t.Fatalf("%s: admitted by %q, want %q", c.name, n, c.admitted)
			}
			var asserted string
			for _, f := range o.Identity().Fields {
				if f.Name == "flat_asserted" {
					asserted = f.Value
				}
			}
			if asserted != strconv.FormatBool(c.flat) || (c.admitted == "asserted") != (len(warned) == 1) {
				t.Fatalf("%s: flat_asserted %q, warnings %q", c.name, asserted, warned)
			}
			if b, _ := json.Marshal(o.Report().Stats); !strings.Contains(string(b), `"namespace":"`+c.admitted+`"`) {
				t.Fatalf("%s: report stats %s", c.name, b)
			}
		}
		if (asked == 0) != c.dfsNotAsked {
			t.Fatalf("%s: the Data Lake host asked %d times", c.name, asked)
		}
	}
	// --azure-assume-flat on a custom endpoint, which has no Data Lake host:
	// the 401 alone is unknown, and the assertion admits it.
	srv := httptest.NewServer(blob(401, ""))
	t.Cleanup(srv.Close)
	for _, flat := range []bool{false, true} {
		_, err := New(context.Background(), Options{Account: "acct", Container: "c", Endpoint: srv.URL, Anonymous: true, Flat: flat, Workers: 1})
		if (err == nil) != flat {
			t.Fatalf("custom endpoint, flat %v: %v", flat, err)
		}
	}
}

// TestAdmitSharedKeyUnknown: with Shared Key, an account-information
// answer whose x-ms-is-hns-enabled is not exactly "true" or "false" ("0",
// which strconv.ParseBool would read as flat) is unknown: refused unless
// --azure-assume-flat asserts it (no Data Lake host is asked: the endpoint has
// none, and the client does not ask one with Shared Key).
func TestAdmitSharedKeyUnknown(t *testing.T) {
	t.Setenv(EnvKey, "c2VjcmV0LWtleS1mb3ItdGVzdHM=")
	t.Setenv(EnvSAS, "")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.Header.Get("Authorization"), "SharedKey acct:") || r.URL.Query().Get("restype") != "account" {
			t.Errorf("request %s ?%s Authorization %q", r.URL.Path, r.URL.RawQuery, r.Header.Get("Authorization"))
		}
		w.Header().Set("x-ms-is-hns-enabled", "0")
	}))
	t.Cleanup(srv.Close)
	for _, flat := range []bool{false, true} {
		p, err := New(context.Background(), Options{Account: "acct", Container: "c", Endpoint: srv.URL, Flat: flat, Workers: 1, Warn: func(string) {}})
		switch {
		case !flat && !errors.Is(err, source.ErrUnsupported):
			t.Fatalf("not asserted: %v, want a refusal", err)
		case flat && err != nil:
			t.Fatalf("asserted: %v", err)
		case flat && p.namespace != "asserted":
			t.Fatalf("asserted: namespace %q", p.namespace)
		}
	}
}

// TestBoundPolicy: a startFrom is a valid Azure name: at most 1,024 UTF-16
// code units, no control character, no trailing dot.
func TestBoundPolicy(t *testing.T) {
	for _, s := range []string{"a", "a%20b", "café", "a/", strings.Repeat("k", 1024), strings.Repeat("中", 1024), strings.Repeat("\U00010000", 512), "a.b"} {
		if !AcceptBound(s) {
			t.Errorf("refused %q", s)
		}
	}
	for _, s := range []string{"a\x00", "a\x01b", "a\x7f", "a\u0085", "a.", strings.Repeat("k", 1025), strings.Repeat("\U00010000", 512) + "k", "\xff"} {
		if AcceptBound(s) {
			t.Errorf("accepted %q", s)
		}
	}
}

func TestParseURL(t *testing.T) {
	for in, want := range map[string][4]string{
		"az://acct/c":                           {"acct", "c", "", ""},
		"az://acct/c/p/q":                       {"acct", "c", "p/q", ""},
		"az://acct/c/p?sv=2026&sig=abc%3D":      {"acct", "c", "p", "sv=2026&sig=abc%3D"},
		"az://acct/c?sp=rl&sig=x":               {"acct", "c", "", "sp=rl&sig=x"},
		"az://acct/c/what?not=a-signature":      {"acct", "c", "what?not=a-signature", ""},
		"az://acct/c/p?" + RedactedSAS:          {"acct", "c", "p", RedactedSAS},
		"az://acct/c/dir/?":                     {"acct", "c", "dir/?", ""},
		"az://acct/c/a b+c":                     {"acct", "c", "a b+c", ""},
		"az://acct/c/" + strings.Repeat("é", 3): {"acct", "c", strings.Repeat("é", 3), ""},
	} {
		a, c, p, s, err := ParseURL(in)
		if err != nil || [4]string{a, c, p, s} != want {
			t.Fatalf("%s: %q %q %q %q %v", in, a, c, p, s, err)
		}
	}
	for _, in := range []string{"s3://b", "az://", "az://acct", "az://acct/", "az:///c", "gs://acct/c"} {
		if _, _, _, _, err := ParseURL(in); err == nil {
			t.Fatalf("%s accepted", in)
		}
	}
	// A query naming SAS parameters is a credential whether or not it
	// parses: refused unless it is a signed query string, never quoted,
	// never taken into the prefix, always redacted.
	for _, in := range []string{"az://acct?sig=SECRET", "az://acct?sig=SECRET&x=%ZZ", "az://acct/c/p?sig=SECRET&x=%ZZ",
		"az://acct/c/a?b?sig=SECRET", "az://acct/c/p?sv=1&se=SECRET", "az://acct/c/p?%73ig=SECRET%ZZ", "az://acct/c?SIG=SECRET;x"} {
		if _, _, p, s, err := ParseURL(in); err == nil || strings.Contains(err.Error(), "SECRET") {
			t.Fatalf("%s: prefix %q sas %q, %v: want a refusal that does not quote it", in, p, s, err)
		}
		if got := RedactRoot(in); strings.Contains(got, "SECRET") || !strings.HasSuffix(got, "?"+RedactedSAS) {
			t.Fatalf("%s redacted to %q", in, got)
		}
	}
	if got := RedactRoot("az://acct/c/p?sv=1&sig=SECRET"); got != "az://acct/c/p?"+RedactedSAS {
		t.Fatalf("redacted %q", got)
	}
	for _, a := range []string{"az://acct/c/what?x=1", "s3://b?sig=1", "--azure-sas-token"} {
		if got := RedactRoot(a); got != a {
			t.Fatalf("%q redacted to %q", a, got)
		}
	}
}

// TestIdentity: the identity names the source, the canonical endpoint
// (scheme and host case only), the account and the container, never the
// SAS: another SAS, in the root or the options, leaves it equal; another
// account or endpoint changes it. A supplied client that does not name its
// route declines resume.
func TestIdentity(t *testing.T) {
	r := azblobtest.Serve(t, []string{"a", "b"})
	ident := func(root, ep, sas string) (source.Identity, source.Locator) {
		t.Helper()
		src, err := open(context.Background(), registry.Request{Root: root, Options: &Options{Endpoint: ep, SAS: sas, Anonymous: sas == "" && !strings.Contains(root, "sig=")}, Workers: 1, Warn: func(string) {}})
		if err != nil {
			t.Fatal(err)
		}
		if !src.(source.Capable).Capabilities().Resume {
			t.Fatal("no resume")
		}
		return src.(source.Identified).Identity(), src.Locator()
	}
	canon := func(id source.Identity) []string {
		var out []string
		for _, f := range id.Fields {
			v := f.Canonical
			if v == "" {
				v = f.Value
			}
			out = append(out, f.Name+"="+v)
		}
		return out
	}
	root := "az://replay/corpus/p"
	a, loc := ident(root+"?sv=1&sig=SECRET1", base(r), "")
	b, _ := ident(root, "HTTP://"+strings.TrimPrefix(base(r), "http://")+"/", "sv=1&sig=SECRET2")
	want := []string{"source=az://replay/corpus/p", "endpoint=" + base(r) + "/replay", "account=replay", "container=corpus", "flat_asserted=false"}
	if !slices.Equal(canon(a), want) || !slices.Equal(canon(a), canon(b)) || a.Provider != "azblob" {
		t.Fatalf("identities %q %q, want %q", canon(a), canon(b), want)
	}
	if loc.Root != "az://replay/corpus/p" || loc.Kind != "azblob" || strings.Contains(fmt.Sprint(a, loc), "SECRET") {
		t.Fatalf("a SAS reaches the identity or the locator: %+v %+v", a, loc)
	}
	other := azblobtest.Serve(t, []string{"a"})
	if c, _ := ident(root, base(other), ""); slices.Equal(canon(c), canon(a)) {
		t.Fatal("another endpoint leaves the identity equal")
	}
	if d := canon(source.Identity{Fields: identFields(t, "az://other/corpus/p", base(r))}); slices.Equal(d, canon(a)) {
		t.Fatal("another account leaves the identity equal")
	}
	// The default endpoint: the account's host.
	c := &routed{pages: &pages{}}
	src, err := open(context.Background(), registry.Request{Root: "az://acct/c", Options: &Options{Client: c}, Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	if f := src.(source.Identified).Identity().Fields[1]; f.Canonical != "https://acct.blob.core.windows.net" {
		t.Fatalf("default endpoint %+v", f)
	}
	src, err = open(context.Background(), registry.Request{Root: "az://acct/c", Options: &Options{Client: &pages{}}, Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	if src.(source.Capable).Capabilities().Resume {
		t.Fatal("a supplied client without a route declares resume")
	}
}

// identFields is the identity open gives root on ep, through a supplied
// routed client (the account need not exist on the replay).
func identFields(t *testing.T, root, ep string) []source.IdentityField {
	t.Helper()
	acct, _, _, _, _ := ParseURL(root)
	c := &routed{pages: &pages{}, base: ep + "/" + acct + "/"}
	src, err := open(context.Background(), registry.Request{Root: root, Options: &Options{Client: c}, Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	return src.(source.Identified).Identity().Fields
}

// routed names its route as the built client does.
type routed struct {
	*pages
	base string // "" for the default endpoint of account acct
}

func (r *routed) Route(container string) string {
	if r.base == "" {
		return "https://acct.blob.core.windows.net/" + container
	}
	return r.base + container
}

// TestRedactedSASResume: a resumed root whose SAS was redacted needs a SAS
// again (--azure-sas-token or AZURE_STORAGE_SAS_TOKEN), or anonymity.
func TestRedactedSASResume(t *testing.T) {
	t.Setenv(EnvSAS, "")
	root := "az://acct/c?" + RedactedSAS
	if _, err := open(context.Background(), registry.Request{Root: root, Options: &Options{Client: &pages{}}, Workers: 1}); !errors.Is(err, errSASNotRecorded) {
		t.Fatalf("no SAS given again: %v", err)
	}
	for _, o := range []*Options{{Client: &pages{}, SAS: "sig=again"}, {Client: &pages{}, Anonymous: true}} {
		if _, err := open(context.Background(), registry.Request{Root: root, Options: o, Workers: 1}); err != nil {
			t.Fatalf("%+v: %v", o, err)
		}
	}
	t.Setenv(EnvSAS, "sig=env")
	if _, err := open(context.Background(), registry.Request{Root: root, Options: &Options{Client: &pages{}}, Workers: 1}); err != nil {
		t.Fatal(err)
	}
}

// TestEngineFlagsShared: the provider takes the store's and the range
// engine's flag groups the command registered once, and its own prefixed
// flags.
func TestEngineFlagsShared(t *testing.T) {
	r := azblobtest.Serve(t, []string{"a"})
	pf, err := registry.Lookup("az://replay/corpus")
	if err != nil {
		t.Fatal(err)
	}
	f := pflag.NewFlagSet("t", pflag.ContinueOnError)
	v := pf.RegisterFlags(f, true)
	if err := f.Parse([]string{"--structure", "--page-size", "7", "--anonymous", "--endpoint-url", base(r), "--request-timeout", "7s", "--azure-assume-flat"}); err != nil {
		t.Fatal(err)
	}
	src, err := open(context.Background(), registry.Request{Root: "az://replay/corpus", Options: v, Workers: 2})
	if err != nil {
		t.Fatal(err)
	}
	p := src.(*opened).p
	if !p.opt.Seed || p.opt.Structure != keyrange.StructureOn || !p.opt.Anonymous || p.opt.Timeout.Seconds() != 7 || p.Options().PageSize != 7 || !p.opt.Flat {
		t.Fatalf("flags not applied: %+v", p.opt)
	}
}

// TestInvalidPageClass: a page out of order is the client's invalid page,
// which the engine wraps as a protocol violation: the adapter's order
// check classes it as the client's invalid page.
func TestInvalidPageClass(t *testing.T) {
	c := &pages{out: []client.ListOutput{{Items: []client.Item{{Name: "b"}, {Name: "a"}}}}}
	_, _, err := run(t, Options{Account: "acct", Container: "c", Client: c, Policy: keyrange.Policy{Sequential: true}}, 1)
	if !errors.Is(err, objhttp.ErrInvalidPage) || !errors.Is(err, source.ErrProtocol) || errors.Is(err, source.ErrUnsupported) {
		t.Fatalf("err %v class %q, want invalid-page protocol sentinel", err, objhttp.Label(err))
	}
}

// TestCodecRoundTripsEveryField: the spill encoding, plain and delta,
// keeps every field; absent fields stay null columns.
func TestCodecRoundTripsEveryField(t *testing.T) {
	objs := []Object{
		{Size: 1, Mtime: 2, CreationTime: 3, ETag: "0x8D", ContentMD5: "m", ContentType: "t", BlobType: "BlockBlob", AccessTier: "Hot"},
		{Size: 7, Mtime: -8, ETag: "0x8E", BlobType: "BlockBlob", AccessTier: "Hot"},
		{Size: 1 << 40, BlobType: "AppendBlob"},
	}
	codec := Schema.Codec.(schema.DeltaCodec[Object])
	var prev Object
	for _, o := range objs {
		d := schema.NewDecoder(codec.Append(nil, &o))
		var got Object
		codec.Decode(d, &got)
		if err := d.Err(); err != nil || d.Len() != 0 || got != o {
			t.Fatalf("round trip of %+v: %+v, %v", o, got, err)
		}
		d = schema.NewDecoder(codec.AppendDelta(nil, &prev, &o))
		got = Object{}
		codec.DecodeDelta(d, &prev, &got)
		if err := d.Err(); err != nil || d.Len() != 0 || got != o {
			t.Fatalf("delta round trip of %+v: %+v, %v", o, got, err)
		}
		prev = o
	}
	var names []string
	for _, s := range Schema.Specs() {
		names = append(names, s.Name)
	}
	if want := []string{"key", "size", "last_modified", "etag", "content_md5", "content_type", "blob_type", "access_tier", "creation_time"}; !slices.Equal(names, want) {
		t.Fatalf("columns %q", names)
	}
	for _, c := range Schema.Columns {
		if c.Str != nil {
			if _, ok := c.Str(&Object{}); ok {
				t.Fatalf("%s: an absent value is not null", c.Name)
			}
		}
	}
	if Schema.ID != "swath/azblob-objects/v1" || Schema.MaxKeyBytes != 3072 {
		t.Fatalf("schema %+v", Schema.Info)
	}
}

// cappedAPI is a supplied client that reports the client's cut waits.
type cappedAPI struct {
	*sortedAPI
	retryAfter int64
}

func (c cappedAPI) RetryAfterCapped() int64 { return c.retryAfter }

// TestClientCutsReported: retry_after_capped counts the 503 ServerBusy
// answers whose Retry-After (an hour) passed the request timeout, through
// the built client against the replay, and is reported from a supplied
// client that counts it.
func TestClientCutsReported(t *testing.T) {
	keys := azblobtest.Keys()
	r := azblobtest.Serve(t, keys)
	u, _ := url.Parse(base(r))
	px := httputil.NewSingleHostReverseProxy(u)
	var busy atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Query().Get("comp") == "list" && busy.Add(1) <= 2 {
			w.Header().Set("x-ms-error-code", "ServerBusy")
			w.Header().Set("Retry-After", "3600")
			w.WriteHeader(503)
			return
		}
		px.ServeHTTP(w, req)
	}))
	t.Cleanup(srv.Close)
	rows, st := list(t, Options{Account: r.Account, Container: r.Container, Endpoint: srv.URL, Timeout: 300 * time.Millisecond,
		Policy: keyrange.Policy{Sequential: true}}, 1)
	if len(rows) != len(keys) || st.Counters.Value("retry_after_capped") != 2 {
		t.Fatalf("%d rows, retry_after_capped %d", len(rows), st.Counters.Value("retry_after_capped"))
	}
	c := cappedAPI{sortedAPI: &sortedAPI{keys: []string{"a", "b"}}, retryAfter: 3}
	_, st = list(t, Options{Account: "acct", Container: "c", Client: c}, 1)
	if st.Counters.Value("retry_after_capped") != 3 {
		t.Fatalf("supplied client: retry_after_capped %d", st.Counters.Value("retry_after_capped"))
	}
}

// TestAdmissionFallbackPolicy pins code-before-status decisions: changing
// them can skip the DFS proof, or admit an account without the right proof.
func TestAdmissionFallbackPolicy(t *testing.T) {
	for _, c := range []struct {
		status int
		code   string
		want   bool
	}{
		{401, "", true}, {403, "", true}, {500, "", false},
		{400, "AuthenticationFailed", true}, {500, "AuthorizationFailure", true},
		{403, "NoAuthenticationInformation", true}, {403, "InvalidAuthenticationInfo", true},
		{403, "AuthorizationPermissionMismatch", true}, {403, "ContainerNotFound", false},
		{401, "ServerBusy", false}, {403, "OperationTimedOut", false}, {401, "InternalError", false},
	} {
		err := &client.Error{Status: c.status, Code: c.code}
		for _, wrapped := range []error{err, objhttp.Normalize(err), fmt.Errorf("account: %w", objhttp.Normalize(err))} {
			if got := fallback(wrapped); got != c.want {
				t.Errorf("%v: fallback %v, want %v", wrapped, got, c.want)
			}
		}
	}
	for _, c := range []struct {
		err  error
		want bool
	}{
		{client.ErrHNSUnknown, true},
		{errors.Join(client.ErrHNSUnknown, source.Unsupported(errors.New("unsupported"))), true},
		{errors.New("unclassified"), false},
		{nil, false},
		{source.Unsupported(&objhttp.CredentialError{Err: context.DeadlineExceeded}), false},
		{errors.Join(objhttp.ErrInvalidPage, &client.Error{Status: 403}), false},
		{errors.Join(objhttp.ErrSyntax, &client.Error{Status: 401}), false},
	} {
		if got := fallback(c.err); got != c.want {
			t.Errorf("%v: fallback %v, want %v", c.err, got, c.want)
		}
	}
}

func TestFetchRequiresExpandAdapter(t *testing.T) {
	st := azblobtest.NewStore([]string{"a"})
	p, err := New(context.Background(), Options{Account: "acct", Container: "c", Client: st})
	if err != nil {
		t.Fatal(err)
	}
	pg, err := p.fetch(context.Background(), &keyrange.Query{Max: 1})
	if pg != nil || !errors.Is(err, source.ErrProtocol) || st.Responses() != 0 {
		t.Fatalf("page=%+v err=%v responses=%d", pg, err, st.Responses())
	}
}

// Exhaust the status/code cross-product, including contradictory labels
// supplied by third-party clients. Admission uses Azure's status/code policy.
func TestFallbackMatchesAdmissionPolicy(t *testing.T) {
	codes := []string{"", "other", "NoAuthenticationInformation", "AuthenticationFailed", "InvalidAuthenticationInfo",
		"AuthorizationFailure", "AuthorizationPermissionMismatch", "ContainerNotFound", "ServerBusy", "OperationTimedOut", "InternalError"}
	for _, status := range []int{0, 200, 400, 401, 403, 404, 408, 429, 500, 501, 502, 503, 504} {
		for _, code := range codes {
			want := status == 401 || status == 403
			switch code {
			case "NoAuthenticationInformation", "AuthenticationFailed", "InvalidAuthenticationInfo", "AuthorizationFailure", "AuthorizationPermissionMismatch":
				want = true
			case "ContainerNotFound", "ServerBusy", "OperationTimedOut", "InternalError":
				want = false
			}
			raw := &client.Error{Status: status, Code: code}
			normalized := objhttp.Normalize(raw)
			for _, class := range []objhttp.Class{objhttp.ClassAuth, objhttp.ClassAccessDenied, objhttp.ClassOther} {
				normalized.Class = class
				for _, err := range []error{raw, fmt.Errorf("account: %w", raw), normalized, fmt.Errorf("account: %w", normalized)} {
					if got := fallback(err); got != want {
						t.Errorf("status=%d code=%q class=%q: %v want %v", status, code, class, got, want)
					}
				}
			}
		}
	}
}
