// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblob

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
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

	"github.com/varveio/swath/go/internal/order"
	"github.com/varveio/swath/go/internal/registry"
	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source"
	"github.com/varveio/swath/go/internal/source/azblob/azblobtest"
	"github.com/varveio/swath/go/internal/source/azblob/client"
	"github.com/varveio/swath/go/internal/source/keyrange"
	"github.com/varveio/swath/go/internal/source/s3"
)

// lowKeys is the hostile corpus without the names that carry a byte in
// EE..F4 (the staged refusal's): ASCII, U+00E9 and U+4E2D names, folder
// markers, long names and the 5,000 filler names remain.
func lowKeys() []string {
	return slices.DeleteFunc(azblobtest.Keys(), highByte)
}

// base is the replay's endpoint for --azblob-endpoint: without the
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
// sizes 1, 2, 7 and 5,000; the default page is 5,000.
func TestListReplay(t *testing.T) {
	keys := lowKeys()
	r := azblobtest.Serve(t, keys)
	const epoch = 1_767_225_600_000_000
	check := func(name string, rows []Row) {
		t.Helper()
		if got := keysOf(rows); !slices.Equal(got, keys) {
			t.Fatalf("%s: %d keys, want %d", name, len(got), len(keys))
		}
		for i, row := range rows {
			o := row.Data
			if o.Size != int64(i) || o.Mtime != epoch+int64(i)*1_000_000 || o.BlobType != "BlockBlob" || o.AccessTier != "Hot" ||
				o.ETag == "" || o.ContentType != "application/octet-stream" || o.ContentMD5 != "" || o.CreationTime != 0 {
				t.Fatalf("%s: row %d %q: %+v", name, i, row.Key, o)
			}
		}
	}
	for _, c := range []struct {
		name    string
		pol     keyrange.Policy
		workers int
	}{
		{"sequential", keyrange.Policy{Sequential: true}, 1},
		{"8 workers", keyrange.Policy{}, 8},
		{"64 workers, seed, structure", keyrange.Policy{Seed: true, Structure: keyrange.StructureOn}, 64},
		{"page size 1", keyrange.Policy{PageSize: 1, Seed: true, Structure: keyrange.StructureOn}, 64},
		{"page size 2", keyrange.Policy{PageSize: 2}, 8},
		{"page size 7", keyrange.Policy{PageSize: 7, Seed: true, Structure: keyrange.StructureOn}, 64},
		{"page size 5000", keyrange.Policy{PageSize: 5000, Sequential: true}, 1},
	} {
		opt := replayOptions(r)
		opt.Policy = c.pol
		rows, st := list(t, opt, c.workers)
		check(c.name, rows)
		v := st.Counters.Value
		if c.pol.Seed && v("seed.requests") == 0 || c.pol.Structure == keyrange.StructureOn && v("structure.requests") == 0 {
			t.Fatalf("%s: seed requests %d, structure requests %d: the delimiter listings did not engage", c.name, v("seed.requests"), v("structure.requests"))
		}
		if c.pol.Sequential {
			size := c.pol.PageSize
			if size == 0 {
				size = client.MaxPageSize
			}
			if want := int64((len(keys) + size - 1) / size); v("pages") != want {
				t.Fatalf("%s: %d pages, want %d of %d", c.name, v("pages"), want, size)
			}
		}
	}
	// The default page is client.MaxPageSize: the corpus lists in two.
	opt := replayOptions(r)
	opt.Sequential = true
	_, st := list(t, opt, 1)
	if n := st.Counters.Value("pages"); n != 2 {
		t.Fatalf("default page size: %d pages for %d keys, want 2", n, len(keys))
	}
	// A prefix scope.
	opt = replayOptions(r)
	opt.Prefix = "f/"
	rows, _ := list(t, opt, 8)
	if len(rows) != 5000 || rows[0].Key != "f/0000" {
		t.Fatalf("prefix f/: %d rows", len(rows))
	}
}

// TestDualRoute: one fixture through the replay's S3 route (the S3
// provider) and its Azure route (this provider) lists the same keys in the
// same order, once each, with the same size and Last-Modified (at the
// Azure wire's second resolution). The ETag, blob type, tier and content
// type are the Azure route's own synthesis (replay/azure/xml.go), not the
// fixture's, so they are not compared.
func TestDualRoute(t *testing.T) {
	keys := lowKeys()
	r := azblobtest.Serve(t, keys)
	for _, c := range []struct {
		pol     keyrange.Policy
		workers int
	}{
		{keyrange.Policy{Sequential: true}, 1},
		{keyrange.Policy{Seed: true, Structure: keyrange.StructureOn}, 64},
		{keyrange.Policy{PageSize: 7, Seed: true}, 64},
	} {
		opt := replayOptions(r)
		opt.Policy = c.pol
		az, _ := list(t, opt, c.workers)
		sp, err := s3.New(context.Background(), s3.Options{Bucket: r.Container, Endpoint: r.S3Endpoint, PathStyle: true, Anonymous: true,
			Workers: c.workers, Policy: c.pol})
		if err != nil {
			t.Fatal(err)
		}
		var sc s3collect
		if _, err := order.Run[s3.Range, s3.Object](context.Background(), sp, &sc, &s3.Schema, order.Options{Workers: c.workers, Budget: 1 << 22}); err != nil {
			t.Fatal(err)
		}
		if len(az) != len(sc.rows) || len(az) != len(keys) {
			t.Fatalf("%+v: azure %d rows, s3 %d, fixture %d", c.pol, len(az), len(sc.rows), len(keys))
		}
		for i := range az {
			a, b := az[i], sc.rows[i]
			if a.Key != b.Key || a.Data.Size != b.Data.Size || a.Data.Mtime/1_000_000 != b.Data.Mtime/1_000_000 {
				t.Fatalf("%+v: row %d: azure %q %d %d, s3 %q %d %d", c.pol, i, a.Key, a.Data.Size, a.Data.Mtime, b.Key, b.Data.Size, b.Data.Mtime)
			}
		}
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

// TestStagedRefusal: over the whole hostile corpus every configuration
// stops with the staged refusal, class unsupported, never requeued and
// not a protocol violation, and emits no key at or past the first name
// that carries a byte in EE..F4; over the corpus without them the same
// configurations list everything (TestListReplay).
func TestStagedRefusal(t *testing.T) {
	keys := azblobtest.Keys()
	first := keys[slices.IndexFunc(keys, highByte)]
	r := azblobtest.Serve(t, keys)
	for _, c := range []struct {
		name    string
		pol     keyrange.Policy
		workers int
	}{
		{"sequential", keyrange.Policy{Sequential: true}, 1},
		{"sequential, page size 1", keyrange.Policy{Sequential: true, PageSize: 1}, 1},
		{"8 workers, page size 7", keyrange.Policy{PageSize: 7}, 8},
		{"64 workers, seed, structure", keyrange.Policy{Seed: true, Structure: keyrange.StructureOn}, 64},
		{"64 workers, page size 2, seed", keyrange.Policy{PageSize: 2, Seed: true}, 64},
	} {
		opt := replayOptions(r)
		opt.Policy = c.pol
		rows, st, err := run(t, opt, c.workers)
		if !errors.Is(err, source.ErrUnsupported) || errors.Is(err, source.ErrProtocol) || classify(err) != "unsupported" {
			t.Fatalf("%s: err %v (class %q), want the staged refusal", c.name, err, classify(err))
		}
		if !strings.Contains(err.Error(), "azblob: names at U+E000 and above are not supported yet (container corpus, first name ") {
			t.Fatalf("%s: message %q", c.name, err)
		}
		if n := st.Counters.Value("requeues"); n != 0 {
			t.Fatalf("%s: %d requeues", c.name, n)
		}
		for _, row := range rows {
			if row.Key >= first {
				t.Fatalf("%s: %q emitted, at or past %q", c.name, row.Key, first)
			}
		}
	}
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

// TestStagedRefusalBounds: a cursor or a bound with such a byte is refused
// before any request is sent; so is a page whose only such name is a
// delimiter prefix, or one whose such name is not its first item, with
// the name in the message.
func TestStagedRefusalBounds(t *testing.T) {
	newP := func(c *pages) *Provider {
		p, err := New(context.Background(), Options{Account: "acct", Container: "c", Client: c, Workers: 1})
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	for _, q := range []keyrange.Query{
		{After: "p\U0001f600", Max: 10},
		{After: "a", Through: "pｚ", Bounded: true, Max: 10},
	} {
		c := &pages{}
		if _, err := newP(c).fetch(context.Background(), &q); !errors.Is(err, source.ErrUnsupported) || c.lists.Load() != 0 {
			t.Fatalf("%+v: err %v after %d requests", q, err, c.lists.Load())
		}
	}
	// An unbounded query ignores Through.
	if _, err := newP(&pages{}).fetch(context.Background(), &keyrange.Query{After: "a", Through: "pｚ", Max: 10}); err != nil {
		t.Fatal(err)
	}
	for _, out := range []client.ListOutput{
		{Items: []client.Item{{Name: "a"}, {Name: "\U00010000/", Prefix: true}}},
		{Items: []client.Item{{Name: "a"}, {Name: "b"}, {Name: "b"}}, NextMarker: "m"},
	} {
		_, err := newP(&pages{out: []client.ListOutput{out}}).fetch(context.Background(), &keyrange.Query{Max: 10})
		last := out.Items[len(out.Items)-1].Name
		if !errors.Is(err, source.ErrUnsupported) || !strings.Contains(err.Error(), fmt.Sprintf("first name %q", last)) {
			t.Fatalf("page %+v: %v", out, err)
		}
	}
	// A page without them maps blobs to rows, prefixes to Prefixes.
	pg, err := newP(&pages{out: []client.ListOutput{{Items: []client.Item{{Name: "a", Size: 3, BlobType: "PageBlob"}, {Name: "b/", Prefix: true},
		{Name: "c"}}, NextMarker: "m"}}}).fetch(context.Background(), &keyrange.Query{Max: 10})
	if err != nil || len(pg.Rows) != 2 || pg.Rows[0].Data.Size != 3 || pg.Rows[0].Data.BlobType != "PageBlob" ||
		!slices.Equal(pg.Prefixes, []string{"b/"}) || !pg.Truncated || pg.Token != "m" {
		t.Fatalf("page %+v, %v", pg, err)
	}
}

// TestUnsafePrefixRefused: a scope prefix that carries a byte in EE..F4
// is refused at open, before any request, not listed as an empty scope.
func TestUnsafePrefixRefused(t *testing.T) {
	c := &pages{err: errors.New("never asked")}
	_, err := New(context.Background(), Options{Account: "acct", Container: "c", Prefix: "pｚ/", Client: c})
	if !errors.Is(err, source.ErrUnsupported) || classify(err) != "unsupported" || c.lists.Load() != 0 || !strings.Contains(err.Error(), `first name "pｚ/"`) {
		t.Fatalf("%v", err)
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

// TestSynthesizedPivotRefused: the engine's pivot alphabet includes every
// scalar from U+E000 up, so over keys that all sit below U+E000 (lead byte
// ED) its frontier splits synthesise a bound at U+10000 and above. The
// real engine sends it to fetch, which refuses it before any request with
// it is sent: the run stops with the staged refusal naming a name that is
// not a key.
func TestSynthesizedPivotRefused(t *testing.T) {
	var keys []string
	for r := rune(0xD000); r < 0xD7A0; r += 2 {
		keys = append(keys, "k"+string(r))
	}
	slices.Sort(keys)
	f := &sortedAPI{keys: keys}
	_, _, err := run(t, Options{Account: "acct", Container: "c", Client: f, Policy: keyrange.Policy{PageSize: 3}}, 64)
	if !errors.Is(err, source.ErrUnsupported) || classify(err) != "unsupported" {
		t.Fatalf("err %v, want the staged refusal of a synthesised bound", err)
	}
	named := err.Error()[strings.Index(err.Error(), "first name ")+len("first name "):]
	name, uerr := strconv.Unquote(strings.TrimSuffix(named, ")"))
	if uerr != nil || slices.Contains(keys, name) || !highByte(name) {
		t.Fatalf("refused %q (%v): want a synthesised bound, not a key", named, uerr)
	}
	for _, from := range f.from {
		if highByte(from) {
			t.Fatalf("startFrom %q was sent", from)
		}
	}
}

// TestAdmission: only a positively flat account is listed. A
// hierarchical namespace (the replay's), a refused account-information
// call (403, 401) and --all-versions refuse the run before any listing
// request, as unsupported.
func TestAdmission(t *testing.T) {
	hns := azblobtest.Serve(t, []string{"a", "b"}, azblobtest.HNS())
	_, err := New(context.Background(), Options{Account: hns.Account, Container: hns.Container, Endpoint: base(hns), Anonymous: true, Workers: 1})
	if !errors.Is(err, source.ErrUnsupported) || classify(err) != "unsupported" || !strings.Contains(err.Error(), "hierarchical namespace") {
		t.Fatalf("HNS: %v (class %q)", err, classify(err))
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
		if !errors.Is(err, source.ErrUnsupported) || classify(err) != "unsupported" || !strings.Contains(err.Error(), "account acct: cannot establish") {
			t.Fatalf("%d: %v (class %q)", status, err, classify(err))
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
	if !errors.Is(err, source.ErrUnsupported) || !strings.Contains(err.Error(), "--all-versions") || classify(err) != "unsupported" {
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
// --azblob-flat asserts a flat namespace (frozen in the identity, shown in
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
		cl, err := client.New(context.Background(), client.Config{Account: "acct", Anonymous: true, HTTPClient: hc, MaxAttempts: 1})
		if err != nil {
			t.Fatal(err)
		}
		var warned []string
		src, err := open(context.Background(), registry.Request{Root: "az://acct/c", Options: &Options{Client: cl, Flat: c.flat}, Workers: 1,
			Warn: func(s string) { warned = append(warned, s) }})
		switch {
		case c.admitted == "":
			if !errors.Is(err, source.ErrUnsupported) || classify(err) != "unsupported" ||
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
	// --azblob-flat on a custom endpoint, which has no Data Lake host:
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
	for _, a := range []string{"az://acct/c/what?x=1", "s3://b?sig=1", "--azblob-sas"} {
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
// again (--azblob-sas or AZURE_STORAGE_SAS_TOKEN), or anonymity.
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

// TestEngineFlagsShared: the provider takes the range engine's flag group
// the command registered once, and its own prefixed flags.
func TestEngineFlagsShared(t *testing.T) {
	r := azblobtest.Serve(t, []string{"a"})
	f := flag.NewFlagSet("t", flag.ContinueOnError)
	other := keyrange.Flags(f, true)
	v := flags(f, true, other)
	if err := f.Parse([]string{"-seed", "-structure", "-page-size", "7", "-azblob-anonymous", "-azblob-endpoint", base(r), "-azblob-request-timeout", "7s"}); err != nil {
		t.Fatal(err)
	}
	src, err := open(context.Background(), registry.Request{Root: "az://replay/corpus", Options: v, Workers: 2})
	if err != nil {
		t.Fatal(err)
	}
	p := src.(*opened).p
	if !p.opt.Seed || p.opt.Structure != keyrange.StructureOn || !p.opt.Anonymous || p.opt.Timeout.Seconds() != 7 || p.Options().PageSize != 7 {
		t.Fatalf("flags not applied: %+v", p.opt)
	}
}

// TestInvalidPageClass: a page out of order is the client's invalid page,
// which the engine wraps as a protocol violation (not the staged refusal).
func TestInvalidPageClass(t *testing.T) {
	c := &pages{out: []client.ListOutput{{Items: []client.Item{{Name: "b"}, {Name: "a"}}}}}
	_, _, err := run(t, Options{Account: "acct", Container: "c", Client: c, Policy: keyrange.Policy{Sequential: true}}, 1)
	if classify(err) != "invalid_page" || !errors.Is(err, source.ErrProtocol) || errors.Is(err, source.ErrUnsupported) {
		t.Fatalf("err %v class %q, want invalid_page", err, classify(err))
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

// cappedAPI is a supplied client that reports the client's cut waits and
// shrunk pages (the body cap is 64 MiB, more than any page the replay can
// serve, so its path is driven in the client's own tests).
type cappedAPI struct {
	*sortedAPI
	retryAfter, shrinks int64
}

func (c cappedAPI) RetryAfterCapped() int64 { return c.retryAfter }
func (c cappedAPI) BodyCapShrinks() int64   { return c.shrinks }

// TestClientCutsReported: retry_after_capped counts the 503 ServerBusy
// answers whose Retry-After (an hour) passed the request timeout, through
// the built client against the replay; body_cap_shrinks is reported from
// the client.
func TestClientCutsReported(t *testing.T) {
	keys := lowKeys()
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
	if len(rows) != len(keys) || st.Counters.Value("retry_after_capped") != 2 || st.Counters.Value("body_cap_shrinks") != 0 {
		t.Fatalf("%d rows, retry_after_capped %d, body_cap_shrinks %d", len(rows), st.Counters.Value("retry_after_capped"), st.Counters.Value("body_cap_shrinks"))
	}
	c := cappedAPI{sortedAPI: &sortedAPI{keys: []string{"a", "b"}}, retryAfter: 3, shrinks: 5}
	_, st = list(t, Options{Account: "acct", Container: "c", Client: c}, 1)
	if st.Counters.Value("retry_after_capped") != 3 || st.Counters.Value("body_cap_shrinks") != 5 {
		t.Fatalf("supplied client: retry_after_capped %d, body_cap_shrinks %d", st.Counters.Value("retry_after_capped"), st.Counters.Value("body_cap_shrinks"))
	}
}

// TestPoolSizeReported: the report's transport.pool_size is the built
// client's pool, one transport per 16 workers, and the client's counters
// follow the engine's.
func TestPoolSizeReported(t *testing.T) {
	r := azblobtest.Serve(t, lowKeys())
	for workers, want := range map[int]int64{1: 1, 40: 3} {
		_, st := list(t, replayOptions(r), workers)
		if n := st.Counters.Value("transport.pool_size"); n != want || st.Counters.Value("attempts") == 0 {
			t.Fatalf("%d workers: transport.pool_size %d (want %d), attempts %d", workers, n, want, st.Counters.Value("attempts"))
		}
	}
}
