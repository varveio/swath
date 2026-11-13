// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package readbudget_test

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	azsdk "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/parquet-go/parquet-go"
	"google.golang.org/api/option"

	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/parquetout"
	srcs3 "github.com/varveio/swath/internal/source/s3"
	"github.com/varveio/swath/objfile"
	"github.com/varveio/swath/objfile/azblob"
	"github.com/varveio/swath/objfile/azblob/azfake"
	"github.com/varveio/swath/objfile/gcs"
	"github.com/varveio/swath/objfile/gcs/gcsfake"
	objs3 "github.com/varveio/swath/objfile/s3"
	"github.com/varveio/swath/objfile/s3/s3fake"
	"github.com/varveio/swath/objfile/spsource"
	"github.com/varveio/swath/reader"
)

// geometry is one fixture: tenants of 20 folders of 50 objects, except
// tenant browse, which has 100 folders of folderObjs objects interleaved with 1,500
// objects of its own, so a 1,000-entry page of it lists folders and
// objects. Parts are written by swath's own part writer, cut at partRows,
// and published as the lister does.
type geometry struct {
	name         string
	tenants      int
	browse       int // the browsed tenant
	folderObjs   int // objects in each of the browsed tenant's 100 folders
	partRows     int64
	tenantFormat string
	mid          int // a tenant inside part 1
	parts        int
	// groups is the row groups per part (checked by TestGeometry);
	// smallBudget is the sum budget of the small-budget browse, which
	// sums exactly smallSummed of the page's prefixes. Zero without it.
	groups, smallBudget, smallSummed int
	expect                           map[string][2]count // exact before/after counts by operation
	once                             sync.Once
	dir                              string
	err                              error
}

// smallGeo: 199,500 objects in 4 parts of up to 50,000 (one row group each,
// about 1.4 MB each).
var smallGeo = &geometry{name: "small", tenants: 169, browse: 50, folderObjs: 300, partRows: 50_000, tenantFormat: "t-%03d/", mid: 60, parts: 4,
	expect: map[string][2]count{
		"open":                    {{2, 1}, {1, 0}},
		"point lookup":            {{5, 1}, {4, 0}},
		"one-part scan":           {{3, 1}, {2, 0}},
		"CheckSchema":             {{16, 4}, {12, 0}},
		"browse page, counts on":  {{8, 1}, {5, 0}},
		"browse page, counts off": {{8, 1}, {5, 0}},
		"count of a prefix":       {{3, 1}, {2, 0}},
		"count of every key":      {{0, 0}, {0, 0}},
		"sum of a prefix":         {{4, 1}, {3, 0}},
	}}

// largeGeo: 1,599,500 objects in 4 parts of up to 400,000 (about 11 MB, two
// row groups each, so the footer is past SeekParquet's 2 MiB tail read).
// The browsed tenant straddles a part boundary and a row-group boundary
// (TestGeometry).
var largeGeo = &geometry{name: "large", tenants: 1399, browse: 300, folderObjs: 2000, partRows: 400_000, tenantFormat: "t-%04d/", mid: 450, parts: 4,
	groups: 2, smallBudget: 64 << 10, smallSummed: 9,
	expect: map[string][2]count{
		"open":                      {{2, 1}, {1, 0}},
		"point lookup":              {{7, 1}, {6, 0}},
		"one-part scan":             {{5, 1}, {4, 0}},
		"CheckSchema":               {{20, 4}, {16, 0}},
		"browse page, counts on":    {{19, 2}, {15, 0}},
		"browse page, counts off":   {{19, 2}, {15, 0}},
		"count of a prefix":         {{10, 2}, {8, 0}},
		"count of every key":        {{0, 0}, {0, 0}},
		"sum of a prefix":           {{13, 2}, {11, 0}},
		"browse page, small budget": {{18, 2}, {14, 0}},
	}}

func (g *geometry) browseRows() int { return 1600 - 100 + 100*g.folderObjs }

func (g *geometry) tenant(t int) string { return fmt.Sprintf(g.tenantFormat, t) }

func (g *geometry) keys() []string {
	var keys []string
	for t := range g.tenants {
		p := g.tenant(t)
		if t == g.browse {
			for i := range 1600 {
				if i%16 == 0 {
					for j := range g.folderObjs {
						keys = append(keys, fmt.Sprintf("%sdir-%04d/obj-%05d", p, i, j))
					}
				} else {
					keys = append(keys, fmt.Sprintf("%sobj-%04d", p, i))
				}
			}
			continue
		}
		for d := range 20 {
			for j := range 50 {
				keys = append(keys, fmt.Sprintf("%sd-%02d/f-%04d", p, d, j))
			}
		}
	}
	slices.Sort(keys)
	return keys
}

var geometries = []*geometry{smallGeo, largeGeo}

// fixture writes the dataset once per test binary.
func fixture(t *testing.T, g *geometry) string {
	t.Helper()
	g.once.Do(func() {
		dir, err := os.MkdirTemp("", "readbudget")
		if err != nil {
			g.err = err
			return
		}
		g.dir, g.err = dir, write(dir, g)
	})
	if g.err != nil {
		t.Fatal(g.err)
	}
	return g.dir
}

func TestMain(m *testing.M) {
	code := m.Run()
	for _, g := range geometries {
		if g.dir != "" {
			os.RemoveAll(g.dir)
		}
	}
	os.Exit(code)
}

func write(dir string, g *geometry) error {
	ctx := context.Background()
	if err := os.MkdirAll(filepath.Join(dir, dataset.DataDir), 0o755); err != nil {
		return err
	}
	router, err := parquetout.NewRouter(parquetout.Options{Out: dir, PartSize: 1 << 30, Encoders: 2, MaxRows: g.partRows}, &srcs3.Schema)
	if err != nil {
		return err
	}
	rng := rand.New(rand.NewPCG(42, 1))
	keys := g.keys()
	for len(keys) > 0 {
		n := min(len(keys), 1000)
		rows := make([]srcs3.Row, n)
		for i, k := range keys[:n] {
			rows[i] = srcs3.Row{Key: k, Data: srcs3.Object{Size: rng.Int64N(1 << 24), Mtime: 1_704_067_200_000_000 + rng.Int64N(1<<45),
				ETag: fmt.Sprintf("%016x%016x", rng.Uint64(), rng.Uint64()), StorageClass: "STANDARD"}}
		}
		if err := router.Write(ctx, [][]srcs3.Row{rows}); err != nil {
			return err
		}
		keys = keys[n:]
	}
	parts, err := router.Close()
	if err != nil {
		return err
	}
	meta := dataset.Meta{SchemaID: srcs3.Schema.ID, Groups: srcs3.Schema.Groups,
		Source: dataset.Source{Kind: "s3", URI: "s3://budget", Container: "budget"}, Scope: dataset.Scope{Kind: "whole"},
		Coverage: dataset.Coverage{Complete: true, StopReason: "completed", Exclusions: []string{}, Filters: []string{}}}
	return dataset.Publish(dir, meta, parts, nil)
}

// kind is what a request to a store is.
type kind int

const (
	metadata kind = iota // size and version: HEAD, GCS object attributes
	ranged               // one byte range
	whole                // a whole object
)

type request struct{ kind kind }

// recorder counts requests on arrival, before the handler can send any
// response bytes. beforeServe optionally holds requests at a test barrier.
type recorder struct {
	classify    func(*http.Request) kind
	mu          sync.Mutex
	reqs        []request
	beforeServe func(*http.Request)
}

func (rec *recorder) wrap(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.mu.Lock()
		rec.reqs = append(rec.reqs, request{kind: rec.classify(r)})
		beforeServe := rec.beforeServe
		rec.mu.Unlock()
		if beforeServe != nil {
			beforeServe(r)
		}
		h.ServeHTTP(w, r)
	})
}

func (rec *recorder) take() []request {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	r := rec.reqs
	rec.reqs = nil
	return r
}

func TestRecorderRecordsArrival(t *testing.T) {
	rec := &recorder{classify: func(*http.Request) kind { return ranged }}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	h := rec.wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		w.WriteHeader(http.StatusPartialContent)
	}))
	go func() {
		defer close(done)
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/part", nil))
	}()
	<-entered
	got := tally(rec.take())
	close(release)
	<-done
	if got.requests != 1 || got.metadata != 0 {
		t.Errorf("while serving: %s, want 1 request and 0 metadata", got)
	}
	if got := tally(rec.take()); got.requests != 0 {
		t.Errorf("completion leaked into the next operation: %s", got)
	}
}

// count is what an operation requested, independent of request timing.
type count struct{ requests, metadata int }

func (c count) String() string {
	return fmt.Sprintf("%d (%d metadata)", c.requests, c.metadata)
}

func tally(reqs []request) count {
	c := count{requests: len(reqs)}
	for _, r := range reqs {
		if r.kind == metadata {
			c.metadata++
		}
	}
	return c
}

// store is a fake object store holding the fixture under "ds".
type store struct {
	name   string
	opener objfile.Opener
	rec    *recorder
}

func upload(t *testing.T, dir string, put func(name string, data []byte)) {
	t.Helper()
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(dir, path)
		if strings.HasPrefix(rel, ".") { // the writer's work directory
			return nil
		}
		data, err := os.ReadFile(path)
		if err == nil {
			put("ds/"+filepath.ToSlash(rel), data)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

func rangeOf(r *http.Request) kind {
	if r.Header.Get("Range") != "" || r.Header.Get("x-ms-range") != "" {
		return ranged
	}
	return whole
}

type staticCreds struct{}

func (staticCreds) Retrieve(context.Context) (aws.Credentials, error) {
	return aws.Credentials{AccessKeyID: "AKIDBUDGET", SecretAccessKey: "secret", Source: "budget"}, nil
}

func stores(t *testing.T, dir string) []store {
	transport := func() *http.Client {
		return &http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone()}
	}

	s3rec := &recorder{classify: func(r *http.Request) kind {
		if r.Method == http.MethodHead {
			return metadata
		}
		return rangeOf(r)
	}}
	s3f := s3fake.New("budget")
	upload(t, dir, func(n string, b []byte) { s3f.Put(n, b) })
	s3srv := httptest.NewServer(s3rec.wrap(s3f))
	t.Cleanup(s3srv.Close)
	hc := transport()
	hc.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	s3c := awss3.New(awss3.Options{BaseEndpoint: aws.String(s3srv.URL), UsePathStyle: true, Region: "us-east-1",
		Credentials: aws.NewCredentialsCache(staticCreds{}), HTTPClient: hc})

	gcsrec := &recorder{classify: func(r *http.Request) kind {
		if strings.HasPrefix(r.URL.Path, "/storage/v1/") {
			return metadata
		}
		return rangeOf(r)
	}}
	gcsf := gcsfake.New("budget")
	upload(t, dir, func(n string, b []byte) { gcsf.Put(n, b) })
	gcssrv := httptest.NewServer(gcsrec.wrap(gcsf))
	t.Cleanup(gcssrv.Close)
	gcsc, err := storage.NewClient(context.Background(), option.WithEndpoint(gcssrv.URL+"/storage/v1/"),
		option.WithoutAuthentication(), option.WithHTTPClient(transport()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { gcsc.Close() })

	azrec := &recorder{classify: func(r *http.Request) kind {
		if r.Method == http.MethodHead {
			return metadata
		}
		return rangeOf(r)
	}}
	azf := azfake.New("acct", "ctr")
	upload(t, dir, func(n string, b []byte) { azf.Put(n, b) })
	azsrv := httptest.NewServer(azrec.wrap(azf))
	t.Cleanup(azsrv.Close)
	azc, err := azsdk.NewClientWithNoCredential(azsrv.URL+"/acct/", &azsdk.ClientOptions{ClientOptions: azcore.ClientOptions{
		Transport: transport(), Retry: policy.RetryOptions{MaxRetries: 1, RetryDelay: time.Millisecond, MaxRetryDelay: time.Millisecond}}})
	if err != nil {
		t.Fatal(err)
	}

	return []store{
		{"s3", objfile.Root(objs3.New(s3c, "budget"), "ds"), s3rec},
		{"gcs", objfile.Root(gcs.New(gcsc, "budget"), "ds"), gcsrec},
		{"azure", objfile.Root(azblob.New(azc, "ctr"), "ds"), azrec},
	}
}

// openOnly is the Source a consumer wrote before spsource (varve's
// swathstore.ObjectSource): Open alone, every part opened by a metadata
// request, every read one ReadAt.
type openOnly struct{ o objfile.Opener }

func (s openOnly) Open(ctx context.Context, name string) (seekparquet.SourceFile, error) {
	return s.o.Open(ctx, name)
}

// stack is the Source stack of a consumer: Pin over Limit(64), as varve
// builds it, with spsource or without.
func stack(o objfile.Opener, after bool) seekparquet.Source {
	p := objfile.Pin(objfile.Limit(o, 64), "budget")
	if after {
		return spsource.New(p)
	}
	return openOnly{p}
}

// operation is one measured reader operation on a dataset just opened
// (cold): run makes the requests counted. Exact request and metadata-request
// counts, before spsource (openOnly) and after it, both on the pinned
// SeekParquet dependency, are the geometry's expect for its name. Admission
// concurrency is checked separately by TestAdmissionRounds.
type operation struct {
	name string
	run  func(ctx context.Context, t *testing.T, d *reader.Dataset)
}

func mustDo(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func operations(t *testing.T, dir string, g *geometry) []operation {
	ctx := context.Background()
	local, err := reader.OpenDir(ctx, dir, seekparquet.OpenOptions{})
	mustDo(t, err)
	parts := local.Metadata().Manifest.Parts
	mustDo(t, local.Close())
	if len(parts) != g.parts {
		t.Fatalf("the fixture has %d parts, want %d", len(parts), g.parts)
	}
	part := parts[1]
	mid := []byte(fmt.Sprintf("%sd-10/f-0025", g.tenant(g.mid))) // inside part 1
	if bytes.Compare(mid, part.MinKey) <= 0 || bytes.Compare(mid, part.MaxKey) >= 0 {
		t.Fatalf("%q is not inside part 1 [%q, %q]", mid, part.MinKey, part.MaxKey)
	}
	browse := []byte(g.tenant(g.browse))
	// browsePage lists the first 1,000 entries of the browsed tenant and
	// returns how many of its prefixes were counted and summed. Every
	// prefix is counted exactly when counts are on.
	browsePage := func(ctx context.Context, t *testing.T, d *reader.Dataset, counts bool, budget int64) (prefixes, counted, summed int) {
		reductions := []seekparquet.Reduction{{Op: seekparquet.Sum, Column: "size"}}
		if counts {
			reductions = append(reductions, seekparquet.Reduction{Op: seekparquet.CountRows})
		}
		page, err := d.SeekParquet().Page(ctx, seekparquet.PageQuery{ListQuery: seekparquet.ListQuery{
			Prefix: browse, Delimiter: '/', Limit: 1000, Columns: []string{"size", "last_modified"}},
			Reductions: reductions, Aggregates: seekparquet.AggregateOptions{ValueBudgetBytes: budget}}, nil)
		mustDo(t, err)
		if len(page.Entries) != 1000 || page.Next == nil {
			t.Fatalf("%d entries, next %v", len(page.Entries), page.Next)
		}
		for _, e := range page.Entries {
			if e.CommonPrefix == nil {
				continue
			}
			prefixes++
			if len(e.Aggregates) != len(reductions) {
				t.Fatalf("prefix %q has no aggregates", e.CommonPrefix)
			}
			if counts && e.Aggregates[1].Status == seekparquet.AggregateExact {
				counted++
				if n := e.Aggregates[1].Value.Int64(); n != int64(g.folderObjs) {
					t.Fatalf("prefix %q counted %d, want %d", e.CommonPrefix, n, g.folderObjs)
				}
			}
			if e.Aggregates[0].Status == seekparquet.AggregateExact {
				summed++
			}
		}
		if counts && counted != prefixes {
			t.Fatalf("counts on: %d of %d prefixes counted", counted, prefixes)
		}
		return prefixes, counted, summed
	}
	browseOp := func(counts bool) func(context.Context, *testing.T, *reader.Dataset) {
		return func(ctx context.Context, t *testing.T, d *reader.Dataset) {
			prefixes, _, summed := browsePage(ctx, t, d, counts, 4<<20)
			if prefixes == 0 || summed != prefixes {
				t.Fatalf("%d of %d listed prefixes summed under the default budget", summed, prefixes)
			}
		}
	}
	ops := []operation{
		{name: "open"},
		{name: "point lookup", run: func(ctx context.Context, t *testing.T, d *reader.Dataset) {
			rows, err := d.SeekParquet().Rows(ctx, seekparquet.Query{Selection: seekparquet.Selection{Span: seekparquet.KeySpan(mid)},
				Limit: 1, Columns: []string{"size", "last_modified"}}, nil)
			mustDo(t, err)
			if len(rows) != 1 {
				t.Fatalf("%d rows", len(rows))
			}
		}},
		{name: "one-part scan", run: func(ctx context.Context, t *testing.T, d *reader.Dataset) {
			n := 0
			bulk := objfile.WithCacheMode(ctx, objfile.CacheBypass)
			mustDo(t, d.SeekParquet().Scan(bulk, seekparquet.Query{Selection: seekparquet.Selection{Span: seekparquet.Span{
				Start: part.MinKey, End: append(bytes.Clone(part.MaxKey), 0)}}, Columns: []string{"size"}}, nil,
				func(b *seekparquet.Batch) error { n += b.Len(); return nil }))
			if int64(n) != part.Rows {
				t.Fatalf("scanned %d rows of %d", n, part.Rows)
			}
		}},
		{name: "CheckSchema", run: func(ctx context.Context, t *testing.T, d *reader.Dataset) {
			mustDo(t, d.CheckSchema(ctx))
		}},
		{name: "browse page, counts on", run: browseOp(true)},
		{name: "browse page, counts off", run: browseOp(false)},
		{name: "count of a prefix", run: func(ctx context.Context, t *testing.T, d *reader.Dataset) {
			got, err := d.SeekParquet().Aggregate(ctx, []seekparquet.Selection{{Span: seekparquet.PrefixSpan(browse)}}, []seekparquet.Reduction{{Op: seekparquet.CountRows}}, seekparquet.AggregateOptions{}, nil)
			mustDo(t, err)
			if got[0][0].Status != seekparquet.AggregateExact || got[0][0].Value.Int64() != int64(g.browseRows()) {
				t.Fatalf("count %+v", got[0][0])
			}
		}},
		{name: "count of every key", run: func(ctx context.Context, t *testing.T, d *reader.Dataset) {
			got, err := d.SeekParquet().Aggregate(ctx, []seekparquet.Selection{{}}, []seekparquet.Reduction{{Op: seekparquet.CountRows}}, seekparquet.AggregateOptions{}, nil)
			mustDo(t, err)
			if got[0][0].Status != seekparquet.AggregateExact || got[0][0].Value.Int64() != int64(len(g.keys())) {
				t.Fatalf("count %+v", got[0][0])
			}
		}},
		{name: "sum of a prefix", run: func(ctx context.Context, t *testing.T, d *reader.Dataset) {
			got, err := d.SeekParquet().Aggregate(ctx, []seekparquet.Selection{{Span: seekparquet.PrefixSpan(browse)}}, []seekparquet.Reduction{{Op: seekparquet.CountRows}, {Op: seekparquet.Sum, Column: "size"}}, seekparquet.AggregateOptions{ValueBudgetBytes: -1}, nil)
			mustDo(t, err)
			if got[0][1].Status != seekparquet.AggregateExact {
				t.Fatal("no sum")
			}
		}},
	}
	if g.smallBudget > 0 {
		ops = append(ops, operation{name: "browse page, small budget", run: func(ctx context.Context, t *testing.T, d *reader.Dataset) {
			prefixes, _, summed := browsePage(ctx, t, d, true, int64(g.smallBudget))
			if summed != g.smallSummed || summed == prefixes || summed == 0 {
				t.Fatalf("%d of %d listed prefixes summed under a %d-byte budget, want exactly %d and not all", summed, prefixes, g.smallBudget, g.smallSummed)
			}
		}})
	}
	return ops
}

// TestRequestBudgets measures each operation cold, on each store, with the
// Source a consumer wrote before (open only) and with spsource, and fails
// when a count differs from its expected value. The table of counts is logged (-v).
func TestRequestBudgets(t *testing.T) {
	if testing.Short() {
		t.Skip("writes a 200,000-object fixture")
	}
	measure(t, smallGeo)
}

// TestRequestBudgetsLarge is TestRequestBudgets on a geometry that does not
// flatter the numbers: 1.6 million objects, parts of 11 MB with two row
// groups (beyond SeekParquet's tail read), and a browsed tenant across a
// part boundary and a row-group boundary.
func TestRequestBudgetsLarge(t *testing.T) {
	if testing.Short() {
		t.Skip("writes a 1.6-million-object fixture")
	}
	measure(t, largeGeo)
}

func measure(t *testing.T, g *geometry) {
	dir := fixture(t, g)
	ops := operations(t, dir, g)
	if err := checkOperationNames(ops, g.expect); err != nil {
		t.Fatalf("%s geometry: %v", g.name, err)
	}
	var table strings.Builder
	fmt.Fprintf(&table, "| operation | store | before: requests | after: requests |\n|---|---|---|---|\n")
	for _, s := range stores(t, dir) {
		for _, op := range ops {
			var got [2]count
			for i, after := range []bool{false, true} {
				ctx := context.Background()
				s.rec.take()
				d, err := reader.Open(ctx, stack(s.opener, after), seekparquet.OpenOptions{})
				mustDo(t, err)
				if op.run != nil {
					if op.name == "count of every key" {
						mustDo(t, d.CheckSchema(ctx))
					}
					s.rec.take()
					op.run(ctx, t, d)
				}
				got[i] = tally(s.rec.take())
				mustDo(t, d.Close())
			}
			fmt.Fprintf(&table, "| %s | %s | %s | %s |\n", op.name, s.name, got[0], got[1])
			want := g.expect[op.name]
			for i := range want {
				if got[i] != want[i] {
					t.Errorf("%s geometry, %s, %s: %s, want exactly %s", g.name, s.name, op.name+[]string{" before", " after"}[i], got[i], want[i])
				}
			}
		}
	}
	t.Log("\n" + table.String())
}

// Check both directions before measuring: an unused expectation must not
// silently survive an operation being removed or renamed.
func checkOperationNames(ops []operation, expect map[string][2]count) error {
	for _, op := range ops {
		if _, ok := expect[op.name]; !ok {
			return fmt.Errorf("no expected counts for %q", op.name)
		}
	}
	for name := range expect {
		if !slices.ContainsFunc(ops, func(op operation) bool { return op.name == name }) {
			return fmt.Errorf("expected counts for unknown operation %q", name)
		}
	}
	return nil
}

func TestCheckOperationNames(t *testing.T) {
	ops := []operation{{name: "open"}, {name: "point lookup"}}
	for _, tc := range []struct {
		name   string
		expect map[string][2]count
		want   string
	}{
		{"matching", map[string][2]count{"open": {}, "point lookup": {}}, ""},
		{"unused expectation", map[string][2]count{"open": {}, "point lookup": {}, "typo": {}}, `expected counts for unknown operation "typo"`},
		{"missing expectation", map[string][2]count{"open": {}}, `no expected counts for "point lookup"`},
		{"same size, different names", map[string][2]count{"open": {}, "typo": {}}, `no expected counts for "point lookup"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checkOperationNames(ops, tc.expect)
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || err.Error() != tc.want {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
}

// TestAdmissionRounds holds every request in a phase until all expected
// requests have arrived. No response in that phase can enable another
// request counted in the same phase. A serialized probe or part open
// therefore fails at the barrier rather than depending on timestamp overlap.
func TestAdmissionRounds(t *testing.T) {
	if testing.Short() {
		t.Skip("writes a 200,000-object fixture")
	}
	for _, s := range stores(t, fixture(t, smallGeo)) {
		for _, after := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/after=%t", s.name, after), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
				defer cancel()
				d, err := reader.Open(ctx, stack(s.opener, after), seekparquet.OpenOptions{})
				mustDo(t, err)
				defer d.Close()
				s.rec.take()

				type arrival struct {
					kind    kind
					release chan struct{}
				}
				arrivals := make(chan arrival)
				s.rec.mu.Lock()
				s.rec.beforeServe = func(r *http.Request) {
					a := arrival{kind: s.rec.classify(r), release: make(chan struct{})}
					select {
					case arrivals <- a:
					case <-ctx.Done():
						return
					}
					select {
					case <-a.release:
					case <-ctx.Done():
					}
				}
				s.rec.mu.Unlock()
				defer func() {
					s.rec.mu.Lock()
					s.rec.beforeServe = nil
					s.rec.mu.Unlock()
				}()

				done, finished := make(chan error, 1), make(chan struct{})
				go func() {
					defer close(finished)
					done <- d.CheckSchema(ctx)
				}()
				defer func() { cancel(); <-finished }()

				// Four independent parts: before the adapter each needs a
				// metadata request, then a footer read. Both stacks then
				// fetch first and last key pages together (eight GETs).
				type phase struct {
					kind kind
					n    int
				}
				phases := []phase{{ranged, 4}, {ranged, 8}}
				if !after {
					phases = append([]phase{{metadata, 4}}, phases...)
				}
				for phase, want := range phases {
					var releases []chan struct{}
					for i := range want.n {
						select {
						case a := <-arrivals:
							if a.kind != want.kind {
								t.Fatalf("phase %d, request %d: kind %d, want %d", phase, i, a.kind, want.kind)
							}
							releases = append(releases, a.release)
						case err := <-done:
							t.Fatalf("phase %d: completed after %d of %d arrivals: %v", phase, i, want.n, err)
						case <-ctx.Done():
							t.Fatalf("phase %d: only %d of %d concurrent requests arrived: %v", phase, i, want.n, ctx.Err())
						}
					}
					for _, release := range releases {
						close(release)
					}
				}
				select {
				case err := <-done:
					mustDo(t, err)
				case <-arrivals:
					t.Fatal("unexpected request after the final admission phase")
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			})
		}
	}
}

// TestGeometry checks what each fixture is meant to be: the parts and row
// groups it has, and that the browsed tenant straddles a part boundary and
// a row-group boundary (the large one, where each part holds several groups).
func TestGeometry(t *testing.T) {
	if testing.Short() {
		t.Skip("writes the fixtures")
	}
	for _, g := range geometries {
		t.Run(g.name, func(t *testing.T) {
			dir := fixture(t, g)
			local, err := reader.OpenDir(context.Background(), dir, seekparquet.OpenOptions{})
			mustDo(t, err)
			parts := local.Metadata().Manifest.Parts
			mustDo(t, local.Close())
			keys := g.keys()
			first := slices.IndexFunc(keys, func(k string) bool { return strings.HasPrefix(k, g.tenant(g.browse)) })
			last := first + g.browseRows() - 1
			if !strings.HasPrefix(keys[last], g.tenant(g.browse)) || strings.HasPrefix(keys[last+1], g.tenant(g.browse)) {
				t.Fatalf("the browsed tenant is not rows %d..%d", first, last)
			}
			var starts []int64 // row-group starts inside a part
			var ordinal int64
			for i, part := range parts {
				f, err := os.Open(filepath.Join(dir, part.Path))
				mustDo(t, err)
				info, err := f.Stat()
				mustDo(t, err)
				pf, err := parquet.OpenFile(f, info.Size())
				mustDo(t, err)
				groups := pf.RowGroups()
				t.Logf("part %d: %d rows, %d bytes, %d row groups", i, part.Rows, info.Size(), len(groups))
				if g.groups > 0 && len(groups) != g.groups && i < len(parts)-1 {
					t.Errorf("part %d has %d row groups, want %d", i, len(groups), g.groups)
				}
				if g.groups > 0 && info.Size() < 2<<20+1 {
					t.Errorf("part %d is %d bytes, not beyond a 2 MiB tail read", i, info.Size())
				}
				for gi, rg := range groups {
					if gi > 0 { // inside a part: a part start is no row-group boundary
						starts = append(starts, ordinal)
					}
					ordinal += rg.NumRows()
				}
				f.Close()
			}
			if ordinal != int64(len(keys)) {
				t.Fatalf("parts hold %d rows, the fixture has %d", ordinal, len(keys))
			}
			if g.groups == 0 {
				return
			}
			var partBoundary, groupBoundary bool
			var ordinalOfPart int64
			for _, part := range parts {
				if ordinalOfPart > int64(first) && ordinalOfPart <= int64(last) {
					partBoundary = true
				}
				ordinalOfPart += part.Rows
			}
			for _, start := range starts {
				if start > int64(first) && start <= int64(last) {
					groupBoundary = true
				}
			}
			t.Logf("browsed tenant: rows %d..%d; part boundaries at multiples of %d; row-group starts inside parts: %v", first, last, g.partRows, starts)
			if !partBoundary || !groupBoundary {
				t.Errorf("the browsed tenant (rows %d..%d) straddles a part boundary %t, a row-group boundary %t", first, last, partBoundary, groupBoundary)
			}
		})
	}
}
