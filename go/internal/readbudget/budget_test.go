// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package readbudget_test

import (
	"bytes"
	"cmp"
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
	"google.golang.org/api/option"

	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/parquetout"
	srcs3 "github.com/varveio/swath/go/internal/source/s3"
	"github.com/varveio/swath/go/objfile"
	"github.com/varveio/swath/go/objfile/azblob"
	"github.com/varveio/swath/go/objfile/azblob/azfake"
	"github.com/varveio/swath/go/objfile/gcs"
	"github.com/varveio/swath/go/objfile/gcs/gcsfake"
	objs3 "github.com/varveio/swath/go/objfile/s3"
	"github.com/varveio/swath/go/objfile/s3/s3fake"
	"github.com/varveio/swath/go/objfile/spsource"
	"github.com/varveio/swath/go/reader"
)

// latency is the time each fake store waits before it serves a request, so
// that requests made one after another do not overlap and requests made
// together do: rounds are counted from the overlaps.
const latency = 10 * time.Millisecond

// The fixture: 199,500 objects in 4 parts of up to 50,000 (one row group
// each),
// written by swath's own part writer and published as the lister does.
// Tenant browseTenant has 100 folders of 300 objects interleaved with 1,500
// objects of its own, so a 1,000-entry page of it lists folders and
// objects; every other tenant has 20 folders of 50 objects.
const (
	tenants      = 169
	browseTenant = 50
	partRows     = 50_000
)

func tenant(t int) string { return fmt.Sprintf("t-%03d/", t) }

func fixtureKeys() []string {
	var keys []string
	for t := range tenants {
		p := tenant(t)
		if t == browseTenant {
			for i := range 1600 {
				if i%16 == 0 {
					for j := range 300 {
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

var (
	fixtureOnce sync.Once
	fixtureDir  string
	fixtureErr  error
)

// fixture writes the dataset once per test binary.
func fixture(t *testing.T) string {
	t.Helper()
	fixtureOnce.Do(func() {
		dir, err := os.MkdirTemp("", "readbudget")
		if err != nil {
			fixtureErr = err
			return
		}
		fixtureDir, fixtureErr = dir, write(dir)
	})
	if fixtureErr != nil {
		t.Fatal(fixtureErr)
	}
	return fixtureDir
}

func TestMain(m *testing.M) {
	code := m.Run()
	if fixtureDir != "" {
		os.RemoveAll(fixtureDir)
	}
	os.Exit(code)
}

func write(dir string) error {
	ctx := context.Background()
	if err := os.MkdirAll(filepath.Join(dir, dataset.DataDir), 0o755); err != nil {
		return err
	}
	router, err := parquetout.NewRouter(parquetout.Options{Out: dir, PartSize: 1 << 30, Encoders: 2, MaxRows: partRows}, &srcs3.Schema)
	if err != nil {
		return err
	}
	rng := rand.New(rand.NewPCG(42, 1))
	keys := fixtureKeys()
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

type request struct {
	kind       kind
	start, end time.Time
}

// recorder counts the requests a store serves, after waiting latency
// before each.
type recorder struct {
	classify func(*http.Request) kind
	mu       sync.Mutex
	reqs     []request
	latency  time.Duration // latency when 0
}

func (rec *recorder) wrap(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec.mu.Lock()
		lat := cmp.Or(rec.latency, latency)
		rec.mu.Unlock()
		time.Sleep(lat)
		h.ServeHTTP(w, r)
		rec.mu.Lock()
		rec.reqs = append(rec.reqs, request{kind: rec.classify(r), start: start, end: time.Now()})
		rec.mu.Unlock()
	})
}

func (rec *recorder) take() []request {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	r := rec.reqs
	rec.reqs = nil
	return r
}

// count is what an operation requested: requests, of which metadata
// requests, and rounds, the longest chain of requests each made after the
// one before had been answered.
type count struct{ requests, metadata, rounds int }

func (c count) String() string {
	return fmt.Sprintf("%d / %d (%d metadata)", c.requests, c.rounds, c.metadata)
}

func tally(reqs []request) count {
	slices.SortFunc(reqs, func(a, b request) int { return a.start.Compare(b.start) })
	c := count{requests: len(reqs)}
	depth := make([]int, len(reqs))
	for i, r := range reqs {
		if r.kind == metadata {
			c.metadata++
		}
		depth[i] = 1
		for j := range i {
			if !reqs[j].end.After(r.start) {
				depth[i] = max(depth[i], depth[j]+1)
			}
		}
		c.rounds = max(c.rounds, depth[i])
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
func stack(o objfile.Opener, after bool) reader.Source {
	p := objfile.Pin(objfile.Limit(o, 64), "budget")
	if after {
		return spsource.New(p)
	}
	return openOnly{p}
}

// operation is one measured reader operation on a dataset just opened
// (cold): run makes the requests counted.
type operation struct {
	name string
	run  func(ctx context.Context, t *testing.T, d *reader.Dataset)
	// latency, when set, replaces the stores' latency: long enough that
	// requests made together overlap under the race detector too.
	latency time.Duration
	// before and after are the counts the operation must stay within,
	// requests, metadata requests and rounds, before spsource (openOnly)
	// and after it, both on the pinned SeekParquet. They are the counts
	// measured. The after budgets may only be
	// tightened.
	before, after count
}

func mustDo(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func operations(t *testing.T, dir string) []operation {
	ctx := context.Background()
	local, err := reader.OpenDir(ctx, dir, reader.Options{})
	mustDo(t, err)
	parts := local.Metadata().Manifest.Parts
	mustDo(t, local.Close())
	if len(parts) != 4 {
		t.Fatalf("the fixture has %d parts, want 4", len(parts))
	}
	part := parts[1]
	mid := []byte(fmt.Sprintf("%sd-10/f-0025", tenant(60))) // inside part 1
	if bytes.Compare(mid, part.MinKey) <= 0 || bytes.Compare(mid, part.MaxKey) >= 0 {
		t.Fatalf("%q is not inside part 1 [%q, %q]", mid, part.MinKey, part.MaxKey)
	}
	browse := []byte(tenant(browseTenant))
	return []operation{
		{name: "open", run: nil,
			before: count{2, 1, 2}, after: count{1, 0, 1}},
		{name: "cold point lookup, 2 columns", run: func(ctx context.Context, t *testing.T, d *reader.Dataset) {
			rows, err := d.SeekParquet().Rows(ctx, seekparquet.Query{Selection: seekparquet.Selection{Span: seekparquet.KeySpan(mid)},
				Limit: 1, Columns: []string{"size", "last_modified"}}, nil)
			mustDo(t, err)
			if len(rows) != 1 {
				t.Fatalf("%d rows", len(rows))
			}
		}, before: count{5, 1, 5}, after: count{4, 0, 4}},
		{name: "one-part scan, 1 column", run: func(ctx context.Context, t *testing.T, d *reader.Dataset) {
			n := 0
			mustDo(t, d.SeekParquet().Scan(ctx, seekparquet.Query{Selection: seekparquet.Selection{Span: seekparquet.Span{
				Start: part.MinKey, End: append(bytes.Clone(part.MaxKey), 0)}}, Columns: []string{"size"}}, nil,
				func(b *seekparquet.Batch) error { n += b.Len(); return nil }))
			if int64(n) != part.Rows {
				t.Fatalf("scanned %d rows of %d", n, part.Rows)
			}
		}, before: count{100, 1, 100}, after: count{99, 0, 99}},
		{name: "admission (CheckSchema), 4 parts", latency: 60 * time.Millisecond, run: func(ctx context.Context, t *testing.T, d *reader.Dataset) {
			mustDo(t, d.CheckSchema(ctx))
		}, before: count{16, 4, 3}, after: count{12, 0, 2}},
		{name: "browse page: 1,000 entries, counts and budgeted sums", run: func(ctx context.Context, t *testing.T, d *reader.Dataset) {
			page, err := d.Page(ctx, reader.PageRequest{ListRequest: reader.ListRequest{Prefix: browse, Delimiter: '/', Limit: 1000},
				Aggregates: reader.AggregateOptions{Counts: true, Sums: reader.SumsWithinBudget}})
			mustDo(t, err)
			if len(page.Entries) != 1000 || page.Next == nil {
				t.Fatalf("%d entries, next %v", len(page.Entries), page.Next)
			}
		}, before: count{547, 1, 547}, after: count{546, 0, 546}},
		{name: "count of a prefix", run: func(ctx context.Context, t *testing.T, d *reader.Dataset) {
			got, err := d.Aggregates(ctx, [][]byte{browse}, reader.AggregateOptions{Counts: true})
			mustDo(t, err)
			if got[0].Count == nil || *got[0].Count != 1600-100+100*300 {
				t.Fatalf("count %v", got[0].Count)
			}
		}, before: count{4, 1, 4}, after: count{3, 0, 3}},
		{name: "count of every key, after CheckSchema", run: func(ctx context.Context, t *testing.T, d *reader.Dataset) {
			got, err := d.Aggregates(ctx, [][]byte{nil}, reader.AggregateOptions{Counts: true})
			mustDo(t, err)
			if got[0].Count == nil || *got[0].Count != int64(len(fixtureKeys())) {
				t.Fatalf("count %v", got[0].Count)
			}
		}, before: count{0, 0, 0}, after: count{0, 0, 0}},
		{name: "sum of a prefix", run: func(ctx context.Context, t *testing.T, d *reader.Dataset) {
			got, err := d.Aggregates(ctx, [][]byte{browse}, reader.AggregateOptions{Counts: true, Sums: reader.SumsAlways})
			mustDo(t, err)
			if got[0].Bytes == nil {
				t.Fatal("no sum")
			}
		}, before: count{68, 1, 68}, after: count{67, 0, 67}},
	}
}

// TestRequestBudgets measures each operation cold, on each store, with the
// Source a consumer wrote before (open only) and with spsource, and fails
// when a count exceeds its budget. The table of counts is logged (-v).
func TestRequestBudgets(t *testing.T) {
	if testing.Short() {
		t.Skip("writes a 200,000-object fixture and waits on fake round trips")
	}
	dir := fixture(t)
	ops := operations(t, dir)
	var table strings.Builder
	fmt.Fprintf(&table, "| operation | store | before: requests / rounds | after: requests / rounds |\n|---|---|---|---|\n")
	for _, s := range stores(t, dir) {
		for _, op := range ops {
			var got [2]count
			for i, after := range []bool{false, true} {
				ctx := context.Background()
				s.rec.take()
				d, err := reader.Open(ctx, stack(s.opener, after), reader.Options{})
				mustDo(t, err)
				if op.run != nil {
					if strings.Contains(op.name, "after CheckSchema") {
						mustDo(t, d.CheckSchema(ctx))
					}
					s.rec.take()
					s.rec.mu.Lock()
					s.rec.latency = op.latency
					s.rec.mu.Unlock()
					op.run(ctx, t, d)
					s.rec.mu.Lock()
					s.rec.latency = 0
					s.rec.mu.Unlock()
				}
				got[i] = tally(s.rec.take())
				mustDo(t, d.Close())
			}
			fmt.Fprintf(&table, "| %s | %s | %s | %s |\n", op.name, s.name, got[0], got[1])
			for i, want := range []count{op.before, op.after} {
				g := got[i]
				if g.requests > want.requests || g.rounds > want.rounds || g.metadata > want.metadata {
					t.Errorf("%s, %s, %s: %s, budget %s", s.name, op.name, []string{"before", "after"}[i], g, want)
				}
			}
		}
	}
	t.Log("\n" + table.String())
}
