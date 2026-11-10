// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/johannesboyne/gofakes3/backend/s3mem"

	"github.com/varveio/swath/go/internal/order"
	"github.com/varveio/swath/go/internal/source/keyrange"
	"github.com/varveio/swath/go/internal/source/s3/client"
	"github.com/varveio/swath/go/internal/source/s3/s3test"
)

// clock gives every put its own, increasing modification time.
type clock struct{ t time.Time }

func (c *clock) Now() time.Time                  { c.t = c.t.Add(time.Second); return c.t }
func (c *clock) Since(t time.Time) time.Duration { return c.t.Sub(t) }

// fakeBucket serves keys from an in-process gofakes3. Every object's body is
// its key, so sizes and ETags differ per object. fail503 makes the first n
// requests fail with 503 Slow Down.
func fakeBucket(t testing.TB, keys []string, fail503 int) (url string) {
	t.Helper()
	return s3test.Serve(t, fakeHandler(t, keys, fail503))
}

// fakeHandler is fakeBucket's handler.
func fakeHandler(t testing.TB, keys []string, fail503 int) http.Handler {
	t.Helper()
	h := s3test.Handler(s3test.Backend(t, keys, s3mem.WithTimeSource(&clock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)})))
	var n atomic.Int64
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1) <= int64(fail503) {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>SlowDown</Code><Message>slow down</Message></Error>`)
			return
		}
		h.ServeHTTP(w, r)
	})
}

func testKeys() []string {
	keys := []string{"a", "a-b", "a.b", "a/", "a/x", "a/y/z", "a0", "ab", "b/c", "é", "日本/x", "z~", "\x7f"}
	for i := range 2500 {
		keys = append(keys, fmt.Sprintf("data/year=%d/part-%05d.parquet", 2020+i%5, i))
	}
	slices.Sort(keys)
	return keys
}

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

func list(t *testing.T, opt Options, workers int) ([]Row, Stats) {
	t.Helper()
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	t.Setenv("AWS_CONFIG_FILE", "/nonexistent")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent")
	p, err := New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	c := &collect{}
	if _, err := order.Run[Range, Object](context.Background(), p, c, &Schema, order.Options{Workers: workers, Budget: 100 * 256}); err != nil {
		t.Fatal(err)
	}
	return c.rows, p.Stats()
}

func keysOf(rows []Row) []string {
	out := make([]string, len(rows))
	for i := range rows {
		out[i] = rows[i].Key
	}
	return out
}

func TestListFakeBucket(t *testing.T) {
	keys := testKeys()
	url := fakeBucket(t, keys, 0)
	rows, st := list(t, Options{Bucket: "bkt", Endpoint: url, PathStyle: true, Anonymous: true, Policy: keyrange.Policy{Sequential: true}}, 1)
	if !slices.Equal(keysOf(rows), keys) {
		t.Fatalf("got %d keys, want %d", len(rows), len(keys))
	}
	if st.Counters.Value("pages") != 3 || st.Counters.Value("objects") != int64(len(keys)) || !st.Anonymous {
		t.Fatalf("stats %+v", st)
	}
	r := rows[0]
	if r.Data.Size != int64(len(r.Key)) || r.Data.Mtime == 0 || len(r.Data.ETag) != 32 || r.Data.ETag[0] == '"' {
		t.Fatalf("row %+v", r)
	}
}

func TestPrefixAndLimit(t *testing.T) {
	keys := testKeys()
	url := fakeBucket(t, keys, 0)
	seq, _ := list(t, Options{Bucket: "bkt", Endpoint: url, PathStyle: true, Anonymous: true, Policy: keyrange.Policy{Sequential: true}}, 1)
	if !slices.Equal(keysOf(seq), keys) {
		t.Fatalf("sequential: %d keys", len(seq))
	}
	for _, w := range []int{1, 4, 8} {
		for _, seed := range []bool{false, true} {
			par, st := list(t, Options{Bucket: "bkt", Endpoint: url, PathStyle: true, Anonymous: true, Workers: w, Policy: keyrange.Policy{Seed: seed}}, w)
			if !slices.Equal(keysOf(par), keys) {
				t.Fatalf("parallel w%d seed=%v: %d keys", w, seed, len(par))
			}
			for i := range par {
				if par[i] != seq[i] {
					t.Fatalf("w%d seed=%v: row %d differs", w, seed, i)
				}
			}
			if seed && st.Counters.Value("seed.cuts") == 0 {
				t.Fatalf("seed produced no cuts: %+v", st.Counters)
			}
		}
	}
	pre, _ := list(t, Options{Bucket: "bkt", Prefix: "a/", Endpoint: url, PathStyle: true, Anonymous: true, Workers: 2}, 2)
	if !slices.Equal(keysOf(pre), []string{"a/", "a/x", "a/y/z"}) {
		t.Fatalf("prefix: %q", keysOf(pre))
	}
	lim, st := list(t, Options{Bucket: "bkt", Endpoint: url, PathStyle: true, Anonymous: true, Policy: keyrange.Policy{Sequential: true, MaxKeys: 1500}}, 1)
	if len(lim) != 1500 || st.Counters.Value("truncated_at_max_keys") == 0 || !slices.Equal(keysOf(lim), keys[:1500]) {
		t.Fatalf("limit: %d rows truncated=%v", len(lim), st.Counters.Value("truncated_at_max_keys"))
	}
	if _, err := New(context.Background(), Options{Bucket: "bkt", Policy: keyrange.Policy{MaxKeys: 1}}); err == nil {
		t.Fatal("a key limit without -sequential accepted")
	}
}

// stub returns canned pages.
type stub struct{ pages []*client.Page }

func (s *stub) ListObjectsV2(_ context.Context, in *client.ListInput) (*client.Page, error) {
	i := 0
	if in.ContinuationToken != "" {
		fmt.Sscan(in.ContinuationToken, &i)
	}
	out := *s.pages[i]
	if i+1 < len(s.pages) {
		out.IsTruncated, out.NextContinuationToken = true, fmt.Sprint(i+1)
	}
	return &out, nil
}

func TestObjectFields(t *testing.T) {
	when := time.Date(2026, 3, 4, 5, 6, 7, 891_234_000, time.UTC)
	page := &client.Page{Contents: []client.Object{
		{Key: "a", Size: 7, Mtime: when.UnixMicro(), ETag: `"0123456789abcdef0123456789abcdef-3"`,
			StorageClass: "GLACIER", ChecksumAlgorithm: "CRC32", ChecksumType: "FULL_OBJECT", OwnerID: "o1", OwnerDisplayName: "me"},
		{Key: "b", ETag: "unquoted"},
	}}
	for _, owner := range []bool{false, true} {
		rows, _ := list(t, Options{Bucket: "x", Client: &stub{pages: []*client.Page{page}}, FetchOwner: owner, Policy: keyrange.Policy{Sequential: true}}, 1)
		a, b := rows[0], rows[1]
		want := Object{Size: 7, Mtime: when.UnixMicro(), ETag: "0123456789abcdef0123456789abcdef-3", StorageClass: "GLACIER", ChecksumAlgorithm: "CRC32", ChecksumType: "FULL_OBJECT"}
		if owner {
			want.OwnerID, want.OwnerDisplayName = "o1", "me"
		}
		if a.Data != want {
			t.Fatalf("owner=%v: %+v", owner, a)
		}
		if b.Data != (Object{ETag: "unquoted"}) {
			t.Fatalf("row b %+v", b)
		}
	}
}

func TestOutOfOrderListingFails(t *testing.T) {
	pages := []*client.Page{
		{Contents: []client.Object{{Key: "b"}}},
		{Contents: []client.Object{{Key: "a"}}},
	}
	p, _ := New(context.Background(), Options{Bucket: "x", Client: &stub{pages: pages}, Policy: keyrange.Policy{Sequential: true}})
	if _, err := order.Run[Range, Object](context.Background(), p, &collect{}, &Schema, order.Options{Workers: 1, Budget: 10 * 256}); err == nil {
		t.Fatal("out-of-order pages accepted")
	}
}

func TestParseURL(t *testing.T) {
	for in, want := range map[string][2]string{"s3://b": {"b", ""}, "s3://b/": {"b", ""}, "s3://b/p/q/": {"b", "p/q/"}} {
		b, p, err := ParseURL(in)
		if err != nil || b != want[0] || p != want[1] {
			t.Errorf("%s: %q %q %v", in, b, p, err)
		}
	}
	for _, bad := range []string{"b/p", "s3://", "s3:///p"} {
		if _, _, err := ParseURL(bad); err == nil {
			t.Errorf("%s accepted", bad)
		}
	}
}

// TestS3BoundPolicy: S3's bound policy is the pivot alphabet plus '%':
// no control character, surrogate or non-character is sent in start-after,
// while '/', '%' (copied from real keys), non-ASCII letters and long keys
// are.
func TestS3BoundPolicy(t *testing.T) {
	for _, s := range []string{"a/b", "café", "世", strings.Repeat("k", 1024), "a%20b", "%", "web/https%3A%2F%2F"} {
		if !AcceptBound(s) {
			t.Errorf("refused %q", s)
		}
	}
	for _, s := range []string{"a%\x01", "a\x01", "a\x7f", "a\u0085", "a﷐", "a￿", "\xff"} {
		if AcceptBound(s) {
			t.Errorf("accepted %q", s)
		}
	}
}

func TestParseURLDiagnostics(t *testing.T) {
	for _, c := range []struct{ root, want string }{
		{"s3://", `not an s3://bucket[/prefix] URL: "s3://"`},
		{"s3:///prefix", `no bucket in "s3:///prefix"`},
		{"other://user:SECRET@bucket/path", `not an s3://bucket[/prefix] URL: "other://[redacted]"`},
		{"other://bucket?sig=SECRET/path", `not an s3://bucket[/prefix] URL: "other://[redacted]"`},
		{"other://bucket#SECRET/path", `not an s3://bucket[/prefix] URL: "other://[redacted]"`},
	} {
		_, _, err := ParseURL(c.root)
		if err == nil || err.Error() != c.want {
			t.Errorf("ParseURL: %v, want %q", err, c.want)
		}
	}
}
