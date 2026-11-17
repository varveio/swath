// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/varveio/swath"
	"github.com/varveio/swath/internal/replay/server"
	"github.com/varveio/swath/internal/replay/store/memstore"
	"github.com/varveio/swath/internal/source/s3/s3test"
	"github.com/varveio/swath/objfile"
	"github.com/varveio/swath/reader/listing"
	"github.com/varveio/swath/s3"
)

// open opens bucket at endpoint, path-style and unsigned, until the test
// ends.
func open(t *testing.T, endpoint, bucket string) *swath.Source {
	t.Helper()
	src, err := s3.Open(context.Background(), bucket, s3.Options{Endpoint: endpoint, PathStyle: true, Anonymous: true, Region: "us-east-1"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { src.Close() })
	return src
}

// requests records the requests a fake store is sent, as "METHOD path".
type requests struct {
	mu   sync.Mutex
	seen []string
}

func (r *requests) record(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.mu.Lock()
		r.seen = append(r.seen, req.Method+" "+req.URL.Path)
		r.mu.Unlock()
		h.ServeHTTP(w, req)
	})
}

func (r *requests) list() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.seen)
}

// fake serves gofakes3's bucket s3test.Name holding keys and records its
// requests.
func fake(t *testing.T, keys ...string) (endpoint string, sent *requests) {
	t.Helper()
	sent = &requests{}
	return s3test.Serve(t, sent.record(s3test.Handler(s3test.Backend(t, keys)))), sent
}

// level is the level of prefix in keys, as S3's delimiter listing has it:
// the object at exactly prefix, then each child in byte order.
func level(keys []string, prefix string) (self string, children []string) {
	seen := map[string]bool{}
	for _, k := range keys {
		rest, ok := strings.CutPrefix(k, prefix)
		switch {
		case !ok:
		case rest == "":
			self = k
		case strings.Contains(rest, "/"):
			seen[prefix+rest[:strings.Index(rest, "/")+1]] = true
		default:
			seen[k] = true
		}
	}
	for k := range seen {
		children = append(children, k)
	}
	slices.Sort(children)
	return self, children
}

// TestPageOrderAtLimitOne: paged one entry at a time from the replay
// server, which pages a delimiter listing as live S3 does, every level
// comes back whole, once, in byte order (a file "a.txt" before a prefix
// "a/"), with the object at exactly the prefix as Self and never as a
// child, and a prefix taken literally ("a" holds "a0" and "a/").
func TestPageOrderAtLimitOne(t *testing.T) {
	s3test.NoCredentials(t)
	keys := []string{"a", "a.txt", "a/", "a/b", "a/b/c", "a/b/d", "a/c/x", "a/c/y", "a/d", "a0", "b/x", "b/y/z", "c"}
	srv, err := server.New(server.Config{
		Bucket: "bucket", Host: "127.0.0.1", MetricsPort: -1, MaxConcurrentRequests: server.DefaultMaxConcurrentRequests,
		ResponseBufferBudget: server.DefaultResponseBufferBudget, MaxResponseBytes: server.DefaultMaxResponseBytes,
		StopTimeout: server.DefaultStopTimeout, IdleTimeout: server.DefaultIdleTimeout, WriteTimeout: server.DefaultWriteTimeout,
		SlowRequestLog: -1, Store: memstore.Keys(memstore.Options{Rollup: true}, keys...), ServingMode: "SORTED", FixtureIdentity: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	t.Cleanup(func() { srv.Shutdown(time.Second) })
	src := open(t, "http://127.0.0.1:"+strconv.Itoa(srv.Port()), "bucket")

	for _, prefix := range []string{"", "a/", "a", "a/b/", "b/", "z"} {
		for _, limit := range []int{1, 2} {
			req := listing.PageRequest{Prefix: []byte(prefix), Limit: limit}
			var self string
			var children []string
			for pages := 0; ; pages++ {
				if pages > len(keys)+1 {
					t.Fatalf("%q, limit %d: the level does not end", prefix, limit)
				}
				page, err := src.Page(context.Background(), req)
				if err != nil {
					t.Fatalf("%q, limit %d: %v", prefix, limit, err)
				}
				if len(page.Entries) > limit || page.Order != listing.OrderBytes || page.Evidence.Stop != listing.StopCompleted {
					t.Fatalf("%q, limit %d: %d entries, order %q, stop %q", prefix, limit, len(page.Entries), page.Order, page.Evidence.Stop)
				}
				if page.Self != nil {
					if self != "" || page.Self.Kind != listing.KindObject {
						t.Fatalf("%q: Self %q (%v) after %q", prefix, page.Self.Key, page.Self.Kind, self)
					}
					self = string(page.Self.Key)
				}
				for _, e := range page.Entries {
					want := listing.KindObject
					if strings.HasSuffix(string(e.Key), "/") {
						want = listing.KindPrefix
					}
					if e.Kind != want {
						t.Errorf("%q: %q is %v, want %v", prefix, e.Key, e.Kind, want)
					}
					children = append(children, string(e.Key))
				}
				if page.Next == "" {
					break
				}
				req.Token = page.Next
			}
			wantSelf, want := level(keys, prefix)
			if self != wantSelf || !slices.Equal(children, want) {
				t.Errorf("%q, limit %d: Self %q and %q, want %q and %q", prefix, limit, self, children, wantSelf, want)
			}
		}
	}
}

// listResult is a ListBucketResult holding objects and common prefixes,
// truncated and continuing with next when next is not empty.
func listResult(next string, objects, prefixes []string) string {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>bkt</Name>`)
	fmt.Fprintf(&b, "<IsTruncated>%t</IsTruncated>", next != "")
	if next != "" {
		fmt.Fprintf(&b, "<NextContinuationToken>%s</NextContinuationToken>", next)
	}
	for _, k := range objects {
		fmt.Fprintf(&b, "<Contents><Key>%s</Key><LastModified>2026-11-17T10:00:00.000Z</LastModified><Size>1</Size><StorageClass>STANDARD</StorageClass></Contents>", k)
	}
	for _, p := range prefixes {
		fmt.Fprintf(&b, "<CommonPrefixes><Prefix>%s</Prefix></CommonPrefixes>", p)
	}
	b.WriteString("</ListBucketResult>")
	return b.String()
}

// scripted serves a store whose ListObjectsV2 answers are pages, by the
// continuation token the request sends ("" for none).
func scripted(t *testing.T, pages map[string]string) string {
	t.Helper()
	return s3test.Serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page, ok := pages[r.URL.Query().Get("continuation-token")]
		if !ok {
			t.Errorf("unexpected request %s", r.URL)
			http.Error(w, "unexpected", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprint(w, page)
	}))
}

// TestSelfAfterAnEmptyContinuingPage: a store may answer the start of a
// level with an empty page that goes on. That page is empty, with a
// token; the next one holds the object at exactly the prefix as Self, and
// the children after it.
func TestSelfAfterAnEmptyContinuingPage(t *testing.T) {
	s3test.NoCredentials(t)
	src := open(t, scripted(t, map[string]string{
		"":   listResult("t1", nil, nil),
		"t1": listResult("", []string{"a/", "a/x"}, []string{"a/y/"}),
	}), "bkt")
	ctx := context.Background()
	req := listing.PageRequest{Prefix: []byte("a/"), Limit: 10}
	first, err := src.Page(ctx, req)
	if err != nil || len(first.Entries) != 0 || first.Self != nil || first.Next == "" {
		t.Fatalf("first page %+v, %v", first, err)
	}
	req.Token = first.Next
	second, err := src.Page(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if second.Self == nil || string(second.Self.Key) != "a/" || len(second.Entries) != 2 || second.Next != "" ||
		string(second.Entries[0].Key) != "a/x" || second.Entries[1].Kind != listing.KindPrefix {
		t.Fatalf("second page %+v", second)
	}
	if e := second.Self; e.Size == nil || *e.Size != 1 || e.LastModified == nil || !e.LastModified.Equal(time.Date(2026, 11, 17, 10, 0, 0, 0, time.UTC)) ||
		e.StorageClass == nil || *e.StorageClass != "STANDARD" {
		t.Errorf("Self %+v", e)
	}
}

// TestInvalidPages: a page that breaks the contract is ErrInvalidPage, not
// retried: a continuation token that repeats the one sent, a key outside
// the prefix, a key below one of its children, a key out of order across
// pages.
func TestInvalidPages(t *testing.T) {
	s3test.NoCredentials(t)
	for _, c := range []struct {
		name  string
		pages map[string]string
	}{
		{"a repeated token", map[string]string{"": listResult("t1", []string{"a/1"}, nil), "t1": listResult("t1", []string{"a/2"}, nil)}},
		{"a key outside the prefix", map[string]string{"": listResult("", []string{"a/1", "b/2"}, nil)}},
		{"an object below a child", map[string]string{"": listResult("", []string{"a/b/c"}, nil)}},
		{"a common prefix below a child", map[string]string{"": listResult("", nil, []string{"a/b/c/"})}},
		{"a truncated page without a token", map[string]string{"": strings.Replace(listResult("", []string{"a/1"}, nil), "false", "true", 1)}},
		{"a key repeated across pages", map[string]string{"": listResult("t1", []string{"a/1"}, nil), "t1": listResult("", []string{"a/1"}, nil)}},
		{"an object and a prefix of one key", map[string]string{"": listResult("", []string{"a/b/"}, []string{"a/b/"})}},
	} {
		t.Run(c.name, func(t *testing.T) {
			src := open(t, scripted(t, c.pages), "bkt")
			req := listing.PageRequest{Prefix: []byte("a/"), Limit: 10}
			for range 2 {
				page, err := src.Page(context.Background(), req)
				if err != nil {
					if !errors.Is(err, listing.ErrInvalidPage) || listing.Retryable(err) {
						t.Fatalf("%v, want ErrInvalidPage", err)
					}
					return
				}
				req.Token = page.Next
			}
			t.Fatal("no error")
		})
	}
}

// TestTokenBinding: a token works only for the level it was issued for:
// on another prefix, or through another endpoint with a bucket of the same
// name, it is ErrInvalidToken, before any request; so is a token that is
// not one.
func TestTokenBinding(t *testing.T) {
	s3test.NoCredentials(t)
	keys := []string{"a/1", "a/2", "b/1", "b/2"}
	endpoint, sent := fake(t, keys...)
	other, otherSent := fake(t, keys...)
	src, elsewhere := open(t, endpoint, s3test.Name), open(t, other, s3test.Name)
	ctx := context.Background()
	page, err := src.Page(ctx, listing.PageRequest{Prefix: []byte("a/"), Limit: 1})
	if err != nil || page.Next == "" {
		t.Fatalf("%+v, %v", page, err)
	}
	if _, err := src.Page(ctx, listing.PageRequest{Prefix: []byte("a/"), Limit: 1, Token: page.Next}); err != nil {
		t.Fatalf("the token on its own level: %v", err)
	}

	before := len(sent.list())
	for _, c := range []struct {
		name string
		src  *swath.Source
		req  listing.PageRequest
	}{
		{"another prefix", src, listing.PageRequest{Prefix: []byte("b/"), Limit: 1, Token: page.Next}},
		{"another endpoint", elsewhere, listing.PageRequest{Prefix: []byte("a/"), Limit: 1, Token: page.Next}},
		{"no token", src, listing.PageRequest{Prefix: []byte("a/"), Limit: 1, Token: "live1.x.y.z"}},
	} {
		if _, err := c.src.Page(ctx, c.req); !errors.Is(err, listing.ErrInvalidToken) {
			t.Errorf("%s: %v, want ErrInvalidToken", c.name, err)
		}
	}
	if n := len(sent.list()) - before; n != 0 || len(otherSent.list()) != 0 {
		t.Errorf("%d and %d requests after the refused tokens", n, len(otherSent.list()))
	}
}

// TestLookup: a present key is its entry; a missing key is absent only
// after HeadBucket shows the bucket is there; in a missing bucket it is
// unknown (ErrNotFound), and a page of it is ErrNotFound. Every answer's
// evidence is live, over the call's own interval.
func TestLookup(t *testing.T) {
	s3test.NoCredentials(t)
	endpoint, sent := fake(t, "a/", "a/b")
	src := open(t, endpoint, s3test.Name)
	ctx := context.Background()

	before := time.Now()
	found, err := src.Lookup(ctx, []byte("a/"))
	if err != nil {
		t.Fatal(err)
	}
	if e := found.Entry; found.State != listing.LookupPresent || e == nil || string(e.Key) != "a/" || e.Kind != listing.KindObject ||
		e.Size == nil || *e.Size != 2 || e.StorageClass == nil || *e.StorageClass != "STANDARD" {
		t.Fatalf("present: %+v %+v", found, found.Entry)
	}
	ev := found.Evidence
	if ev.Origin != listing.OriginLive || ev.Manifest != "" || ev.Started == nil || ev.Finished == nil || ev.Started.Before(before) ||
		ev.Finished.Before(*ev.Started) || ev.Unlocated == nil || *ev.Unlocated != 0 || ev.Stop != listing.StopCompleted {
		t.Errorf("evidence %+v", ev)
	}

	n := len(sent.list())
	missing, err := src.Lookup(ctx, []byte("a"))
	if err != nil || missing.State != listing.LookupAbsent || missing.Entry != nil || missing.Reason != nil {
		t.Fatalf("missing: %+v, %v", missing, err)
	}
	if got, want := sent.list()[n:], []string{"HEAD /bkt/a", "HEAD /bkt"}; !slices.Equal(got, want) {
		t.Errorf("a missing key's requests %q, want %q", got, want)
	}

	nobucket := open(t, endpoint, "nobucket")
	r, err := nobucket.Lookup(ctx, []byte("a/b"))
	if err != nil || r.State != listing.LookupUnknown || !errors.Is(r.Reason, listing.ErrNotFound) {
		t.Errorf("a missing bucket's key: %+v, %v", r, err)
	}
	if _, err := nobucket.Page(ctx, listing.PageRequest{Limit: 10}); !errors.Is(err, listing.ErrNotFound) {
		t.Errorf("a missing bucket's page: %v", err)
	}
}

// TestLookupHeaders: a present key's entry is what the HeadObject answer's
// headers say: its Content-Length, which describes no body of the answer,
// its Last-Modified and its storage class.
func TestLookupHeaders(t *testing.T) {
	s3test.NoCredentials(t)
	endpoint := s3test.Serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "123456789")
		w.Header().Set("Last-Modified", "Tue, 17 Nov 2026 10:00:00 GMT")
		w.Header().Set("X-Amz-Storage-Class", "GLACIER")
	}))
	r, err := open(t, endpoint, "bkt").Lookup(context.Background(), []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if e := r.Entry; r.State != listing.LookupPresent || e == nil || e.Size == nil || *e.Size != 123456789 || e.LastModified == nil ||
		!e.LastModified.Equal(time.Date(2026, 11, 17, 10, 0, 0, 0, time.UTC)) || e.StorageClass == nil || *e.StorageClass != "GLACIER" {
		t.Fatalf("%+v %+v", r, r.Entry)
	}
}

// TestLookupUnknown: a 403 to HeadObject, S3's answer to a missing key for
// credentials that may not list, is unknown (ErrAuth) and probes nothing;
// so is a missing key whose bucket probe is refused.
func TestLookupUnknown(t *testing.T) {
	s3test.NoCredentials(t)
	for _, c := range []struct {
		name   string
		object int // HeadObject's status; HeadBucket's is 403
		want   []string
	}{
		{"a 403", http.StatusForbidden, []string{"HEAD /bkt/k"}},
		{"a failed probe", http.StatusNotFound, []string{"HEAD /bkt/k", "HEAD /bkt"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			sent := &requests{}
			endpoint := s3test.Serve(t, sent.record(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/bkt/k" {
					w.WriteHeader(c.object)
					return
				}
				w.WriteHeader(http.StatusForbidden)
			})))
			r, err := open(t, endpoint, "bkt").Lookup(context.Background(), []byte("k"))
			if err != nil || r.State != listing.LookupUnknown || !errors.Is(r.Reason, listing.ErrAuth) || r.Entry != nil {
				t.Errorf("%+v, %v; want unknown, ErrAuth", r, err)
			}
			if r.Evidence.Origin != listing.OriginLive {
				t.Errorf("evidence %+v", r.Evidence)
			}
			if got := sent.list(); !slices.Equal(got, c.want) {
				t.Errorf("requests %q, want %q", got, c.want)
			}
		})
	}
}

// TestPageTimeBudget: a page MaxDuration ends holds nothing, says so, and
// asks again with the token it was given.
func TestPageTimeBudget(t *testing.T) {
	s3test.NoCredentials(t)
	endpoint := s3test.Serve(t, s3test.Delayed(s3test.Handler(s3test.Backend(t, []string{"a"})), time.Second))
	page, err := open(t, endpoint, s3test.Name).Page(context.Background(), listing.PageRequest{Limit: 10, MaxDuration: 50 * time.Millisecond})
	if err != nil || len(page.Entries) != 0 || page.Next != "" || page.Evidence.Stop != listing.StopTimeBudget {
		t.Fatalf("%+v, %v", page, err)
	}
}

// TestInvalidRequests are refused before any request.
func TestInvalidRequests(t *testing.T) {
	s3test.NoCredentials(t)
	endpoint, sent := fake(t, "a")
	src := open(t, endpoint, s3test.Name)
	ctx := context.Background()
	for name, req := range map[string]listing.PageRequest{
		"no limit":   {},
		"a NUL":      {Prefix: []byte("a\x00"), Limit: 1},
		"not UTF-8":  {Prefix: []byte{0xff}, Limit: 1},
		"a negative": {Limit: 1, MaxDuration: -1},
	} {
		if _, err := src.Page(ctx, req); !errors.Is(err, listing.ErrInvalidRequest) {
			t.Errorf("page, %s: %v", name, err)
		}
	}
	if _, err := src.Page(ctx, listing.PageRequest{Limit: 1, ChildTotals: &listing.SummaryOptions{}}); !errors.Is(err, listing.ErrUnsupported) {
		t.Errorf("page with child totals: %v", err)
	}
	for _, key := range [][]byte{nil, []byte("a\x00"), {0xff}} {
		if _, err := src.Lookup(ctx, key); !errors.Is(err, listing.ErrInvalidRequest) {
			t.Errorf("lookup %q: %v", key, err)
		}
	}
	if _, err := s3.Open(ctx, "a/b", s3.Options{Anonymous: true}); !errors.Is(err, listing.ErrInvalidRequest) {
		t.Errorf("open a/b: %v", err)
	}
	if _, err := s3.Open(ctx, "dir--x-s3", s3.Options{Anonymous: true}); !errors.Is(err, listing.ErrUnsupported) {
		t.Errorf("open a directory bucket: %v", err)
	}
	if got := sent.list(); len(got) != 0 {
		t.Errorf("requests %q", got)
	}
}

// TestRedaction: a message that quotes what the store answered or what
// the caller gave keeps no URL's credentials: a key outside the prefix
// that holds one, and a bucket argument that does.
func TestRedaction(t *testing.T) {
	s3test.NoCredentials(t)
	const secret = "https://user:hunter2@example.com/x?token=hunter2"
	src := open(t, scripted(t, map[string]string{"": listResult("", []string{secret}, nil)}), "bkt")
	_, err := src.Page(context.Background(), listing.PageRequest{Prefix: []byte("a/"), Limit: 10})
	if !errors.Is(err, listing.ErrInvalidPage) || strings.Contains(err.Error(), "hunter2") {
		t.Errorf("a page: %v, want ErrInvalidPage without the credentials", err)
	}

	_, err = s3.Open(context.Background(), secret, s3.Options{Anonymous: true})
	if !errors.Is(err, listing.ErrInvalidRequest) || strings.Contains(err.Error(), "hunter2") {
		t.Errorf("open: %v, want ErrInvalidRequest without the credentials", err)
	}
}

// observed is an objfile.Observer that records each request and its
// outcome.
type observed struct {
	mu   sync.Mutex
	seen []string
}

func (o *observed) Begin(r objfile.Request) func(objfile.RequestResult) {
	return func(res objfile.RequestResult) {
		o.mu.Lock()
		defer o.mu.Unlock()
		o.seen = append(o.seen, fmt.Sprintf("%v %q retry=%t failed=%t", r.Kind, r.Name, r.Retry, res.Err != nil))
	}
}

func (o *observed) BatchPlanned(int64, int64, int64) {}

func (o *observed) CacheHit(int64) {}

// TestObserved: every request a call sends, retries included, reaches the
// observer on its context: a page answered 503 and then retried, and a
// missing key's HeadObject and HeadBucket.
func TestObserved(t *testing.T) {
	s3test.NoCredentials(t)
	var failed sync.Once
	backend := s3test.Handler(s3test.Backend(t, []string{"a/1"}))
	endpoint := s3test.Serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		first := false
		failed.Do(func() { first = true })
		if first {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		backend.ServeHTTP(w, r)
	}))
	src := open(t, endpoint, s3test.Name)
	obs := &observed{}
	ctx := objfile.WithObserver(context.Background(), obs)

	if _, err := src.Page(ctx, listing.PageRequest{Prefix: []byte("a/"), Limit: 10}); err != nil {
		t.Fatal(err)
	}
	if r, err := src.Lookup(ctx, []byte("a/2")); err != nil || r.State != listing.LookupAbsent {
		t.Fatalf("%+v, %v", r, err)
	}
	want := []string{
		`list "a/" retry=false failed=true`,
		`list "a/" retry=true failed=false`,
		`metadata "a/2" retry=false failed=true`,
		`metadata "" retry=false failed=false`,
	}
	if !slices.Equal(obs.seen, want) {
		t.Errorf("observed %q, want %q", obs.seen, want)
	}
}
