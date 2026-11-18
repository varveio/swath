// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/varveio/swath/internal/source/gcs/gcstest"
	"github.com/varveio/swath/objfile"
	"github.com/varveio/swath/reader/listing"
	"github.com/varveio/swath/source"
	"github.com/varveio/swath/source/gcs"
)

// open opens bucket at endpoint, without credentials, until the test ends.
func open(t *testing.T, endpoint, bucket string) *source.Source {
	t.Helper()
	src, err := gcs.Open(context.Background(), bucket, gcs.Options{Endpoint: endpoint, Anonymous: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { src.Close() })
	return src
}

// serve serves h and returns its JSON API endpoint, closed at the test's
// end.
func serve(t *testing.T, h http.Handler) string {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv.URL + gcstest.Path
}

// requests records the requests a fake store is sent, as "METHOD path",
// the path escaped as sent.
type requests struct {
	mu   sync.Mutex
	seen []string
}

func (r *requests) record(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.mu.Lock()
		r.seen = append(r.seen, req.Method+" "+req.URL.EscapedPath())
		r.mu.Unlock()
		h.ServeHTTP(w, req)
	})
}

func (r *requests) list() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.seen)
}

// fake serves the strict fake over keys and records its requests. An
// object whose name ends in "/" is zero-length, as a folder placeholder
// is.
func fake(t *testing.T, keys ...string) (endpoint string, sent *requests) {
	t.Helper()
	f := gcstest.NewStrict(keys)
	f.Resource = func(i int, k string) map[string]any {
		size := len(k)
		if strings.HasSuffix(k, "/") {
			size = 0
		}
		return map[string]any{"name": k, "size": strconv.Itoa(size), "updated": "2026-11-17T10:00:00.000Z", "storageClass": "NEARLINE"}
	}
	sent = &requests{}
	return serve(t, sent.record(f)), sent
}

// level is the level of prefix in keys, as GCS's delimiter listing has it:
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

// TestPageOrderAtLimitOne: paged one entry at a time from the strict fake,
// which answers every third request with an empty page that goes on, as
// the JSON API may, every level comes back whole, once, in byte order (a
// file "a.txt" before a prefix "a/"), with the zero-length object at
// exactly the prefix as Self and never as a child, and a prefix taken
// literally ("a" holds "a0" and "a/").
func TestPageOrderAtLimitOne(t *testing.T) {
	keys := []string{"a", "a.txt", "a/", "a/b", "a/b/c", "a/b/d", "a/c/x", "a/c/y", "a/d", "a0", "b/x", "b/y/z", "c"}
	f := gcstest.NewStrict(keys)
	f.Lean, f.Empty = true, 3
	src := open(t, f.Serve(t), "bkt")

	for _, prefix := range []string{"", "a/", "a", "a/b/", "b/", "z"} {
		for _, limit := range []int{1, 2} {
			req := listing.PageRequest{Prefix: []byte(prefix), Limit: limit}
			var self string
			var children []string
			empty := 0
			for pages := 0; ; pages++ {
				if pages > 2*len(keys)+2 {
					t.Fatalf("%q, limit %d: the level does not end", prefix, limit)
				}
				page, err := src.Page(context.Background(), req)
				if err != nil {
					t.Fatalf("%q, limit %d: %v", prefix, limit, err)
				}
				if len(page.Entries) > limit {
					t.Fatalf("%q, limit %d: %d entries", prefix, limit, len(page.Entries))
				}
				if page.Self != nil {
					if self != "" || page.Self.Kind != listing.KindObject || page.Self.Size == nil || *page.Self.Size != int64(len(page.Self.Key)) {
						t.Fatalf("%q: Self %+v after %q", prefix, page.Self, self)
					}
					self = string(page.Self.Key)
				}
				if len(page.Entries) == 0 && page.Self == nil && page.Next != "" {
					empty++
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
			if prefix == "" && limit == 1 && empty == 0 {
				t.Errorf("no empty page that goes on: the fake did not send one")
			}
		}
	}
}

// TestSelf: the zero-length object "a/" is the Self of a page of "a/", an
// object like any other with its listed size, time and storage class.
func TestSelf(t *testing.T) {
	endpoint, _ := fake(t, "a/", "a/x")
	page, err := open(t, endpoint, "bkt").Page(context.Background(), listing.PageRequest{Prefix: []byte("a/"), Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	e := page.Self
	if e == nil || string(e.Key) != "a/" || e.Kind != listing.KindObject || e.Size == nil || *e.Size != 0 || e.LastModified == nil ||
		!e.LastModified.Equal(time.Date(2026, 11, 17, 10, 0, 0, 0, time.UTC)) || e.StorageClass == nil || *e.StorageClass != "NEARLINE" {
		t.Fatalf("Self %+v", e)
	}
	if len(page.Entries) != 1 || string(page.Entries[0].Key) != "a/x" {
		t.Errorf("entries %+v", page.Entries)
	}
}

// scripted serves a store whose objects.list answers are the JSON objects
// pages, by the page token the request sends ("" for none).
func scripted(t *testing.T, pages map[string]map[string]any) string {
	t.Helper()
	return serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page, ok := pages[r.URL.Query().Get("pageToken")]
		if !ok {
			t.Errorf("unexpected request %s", r.URL)
			http.Error(w, "unexpected", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(page)
	}))
}

// objects is an objects.list answer holding the named objects and the
// prefixes, going on with next when it is not empty.
func objects(next string, names, prefixes []string) map[string]any {
	items := []any{}
	for _, n := range names {
		items = append(items, map[string]any{"name": n, "size": "1"})
	}
	page := map[string]any{"kind": "storage#objects", "items": items, "prefixes": prefixes}
	if next != "" {
		page["nextPageToken"] = next
	}
	return page
}

// TestInvalidPages: a page that breaks the contract is ErrInvalidPage, not
// retried: a key outside the prefix, an object below a child, a page token
// that repeats the one sent.
func TestInvalidPages(t *testing.T) {
	for _, c := range []struct {
		name  string
		pages map[string]map[string]any
	}{
		{"a key outside the prefix", map[string]map[string]any{"": objects("", []string{"a/1", "b/2"}, nil)}},
		{"an object below a child", map[string]map[string]any{"": objects("", []string{"a/b/c"}, nil)}},
		{"a repeated token", map[string]map[string]any{"": objects("t1", []string{"a/1"}, nil), "t1": objects("t1", []string{"a/2"}, nil)}},
	} {
		t.Run(c.name, func(t *testing.T) {
			src := open(t, scripted(t, c.pages), "bkt")
			req := listing.PageRequest{Prefix: []byte("a/"), Limit: 10}
			for range 2 {
				page, err := src.Page(context.Background(), req)
				if err != nil {
					if !errors.Is(err, listing.ErrInvalidPage) {
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
// on another prefix, or through another endpoint with a bucket of the
// same name, it is ErrInvalidToken, before any request.
func TestTokenBinding(t *testing.T) {
	keys := []string{"a/1", "a/2", "b/1", "b/2"}
	endpoint, sent := fake(t, keys...)
	other, otherSent := fake(t, keys...)
	src, elsewhere := open(t, endpoint, "bkt"), open(t, other, "bkt")
	ctx := context.Background()
	page, err := src.Page(ctx, listing.PageRequest{Prefix: []byte("a/"), Limit: 1})
	if err != nil || page.Next == "" {
		t.Fatalf("%+v, %v", page, err)
	}
	if _, err := src.Page(ctx, listing.PageRequest{Prefix: []byte("a/"), Limit: 1, Token: page.Next}); err != nil {
		t.Fatalf("the token on its own level: %v", err)
	}

	before := len(sent.list())
	if _, err := src.Page(ctx, listing.PageRequest{Prefix: []byte("b/"), Limit: 1, Token: page.Next}); !errors.Is(err, listing.ErrInvalidToken) {
		t.Errorf("another prefix: %v, want ErrInvalidToken", err)
	}
	if _, err := elsewhere.Page(ctx, listing.PageRequest{Prefix: []byte("a/"), Limit: 1, Token: page.Next}); !errors.Is(err, listing.ErrInvalidToken) {
		t.Errorf("another endpoint: %v, want ErrInvalidToken", err)
	}
	if n := len(sent.list()) - before; n != 0 || len(otherSent.list()) != 0 {
		t.Errorf("%d and %d requests after the refused tokens", n, len(otherSent.list()))
	}
}

// TestLookup: a present key is its entry; a missing key is absent only
// after buckets.get shows the bucket is there; in a missing bucket it is
// unknown (ErrNotFound), and a page of it is ErrNotFound. Every answer's
// evidence is live, over the call's own interval.
func TestLookup(t *testing.T) {
	keys := []string{"a/", "a/b"}
	f := gcstest.NewStrict(keys)
	f.Lean = true
	sent := &requests{}
	endpoint := serve(t, sent.record(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.EscapedPath(), gcstest.Path+"b/nobucket") {
			http.Error(w, `{"error":{"code":404,"message":"The specified bucket does not exist.","errors":[{"reason":"notFound"}]}}`, http.StatusNotFound)
			return
		}
		f.ServeHTTP(w, r)
	})))
	src := open(t, endpoint, "bkt")
	ctx := context.Background()

	before := time.Now()
	found, err := src.Lookup(ctx, []byte("a/"))
	if err != nil {
		t.Fatal(err)
	}
	if e := found.Entry; found.State != listing.LookupPresent || e == nil || string(e.Key) != "a/" || e.Kind != listing.KindObject ||
		e.Size == nil || *e.Size != 2 || e.LastModified == nil {
		t.Fatalf("present: %+v %+v", found, found.Entry)
	}
	ev := found.Evidence
	if ev.Started == nil || ev.Finished == nil || ev.Started.Before(before) ||
		ev.Finished.Before(*ev.Started) || ev.Unlocated == nil || *ev.Unlocated != 0 {
		t.Errorf("evidence %+v", ev)
	}

	n := len(sent.list())
	missing, err := src.Lookup(ctx, []byte("a"))
	if err != nil || missing.State != listing.LookupAbsent || missing.Entry != nil || missing.Reason != nil {
		t.Fatalf("missing: %+v, %v", missing, err)
	}
	if got, want := sent.list()[n:], []string{"GET /storage/v1/b/bkt/o/a", "GET /storage/v1/b/bkt"}; !slices.Equal(got, want) {
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

// TestLookupUnknown: a 403 to objects.get is unknown (ErrAuth) and probes
// nothing; a missing key whose bucket probe is refused is unknown
// (ErrAuth) too, never absent.
func TestLookupUnknown(t *testing.T) {
	const denied = `{"error":{"code":403,"message":"denied","errors":[{"reason":"forbidden"}]}}`
	for _, c := range []struct {
		name   string
		object int // objects.get's status; buckets.get's is 403
		want   []string
	}{
		{"a 403", http.StatusForbidden, []string{"GET /storage/v1/b/bkt/o/k"}},
		{"a failed probe", http.StatusNotFound, []string{"GET /storage/v1/b/bkt/o/k", "GET /storage/v1/b/bkt"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			sent := &requests{}
			endpoint := serve(t, sent.record(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.EscapedPath() == gcstest.Path+"b/bkt/o/k" && c.object == http.StatusNotFound {
					http.Error(w, `{"error":{"code":404,"message":"No such object: bkt/k","errors":[{"reason":"notFound"}]}}`, c.object)
					return
				}
				http.Error(w, denied, http.StatusForbidden)
			})))
			r, err := open(t, endpoint, "bkt").Lookup(context.Background(), []byte("k"))
			if err != nil || r.State != listing.LookupUnknown || !errors.Is(r.Reason, listing.ErrAuth) || r.Entry != nil {
				t.Errorf("%+v, %v; want unknown, ErrAuth", r, err)
			}
			if r.Evidence.Unlocated != nil {
				t.Errorf("evidence %+v", r.Evidence)
			}
			if got := sent.list(); !slices.Equal(got, c.want) {
				t.Errorf("requests %q, want %q", got, c.want)
			}
		})
	}
}

// TestLookupUnreasoned404: a 404 to objects.get without GCS's reason
// notFound (a proxy's "no route") is unknown (ErrUnsupported) and probes
// nothing, although the bucket would answer.
func TestLookupUnreasoned404(t *testing.T) {
	f := gcstest.NewStrict([]string{"a/b"})
	f.Lean = true
	sent := &requests{}
	endpoint := serve(t, sent.record(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.EscapedPath() == gcstest.Path+"b/bkt/o/k" {
			http.Error(w, `{"error":{"code":404,"message":"no route"}}`, http.StatusNotFound)
			return
		}
		f.ServeHTTP(w, r)
	})))

	r, err := open(t, endpoint, "bkt").Lookup(context.Background(), []byte("k"))
	if err != nil || r.State != listing.LookupUnknown || !errors.Is(r.Reason, listing.ErrUnsupported) || r.Entry != nil {
		t.Errorf("%+v, %v; want unknown, ErrUnsupported", r, err)
	}
	if got, want := sent.list(), []string{"GET /storage/v1/b/bkt/o/k"}; !slices.Equal(got, want) {
		t.Errorf("requests %q, want %q", got, want)
	}
}

// TestOpenRefusals: Open refuses a bucket name of the wrong form, a
// malformed endpoint and credentials that do not load, each with its
// class and before any request.
func TestOpenRefusals(t *testing.T) {
	ctx := context.Background()
	for _, c := range []struct {
		name   string
		bucket string
		opts   gcs.Options
		want   error
	}{
		{"a slash", "a/b", gcs.Options{Anonymous: true}, listing.ErrInvalidRequest},
		{"no bucket", "", gcs.Options{Anonymous: true}, listing.ErrInvalidRequest},
		{"an endpoint with a query", "bkt", gcs.Options{Anonymous: true, Endpoint: "https://example.com/storage/v1/?x=1"}, listing.ErrUnsupported},
		{"a credentials file not there", "bkt", gcs.Options{CredentialsFile: t.TempDir() + "/missing.json"}, listing.ErrAuth},
	} {
		if _, err := gcs.Open(ctx, c.bucket, c.opts); !errors.Is(err, c.want) {
			t.Errorf("%s: %v, want %v", c.name, err, c.want)
		}
	}
}

// TestRedaction: a message that quotes what the store answered or what
// the caller gave keeps no URL's credentials: a key outside the prefix
// that holds one, and a bucket argument that does.
func TestRedaction(t *testing.T) {
	const secret = "https://user:hunter2@example.com/x?token=hunter2"
	src := open(t, scripted(t, map[string]map[string]any{"": objects("", []string{secret}, nil)}), "bkt")
	_, err := src.Page(context.Background(), listing.PageRequest{Prefix: []byte("a/"), Limit: 10})
	if !errors.Is(err, listing.ErrInvalidPage) || strings.Contains(err.Error(), "hunter2") {
		t.Errorf("a page: %v, want ErrInvalidPage without the credentials", err)
	}

	_, err = gcs.Open(context.Background(), secret, gcs.Options{Anonymous: true})
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
// missing key's objects.get and buckets.get.
func TestObserved(t *testing.T) {
	var failed sync.Once
	f := gcstest.NewStrict([]string{"a/1"})
	f.Lean = true
	endpoint := serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		first := false
		failed.Do(func() { first = true })
		if first {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		f.ServeHTTP(w, r)
	}))
	src := open(t, endpoint, "bkt")
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
