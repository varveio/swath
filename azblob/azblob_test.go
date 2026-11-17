// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblob_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath"
	"github.com/varveio/swath/azblob"
	"github.com/varveio/swath/internal/source/azblob/azblobtest"
	"github.com/varveio/swath/internal/source/azblob/client"
	"github.com/varveio/swath/objfile"
	"github.com/varveio/swath/reader/listing"
)

// The account and container every fake serves.
const (
	account   = "devstoreaccount1"
	container = "corpus"
)

// noCredentials hides the host's Azure credentials.
func noCredentials(t *testing.T) {
	t.Helper()
	t.Setenv("AZURE_STORAGE_SAS_TOKEN", "")
	t.Setenv("AZURE_STORAGE_KEY", "")
}

// open opens the container at endpoint, unsigned, until the test ends.
func open(t *testing.T, endpoint string) *swath.Source {
	t.Helper()
	src, err := azblob.Open(context.Background(), account+"/"+container, azblob.Options{Endpoint: endpoint, Anonymous: true})
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

// serve serves h until the test ends and returns its endpoint.
func serve(t *testing.T, h http.Handler) string {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv.URL
}

// fake serves s as Azure serves the container of a flat account and
// records its requests.
func fake(t *testing.T, s *azblobtest.Store) (endpoint string, sent *requests) {
	t.Helper()
	return fakeIn(t, "false", s)
}

// fakeIn serves s as fake does, in an account whose Get Account
// Information says x-ms-is-hns-enabled hns.
func fakeIn(t *testing.T, hns string, s *azblobtest.Store) (endpoint string, sent *requests) {
	t.Helper()
	sent = &requests{}
	return serve(t, sent.record(namespace(hns, new(atomic.Int32), azblobtest.Handler(s, account, container)))), sent
}

// namespace answers Get Account Information with x-ms-is-hns-enabled hns
// ("" for none), counting the calls in asked, and passes every other
// request to h.
func namespace(hns string, asked *atomic.Int32, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if q := r.URL.Query(); q.Get("restype") != "account" || q.Get("comp") != "properties" {
			h.ServeHTTP(w, r)
			return
		}
		asked.Add(1)
		if hns != "" {
			w.Header().Set("x-ms-is-hns-enabled", hns)
		}
	})
}

// level is the level of prefix in keys, in byte order: the object at
// exactly prefix, then each child.
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

// page reads the whole level of prefix at limit: Self and the children,
// each page checked.
func page(t *testing.T, src *swath.Source, prefix string, limit int) (self string, children []string, err error) {
	t.Helper()
	req := listing.PageRequest{Prefix: []byte(prefix), Limit: limit}
	for pages := 0; ; pages++ {
		if pages > 100 {
			t.Fatalf("%q, limit %d: the level does not end", prefix, limit)
		}
		p, err := src.Page(context.Background(), req)
		if err != nil {
			return self, children, err
		}
		if len(p.Entries) > limit || p.Order != listing.OrderBytes || p.Evidence.Stop != listing.StopCompleted {
			t.Fatalf("%q, limit %d: %d entries, order %q, stop %q", prefix, limit, len(p.Entries), p.Order, p.Evidence.Stop)
		}
		if p.Self != nil {
			if self != "" || p.Self.Kind != listing.KindObject {
				t.Fatalf("%q: Self %q (%v) after %q", prefix, p.Self.Key, p.Self.Kind, self)
			}
			self = string(p.Self.Key)
		}
		for _, e := range p.Entries {
			want := listing.KindObject
			if strings.HasSuffix(string(e.Key), "/") {
				want = listing.KindPrefix
			}
			if e.Kind != want {
				t.Errorf("%q: %q is %v, want %v", prefix, e.Key, e.Kind, want)
			}
			children = append(children, string(e.Key))
		}
		if p.Next == "" {
			return self, children, nil
		}
		req.Token = p.Next
	}
}

// TestPageOrderAtLimitOne: paged one or two entries at a time, every level
// comes back whole, once, in byte order (a blob "a.txt" before a prefix
// "a/"), with the object at exactly the prefix as Self and never as a
// child, and a prefix taken literally ("a" holds "a0" and "a/"). Under
// "e/" Azure's UTF-16 order lists the names at U+10000 and above before
// those in U+E000..U+FFFF: one answer holds the level, which is sorted.
// With answers of two entries, a level in byte order is paged through
// Azure's markers and new starts.
func TestPageOrderAtLimitOne(t *testing.T) {
	noCredentials(t)
	keys := []string{"a", "a.txt", "a/", "a/b", "a/b/c", "a/b/d", "a/c/x", "a/c/y", "a/d", "a0", "b/x", "b/y/z", "c",
		"e/z", "e/！", "e/！/x", "e/\U0001F600", "e/\U0001F600/y", "e/\U0001F601"}
	for _, short := range []int{0, 2} {
		s := azblobtest.NewStore(keys)
		if short > 0 {
			s.Short = func(int) int { return short }
		}
		endpoint, _ := fake(t, s)
		src := open(t, endpoint)
		for _, prefix := range []string{"", "a/", "a", "a/b/", "b/", "z", "e/"} {
			if short > 0 && prefix == "e/" {
				continue // TestUnorderedLevel
			}
			for _, limit := range []int{1, 2} {
				self, children, err := page(t, src, prefix, limit)
				if err != nil {
					t.Fatalf("short %d, %q, limit %d: %v", short, prefix, limit, err)
				}
				wantSelf, want := level(keys, prefix)
				if self != wantSelf || !slices.Equal(children, want) {
					t.Errorf("short %d, %q, limit %d: Self %q and %q, want %q and %q", short, prefix, limit, self, children, wantSelf, want)
				}
			}
		}
	}
}

// TestUnorderedLevel: a level whose answers Azure cuts short, so no one
// answer holds it, is paged in Azure's order; once a key comes out of byte
// order the page is ErrUnsupported, not retryable.
func TestUnorderedLevel(t *testing.T) {
	noCredentials(t)
	s := azblobtest.NewStore([]string{"e/！", "e/\U0001F600"})
	s.Short = func(int) int { return 1 }
	endpoint, _ := fake(t, s)
	_, children, err := page(t, open(t, endpoint), "e/", 10)
	if !errors.Is(err, listing.ErrUnsupported) || listing.Retryable(err) {
		t.Fatalf("%q, %v; want ErrUnsupported", children, err)
	}
}

// directory is a hierarchical namespace's directory's blob.
func directory(name string) client.Item {
	return client.Item{Name: name, BlobType: "BlockBlob", Props: client.Props{Metadata: `{"hdi_isfolder":"true"}`}}
}

// TestHierarchicalDirectories: in an account Get Account Information
// says is hierarchical, a directory's blob is the directory's prefix, once,
// beside the prefix Azure lists for its contents, and an empty directory
// is a prefix too; a lookup of it is present, as its prefix. A level that holds a directory's blob and is longer than one
// answer is ErrUnsupported, even where its answers ascend in byte order:
// a hierarchical namespace's order is not, which a new start in it would
// meet.
func TestHierarchicalDirectories(t *testing.T) {
	noCredentials(t)
	entries := []client.Item{directory("d"), {Name: "d-x", Size: 1}, {Name: "d.txt", Size: 1}, directory("d/e"), {Name: "d/e/f", Size: 1}, directory("g")}
	endpoint, _ := fakeIn(t, "true", azblobtest.NewEntryStore(entries))
	src := open(t, endpoint)
	for _, limit := range []int{1, 10} {
		_, children, err := page(t, src, "", limit)
		if want := []string{"d-x", "d.txt", "d/", "g/"}; err != nil || !slices.Equal(children, want) {
			t.Errorf("limit %d: %q, %v; want %q", limit, children, err, want)
		}
	}
	if _, children, err := page(t, src, "d/", 1); err != nil || !slices.Equal(children, []string{"d/e/"}) {
		t.Errorf("d/: %q, %v", children, err)
	}

	r, err := src.Lookup(context.Background(), []byte("d"))
	if err != nil || r.State != listing.LookupPresent || r.Entry == nil || string(r.Entry.Key) != "d/" || r.Entry.Kind != listing.KindPrefix {
		t.Errorf("lookup d: %+v, %v", r, err)
	}

	// An answer in a hierarchical namespace's order, "/" below every other
	// character, is sorted too.
	hns := &client.ListOutput{Items: []client.Item{directory("a"), {Name: "a/", Prefix: true}, directory("a-b"), {Name: "a-b/", Prefix: true},
		{Name: "a.txt", Size: 1}}}
	ordered := serve(t, namespace("true", new(atomic.Int32), scriptedHandler(t, map[string]*client.ListOutput{"": hns})))
	_, children, err := page(t, open(t, ordered), "", 1)
	if want := []string{"a-b/", "a.txt", "a/"}; err != nil || !slices.Equal(children, want) {
		t.Errorf("the namespace's order: %q, %v; want %q", children, err, want)
	}

	s := azblobtest.NewEntryStore([]client.Item{{Name: "a", Size: 1}, directory("d"), {Name: "d/f", Size: 1}, {Name: "z", Size: 1}})
	s.Short = func(int) int { return 2 }
	endpoint, _ = fakeIn(t, "true", s)
	if _, children, err := page(t, open(t, endpoint), "", 10); !errors.Is(err, listing.ErrUnsupported) {
		t.Errorf("short answers: %q, %v; want ErrUnsupported", children, err)
	}
}

// TestFlatDirectoryMetadata: in an account Get Account Information says is
// flat, a blob whose user metadata says hdi_isfolder=true is an object,
// with its size, beside the prefix of the blobs under it, Self of its own
// name, and present as itself. The namespace is asked once, and the
// request reaches the observer. In an account whose namespace cannot be
// read, such a blob makes a page ErrUnsupported and a lookup unknown; a
// level without one is paged without asking.
func TestFlatDirectoryMetadata(t *testing.T) {
	noCredentials(t)
	marked := directory("report")
	marked.Size = 7
	store := func() *azblobtest.Store {
		return azblobtest.NewEntryStore([]client.Item{marked, {Name: "report/x", Size: 1}, {Name: "z", Size: 1}})
	}

	var asked atomic.Int32
	src := open(t, serve(t, namespace("false", &asked, azblobtest.Handler(store(), account, container))))
	obs := &observed{}
	ctx := objfile.WithObserver(context.Background(), obs)
	p, err := src.Page(ctx, listing.PageRequest{Limit: 10})
	if err != nil || len(p.Entries) != 3 || string(p.Entries[0].Key) != "report" || p.Entries[0].Kind != listing.KindObject ||
		p.Entries[0].Size == nil || *p.Entries[0].Size != 7 || string(p.Entries[1].Key) != "report/" {
		t.Fatalf("%+v, %v", p, err)
	}
	p, err = src.Page(ctx, listing.PageRequest{Prefix: []byte("report"), Limit: 10})
	if err != nil || p.Self == nil || string(p.Self.Key) != "report" || len(p.Entries) != 1 || string(p.Entries[0].Key) != "report/" {
		t.Fatalf("report: %+v, %v", p, err)
	}
	r, err := src.Lookup(ctx, []byte("report"))
	if err != nil || r.State != listing.LookupPresent || r.Entry == nil || string(r.Entry.Key) != "report" ||
		r.Entry.Kind != listing.KindObject || r.Entry.Size == nil || *r.Entry.Size != 7 {
		t.Fatalf("lookup: %+v, %v", r, err)
	}
	if n := asked.Load(); n != 1 {
		t.Errorf("the namespace asked %d times, want 1", n)
	}
	if !slices.Contains(obs.seen, `metadata "" retry=false failed=false`) {
		t.Errorf("observed %q, want the namespace's request", obs.seen)
	}

	src = open(t, serve(t, azblobtest.Handler(store(), account, container)))
	if _, err := src.Page(context.Background(), listing.PageRequest{Limit: 10}); !errors.Is(err, listing.ErrUnsupported) || listing.Retryable(err) {
		t.Errorf("an unknown namespace's page: %v, want ErrUnsupported", err)
	}
	r, err = src.Lookup(context.Background(), []byte("report"))
	if err != nil || r.State != listing.LookupUnknown || !errors.Is(r.Reason, listing.ErrUnsupported) {
		t.Errorf("an unknown namespace's lookup: %+v, %v", r, err)
	}
	if _, children, err := page(t, src, "report/", 10); err != nil || !slices.Equal(children, []string{"report/x"}) {
		t.Errorf("a level without a marked blob: %q, %v", children, err)
	}
}

// scripted serves scriptedHandler until the test ends and returns its
// endpoint.
func scripted(t *testing.T, pages map[string]*client.ListOutput) string {
	t.Helper()
	return serve(t, scriptedHandler(t, pages))
}

// scriptedHandler answers List Blobs with pages, by the marker the request
// sends ("" for none).
func scriptedHandler(t *testing.T, pages map[string]*client.ListOutput) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		out, ok := pages[r.URL.Query().Get("marker")]
		if !ok {
			t.Errorf("unexpected request %s", r.URL)
			http.Error(w, "unexpected", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprint(w, azblobtest.ListXML(out))
	})
}

// blobs is an answer of blobs names, continuing at next.
func blobs(next string, names ...string) *client.ListOutput {
	out := &client.ListOutput{NextMarker: next}
	for _, n := range names {
		out.Items = append(out.Items, client.Item{Name: n, Size: 1, BlobType: "BlockBlob"})
	}
	return out
}

// TestInvalidPages: an answer that breaks the contract is ErrInvalidPage,
// not retried: a key outside the prefix, a marker that repeats the one
// sent, a key repeated in a level one answer holds, in a longer level's
// answer and across its answers.
func TestInvalidPages(t *testing.T) {
	noCredentials(t)
	for _, c := range []struct {
		name  string
		pages map[string]*client.ListOutput
	}{
		{"a key outside the prefix", map[string]*client.ListOutput{"": blobs("", "a/1", "b/2")}},
		{"a repeated marker", map[string]*client.ListOutput{"": blobs("m1", "a/1"), "m1": blobs("m1", "a/2")}},
		{"a key below a child", map[string]*client.ListOutput{"": blobs("", "a/b/c")}},
		{"a key repeated in one answer", map[string]*client.ListOutput{"": blobs("", "a/1", "a/1")}},
		{"a key repeated in a chain's answer", map[string]*client.ListOutput{"": blobs("m1", "a/1", "a/1"), "m1": blobs("", "a/2")}},
		{"a key repeated across answers", map[string]*client.ListOutput{"": blobs("m1", "a/1"), "m1": blobs("", "a/1")}},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, children, err := page(t, open(t, scripted(t, c.pages)), "a/", 10)
			if !errors.Is(err, listing.ErrInvalidPage) || listing.Retryable(err) {
				t.Fatalf("%q, %v; want ErrInvalidPage", children, err)
			}
		})
	}
}

// TestTokenBinding: a token works only for the level it was issued for:
// on another prefix, through another endpoint with a container of the
// same name, or with child totals asked for, it is ErrInvalidToken, before
// any request; so is a token that is not one.
func TestTokenBinding(t *testing.T) {
	noCredentials(t)
	keys := []string{"a/1", "a/2", "b/1", "b/2"}
	endpoint, sent := fake(t, azblobtest.NewStore(keys))
	other, otherSent := fake(t, azblobtest.NewStore(keys))
	src, elsewhere := open(t, endpoint), open(t, other)
	ctx := context.Background()
	p, err := src.Page(ctx, listing.PageRequest{Prefix: []byte("a/"), Limit: 1})
	if err != nil || p.Next == "" {
		t.Fatalf("%+v, %v", p, err)
	}
	if _, err := src.Page(ctx, listing.PageRequest{Prefix: []byte("a/"), Limit: 1, Token: p.Next}); err != nil {
		t.Fatalf("the token on its own level: %v", err)
	}

	before := len(sent.list())
	for _, c := range []struct {
		name string
		src  *swath.Source
		req  listing.PageRequest
	}{
		{"another prefix", src, listing.PageRequest{Prefix: []byte("b/"), Limit: 1, Token: p.Next}},
		{"another endpoint", elsewhere, listing.PageRequest{Prefix: []byte("a/"), Limit: 1, Token: p.Next}},
		{"child totals", src, listing.PageRequest{Prefix: []byte("a/"), Limit: 1, Token: p.Next, ChildTotals: &listing.SummaryOptions{}}},
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

// TestLookup: a present key is its entry, as Get Blob Properties answers
// it; a missing key is absent only after a listing shows the container is
// there; in a missing container it is unknown (ErrNotFound), and a page of
// it is ErrNotFound. Every answer's evidence is live, over the call's own
// interval.
func TestLookup(t *testing.T) {
	noCredentials(t)
	modified := time.Date(2026, 11, 17, 10, 0, 0, 0, time.UTC)
	endpoint, sent := fake(t, azblobtest.NewEntryStore([]client.Item{
		{Name: "a/", Size: 2, BlobType: "BlockBlob", AccessTier: "Cool", LastModified: modified.UnixMicro()},
		{Name: "a/b", Size: 1, BlobType: "BlockBlob"},
	}))
	src := open(t, endpoint)
	ctx := context.Background()

	before := time.Now()
	found, err := src.Lookup(ctx, []byte("a/"))
	if err != nil {
		t.Fatal(err)
	}
	if e := found.Entry; found.State != listing.LookupPresent || e == nil || string(e.Key) != "a/" || e.Kind != listing.KindObject ||
		e.Size == nil || *e.Size != 2 || e.StorageClass == nil || *e.StorageClass != "Cool" || e.LastModified == nil || !e.LastModified.Equal(modified) {
		t.Fatalf("present: %+v %+v", found, found.Entry)
	}
	ev := found.Evidence
	if ev.Origin != listing.OriginLive || ev.Started == nil || ev.Finished == nil || ev.Started.Before(before) ||
		ev.Finished.Before(*ev.Started) || ev.Unlocated == nil || *ev.Unlocated != 0 || ev.Stop != listing.StopCompleted {
		t.Errorf("evidence %+v", ev)
	}

	n := len(sent.list())
	missing, err := src.Lookup(ctx, []byte("a"))
	if err != nil || missing.State != listing.LookupAbsent || missing.Entry != nil || missing.Reason != nil {
		t.Fatalf("missing: %+v, %v", missing, err)
	}
	if got, want := sent.list()[n:], []string{"HEAD /" + account + "/" + container + "/a", "GET /" + account + "/" + container}; !slices.Equal(got, want) {
		t.Errorf("a missing key's requests %q, want %q", got, want)
	}

	nocontainer, err := azblob.Open(ctx, account+"/nocontainer", azblob.Options{Endpoint: endpoint, Anonymous: true})
	if err != nil {
		t.Fatal(err)
	}
	defer nocontainer.Close()
	r, err := nocontainer.Lookup(ctx, []byte("a/b"))
	if err != nil || r.State != listing.LookupUnknown || !errors.Is(r.Reason, listing.ErrNotFound) {
		t.Errorf("a missing container's key: %+v, %v", r, err)
	}
	if _, err := nocontainer.Page(ctx, listing.PageRequest{Limit: 10}); !errors.Is(err, listing.ErrNotFound) {
		t.Errorf("a missing container's page: %v", err)
	}
}

// TestLookupUnknown: a failure of Get Blob Properties other than the
// blob's own 404 is unknown with its class and probes nothing: a 403, a
// missing container, an authentication code that comes with a 404. A
// missing blob whose container probe fails is unknown with the probe's
// class, an authentication code before a 404 there too.
func TestLookupUnknown(t *testing.T) {
	noCredentials(t)
	blob := "/" + account + "/" + container + "/k"
	head := []string{"HEAD " + blob}
	probed := []string{"HEAD " + blob, "GET /" + account + "/" + container}
	for _, c := range []struct {
		name  string
		head  answer // Get Blob Properties'
		probe answer // the container probe's; zero is a listing of nothing
		want  error
		sent  []string
	}{
		{"a 403", answer{http.StatusForbidden, "AuthorizationPermissionMismatch"}, answer{}, listing.ErrAuth, head},
		{"a missing container", answer{http.StatusNotFound, "ContainerNotFound"}, answer{}, listing.ErrNotFound, head},
		{"an authentication failure as a 404", answer{http.StatusNotFound, "AuthenticationFailed"}, answer{}, listing.ErrAuth, head},
		{"a refused probe", answer{http.StatusNotFound, "BlobNotFound"}, answer{http.StatusForbidden, "AuthorizationPermissionMismatch"}, listing.ErrAuth, probed},
		{"a probe of a missing container", answer{http.StatusNotFound, "BlobNotFound"}, answer{http.StatusNotFound, "ContainerNotFound"}, listing.ErrNotFound, probed},
		{"a probe's authentication failure as a 404", answer{http.StatusNotFound, "BlobNotFound"}, answer{http.StatusNotFound, "AuthenticationFailed"}, listing.ErrAuth, probed},
	} {
		t.Run(c.name, func(t *testing.T) {
			sent := &requests{}
			endpoint := serve(t, sent.record(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				a := c.probe
				if r.URL.Path == blob {
					a = c.head
				}
				if a.status == 0 {
					w.Header().Set("Content-Type", "application/xml")
					fmt.Fprint(w, azblobtest.ListXML(&client.ListOutput{}))
					return
				}
				w.Header().Set("x-ms-error-code", a.code)
				w.WriteHeader(a.status)
			})))
			r, err := open(t, endpoint).Lookup(context.Background(), []byte("k"))
			if err != nil || r.State != listing.LookupUnknown || !errors.Is(r.Reason, c.want) || r.Entry != nil {
				t.Errorf("%+v, %v; want unknown, %v", r, err, c.want)
			}
			if c.want != listing.ErrNotFound && errors.Is(r.Reason, listing.ErrNotFound) {
				t.Errorf("%v, also ErrNotFound", r.Reason)
			}
			if got := sent.list(); !slices.Equal(got, c.sent) {
				t.Errorf("requests %q, want %q", got, c.sent)
			}
		})
	}
}

// answer is a fake's error answer: its status and x-ms-error-code.
type answer struct {
	status int
	code   string
}

// TestInvalidRequests are refused before any request.
func TestInvalidRequests(t *testing.T) {
	noCredentials(t)
	endpoint, sent := fake(t, azblobtest.NewStore([]string{"a"}))
	src := open(t, endpoint)
	ctx := context.Background()
	for name, req := range map[string]listing.PageRequest{
		"no limit":  {},
		"a NUL":     {Prefix: []byte("a\x00"), Limit: 1},
		"not UTF-8": {Prefix: []byte{0xff}, Limit: 1},
	} {
		if _, err := src.Page(ctx, req); !errors.Is(err, listing.ErrInvalidRequest) {
			t.Errorf("page, %s: %v", name, err)
		}
	}
	for _, key := range [][]byte{nil, []byte("a\x00"), {0xff}} {
		if _, err := src.Lookup(ctx, key); !errors.Is(err, listing.ErrInvalidRequest) {
			t.Errorf("lookup %q: %v", key, err)
		}
	}
	for _, location := range []string{"", account, account + "/", "Account/c", "ab/c", account + "/c/d"} {
		if _, err := azblob.Open(ctx, location, azblob.Options{Anonymous: true}); !errors.Is(err, listing.ErrInvalidRequest) {
			t.Errorf("open %q: %v", location, err)
		}
	}
	if _, err := azblob.Open(ctx, account+"/c", azblob.Options{Anonymous: true, MaxRequests: -1}); !errors.Is(err, listing.ErrInvalidRequest) {
		t.Errorf("open with a negative bound: %v", err)
	}
	if _, err := azblob.Open(ctx, account+"/c", azblob.Options{Anonymous: true, Endpoint: "ftp://x"}); !errors.Is(err, listing.ErrUnsupported) {
		t.Errorf("open with an ftp endpoint: %v", err)
	}
	if got := sent.list(); len(got) != 0 {
		t.Errorf("requests %q", got)
	}
}

// TestRedaction: a message that quotes what the store answered or what
// the caller gave keeps no URL's credentials: a key outside the prefix
// that holds one, and a location argument or an endpoint that does.
func TestRedaction(t *testing.T) {
	noCredentials(t)
	const secret = "https://user:hunter2@example.com/x?token=hunter2"
	src := open(t, scripted(t, map[string]*client.ListOutput{"": blobs("", secret)}))
	_, err := src.Page(context.Background(), listing.PageRequest{Prefix: []byte("a/"), Limit: 10})
	if !errors.Is(err, listing.ErrInvalidPage) || strings.Contains(err.Error(), "hunter2") {
		t.Errorf("a page: %v, want ErrInvalidPage without the credentials", err)
	}

	_, err = azblob.Open(context.Background(), secret, azblob.Options{Anonymous: true})
	if !errors.Is(err, listing.ErrInvalidRequest) || strings.Contains(err.Error(), "hunter2") {
		t.Errorf("open: %v, want ErrInvalidRequest without the credentials", err)
	}
	_, err = azblob.Open(context.Background(), account+"/c", azblob.Options{Anonymous: true, Endpoint: secret})
	if !errors.Is(err, listing.ErrUnsupported) || strings.Contains(err.Error(), "hunter2") {
		t.Errorf("open an endpoint: %v, want ErrUnsupported without the credentials", err)
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
// missing key's Get Blob Properties and the container's probe.
func TestObserved(t *testing.T) {
	noCredentials(t)
	var failed sync.Once
	backend := azblobtest.Handler(azblobtest.NewStore([]string{"a/1"}), account, container)
	endpoint := serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		first := false
		failed.Do(func() { first = true })
		if first {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		backend.ServeHTTP(w, r)
	}))
	src := open(t, endpoint)
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
		`list "a/2" retry=false failed=false`,
	}
	if !slices.Equal(obs.seen, want) {
		t.Errorf("observed %q, want %q", obs.seen, want)
	}
}
