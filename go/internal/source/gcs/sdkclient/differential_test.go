// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package sdkclient

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/varveio/swath/go/internal/source/gcs/client"
	"github.com/varveio/swath/go/internal/source/gcs/gcstest"
)

// lister is either client.
type lister interface {
	List(ctx context.Context, in *client.ListInput) (*client.Page, error)
	Route(bucket string) string
	Counters() client.Counters
}

// pair builds the hand-rolled and the SDK client from one config.
func pair(t *testing.T, cfg client.Config) (hand, sdk lister) {
	t.Helper()
	h, err := client.New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	return h, s
}

// redirect sends requests addressed to Google's endpoint to a test server,
// so a client takes its production path (the fields selector included),
// recording each request's URL as the client built it.
type redirect struct {
	to   *url.URL
	mu   sync.Mutex
	urls []*url.URL
}

func (r *redirect) RoundTrip(req *http.Request) (*http.Response, error) {
	r.mu.Lock()
	r.urls = append(r.urls, req.URL)
	r.mu.Unlock()
	req = req.Clone(req.Context())
	req.URL.Scheme, req.URL.Host, req.Host = r.to.Scheme, r.to.Host, r.to.Host
	return http.DefaultTransport.RoundTrip(req)
}

func (r *redirect) last() *url.URL {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.urls[len(r.urls)-1]
}

// keys are the oracle's (gcs.TestClientPagesMatchGoogleClient), hostile
// ones included.
func keys() []string {
	k := []string{"a", "a-b", "a.b", "a/", "a/x", "a/y/z", "a0", "ab", "b/c", "é", "日本/x", "z~", "\x7f",
		"sp ace", "pl+us", "pct%41", "amp&<>", "q\"'", "a?b", "a#b", "ctl\x01", "ctl\x02x", "astral\U0001F600"}
	for i := range 2500 {
		k = append(k, fmt.Sprintf("data/year=%d/part-%05d.parquet", 2020+i%5, i))
	}
	return k
}

// shapes are the oracle's inputs, and generations listings.
var shapes = []client.ListInput{
	{MaxResults: 7},
	{},
	{Prefix: "data/year=2021/", MaxResults: 100},
	{StartOffset: "a/x", EndOffset: "data/year=2020/part-00100.parquet\x00", MaxResults: 50},
	{Delimiter: "/", MaxResults: 3},
	{Prefix: "a/", Delimiter: "/"},
	{StartOffset: "pct%41", EndOffset: "sp ace"},
	{StartOffset: "ctl\x01", MaxResults: 1},
	{Versions: true, MaxResults: 9},
	{Versions: true, StartOffset: "a\x00", Delimiter: "/", MaxResults: 4},
}

// TestPagesMatchHandRolled: on the strict fake, every page of every shape
// is the hand-rolled client's, deep-equal, at the fake's endpoint (full
// resources) and addressed to Google's endpoint (the production fields
// selector, which the fake applies, kind included).
func TestPagesMatchHandRolled(t *testing.T) {
	f := gcstest.NewStrict(keys())
	ep := f.Serve(t)
	u, _ := url.Parse(ep)
	for _, mode := range []string{"plain", "production fields"} {
		cfg := client.Config{Endpoint: ep, Anonymous: true}
		if mode == "production fields" {
			cfg = client.Config{Anonymous: true, HTTPClient: &http.Client{Transport: &redirect{to: u}}}
		}
		hand, sdk := pair(t, cfg)
		for _, start := range shapes {
			a, b := start, start
			a.Bucket, b.Bucket = "bkt", "bkt"
			for pages := 0; ; pages++ {
				pa, err := hand.List(context.Background(), &a)
				if err != nil {
					t.Fatal(err)
				}
				pb, err := sdk.List(context.Background(), &b)
				if err != nil {
					t.Fatalf("%s %+v page %d: %v", mode, start, pages, err)
				}
				if mode == "production fields" && !strings.HasPrefix(f.Last().Get("fields"), "kind,") {
					t.Fatalf("fields %q", f.Last().Get("fields"))
				}
				if !reflect.DeepEqual(pa, pb) {
					t.Fatalf("%s %+v page %d:\nhand-rolled %+v\nsdk         %+v", mode, start, pages, pa, pb)
				}
				if len(pa.Items) > 0 && pa.Items[0].Updated == 0 {
					t.Fatalf("%s: no update time", mode)
				}
				if pa.NextPageToken == "" {
					break
				}
				a.PageToken, b.PageToken = pa.NextPageToken, pb.NextPageToken
			}
		}
	}
}

// TestEmptyListingCarriesKind: an empty listing under the production
// fields selector still names its kind (the selector asks for it), so the
// SDK client reads it as an empty final page.
func TestEmptyListingCarriesKind(t *testing.T) {
	f := gcstest.NewStrict([]string{"a"})
	u, _ := url.Parse(f.Serve(t))
	_, sdk := pair(t, client.Config{Anonymous: true, HTTPClient: &http.Client{Transport: &redirect{to: u}}})
	for _, versions := range []bool{false, true} {
		p, err := sdk.List(context.Background(), &client.ListInput{Bucket: "bkt", Prefix: "none/", Versions: versions})
		if err != nil || len(p.Items) != 0 || p.NextPageToken != "" {
			t.Fatalf("versions %v: %+v, %v", versions, p, err)
		}
	}
}

// TestRequestsMatchHandRolled: for one input and config, the SDK client's
// first request goes to Route and carries the hand-rolled client's query
// parameters, plus the generated client's alt=json and prettyPrint=false.
func TestRequestsMatchHandRolled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"kind":"storage#objects"}`)
	}))
	defer srv.Close()
	u, _ := url.Parse(srv.URL)
	for _, project := range []string{"", "billed-p"} {
		for _, start := range append(shapes, client.ListInput{Prefix: "p", Delimiter: "/", StartOffset: "s+%", EndOffset: "e&=#?", PageToken: "tok/+=", MaxResults: 1000, Versions: true}) {
			start.Bucket = "bkt"
			var got []url.Values
			for _, build := range []func(context.Context, client.Config) (lister, error){
				func(ctx context.Context, cfg client.Config) (lister, error) { return client.New(ctx, cfg) },
				func(ctx context.Context, cfg client.Config) (lister, error) { return New(ctx, cfg) },
			} {
				rt := &redirect{to: u}
				c, err := build(context.Background(), client.Config{Anonymous: true, UserProject: project, HTTPClient: &http.Client{Transport: rt}})
				if err != nil {
					t.Fatal(err)
				}
				in := start
				if _, err := c.List(context.Background(), &in); err != nil {
					t.Fatal(err)
				}
				sent := rt.last()
				if route := c.Route("bkt"); !strings.HasPrefix(sent.String(), route+"?") {
					t.Fatalf("request %s, route %s", sent, route)
				}
				got = append(got, sent.Query())
			}
			hand, sdk := got[0], got[1]
			if sdk.Get("alt") != "json" || sdk.Get("prettyPrint") != "false" {
				t.Fatalf("sdk query %v", sdk)
			}
			sdk.Del("alt")
			sdk.Del("prettyPrint")
			if !reflect.DeepEqual(hand, sdk) {
				t.Fatalf("project %q %+v:\nhand-rolled %v\nsdk         %v", project, start, hand, sdk)
			}
		}
	}
}

// capture is a live versions=true page from the hand-rolled client's
// testdata, with the kind the production selector now asks for: the
// captures were taken with a selector that did not name it.
func capture(t *testing.T, name string, kind bool) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "client", "testdata", "versions", name))
	if err != nil {
		t.Fatal(err)
	}
	if kind {
		b = bytes.Replace(b, []byte("{"), []byte(`{"kind":"storage#objects",`), 1)
	}
	return b
}

// TestVersionsCapturesMatchHandRolled: the live generations pages read
// the same through both clients (timeDeleted, generations, hashes
// verbatim); without kind the SDK client refuses them.
func TestVersionsCapturesMatchHandRolled(t *testing.T) {
	var body []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write(body) }))
	defer srv.Close()
	hand, sdk := pair(t, client.Config{Endpoint: srv.URL + "/storage/v1/", Anonymous: true, MaxAttempts: 1})
	for _, name := range []string{"page20.json", "delim.json", "delim2.json", "single.json", "single2.json", "empty.json"} {
		body = capture(t, name, true)
		in := client.ListInput{Bucket: "b", Versions: true}
		a, err := hand.List(context.Background(), &in)
		if err != nil {
			t.Fatal(err)
		}
		b, err := sdk.List(context.Background(), &in)
		if err != nil || !reflect.DeepEqual(a, b) || len(b.Items) == 0 {
			t.Fatalf("%s: hand-rolled %+v\nsdk %+v, %v", name, a, b, err)
		}
		body = capture(t, name, false)
		if _, err := sdk.List(context.Background(), &in); client.Classify(err) != "parse" {
			t.Fatalf("%s without kind: %v", name, err)
		}
	}
	for _, in := range []string{
		`{"kind":"storage#objects","items":[{"name":"a","timeDeleted":"2026-02-19T01:01:22.061123+01:00"}]}`,
		`{"kind":"storage#objects","items":[{"name":"a","timeDeleted":"1969-12-31T23:59:59.999999Z","generation":"9"}]}`,
		`{"kind":"storage#objects","items":[{"name":"a","timeDeleted":"1970-01-01T00:00:00.0000019Z"}]}`,
	} {
		body = []byte(in)
		a, err := hand.List(context.Background(), &client.ListInput{Bucket: "b", Versions: true})
		if err != nil {
			t.Fatal(err)
		}
		b, err := sdk.List(context.Background(), &client.ListInput{Bucket: "b", Versions: true})
		if err != nil || !reflect.DeepEqual(a, b) || !slices.ContainsFunc(b.Items, func(o client.Object) bool { return o.TimeDeleted != 0 }) {
			t.Fatalf("%s: %+v, %+v, %v", in, a, b, err)
		}
	}
}

// TestRouteNotRepresentable: an endpoint path with an escaped separator or
// a dot segment is sent by the hand-rolled client as its Route names it;
// the generated client would send another path, so sdkclient.New refuses
// it rather than list another route than the resume identity freezes.
func TestRouteNotRepresentable(t *testing.T) {
	var mu sync.Mutex
	var paths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.EscapedPath())
		mu.Unlock()
		fmt.Fprint(w, `{"kind":"storage#objects"}`)
	}))
	defer srv.Close()
	for _, path := range []string{"/proxy%2Ftenant/storage/v1/", "/a/../storage/v1/", "/a/./storage/v1/"} {
		cfg := client.Config{Endpoint: srv.URL + path, Anonymous: true}
		hand, err := client.New(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := hand.List(context.Background(), &client.ListInput{Bucket: "bkt"}); err != nil {
			t.Fatalf("%s: hand-rolled: %v", path, err)
		}
		mu.Lock()
		got := paths[len(paths)-1]
		mu.Unlock()
		if want := strings.TrimPrefix(hand.Route("bkt"), srv.URL); got != want {
			t.Fatalf("%s: hand-rolled sent %s, route %s", path, got, want)
		}
		if _, err := New(context.Background(), cfg); err == nil || !strings.Contains(err.Error(), "not representable") {
			t.Fatalf("%s: sdkclient.New: %v", path, err)
		}
	}
	for _, ep := range []string{"", srv.URL + "/storage/v1/", srv.URL + "/custom%20base/v1"} {
		if _, err := New(context.Background(), client.Config{Endpoint: ep, Anonymous: true}); err != nil {
			t.Fatalf("%q: %v", ep, err)
		}
	}
}
