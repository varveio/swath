// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"context"
	"flag"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/varveio/swath/go/internal/order"
	"github.com/varveio/swath/go/internal/registry"
	"github.com/varveio/swath/go/internal/source"
	"github.com/varveio/swath/go/internal/source/gcs/client"
	"github.com/varveio/swath/go/internal/source/keyrange"
)

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

func list(t *testing.T, opt Options, workers int) ([]string, Stats) {
	t.Helper()
	opt.Anonymous, opt.Workers = true, workers
	p, err := New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	c := &collect{}
	if _, err := order.Run[Range, Object](context.Background(), p, c, &Schema, order.Options{Workers: workers, Budget: 100 * 256}); err != nil {
		t.Fatal(err)
	}
	keys := make([]string, len(c.rows))
	for i := range c.rows {
		keys[i] = c.rows[i].Key
	}
	return keys, p.Stats()
}

// TestListFakeBucket: every configuration lists exactly the bucket, in
// order, on the shared engine (the inclusive startOffset translated, the
// exclusive endOffset bounding each range).
func TestListFakeBucket(t *testing.T) {
	keys := testKeys()
	_, ep := newFake(t, keys)
	for _, c := range []struct {
		name    string
		opt     Options
		workers int
	}{
		{"one worker", Options{}, 1},
		{"8 workers", Options{}, 8},
		{"64 workers, seed", Options{Policy: keyrange.Policy{Seed: true, SeedDescent: true}}, 64},
		{"64 workers, floors, structure", Options{Policy: keyrange.Policy{Floors: true, Structure: true}}, 64},
	} {
		opt := c.opt
		opt.Bucket, opt.Endpoint = "bkt", ep
		got, st := list(t, opt, c.workers)
		if !slices.Equal(got, keys) {
			t.Fatalf("%s: %d keys, want %d", c.name, len(got), len(keys))
		}
		if st.Counters.Value("requests") == 0 {
			t.Fatalf("%s: no requests counted", c.name)
		}
	}
	opt := Options{Bucket: "bkt", Prefix: "data/year=2021/", Endpoint: ep}
	got, _ := list(t, opt, 8)
	if len(got) != 500 {
		t.Fatalf("prefix: %d keys", len(got))
	}
}

// TestBoundsAtExactKeys: a request for (after, through] sends startOffset
// = after (inclusive: the engine drops a key equal to it) and endOffset =
// through + "\x00" (exclusive: through itself is listed); a resume after
// an existing key lists strictly past it.
func TestBoundsAtExactKeys(t *testing.T) {
	keys := testKeys()
	f, ep := newFake(t, keys)
	p, err := New(context.Background(), Options{Bucket: "bkt", Endpoint: ep, Anonymous: true})
	if err != nil {
		t.Fatal(err)
	}
	pg, err := p.fetch(context.Background(), &keyrange.Query{After: "a/x", Through: "b/c", Bounded: true, Max: 1000})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, r := range pg.Rows {
		got = append(got, r.Key)
	}
	i, j := slices.Index(keys, "a/x"), slices.Index(keys, "b/c")
	if !slices.Equal(got, keys[i:j+1]) {
		t.Fatalf("(a/x, b/c]: got %q", got)
	}
	q := f.last.Load().(url.Values)
	if q.Get("startOffset") != "a/x" || q.Get("endOffset") != "b/c\x00" {
		t.Fatalf("offsets sent: %q %q", q.Get("startOffset"), q.Get("endOffset"))
	}
	got, _ = list(t, Options{Bucket: "bkt", Endpoint: ep, Resume: &Resume{After: "a/x"}}, 8)
	if !slices.Equal(got, keys[i+1:]) {
		t.Fatalf("resume after a/x: %d keys, want %d", len(got), len(keys)-i-1)
	}
}

func TestParseURL(t *testing.T) {
	for in, want := range map[string][2]string{"gs://b": {"b", ""}, "gs://b/p/q": {"b", "p/q"}} {
		b, p, err := ParseURL(in)
		if err != nil || b != want[0] || p != want[1] {
			t.Fatalf("%s: %q %q %v", in, b, p, err)
		}
	}
	for _, in := range []string{"s3://b", "gs://", "gs:///p"} {
		if _, _, err := ParseURL(in); err == nil {
			t.Fatalf("%s accepted", in)
		}
	}
}

// TestTokenChainsAndEmptyPages: tokens bound to their chain's parameters
// (startOffset, endOffset, prefix, delimiter, maxResults) and empty
// continuing pages, both of which the JSON API allows: every configuration
// still lists exactly the bucket.
func TestTokenChainsAndEmptyPages(t *testing.T) {
	keys := testKeys()
	f, ep := newFake(t, keys)
	f.empty = 3
	for _, c := range []struct {
		opt     Options
		workers int
	}{
		{Options{Policy: keyrange.Policy{Sequential: true}}, 1},
		{Options{}, 8},
		{Options{Policy: keyrange.Policy{Seed: true, SeedDescent: true, Structure: true, Probe: true}}, 64},
		{Options{Resume: &Resume{After: keys[len(keys)/2]}}, 16},
	} {
		opt := c.opt
		opt.Bucket, opt.Endpoint = "bkt", ep
		got, st := list(t, opt, c.workers)
		want := keys
		if opt.Resume != nil {
			want = keys[slices.Index(keys, opt.Resume.After)+1:]
		}
		if !slices.Equal(got, want) {
			t.Fatalf("%+v: %d keys, want %d", c.opt, len(got), len(want))
		}
		if st.Counters.Value("empty_pages") == 0 {
			t.Fatalf("%+v: no empty page followed", c.opt)
		}
	}
}

// TestBoundPolicy: GCS accepts '%', controls and non-ASCII names as start
// bounds and omits an end bound past 1,024 bytes (the engine trims).
func TestBoundPolicy(t *testing.T) {
	for _, s := range []string{"a%20b", "a\x01", "café", strings.Repeat("k", 1024)} {
		if !AcceptBound(s) {
			t.Errorf("refused %q", s)
		}
	}
	for _, s := range []string{"a\nb", "a\rb", strings.Repeat("k", 1025)} {
		if AcceptBound(s) {
			t.Errorf("accepted %q", s)
		}
	}
	keys := []string{"p%1", "p%2", strings.Repeat("L", 1024), "q\x01", "q\x02", "ré"}
	for i := range 300 {
		keys = append(keys, fmt.Sprintf("p%%%03d", i), strings.Repeat("L", 1021)+fmt.Sprintf("%03d", i))
	}
	slices.Sort(keys)
	keys = slices.Compact(keys)
	_, ep := newFake(t, keys)
	got, st := list(t, Options{Bucket: "bkt", Endpoint: ep}, 16)
	if !slices.Equal(got, keys) {
		t.Fatalf("%d keys, want %d", len(got), len(keys))
	}
	t.Logf("end bounds omitted: %d, splits %d", st.Counters.Value("end_offset_omitted"), st.Counters.Value("split.accepted_bounded")+st.Counters.Value("split.accepted_frontier"))
}

// TestRouteIdentity: the identity freezes the bucket's route (base and
// bucket); a supplied client that does not name its route declines resume.
func TestRouteIdentity(t *testing.T) {
	route := func(ep string) string {
		src, err := open(context.Background(), registry.Request{Root: "gs://bkt/p", Options: &Options{Endpoint: ep, Anonymous: true}, Workers: 1})
		if err != nil {
			t.Fatal(err)
		}
		if !src.(source.Capable).Capabilities().Resume {
			t.Fatal("no resume")
		}
		for _, f := range src.(source.Identified).Identity().Fields {
			if f.Name == "route" {
				return f.Canonical
			}
		}
		return ""
	}
	if a, b, c := route("HTTP://Host:1/storage/v1"), route("http://host:1/storage/v1/"), route("http://host:1/other/v1/"); a != b || a == c || a != "http://host:1/storage/v1/b/bkt/o" {
		t.Fatalf("routes %q %q %q", a, b, c)
	}
	src, err := open(context.Background(), registry.Request{Root: "gs://bkt", Options: &Options{Client: unrouted{}}, Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	if src.(source.Capable).Capabilities().Resume {
		t.Fatal("a supplied client without a route declares resume")
	}
}

type unrouted struct{}

func (unrouted) List(context.Context, *client.ListInput) (*client.Page, error) {
	return &client.Page{}, nil
}

// TestEngineFlagsShared: GCS takes the range engine's flags from the
// shared group, so with another engine provider on the same flag set
// (here the engine's own Flags first) nothing panics and GCS opens with
// the parsed policy.
func TestEngineFlagsShared(t *testing.T) {
	f := flag.NewFlagSet("t", flag.ContinueOnError)
	other := keyrange.Flags(f, true)
	v := flags(f, true)
	if err := f.Parse([]string{"-seed", "-structure", "-gcs-anonymous", "-gcs-endpoint", "http://127.0.0.1:1/storage/v1/"}); err != nil {
		t.Fatal(err)
	}
	src, err := open(context.Background(), registry.Request{Root: "gs://bkt", Options: v, Workers: 2})
	if err != nil {
		t.Fatal(err)
	}
	p := src.(*opened).p
	if !other.Seed || !p.opt.Seed || !p.opt.Structure || !p.opt.SeedDescent || !p.opt.Anonymous {
		t.Fatalf("policy not shared: %+v", p.opt)
	}
}
