// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"context"
	"flag"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/varveio/swath/go/internal/order"
	"github.com/varveio/swath/go/internal/registry"
	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source"
	"github.com/varveio/swath/go/internal/source/gcs/client"
	"github.com/varveio/swath/go/internal/source/gcs/gcstest"
	"github.com/varveio/swath/go/internal/source/keyrange"
	"github.com/varveio/swath/go/internal/source/objhttp"
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
		{"64 workers, seed", Options{Policy: keyrange.Policy{Seed: true}}, 64},
		{"64 workers, floors, structure", Options{Policy: keyrange.Policy{Floors: true, Structure: keyrange.StructureOn}}, 64},
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
	q := f.Last()
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

// TestTokenChainsAndEmptyPages: opaque tokens bound to their chain's
// parameters (startOffset, endOffset, prefix, delimiter, maxResults), empty
// continuing pages and nonempty pages cut short at varying lengths, all of
// which the JSON API allows: every configuration still lists exactly the
// bucket, control-character names included.
func TestTokenChainsAndEmptyPages(t *testing.T) {
	keys := append(testKeys(), "ctl\x01", "ctl\x01\x01", "ctl\x02", "ctl\x1f/x")
	slices.Sort(keys)
	f, ep := newFake(t, keys)
	f.Empty, f.Short = 3, true
	for _, c := range []struct {
		opt     Options
		workers int
	}{
		{Options{Policy: keyrange.Policy{Sequential: true}}, 1},
		{Options{}, 8},
		{Options{Policy: keyrange.Policy{Seed: true, Structure: keyrange.StructureOn}}, 64},
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
	if st.Counters.Value("requests") == 0 {
		t.Fatal("no requests")
	}
}

// TestEndOffsetOmitted: a bounded range whose end is a 1,024-byte name
// cannot send endOffset = end+NUL (1,025 bytes): the request goes without
// endOffset, end_offset_omitted counts it, the service lists past the end
// and the engine trims exactly there. First one fetch through the adapter,
// then whole listings whose split pivots fall among 1,024-byte names (a
// 1,023-byte stem and one more byte), with short pages so that ranges split.
func TestEndOffsetOmitted(t *testing.T) {
	stem := strings.Repeat("L", 1023)
	keys := []string{"A", "K", "M", "Z"}
	for c := byte('!'); c <= '~'; c++ {
		keys = append(keys, stem+string(c))
	}
	slices.Sort(keys)
	f, ep := newFake(t, keys)
	p, err := New(context.Background(), Options{Bucket: "bkt", Endpoint: ep, Anonymous: true})
	if err != nil {
		t.Fatal(err)
	}
	through := stem + "m"
	pg, err := p.fetch(context.Background(), &keyrange.Query{After: stem + "a", Through: through, Bounded: true, Max: 1000})
	if err != nil {
		t.Fatal(err)
	}
	q := f.Last()
	if _, sent := q["endOffset"]; sent || q.Get("startOffset") != stem+"a" {
		t.Fatalf("endOffset sent (%d bytes) or startOffset wrong", len(q.Get("endOffset")))
	}
	if n := p.Set().Load(p.ID(cEndOmitted)); n != 1 {
		t.Fatalf("end_offset_omitted %d, want 1", n)
	}
	if last := pg.Rows[len(pg.Rows)-1].Key; last <= through {
		t.Fatalf("the page ends at %q: nothing past the end to trim", last)
	}

	f.Cap = 3 // short pages: every range continues, and idle workers split it
	for _, workers := range []int{4, 16, 64} {
		before := len(f.Sent())
		got, st := list(t, Options{Bucket: "bkt", Endpoint: ep}, workers)
		if !slices.Equal(got, keys) {
			t.Fatalf("%d workers: %d keys, want %d", workers, len(got), len(keys))
		}
		omitted := st.Counters.Value("end_offset_omitted")
		if omitted == 0 || st.Counters.Value("overfetch_keys") == 0 {
			t.Fatalf("%d workers: end_offset_omitted %d, overfetch_keys %d: the omission path did not engage (splits %d)", workers,
				omitted, st.Counters.Value("overfetch_keys"), st.Counters.Value("split.accepted_bounded")+st.Counters.Value("split.accepted_frontier"))
		}
		var without int64
		for _, q := range f.Sent()[before:] {
			e, sent := q["endOffset"]
			if sent && len(e[0]) > 1024 {
				t.Fatalf("an endOffset of %d bytes was sent", len(e[0]))
			}
			if !sent && strings.HasPrefix(q.Get("startOffset"), stem) && q.Get("delimiter") == "" {
				without++
			}
		}
		if without == 0 {
			t.Fatalf("%d workers: no request among the long names went without endOffset", workers)
		}
	}
}

// TestDelimiterBoundaries: a folder marker ("a/"), its descendants, the
// objects adjacent to it in byte order ("a-b", "a.b" below, "a0" above) and
// a nested marker list exactly at page size one, through every policy that
// sends delimiter requests (the seed, structure probes); and a delimiter request whose inclusive startOffset is the
// marker reports the folder as a prefix, never as an object.
func TestDelimiterBoundaries(t *testing.T) {
	keys := []string{"a", "a-b", "a.b", "a/", "a/b/", "a/b/c", "a/x", "a/y/z", "a0", "a0/", "a0/q", "b"}
	f, ep := newFake(t, keys)
	f.Cap = 1
	for _, c := range []struct {
		name    string
		pol     keyrange.Policy
		workers int
	}{
		{"sequential", keyrange.Policy{Sequential: true}, 1},
		{"demand splits", keyrange.Policy{}, 8},
		{"seed, structure", keyrange.Policy{Seed: true, Structure: keyrange.StructureOn}, 8},
		{"one-page seed, structure", keyrange.Policy{Seed: true, Structure: keyrange.StructureOn}, 64},
	} {
		before := len(f.Sent())
		got, _ := list(t, Options{Bucket: "bkt", Endpoint: ep, Policy: c.pol}, c.workers)
		if !slices.Equal(got, keys) {
			t.Fatalf("%s: %q, want %q", c.name, got, keys)
		}
		delimited := 0
		for _, q := range f.Sent()[before:] {
			if q.Get("delimiter") != "" {
				delimited++
			}
		}
		if (c.pol.Seed || c.pol.Structure != keyrange.StructureOff) != (delimited > 0) {
			t.Fatalf("%s: %d delimiter requests", c.name, delimited)
		}
	}
	for _, r := range []struct {
		after, prefix string
		want          []string // prefixes, then objects, of the first entry
	}{
		{"a/", "", []string{"a/"}},
		{"a.b", "", []string{"", "a.b"}},
		{"a/x", "a/", []string{"", "a/x"}},
		{"a/b/", "a/", []string{"a/b/"}},
	} {
		in := &client.ListInput{Bucket: "bkt", Delimiter: "/", StartOffset: r.after, Prefix: r.prefix, MaxResults: 1}
		c, err := client.New(context.Background(), client.Config{Endpoint: ep, Anonymous: true})
		if err != nil {
			t.Fatal(err)
		}
		pg, err := c.List(context.Background(), in)
		if err != nil {
			t.Fatal(err)
		}
		var first []string
		if len(pg.Prefixes) > 0 {
			first = []string{pg.Prefixes[0]}
		} else if len(pg.Items) > 0 {
			first = []string{"", pg.Items[0].Name}
		}
		if !slices.Equal(first, r.want) || pg.NextPageToken == "" {
			t.Fatalf("startOffset %q prefix %q: first entry %q (token %q), want %q", r.after, r.prefix, first, pg.NextPageToken, r.want)
		}
	}
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

// routeOnly names its route but not its billed project.
type routeOnly struct{ unrouted }

func (routeOnly) Route(bucket string) string { return "http://host/storage/v1/b/" + bucket + "/o" }

// billed names both.
type billed struct {
	routeOnly
	project string
}

func (b billed) UserProject() string { return b.project }

// TestSuppliedClientBilling: a supplied client's identity is the billed
// project it sends, not the options' (a requester-pays resume must bill
// the same project); one that names its route but not its project
// declines resume.
func TestSuppliedClientBilling(t *testing.T) {
	ident := func(c API, project string) (source.Identity, bool) {
		src, err := open(context.Background(), registry.Request{Root: "gs://bkt", Options: &Options{Client: c, UserProject: project}, Workers: 1})
		if err != nil {
			t.Fatal(err)
		}
		return src.(source.Identified).Identity(), src.(source.Capable).Capabilities().Resume
	}
	field := func(id source.Identity, name string) string {
		for _, f := range id.Fields {
			if f.Name == name {
				return f.Value
			}
		}
		t.Fatalf("no %s field", name)
		return ""
	}
	a, ok := ident(billed{project: "p1"}, "options-project")
	if !ok || field(a, "user project") != "p1" {
		t.Fatalf("resume %v, user project %q: want the client's p1", ok, field(a, "user project"))
	}
	b, _ := ident(billed{project: "p2"}, "options-project")
	if field(a, "user project") == field(b, "user project") {
		t.Fatal("a changed billed project leaves the identity equal")
	}
	if _, ok := ident(routeOnly{}, "p1"); ok {
		t.Fatal("a supplied client that does not name its billed project declares resume")
	}
	// The built-in client names the options' project.
	src, err := open(context.Background(), registry.Request{Root: "gs://bkt", Options: &Options{Anonymous: true, UserProject: "p3",
		Endpoint: "http://127.0.0.1:1/storage/v1/"}, Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	if v := field(src.(source.Identified).Identity(), "user project"); v != "p3" {
		t.Fatalf("built-in client: user project %q", v)
	}
}

// TestEngineFlagsShared: GCS takes the range engine's flag group the
// command registered once and passed to it, and opens with the parsed
// policy.
func TestEngineFlagsShared(t *testing.T) {
	f := flag.NewFlagSet("t", flag.ContinueOnError)
	other := keyrange.Flags(f, true)
	v := flags(f, true, other)
	if err := f.Parse([]string{"-seed", "-structure", "-gcs-anonymous", "-gcs-endpoint", "http://127.0.0.1:1/storage/v1/"}); err != nil {
		t.Fatal(err)
	}
	src, err := open(context.Background(), registry.Request{Root: "gs://bkt", Options: v, Workers: 2})
	if err != nil {
		t.Fatal(err)
	}
	p := src.(*opened[Object]).p
	if !other.Policy.Seed || !p.opt.Seed || p.opt.Structure != keyrange.StructureOn || !p.opt.Anonymous {
		t.Fatalf("policy not shared: %+v", p.opt)
	}
}

// TestBrokenADCRefused: malformed configured credentials refuse the
// provider with the auth class (the run outcome's cause), not anonymity.
func TestBrokenADCRefused(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	f := dir + "/adc.json"
	if err := os.WriteFile(f, []byte(`{"type":"authorized_user",`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", f)
	_, err := New(context.Background(), Options{Bucket: "b"})
	if err == nil || classify(err) != "auth" {
		t.Fatalf("err %v class %q, want auth", err, classify(err))
	}
}

// pages is a supplied client that answers a fixed page.
type pages struct{ page client.Page }

func (p *pages) List(context.Context, *client.ListInput) (*client.Page, error) {
	out := p.page
	return &out, nil
}

// TestInvalidPageClass: a page that parses but breaks the contract (out of
// order) is an invalid page, not a parse failure; the run stops with that
// cause.
func TestInvalidPageClass(t *testing.T) {
	p, err := New(context.Background(), Options{Bucket: "b", Workers: 1, Policy: keyrange.Policy{Sequential: true},
		Client: &pages{client.Page{Items: []client.Object{{Name: "b"}, {Name: "a"}}}}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = order.Run[Range, Object](context.Background(), p, &collect{}, &Schema, order.Options{Workers: 1, Budget: 1 << 20})
	if classify(err) != "invalid_page" || client.Classify(err) == objhttp.ClassParse {
		t.Fatalf("err %v class %q, want invalid_page", err, classify(err))
	}
}

// newFake serves the strict objects.list fake (gcstest.Strict) over keys.
// TestCodecRoundTripsEveryField: the spill encoding keeps every field of
// an Object, so a spilled row is written as the row listed; a zero
// timeCreated (not listed) stays zero, its column null.
func TestCodecRoundTripsEveryField(t *testing.T) {
	for _, o := range []Object{
		{Size: 1, Mtime: 2, TimeCreated: 3, ETag: "e", Generation: 4, Metageneration: 5, StorageClass: "s", MD5: "m", CRC32C: "c", ContentType: "t"},
		{Size: 7, Mtime: -8, ETag: "e2", Generation: 9},
	} {
		d := schema.NewDecoder(Schema.Codec.Append(nil, &o))
		var got Object
		Schema.Codec.Decode(d, &got)
		if err := d.Err(); err != nil || d.Len() != 0 || got != o {
			t.Fatalf("round trip of %+v: %+v, %v", o, got, err)
		}
	}
	last := Schema.Columns[len(Schema.Columns)-1]
	if last.Name != "time_created" || last.Type != schema.TimestampMicros || last.Int == nil {
		t.Fatalf("last column %+v", last.Spec)
	}
	if _, ok := last.Int(&Object{}); ok {
		t.Fatal("an unlisted timeCreated is not null")
	}
}

func newFake(t testing.TB, keys []string) (*gcstest.Strict, string) {
	f := gcstest.NewStrict(keys)
	return f, f.Serve(t)
}

// TestPoolSizeReported: the report's transport.pool_size is the built
// client's pool, one transport per 16 workers, and the listing is exact
// over it.
func TestPoolSizeReported(t *testing.T) {
	keys := testKeys()
	_, ep := newFake(t, keys)
	for workers, want := range map[int]int64{1: 1, 40: 3} {
		got, st := list(t, Options{Bucket: "bkt", Endpoint: ep}, workers)
		if !slices.Equal(got, keys) {
			t.Fatalf("%d workers: %d keys, want %d", workers, len(got), len(keys))
		}
		if n := st.Counters.Value("transport.pool_size"); n != want {
			t.Fatalf("%d workers: transport.pool_size %d, want %d", workers, n, want)
		}
	}
}
