// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/text/unicode/norm"

	"github.com/varveio/swath/internal/live"
	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/keyrange"
	"github.com/varveio/swath/internal/source/s3/client"
	"github.com/varveio/swath/internal/source/s3/s3test"
	"github.com/varveio/swath/internal/source/tree"
	"github.com/varveio/swath/reader/listing"
)

// r2Bucket serves keys from the R2 fake (s3test.R2: R2's measured order,
// NFC prefix matching and grouping, first-page start-after exclusion).
func r2Bucket(t *testing.T, keys []string) (*s3test.R2, string) {
	t.Helper()
	f := s3test.NewR2(keys)
	return f, s3test.Serve(t, f)
}

// r2Options list the fake at url, anonymously.
func r2Options(url string) Options {
	return Options{Endpoint: url, PathStyle: true, Anonymous: true, Region: "us-east-1"}
}

// treeList opens root with opt as the folder walk and lists it through the
// order stage (sorted output, which checks order) with workers.
func treeList(t *testing.T, root string, opt Options, workers int) ([]Row, *OpenedTree, error) {
	t.Helper()
	s3test.NoCredentials(t)
	opt.Workers = workers
	inv, err := Open(context.Background(), root, opt)
	if err != nil {
		return nil, nil, err
	}
	o, ok := inv.(*OpenedTree)
	if !ok {
		t.Fatalf("opened %T, want the folder walk", inv)
	}
	c := &collect{}
	_, err = order.Run[tree.FolderRange, Object](context.Background(), o.t, c, &Schema, order.Options{Workers: workers, Budget: 64 << 20})
	return c.rows, o, err
}

// r2Rows are the rows keys must list as: sorted as raw bytes, each with
// the fake's payload (its size the key's length, its ETag the key's MD5,
// its time a second per position in keys, STANDARD), worked out here,
// not read from the fake.
func r2Rows(keys []string) []Row {
	at := time.Date(2026, 11, 27, 0, 0, 0, 0, time.UTC)
	rows := make([]Row, len(keys))
	for i, k := range keys {
		sum := md5.Sum([]byte(k))
		rows[i] = Row{Key: k, Data: Object{Size: int64(len(k)), ETag: hex.EncodeToString(sum[:]), StorageClass: "STANDARD",
			Mtime: at.Add(time.Duration(i) * time.Second).UnixMicro()}}
	}
	slices.SortFunc(rows, func(a, b Row) int { return strings.Compare(a.Key, b.Key) })
	return rows
}

// sameRows fails unless got is want: keys and payloads, in order.
func sameRows(t *testing.T, what string, got, want []Row) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: %d rows, want %d\n got %q\nwant %q", what, len(got), len(want), keysOf(got), keysOf(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s: row %d\n got %+v\nwant %+v", what, i, got[i], want[i])
		}
	}
}

// noSeek fails if any request sent a start-after, or listed without the
// delimiter.
func noSeek(t *testing.T, f *s3test.R2) {
	t.Helper()
	for _, q := range f.Log() {
		if q.Has("start-after") || q.Get("delimiter") != "/" {
			t.Fatalf("request %v: the folder walk sends no start-after, and always the delimiter", q)
		}
	}
}

// r2Keys are keys R2 orders differently from bytes: markers that sort
// after their children, keys that are proper prefixes of others (sidecar
// files), empty segments and slash-only keys, a deep chain, a decomposed
// folder with one spelling, and high code points.
func r2Keys() []string {
	keys := []string{"/", "//", "///x", "a", "a-b", "a.b", "a.jpg", "a.jpg.orig", "a.jpg_", "a/", "a//", "a//b", "a/x", "a/x/", "a/x/y",
		"a0", "d/", "d/0", "d/0/", "d/9", "d/:", "nfd/é/x", "nfd/é/y", "nfd/f", "x.png", "x.png.metadata.json", "z~", "\x7f", "日本/x", "\U0010FFFF"}
	deep := "deep/"
	for i := range 20 {
		deep += fmt.Sprintf("%d/", i)
		keys = append(keys, deep, deep+"leaf")
	}
	return keys
}

// TestTreeListsR2Exact: the folder walk lists the R2 fake exactly (keys and
// payloads, in byte order) at every page size and worker count, without
// ever sending a start-after.
func TestTreeListsR2Exact(t *testing.T) {
	keys := r2Keys()
	want := r2Rows(keys)
	f, u := r2Bucket(t, keys)
	if slices.IsSorted(f.Keys()) {
		t.Fatal("the fixture lists in byte order on the fake: it tests nothing")
	}
	for _, page := range []int{1, 2, 1000} {
		for _, workers := range []int{1, 3, 16} {
			opt := r2Options(u)
			opt.Listing, opt.Policy.PageSize = source.ListingTree, page
			rows, o, err := treeList(t, "s3://bkt", opt, workers)
			if err != nil {
				t.Fatalf("page %d, workers %d: %v", page, workers, err)
			}
			sameRows(t, fmt.Sprintf("page %d, workers %d", page, workers), rows, want)
			st := o.t.Stats()
			if st.Listing != "tree" || st.ListingReason != ReasonExplicitTree || st.Counters.Value("tree.objects") != int64(len(keys)) ||
				st.Counters.Value("tree.markers") == 0 || o.Staged() != 0 || o.Listed() != int64(len(keys)) {
				t.Fatalf("stats %+v, staged %d", st, o.Staged())
			}
		}
	}
	noSeek(t, f)
}

// markerKeys are the live silent miss's bucket: 40 folders, each a marker
// (f000/) and 30 keys, which R2 lists before the marker.
func markerKeys() []string {
	var keys []string
	for f := range 40 {
		keys = append(keys, fmt.Sprintf("f%03d/", f))
		for k := range 30 {
			keys = append(keys, fmt.Sprintf("f%03d/%04d", f, k))
		}
	}
	return keys
}

// TestTreeMarkersOnR2 lists the 1,240 keys exactly at 32 workers, as asked
// (--listing tree) and as the default on an R2 endpoint chooses, which
// tells Warn why. The R2 endpoint is named in Options.Endpoint; the
// requests go to the fake through a supplied client.
func TestTreeMarkersOnR2(t *testing.T) {
	keys := markerKeys()
	want := r2Rows(keys)
	f, u := r2Bucket(t, keys)
	opt := r2Options(u)
	opt.Listing = source.ListingTree
	rows, _, err := treeList(t, "s3://bkt", opt, 32)
	if err != nil {
		t.Fatal(err)
	}
	sameRows(t, "--listing tree", rows, want)
	noSeek(t, f)

	c, err := client.New(context.Background(), client.Config{Endpoint: u, PathStyle: true, Anonymous: true, Region: "us-east-1"})
	if err != nil {
		t.Fatal(err)
	}
	var warned []string
	auto := Options{Endpoint: "https://0123456789abcdef.eu.r2.cloudflarestorage.com", Client: c,
		Warn: func(code, msg string) { warned = append(warned, code+": "+msg) }}
	rows, o, err := treeList(t, "s3://bkt", auto, 32)
	if err != nil {
		t.Fatal(err)
	}
	sameRows(t, "auto on R2", rows, want)
	if st := o.t.Stats(); st.ListingReason != ReasonR2Endpoint || !st.R2Endpoint {
		t.Fatalf("stats %+v", st)
	}
	if len(warned) != 1 || !strings.HasPrefix(warned[0], source.WarnTreeListing+": Cloudflare R2 does not list keys in byte order") {
		t.Fatalf("warned %q", warned)
	}
}

// TestRangeEngineOnR2 documents the hazard the folder walk exists for: the
// range engine, seeded from folder names, against R2's order either stops
// with a protocol violation or, as live on 2026-11-27, ends without an
// error with the folder markers missing (R2 sends f000/0000 before f000/,
// so the range below f000/ ends without it, and the next range's
// start-after=f000/ drops it from its first page). It never lists the
// bucket exactly.
func TestRangeEngineOnR2(t *testing.T) {
	keys := markerKeys()
	_, u := r2Bucket(t, keys)
	silent := 0
	for _, workers := range []int{1, 8, 32} {
		opt := r2Options(u)
		opt.Bucket, opt.Workers, opt.Policy = s3test.Name, workers, keyrange.Policy{Seed: true}
		s3test.NoCredentials(t)
		p, err := New(context.Background(), opt)
		if err != nil {
			t.Fatal(err)
		}
		c := &collect{}
		_, err = order.Run[Range, Object](context.Background(), p, c, &Schema, order.Options{Workers: workers, Budget: 64 << 20})
		got := keysOf(c.rows)
		missing := 0
		for _, k := range keys {
			if !slices.Contains(got, k) {
				missing++
			}
		}
		switch {
		case err != nil && errors.Is(err, source.ErrProtocol):
		case err == nil && missing > 0:
			silent++
			t.Logf("workers %d: exit 0 with %d of %d keys missing", workers, missing, len(keys))
		default:
			t.Fatalf("workers %d: err %v, %d missing: the range engine listed R2's order exactly", workers, err, missing)
		}
	}
	if silent == 0 {
		t.Fatal("no silent miss reproduced: the fake no longer models the live failure")
	}
}

// TestTreeAliasFolders: R2 merges the decomposed and composed spellings of
// one folder into one common prefix; listing it returns keys whose raw
// bytes do not start with it, and the walk stops (protocol) instead of
// misplacing or dropping them.
func TestTreeAliasFolders(t *testing.T) {
	keys := []string{"é/!", "é/x", "f"}
	f, u := r2Bucket(t, keys)
	opt := r2Options(u)
	opt.Listing = source.ListingTree
	_, o, err := treeList(t, "s3://bkt", opt, 2)
	if !errors.Is(err, source.ErrProtocol) {
		t.Fatalf("err %v", err)
	}
	if v := o.t.Stats().Counters.Value("tree.violations.raw_prefix"); v != 1 {
		t.Fatalf("raw-prefix violations %d (%v)", v, err)
	}
	noSeek(t, f)
}

// TestTreeMalformed: an S3 answer that breaks a folder's structure stops
// the walk with protocol, counted by its kind.
func TestTreeMalformed(t *testing.T) {
	keys := []string{"a/b", "a/c", "a/d/x", "e"}
	for _, tc := range []struct {
		name, counter string
		edit          func(q url.Values, p *s3test.R2Page)
	}{
		{"delimiter", "tree.violations.delimiter", func(q url.Values, p *s3test.R2Page) {
			if q.Get("prefix") == "" {
				p.CommonPrefixes = append(p.CommonPrefixes, "a/d/")
			}
		}},
		{"self reference", "tree.violations.self_reference", func(q url.Values, p *s3test.R2Page) {
			if q.Get("prefix") == "a/" {
				p.CommonPrefixes = append(p.CommonPrefixes, "a/")
			}
		}},
		{"duplicate", "tree.violations.duplicate", func(q url.Values, p *s3test.R2Page) {
			if q.Get("prefix") == "a/" && len(p.Contents) > 0 {
				p.Contents = append(p.Contents, p.Contents[0])
			}
		}},
		{"raw prefix", "tree.violations.raw_prefix", func(q url.Values, p *s3test.R2Page) {
			if q.Get("prefix") == "a/" {
				p.Contents = append(p.Contents, s3test.R2Entry{Key: "b"})
			}
		}},
		{"empty token", "tree.violations.token", func(q url.Values, p *s3test.R2Page) {
			if q.Get("prefix") == "a/" {
				p.Token = ""
			}
		}},
		{"token cycle", "tree.violations.token", func(q url.Values, p *s3test.R2Page) {
			if q.Get("prefix") == "a/" && q.Get("continuation-token") != "" && p.Truncated {
				p.Token = q.Get("continuation-token")
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, u := r2Bucket(t, keys)
			f.Edit = tc.edit
			opt := r2Options(u)
			opt.Listing, opt.Policy.PageSize = source.ListingTree, 1
			rows, o, err := treeList(t, "s3://bkt", opt, 2)
			if !errors.Is(err, source.ErrProtocol) {
				t.Fatalf("err %v, rows %q", err, keysOf(rows))
			}
			if v := o.t.Stats().Counters.Value(tc.counter); v != 1 {
				t.Fatalf("%s = %d (%v)", tc.counter, v, err)
			}
		})
	}
}

// TestTreeSourcePrefix: a source prefix lists exactly the keys whose raw
// bytes start with it, whatever it ends in: logs covers logs, logs-old/
// and logs/ but not log; d/e covers the decomposed d/éx (raw
// bytes d/e…), not the composed d/éy, which R2's own prefix e would
// get the other way round; the start folder is listed whole and its
// entries kept by raw bytes.
func TestTreeSourcePrefix(t *testing.T) {
	keys := []string{"lo/x", "log", "logs", "logs-old/a", "logs/", "logs/a", "logs/b/c", "logsx", "m", "d/", "d/éx", "d/éy", "d/ex", "d/f", "d/e/"}
	f, u := r2Bucket(t, keys)
	for _, prefix := range []string{"", "logs", "logs/", "logs/b", "d/e", "d/", "d/é", "nope", "日"} {
		opt := r2Options(u)
		opt.Listing, opt.Policy.PageSize = source.ListingTree, 2
		rows, o, err := treeList(t, "s3://bkt/"+prefix, opt, 4)
		if err != nil {
			t.Fatalf("prefix %q: %v", prefix, err)
		}
		var in []string
		for _, k := range keys {
			if strings.HasPrefix(k, prefix) {
				in = append(in, k)
			}
		}
		var want []Row
		for _, r := range r2Rows(keys) {
			if strings.HasPrefix(r.Key, prefix) {
				want = append(want, r)
			}
		}
		sameRows(t, "prefix "+prefix, rows, want)
		if r := o.Report(); r.Coverage.Prefix != prefix || !r.Coverage.Complete {
			t.Fatalf("prefix %q: coverage %+v", prefix, r.Coverage)
		}
	}
	noSeek(t, f)
	if !norm.NFC.IsNormalString("d/é") {
		t.Fatal("fixture")
	}
}

// TestTreeFolderLimit: a folder past the per-folder bound stops the run
// with the resource error naming it, its staging released.
func TestTreeFolderLimit(t *testing.T) {
	var keys []string
	for i := range 300 {
		keys = append(keys, fmt.Sprintf("big/%04d", i))
	}
	_, u := r2Bucket(t, append(keys, "small/a"))
	opt := r2Options(u)
	opt.Listing, opt.TreeFolderBytes = source.ListingTree, 8<<10
	_, o, err := treeList(t, "s3://bkt", opt, 4)
	var le *tree.FolderLimitError
	if !errors.As(err, &le) || le.Folder != "big/" || le.Class() != "folder_limit" {
		t.Fatalf("err %v", err)
	}
	if o.Staged() != 0 || o.t.Stats().Counters.Value("tree.budget_refusals") != 1 {
		t.Fatalf("staged %d, counters %+v", o.Staged(), o.t.Stats().Counters)
	}
}

// TestTreeCancel: a run cancelled while a folder's second page is held at
// the fake (a barrier, no sleep), its first page staged, returns the
// cancellation and releases what the folders staged.
func TestTreeCancel(t *testing.T) {
	keys := []string{"a/1", "a/2", "b/1", "b/2", "b/3", "c"}
	f, u := r2Bucket(t, keys)
	held := make(chan struct{})
	var once sync.Once
	f.Hook = func(r *http.Request) {
		if q := r.URL.Query(); q.Get("prefix") == "b/" && q.Get("continuation-token") != "" {
			once.Do(func() { close(held) })
			<-r.Context().Done()
		}
	}
	s3test.NoCredentials(t)
	opt := r2Options(u)
	opt.Listing, opt.Workers, opt.Policy.PageSize = source.ListingTree, 2, 1
	inv, err := Open(context.Background(), "s3://bkt", opt)
	if err != nil {
		t.Fatal(err)
	}
	o := inv.(*OpenedTree)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := order.Run[tree.FolderRange, Object](ctx, o.t, &collect{}, &Schema, order.Options{Workers: 2, Budget: 1 << 20})
		done <- err
	}()
	<-held
	if o.Staged() == 0 {
		t.Fatal("the held folder stages nothing: the barrier tests no release")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("err %v", err)
	}
	if o.Staged() != 0 {
		t.Fatalf("staged %d after the cancellation", o.Staged())
	}
}

// TestTreeRefusals: what the folder walk cannot do is refused before any
// request (exit 2: source.ErrUnsupported), naming the way out.
func TestTreeRefusals(t *testing.T) {
	f, u := r2Bucket(t, []string{"a"})
	s3test.NoCredentials(t)
	for _, tc := range []struct {
		name string
		edit func(*Options)
		want string
	}{
		{"sequential", func(o *Options) { o.Policy.Sequential = true }, "--sequential"},
		{"max keys", func(o *Options) { o.Policy.MaxKeys = 5 }, "--max-keys"},
		{"slow split", func(o *Options) { o.Policy.SlowSplit = true }, "--slow-split"},
		{"structure", func(o *Options) { o.Policy.Structure = keyrange.StructureOn }, "--structure=on"},
		{"tiles", func(o *Options) { o.Policy.TilePages = 3 }, "--seed-from-tile-pages"},
	} {
		opt := r2Options(u)
		opt.Listing = source.ListingTree
		tc.edit(&opt)
		if _, err := Open(context.Background(), "s3://bkt", opt); !errors.Is(err, source.ErrUnsupported) || !strings.Contains(err.Error(), tc.want) ||
			!strings.Contains(err.Error(), "--listing range") {
			t.Fatalf("%s: %v", tc.name, err)
		}
	}
	for _, endpoint := range []string{u, "https://acct.r2.cloudflarestorage.com"} {
		opt := r2Options(endpoint)
		if endpoint == u {
			opt.Listing = source.ListingTree
		}
		if _, err := OpenVersions(context.Background(), "s3://bkt", opt); !errors.Is(err, source.ErrUnsupported) || !strings.Contains(err.Error(), "--all-versions") {
			t.Fatalf("versions at %s: %v", endpoint, err)
		}
	}
	if _, err := New(context.Background(), Options{Bucket: "bkt", Listing: source.ListingTree}); !errors.Is(err, errTreeOpen) {
		t.Fatalf("New of a folder walk: %v", err)
	}
	if n := len(f.Log()); n != 0 {
		t.Fatalf("%d requests before the refusals", n)
	}
}

// TestListingSelection: which listing each choice opens, why, and what it
// tells the user; R2 is recognised by its parsed host name alone.
func TestListingSelection(t *testing.T) {
	s3test.NoCredentials(t)
	const r2 = "https://0123abcd.r2.cloudflarestorage.com"
	for _, tc := range []struct {
		endpoint string
		listing  source.Listing
		tree     bool
		reason   string
		warn     string
	}{
		{"", "", false, ReasonDefault, ""},
		{"http://127.0.0.1:1", source.ListingAuto, false, ReasonDefault, ""},
		{r2, "", true, ReasonR2Endpoint, source.WarnTreeListing},
		{r2, source.ListingRange, false, ReasonExplicitRange, source.WarnUnorderedRange},
		{"http://127.0.0.1:1", source.ListingRange, false, ReasonExplicitRange, ""},
		{"http://127.0.0.1:1", source.ListingTree, true, ReasonExplicitTree, ""},
		{r2, source.ListingTree, true, ReasonExplicitTree, ""},
	} {
		var warned []string
		opt := Options{Endpoint: tc.endpoint, Listing: tc.listing, Anonymous: true, Region: "us-east-1", PathStyle: true,
			Warn: func(code, _ string) { warned = append(warned, code) }}
		inv, err := Open(context.Background(), "s3://bkt", opt)
		if err != nil {
			t.Fatalf("%+v: %v", tc, err)
		}
		var st Stats
		switch o := inv.(type) {
		case *OpenedTree:
			st = o.t.Stats()
		case *Opened[Object]:
			st = o.p.Stats()
		}
		_, isTree := inv.(*OpenedTree)
		if isTree != tc.tree || st.ListingReason != tc.reason || (tc.warn == "") != (len(warned) == 0) || tc.warn != "" && (len(warned) != 1 || warned[0] != tc.warn) {
			t.Fatalf("%+v: tree %v, stats %+v, warned %q", tc, isTree, st, warned)
		}
	}
	for host, want := range map[string]bool{
		"https://acct.r2.cloudflarestorage.com": true, "https://acct.eu.r2.cloudflarestorage.com/": true, "HTTPS://ACCT.R2.CLOUDFLARESTORAGE.COM.": true,
		"http://acct.r2.cloudflarestorage.com:8080": true, "https://r2.cloudflarestorage.com.example.com": false, "https://acctr2.cloudflarestorage.com": false,
		"https://s3.amazonaws.com": false, "": false, "::": false,
	} {
		if R2Endpoint(host) != want {
			t.Errorf("R2Endpoint(%q) = %v", host, !want)
		}
	}
}

// TestLiveWalkRefusedOnR2: a library walk, which lists on the range
// engine, is refused on an R2 endpoint as unsupported, before any request:
// OpenLive marks an R2 endpoint (and makes no request), and a walk of a
// store so marked, whose client points at a server that fails the test if
// asked anything, is refused.
func TestLiveWalkRefusedOnR2(t *testing.T) {
	s3test.NoCredentials(t)
	l, err := OpenLive(context.Background(), Options{Bucket: "bkt", Endpoint: "https://acct.r2.cloudflarestorage.com", Anonymous: true, Region: "auto"})
	if err != nil {
		t.Fatal(err)
	}
	if !l.r2 {
		t.Fatal("OpenLive does not mark an R2 endpoint")
	}
	u := s3test.Serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("a walk of an R2 store sent %s", r.URL)
		w.WriteHeader(http.StatusForbidden) // not retried: a broken refusal ends here
		fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>AccessDenied</Code><Message>no</Message></Error>`)
	}))
	c, err := client.New(context.Background(), client.Config{Endpoint: u, PathStyle: true, Anonymous: true, Region: "auto"})
	if err != nil {
		t.Fatal(err)
	}
	marked := &Live{bucket: "bkt", client: c, r2: true}
	if err := marked.Walk(context.Background(), live.Walk{Prefix: []byte("a/")}, nil); !errors.Is(err, listing.ErrUnsupported) {
		t.Fatalf("walk: %v", err)
	}
}
