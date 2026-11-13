// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblob

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/registry"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/azblob/azblobtest"
	"github.com/varveio/swath/internal/source/azblob/client"
	"github.com/varveio/swath/internal/source/keyrange"
	"github.com/varveio/swath/internal/source/objhttp"
	"github.com/varveio/swath/internal/source/tree"
)

// hnsCorpus is a hierarchical namespace for the tree mode: siblings that
// sort around '/' (a directory sorts after a-b by key, before it by name),
// names whose UTF-16 and byte orders differ in one directory (U+E000..U+FFFF
// against U+10000 and above: the byte-order adapter's case), a deep path,
// empty directories, Hive partitions as on bingmlbuildings, and a directory
// of n files beside a directory named as their common start (part, the
// candidate of every pivot part-NNNN).
func hnsCorpus(n int) []string {
	keys := []string{
		"a/x", "a-b", "a-c/y", "a.b", "a0", "a!/z", "b/", "c",
		"u/", "u/x/", "u/\U00010000", "u/\U0001f600/q", "u/ｚ",
		"RegionName=Guinea/p.parquet", "RegionName=Guinea-Bissau/p.parquet", "RegionName=São Tomé and Príncipe/p.parquet",
		"empty/", "deep/1/2/3/4/5/6/7/8/9/10/leaf", "deep/1/2/3/4/5/6/7/8/9/10/empty/",
		"big/part/inner", "big/part-x/", "big/zz",
	}
	for i := range n {
		keys = append(keys, fmt.Sprintf("big/part-%05d.parquet", i))
	}
	return keys
}

// openTree opens a supplied HNS client as the command does (registry
// open): admission reports a hierarchical namespace and routes to Tree.
func openTree(t *testing.T, c API, prefix string, workers, page int) *treeOpened {
	t.Helper()
	root := "az://acct/c"
	if prefix != "" {
		root += "/" + prefix
	}
	opt := &Options{Client: c}
	opt.Policy.PageSize = page
	src, err := open(context.Background(), registry.Request{Root: root, Options: opt, Workers: workers})
	if err != nil {
		t.Fatal(err)
	}
	o, ok := src.(*treeOpened)
	if !ok {
		t.Fatalf("an HNS account opened as %T", src)
	}
	return o
}

func runTree(ctx context.Context, o *treeOpened, workers int) ([]Row, error) {
	c := &collect{}
	_, err := order.Run[tree.OrderedRange, Object](ctx, o.t, c, &Schema, order.Options{Workers: workers, Budget: 1 << 24})
	return c.rows, err
}

// TestTreeListsHNS: an HNS account lists in byte order of keys, every file
// and directory once (the sorted key set), at any worker count and page
// size; directory rows carry the listing's properties; the huge directory
// is split through startFrom seeks.
func TestTreeListsHNS(t *testing.T) {
	store := azblobtest.NewTreeStore(hnsCorpus(3000))
	want := store.Keys()
	for _, workers := range []int{1, 4, 16} {
		for _, page := range []int{1, 7, 100, 1000} {
			t.Run(fmt.Sprintf("w%d/p%d", workers, page), func(t *testing.T) {
				o := openTree(t, store, "", workers, page)
				rows, err := runTree(context.Background(), o, workers)
				if err != nil {
					t.Fatal(err)
				}
				if got := keysOf(rows); !slices.Equal(got, want) {
					t.Fatalf("got %d keys, want %d; first difference %s", len(got), len(want), firstDiff(got, want))
				}
				for _, r := range rows {
					name := strings.TrimSuffix(r.Key, "/")
					size := int64(len(r.Key))
					if strings.HasSuffix(r.Key, "/") {
						size = 0
					}
					if r.Data.ETag != fmt.Sprintf("0x%x", name) || r.Data.Size != size || r.Data.BlobType != "BlockBlob" {
						t.Fatalf("row %q carries %+v", r.Key, r.Data)
					}
				}
				st := o.t.Stats()
				v := st.Counters.Value
				if page <= 100 && (v("tree.dirs_split") == 0 || v("split.accepted_bounded")+v("split.accepted_frontier") == 0) && workers > 1 {
					t.Fatalf("no split of the huge directory: dirs_split %d, splits %d", v("tree.dirs_split"), v("split.accepted_bounded"))
				}
				if v("order_spans")+v("order_splits") == 0 {
					t.Fatal("the byte-order adapter did not engage: u/ holds U+E000 and U+10000 siblings")
				}
				if v("tree.held_dirs") == 0 {
					t.Fatal("no directory row held: a/ sorts after a-b, a.b")
				}
				if st.Listing != "tree" || st.Namespace != "account information" {
					t.Fatalf("stats: listing %q, namespace %q", st.Listing, st.Namespace)
				}
				if n := o.Listed(); n != int64(len(want)) {
					t.Fatalf("Listed %d, want %d", n, len(want))
				}
			})
		}
	}
}

func firstDiff(a, b []string) string {
	for i := range min(len(a), len(b)) {
		if a[i] != b[i] {
			return fmt.Sprintf("at %d: %q, want %q", i, a[i], b[i])
		}
	}
	return fmt.Sprintf("lengths %d and %d", len(a), len(b))
}

// TestTreeScopedToDirectory: a prefix naming a directory lists its
// subtree; one that ends inside a name is refused (a flat prefix match
// crosses directories, which a tree walk does not).
func TestTreeScopedToDirectory(t *testing.T) {
	store := azblobtest.NewTreeStore(hnsCorpus(10))
	o := openTree(t, store, "deep/1/", 2, 1000)
	rows, err := runTree(context.Background(), o, 2)
	if err != nil {
		t.Fatal(err)
	}
	var want []string
	for _, k := range store.Keys() {
		if strings.HasPrefix(k, "deep/1/") && k != "deep/1/" {
			want = append(want, k)
		}
	}
	if got := keysOf(rows); !slices.Equal(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
	_, err = open(context.Background(), registry.Request{Root: "az://acct/c/dee", Options: &Options{Client: store}, Workers: 1})
	if !errors.Is(err, source.ErrUnsupported) || !strings.Contains(err.Error(), "must name a directory") {
		t.Fatalf("prefix inside a name: %v", err)
	}
}

// TestTreeCancelMidDirectory: a run cancelled while the huge directory's
// segments list stops with the cancellation.
func TestTreeCancelMidDirectory(t *testing.T) {
	store := azblobtest.NewTreeStore(hnsCorpus(5000))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store.Fail = func(n int) error {
		if n == 30 {
			cancel()
		}
		return nil
	}
	o := openTree(t, store, "", 8, 50)
	if _, err := runTree(ctx, o, 8); !errors.Is(err, context.Canceled) {
		t.Fatalf("err %v, want the cancellation", err)
	}
	if n := len(store.Log()); n > 200 {
		t.Fatalf("%d requests after cancelling at 30", n)
	}
}

// Resolution may consume multiple stateful adapter tokens before it can
// emit a directory row. Exhausted retries on a later read must preserve
// those held rows and retry the current token, never the consumed input.
func TestTreeResolutionRetry(t *testing.T) {
	keys := []string{"0", "1", "2", "a/", "a-/", "a--/", "z"}
	for _, fail := range []int{5, 6, 7} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			store := azblobtest.NewTreeStore(keys)
			store.Fail = func(n int) error {
				if n == fail {
					return treeExhausted()
				}
				return nil
			}
			o := openTree(t, store, "", 1, 1)
			rows, err := runTree(context.Background(), o, 1)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(keysOf(rows), store.Keys()) {
				t.Fatalf("got %q want %q", keysOf(rows), store.Keys())
			}
			if n := o.t.Stats().Counters.Value("tree.resolve_requeues"); n != 1 {
				t.Fatalf("resolution requeues %d", n)
			}
		})
	}
}

func treeExhausted() error {
	err := &client.Error{Status: 503, Code: "ServerBusy"}
	return &objhttp.ExhaustedError{Attempts: 1, Err: objhttp.Normalize(err)}
}

// A permanent inner failure returns with its classification; cancellation
// during the resolution backoff stops promptly without another request.
func TestTreeResolutionStops(t *testing.T) {
	for _, cancelWait := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel=%v", cancelWait), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			store := azblobtest.NewTreeStore([]string{"0", "1", "2", "a/", "a-/", "a--/", "z"})
			store.Fail = func(n int) error {
				if n != 6 {
					return nil
				}
				if cancelWait {
					time.AfterFunc(10*time.Millisecond, cancel)
					return treeExhausted()
				}
				return objhttp.Normalize(&client.Error{Status: 403, Code: "AuthenticationFailed"})
			}
			o := openTree(t, store, "", 1, 1)
			_, err := runTree(ctx, o, 1)
			if cancelWait {
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancel: %v", err)
				}
				if n := o.t.Stats().Counters.Value("tree.resolve_requeues"); n != 1 {
					t.Fatalf("resolution requeues %d", n)
				}
			} else if objhttp.Classify(err) != objhttp.ClassAuth {
				t.Fatalf("permanent: %v class %q", err, objhttp.Classify(err))
			}
			if n := len(store.Log()); n != 7 {
				t.Fatalf("%d requests; failure was request 7", n)
			}
		})
	}
}

// TestTreeIdentityAndReport: the listing mode is frozen in the identity
// (a field the flat identity does not have, so neither resumes as the
// other) and shown in the report; the tree mode declares no resume and
// takes no prior (not source.Seeded, so the listing refuses --seed-from).
func TestTreeIdentityAndReport(t *testing.T) {
	o := openTree(t, azblobtest.NewTreeStore([]string{"a/b"}), "", 1, 0)
	if _, err := runTree(context.Background(), o, 1); err != nil {
		t.Fatal(err)
	}
	if _, ok := any(o).(source.Seeded); ok {
		t.Fatal("the tree mode takes a prior: --seed-from would tile a flat scope it does not list")
	}
	var listing string
	for _, f := range o.Identity().Fields {
		if f.Name == "listing" {
			listing = f.Value
		}
	}
	if listing != "tree" || o.Capabilities().Resume || o.Capabilities().RangeEngine() {
		t.Fatalf("identity listing %q, capabilities %+v", listing, o.Capabilities())
	}
	b, _ := json.Marshal(o.Report().Stats)
	if !strings.Contains(string(b), `"listing":"tree"`) || !strings.Contains(o.Report().Brief, "files=1 dirs=1") {
		t.Fatalf("report %s / %s", b, o.Report().Brief)
	}
	flat := &opened[Object]{p: &Provider{opt: Options{Account: "acct", Container: "c"}}}
	for _, f := range flat.Identity().Fields {
		if f.Name == "listing" {
			t.Fatal("the flat identity names a listing mode: flat journals would no longer resume")
		}
	}
}

// TestTreeNotNewFlat: the flat provider's constructor still refuses an
// HNS account; only the registered source routes it to the tree mode.
func TestTreeNotNewFlat(t *testing.T) {
	_, err := New(context.Background(), Options{Account: "acct", Container: "c", Client: azblobtest.NewTreeStore(nil), Workers: 1})
	if !errors.Is(err, source.ErrUnsupported) || !strings.Contains(err.Error(), "hierarchical namespace") {
		t.Fatalf("New on HNS: %v", err)
	}
	var _ keyrange.Range // the flat provider's range type is unchanged
}

// refusedInfo answers Get Account Information as a container SAS gets it
// (403 AuthorizationFailure), listing through its store.
type refusedInfo struct{ API }

func (refusedInfo) AccountInfo(context.Context, string) (bool, error) {
	return false, &client.Error{Status: 403, Code: "AuthorizationFailure"}
}

// TestTreeListedNamespace: when neither account call answers (a container
// SAS), one listing that names its entries' resource type shows a
// hierarchical namespace (namespace "listing"); a flat account's listing
// names none, so its namespace stays unknown and the refusal stands.
func TestTreeListedNamespace(t *testing.T) {
	store := azblobtest.NewTreeStore(hnsCorpus(10))
	src, err := open(context.Background(), registry.Request{Root: "az://acct/c/deep/1/", Options: &Options{Client: refusedInfo{store}}, Workers: 2})
	if err != nil {
		t.Fatal(err)
	}
	o, ok := src.(*treeOpened)
	if !ok {
		t.Fatalf("opened as %T", src)
	}
	if st := o.t.Stats(); st.Namespace != "listing" {
		t.Fatalf("namespace %q", st.Namespace)
	}
	if rows, err := runTree(context.Background(), o, 2); err != nil || len(rows) == 0 {
		t.Fatalf("%d rows, %v", len(rows), err)
	}
	_, err = open(context.Background(), registry.Request{Root: "az://acct/c", Options: &Options{Client: refusedInfo{azblobtest.NewStore([]string{"a/b", "c"})}}, Workers: 1})
	if err == nil || strings.Contains(err.Error(), "hierarchical") {
		t.Fatalf("flat account with a refused account call: %v", err)
	}
}

// lookupCorpus is hnsCorpus(n) with 2,000 names before big/part in big/,
// so a split's candidate part lies past the directory's first page and is
// looked up (tree.lookups) rather than found in what was read; and 60
// directories a, a-, a--, ... in held/, each held until the next sorts
// before it by key, so a page of 50 or fewer emits nothing and reads on
// (tree.resolve_pages).
func lookupCorpus(n int) []string {
	keys := hnsCorpus(n)
	for i := range 2000 {
		keys = append(keys, fmt.Sprintf("big/a-%05d", i))
	}
	for i := range 60 {
		keys = append(keys, "held/a"+strings.Repeat("-", i)+"/")
	}
	return append(keys, "held/z")
}

// TestTreeFetchFlags: on a hierarchical namespace the fetch flags fill a
// file's columns as on a flat account (TestFetchFlags): every request that
// lists rows (first, segment and resolve pages) sends the include, a file
// without any gets the empty value and one encrypted with a
// customer-provided key null metadata. A directory entry lists none of
// them, so a directory's row keeps them null whatever was asked, and a
// lookup, whose row is a directory's, sends no include.
func TestTreeFetchFlags(t *testing.T) {
	full := client.Props{LeaseState: "available", TagCount: 1, Metadata: `{"k":"v"}`, Tags: `{"t":"1"}`,
		ImmutabilityMode: "Locked", ImmutabilityUntil: 5, LegalHold: client.True}
	for _, c := range []struct {
		name string
		set  func(*Options)
		want client.Include
		rows map[string]client.Props // by key; any other file's is none, a directory's is always zero
		none client.Props
	}{
		{"default", func(*Options) {}, client.Include{}, map[string]client.Props{
			"a/x": {LeaseState: "available", TagCount: 1}, "c": {CustomerProvidedKeySHA256: "key="}}, client.Props{}},
		{"all", func(o *Options) { o.FetchMetadata, o.FetchTags, o.FetchImmutability = true, true, true },
			client.Include{Metadata: true, Tags: true, Immutability: true}, map[string]client.Props{
				"a/x": full, "c": {CustomerProvidedKeySHA256: "key=", Tags: "{}", LegalHold: client.False}},
			client.Props{Metadata: "{}", Tags: "{}", LegalHold: client.False}},
	} {
		t.Run(c.name, func(t *testing.T) {
			store := azblobtest.NewTreeStore(lookupCorpus(3000))
			store.SetProps("a/x", full)
			store.SetProps("c", client.Props{CustomerProvidedKeySHA256: "key="})
			opt := &Options{Client: store}
			opt.Policy.PageSize = 50
			c.set(opt)
			src, err := open(context.Background(), registry.Request{Root: "az://acct/c", Options: opt, Workers: 8})
			if err != nil {
				t.Fatal(err)
			}
			o := src.(*treeOpened)
			rows, err := runTree(context.Background(), o, 8)
			if err != nil {
				t.Fatal(err)
			}
			if got := keysOf(rows); !slices.Equal(got, store.Keys()) {
				t.Fatalf("got %d keys, want %d; first difference %s", len(got), len(store.Keys()), firstDiff(got, store.Keys()))
			}
			for _, r := range rows {
				want, ok := c.rows[r.Key]
				switch {
				case strings.HasSuffix(r.Key, "/"):
					want = client.Props{}
				case !ok:
					want = c.none
				}
				if r.Data.Props != want {
					t.Fatalf("%s: %+v, want %+v", r.Key, r.Data.Props, want)
				}
			}
			lookups := 0
			for _, r := range store.Log() {
				want := c.want
				if r.In.Prefix != "" && !strings.HasSuffix(r.In.Prefix, "/") { // a lookup: dir + name
					want = client.Include{}
					lookups++
				}
				if r.In.Include != want {
					t.Fatalf("prefix %q sent %+v, want %+v", r.In.Prefix, r.In.Include, want)
				}
			}
			if v := o.t.Stats().Counters.Value; lookups == 0 || v("tree.resolve_pages") == 0 || v("tree.dirs_split") == 0 {
				t.Fatalf("lookups %d, resolve pages %d, split directories %d: a request kind went unchecked",
					lookups, v("tree.resolve_pages"), v("tree.dirs_split"))
			}
		})
	}
}

// TestTreeFetchTagsRefused: a tree listing refused while it asks for tags
// names the permission tags need, its class kept, as a flat one does
// (TestFetchTagsRefused); without the flag the refusal is reported as it is.
func TestTreeFetchTagsRefused(t *testing.T) {
	for _, tags := range []bool{true, false} {
		store := azblobtest.NewTreeStore([]string{"a/b", "c"})
		store.Fail = func(int) error {
			return &client.Error{Status: 403, Code: "AuthorizationPermissionMismatch",
				Message: "This request is not authorized to perform this operation using this permission."}
		}
		src, err := open(context.Background(), registry.Request{Root: "az://acct/c", Options: &Options{Client: store, FetchTags: tags}, Workers: 1})
		if err != nil {
			t.Fatal(err)
		}
		_, err = runTree(context.Background(), src.(*treeOpened), 1)
		if err == nil {
			t.Fatal("listed")
		}
		if f := objhttp.Normalize(err); f == nil || (&client.Error{Status: f.Status, Code: f.Code}).Failure().Class != objhttp.ClassAccessDenied {
			t.Errorf("tags=%v: %v is not access denied", tags, err)
		}
		if strings.Contains(err.Error(), "t permission") != tags {
			t.Errorf("tags=%v: %v", tags, err)
		}
	}
}

// TestTreeReportBrief: the brief is one account of the whole walk: its
// requests are every request the store answered (first pages, segment and
// resolve pages, lookups), not the engine's segment pages alone, and it
// names one bytes, the walk's.
func TestTreeReportBrief(t *testing.T) {
	store := azblobtest.NewTreeStore(lookupCorpus(3000))
	o := openTree(t, store, "", 8, 50)
	rows, err := runTree(context.Background(), o, 8)
	if err != nil {
		t.Fatal(err)
	}
	var files, dirs, bytes int64
	for _, r := range rows {
		if strings.HasSuffix(r.Key, "/") {
			dirs++
		} else {
			files++
		}
		bytes += r.Data.Size
	}
	v := o.t.Stats().Counters.Value
	log := int64(len(store.Log()))
	if v("tree.lookups") == 0 || v("tree.resolve_pages") == 0 || v("requests") >= log {
		t.Fatalf("lookups %d, resolve pages %d, segment requests %d of %d: the fixture does not tell the counts apart",
			v("tree.lookups"), v("tree.resolve_pages"), v("requests"), log)
	}
	brief := o.Report().Brief
	fields := map[string]string{}
	for _, f := range strings.Fields(brief) {
		k, val, _ := strings.Cut(f, "=")
		if _, dup := fields[k]; dup {
			t.Fatalf("brief names %s twice: %s", k, brief)
		}
		fields[k] = val
	}
	for k, want := range map[string]int64{"files": files, "dirs": dirs, "bytes": bytes, "requests": log, "first_pages": v("tree.first_pages")} {
		if fields[k] != fmt.Sprint(want) {
			t.Errorf("%s=%s, want %d (brief %s)", k, fields[k], want, brief)
		}
	}
	if v("list_requests") != log {
		t.Errorf("list_requests %d, the store logged %d", v("list_requests"), log)
	}
}
