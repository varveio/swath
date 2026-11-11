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

	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/registry"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/azblob/azblobtest"
	"github.com/varveio/swath/internal/source/azblob/client"
	"github.com/varveio/swath/internal/source/keyrange"
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

// TestTreeIdentityAndReport: the listing mode is frozen in the identity
// (a field the flat identity does not have, so neither resumes as the
// other) and shown in the report; the tree mode declares no resume.
func TestTreeIdentityAndReport(t *testing.T) {
	o := openTree(t, azblobtest.NewTreeStore([]string{"a/b"}), "", 1, 0)
	if _, err := runTree(context.Background(), o, 1); err != nil {
		t.Fatal(err)
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
