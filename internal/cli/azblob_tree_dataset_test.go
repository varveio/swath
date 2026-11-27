// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/internal/filter"
	"github.com/varveio/swath/internal/inventory"
	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/registry"
	"github.com/varveio/swath/internal/runtrace"
	"github.com/varveio/swath/internal/source/azblob"
	"github.com/varveio/swath/internal/source/azblob/azblobtest"
	"github.com/varveio/swath/internal/source/azblob/client"
	"github.com/varveio/swath/internal/source/objhttp"
	"github.com/varveio/swath/internal/totals"
	"github.com/varveio/swath/reader"
)

// TestTreeDatasetTotals: a dataset written from a hierarchical namespace
// holds the flat listing's schema, which the factory declares
// (registry.SchemaInfo), so du --from-dataset (registry.DatasetTotals)
// totals it as a live du of the account does, directory rows counted as
// objects in both.
func TestTreeDatasetTotals(t *testing.T) {
	ctx := context.Background()
	files := []string{"a/x", "a-b", "a-c/y", "a0", "deep/1/leaf"}
	store := azblobtest.NewTreeStore(append(slices.Clone(files), "b/", "deep/1/empty/"))
	dir := filepath.Join(t.TempDir(), "ds")
	if _, err := inventory.Run(ctx, inventory.Config{
		Source: inventory.SourceConfig{URI: "az://acct/c", Options: &azblob.Options{Client: store}},
		Output: inventory.OutputConfig{Dir: dir, NoJournal: true},
	}); err != nil {
		t.Fatal(err)
	}
	opt := totals.Options{Depth: 3}
	live := totals.New(opt)
	o, err := openAzblob(ctx, registry.Request{Root: "az://acct/c", Options: &azblob.Options{Client: store}, Workers: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := o.Run(ctx, registry.Env{Count: true, Totals: live, Filter: &filter.Spec{}, Order: order.Options{Workers: 2}}); err != nil {
		t.Fatal(err)
	}
	d, err := reader.OpenDir(ctx, dir, seekparquet.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close() // before TempDir's cleanup: Windows will not remove an open file
	if id := string(d.Metadata().SchemaID); id != azblob.Schema.Info.ID {
		t.Fatalf("schema %s, want %s", id, azblob.Schema.Info.ID)
	}
	saved := totals.New(opt)
	if _, err := registry.DatasetTotals(ctx, d, "", false, nil, saved, 2); err != nil {
		t.Fatal(err)
	}
	got, want := saved.Result(), live.Result()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("from the dataset %+v, live %+v", got, want)
	}
	var size int64
	for _, key := range files {
		size += int64(len(key))
	}
	keys := store.Keys()
	if len(want) == 0 || want[0].Objects != int64(len(keys)) || want[0].Size != size {
		t.Fatalf("totals %+v, want %d objects including directories and %d bytes", want, len(keys), size)
	}
	plan, err := d.SeekParquet().Plan(ctx, []seekparquet.Selection{{}}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	var listed []string
	if err := plan.Scan(ctx, nil, func(batch *seekparquet.Batch) error {
		for i := range batch.Len() {
			listed = append(listed, string(batch.Key(i)))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(listed, keys) {
		t.Fatalf("committed keys %q, want %q", listed, keys)
	}
}

// TestTreeTraceRefused: --trace is refused on a hierarchical namespace
// (the tree source is not source.Traced) before any listing request.
func TestTreeTraceRefused(t *testing.T) {
	store := azblobtest.NewTreeStore([]string{"a/b"})
	tr := runtrace.New(io.Discard)
	defer tr.Close(0, "", "")
	_, err := inventory.Run(context.Background(), inventory.Config{
		Source: inventory.SourceConfig{URI: "az://acct/c", Options: &azblob.Options{Client: store}},
		Output: inventory.OutputConfig{Dir: filepath.Join(t.TempDir(), "ds"), NoJournal: true},
		Trace:  tr,
	})
	var ce *inventory.ConfigError
	if !errors.As(err, &ce) || !strings.Contains(err.Error(), "--trace") {
		t.Fatalf("%v", err)
	}
	if n := len(store.Log()); n != 0 {
		t.Fatalf("%d listing requests before the refusal", n)
	}
}

// TestTreeUnreadableRoot: a scope directory the caller may not list is the
// service's refusal, a failed run of class access denied, never an empty
// complete dataset (as an unreadable local root is an error).
func TestTreeUnreadableRoot(t *testing.T) {
	store := azblobtest.NewTreeStore([]string{"d/a", "d/e/f"})
	store.Fail = func(int) error {
		return &client.Error{Status: 403, Code: "AuthorizationPermissionMismatch",
			Message: "This request is not authorized to perform this operation using this permission."}
	}
	dir := filepath.Join(t.TempDir(), "ds")
	_, err := inventory.Run(context.Background(), inventory.Config{
		Source: inventory.SourceConfig{URI: "az://acct/c/d/", Options: &azblob.Options{Client: store}},
		Output: inventory.OutputConfig{Dir: dir, NoJournal: true},
	})
	if objhttp.Classify(err) != objhttp.ClassAccessDenied {
		t.Fatalf("%v (class %q)", err, objhttp.Classify(err))
	}
	if d, err := reader.OpenDir(context.Background(), dir, seekparquet.OpenOptions{}); err == nil {
		d.Close()
		t.Fatal("an unreadable root committed a dataset")
	}
}

// TestTreeNarrowedScope keeps the directory row when the include could
// match it, even though a directory used as the traversal root has no row.
func TestTreeNarrowedScope(t *testing.T) {
	for _, tc := range []struct {
		name, inferred, scope, reason string
		include                       []string
		wantRows                      int64
	}{
		{"matching directory row", "d/sub/", "d/", filter.NarrowDeclined, []string{"sub/", "sub/**"}, 2},
		{"partial leaf narrows to parent", "d/sub/mat", "d/sub/", filter.Narrowed, []string{"sub/mat*"}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := azblobtest.NewTreeStore([]string{"d/sub/match", "d/other/skip", "outside/skip"})
			spec := &filter.Spec{Base: "d/", Include: tc.include}
			inferred, why := spec.NarrowPrefix("d/")
			if inferred != tc.inferred || why != filter.Narrowed {
				t.Fatalf("test include narrowed to %q (%s), want %q", inferred, why, tc.inferred)
			}
			r, err := inventory.Run(context.Background(), inventory.Config{
				Source: inventory.SourceConfig{URI: "az://acct/c/" + inferred, OriginalURI: "az://acct/c/d/",
					Narrowing: why, Options: &azblob.Options{Client: store}},
				Filter: spec,
			})
			if err != nil {
				t.Fatal(err)
			}
			if r.Invocation.Source != "az://acct/c/"+tc.scope || r.Invocation.Narrowing != tc.reason || r.Output.RowsEmitted != tc.wantRows {
				t.Fatalf("source %q, narrowing %q, rows %d; want %q, %q, %d", r.Invocation.Source, r.Invocation.Narrowing,
					r.Output.RowsEmitted, "az://acct/c/"+tc.scope, tc.reason, tc.wantRows)
			}
			for _, req := range store.Log() {
				if strings.HasPrefix(req.In.Prefix, "d/other") {
					t.Error("traversed a subtree disjoint from the inferred include prefix")
				}
				if !strings.HasPrefix(req.In.Prefix, tc.scope) {
					t.Errorf("listed %q outside %q", req.In.Prefix, tc.scope)
				}
			}
		})
	}
}
