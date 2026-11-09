// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/varveio/swath/go/internal/source/localfs"
)

// TestLocalRecordingS3View reads a swath/local-entries/v1 dataset the Go lister
// wrote through the window cache (packed, the placeholders held once per
// window), through the bare reader (each row's Object), through a cache
// whose arena limit sends every fill to the unpacked path, and as a
// rollup, with and without the owner columns, and requires every row's
// ETag the placeholder syntheticETag, its StorageClass STANDARD, no Owner
// or checksum, its size and time as recorded, and the answers equal.
// Before, a local recording's rows had no ETag and no StorageClass.
func TestLocalRecordingS3View(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	tree := filepath.Join(dir, "tree")
	for i := range 240 {
		path := filepath.Join(tree, fmt.Sprintf("d%d", i%3), fmt.Sprintf("f%03d", i))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, make([]byte, i%7), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	ds := listInto(t, dir, "go-local", tree, &localfs.Options{})
	open := func(opts Options) *Sorted {
		s, meta, err := Open(ctx, ds, opts)
		if err != nil {
			t.Fatal(err)
		}
		if meta.SchemaID != "swath/local-entries/v1" {
			t.Fatalf("schema %q", meta.SchemaID)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s.(*Sorted)
	}
	bare := open(Options{Prefetch: Prefetch{Off: true}})
	cached := open(Options{Prefetch: Prefetch{WindowRows: 37, MaxWindows: 5}})
	// A 50-row read's keys (about 300 bytes) do not fit in 100.
	overflow := open(Options{Prefetch: Prefetch{WindowRows: 37, MaxWindows: 5}})
	overflow.arenaLimit = 100
	check := func(how string, got []Object) {
		t.Helper()
		for _, o := range got {
			if o.ETag == nil || *o.ETag != syntheticETag || o.StorageClass == nil || *o.StorageClass != "STANDARD" ||
				o.OwnerID != nil || o.OwnerDisplayName != nil || o.ChecksumAlgorithm != nil || o.ChecksumType != nil ||
				o.Size == nil || o.LastModified == nil {
				t.Fatalf("%s: %s: %+v, want ETag %q, StorageClass STANDARD, no Owner or checksum", how, o.Key, o, syntheticETag)
			}
		}
	}
	var want []Object
	for name, s := range map[string]*Sorted{"bare": bare, "cached": cached, "overflow": overflow} {
		var all []Object
		for page, from := 0, []byte(nil); ; page++ {
			got, err := s.Rows(ctx, from, false, nil, 50, Projection{Owner: page%2 == 0})
			if err != nil {
				t.Fatal(err)
			}
			if len(got) == 0 {
				break
			}
			check(fmt.Sprintf("%s page %d", name, page), got)
			all = append(all, got...)
			from = got[len(got)-1].Key
		}
		if len(all) < 240 {
			t.Fatalf("%s: %d rows", name, len(all))
		}
		if want == nil {
			want = all
		} else if fmt.Sprint(objectValues(all)) != fmt.Sprint(objectValues(want)) {
			t.Errorf("%s answers differ:\n%v\nwant\n%v", name, objectValues(all), objectValues(want))
		}
		// The directory's own row (d1/) and its 80 files.
		entries, ok, err := s.DelimitedRollup(ctx, []byte("d1/"), true, []byte("d10"), []byte("d1/"), []byte("/"), 1000, Projection{Owner: true}, true)
		if err != nil || !ok || len(entries) != 81 {
			t.Fatalf("%s rollup: %d entries, %v %v", name, len(entries), ok, err)
		}
		objects := make([]Object, len(entries))
		for i, e := range entries {
			objects[i] = *e.Object
		}
		check(name+" rollup", objects)
	}
}

// objectValues is each Object's key and values, for comparing answers.
func objectValues(objects []Object) []string {
	out := make([]string, len(objects))
	str := func(s *string) string {
		if s == nil {
			return "<nil>"
		}
		return *s
	}
	for i, o := range objects {
		out[i] = fmt.Sprintf("%s %d %d %s %s", o.Key, *o.Size, o.LastModified.UnixMicro(), str(o.ETag), str(o.StorageClass))
	}
	return out
}
