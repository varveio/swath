// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/replay/legacy"
	"github.com/varveio/swath/go/internal/replay/legacy/legacytest"
)

func objects(keys ...string) []legacytest.Row {
	rows := make([]legacytest.Row, len(keys))
	for i, k := range keys {
		rows[i] = legacytest.Object(k, i)
	}
	return rows
}

func writePart(t *testing.T, path string, part legacytest.Part) {
	t.Helper()
	if err := legacytest.Write(path, part); err != nil {
		t.Fatal(err)
	}
}

// TestOpenRefusesWithTheJavaReasons builds one fixture per refusal and
// requires Open to name the reason the Java server's fallback names.
func TestOpenRefusesWithTheJavaReasons(t *testing.T) {
	stamps := func(index int, final bool, change map[string]string) map[string]string {
		s := legacytest.Stamps(index, final)
		for k, v := range change {
			if v == "" {
				delete(s, k)
			} else {
				s[k] = v
			}
		}
		return s
	}
	mixed := objects("a", "b")
	mixed[1].RowType = "PREFIX"
	cases := []struct {
		reason string
		parts  []legacytest.Part
	}{
		{"no_stamp", []legacytest.Part{{Rows: objects("a")}}},
		{"no_stamp", []legacytest.Part{{Rows: objects("a"), Stamps: stamps(1, true, map[string]string{"swath.sort.order": "key_bytes_signed"})}}},
		{"no_stamp", nil},
		{"unsupported_mode", []legacytest.Part{{Rows: objects("a"), Stamps: stamps(1, true, map[string]string{"swath.sort.mode": "versions"})}}},
		{"unknown_format_version", []legacytest.Part{{Rows: objects("a"), Stamps: stamps(1, true, map[string]string{"swath.sort.format_version": "2"})}}},
		// A publish interrupted after part 1 of 2: contiguous, no final part.
		{"incomplete_multifile", []legacytest.Part{{Rows: objects("a"), Stamps: stamps(1, false, nil)}}},
		{"incomplete_multifile", []legacytest.Part{
			{Rows: objects("a"), Stamps: stamps(1, false, nil)},
			{Rows: objects("b"), Stamps: stamps(1, true, nil)},
		}},
		{"incomplete_multifile", []legacytest.Part{
			{Rows: objects("a"), Stamps: stamps(1, true, nil)},
			{Rows: objects("b"), Stamps: stamps(2, true, nil)},
		}},
		{"mixed_row_types", []legacytest.Part{{Rows: mixed, Stamps: stamps(1, true, nil)}}},
		// Part 2 starts before part 1: the first keys do not ascend.
		{"sanity_failed", []legacytest.Part{
			{Rows: objects("m", "n"), Stamps: stamps(1, false, nil)},
			{Rows: objects("c", "d"), Stamps: stamps(2, true, nil)},
		}},
		// Within one part, row groups whose first keys repeat.
		{"sanity_failed", []legacytest.Part{{Rows: objects("a", "b", "a", "c"), Stamps: stamps(1, true, nil), GroupRows: 2}}},
	}
	for i, c := range cases {
		t.Run(fmt.Sprintf("%d-%s", i, c.reason), func(t *testing.T) {
			dir := t.TempDir()
			for p, part := range c.parts {
				writePart(t, filepath.Join(dir, fmt.Sprintf("part-%05d.parquet", p+1)), part)
			}
			_, _, err := Open(context.Background(), dir, Options{})
			if got := Refusal(err); got != c.reason {
				t.Fatalf("refusal %q (%v), want %q", got, err, c.reason)
			}
		})
	}
}

// TestProjection checks that owner columns are read only when asked, that
// nulls stay null, and that a GCS dataset projects its own columns.
func TestProjection(t *testing.T) {
	dir := t.TempDir()
	rows := objects("a", "b", "c")
	rows[1].Size, rows[1].LastModified, rows[1].ETag = nil, nil, nil
	writePart(t, filepath.Join(dir, "p.parquet"), legacytest.Part{Rows: rows, Stamps: legacytest.Stamps(1, true)})
	s, meta, err := Open(context.Background(), dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if meta.SchemaID != "swath/canonical" || meta.Rows != 3 || meta.Parts != 1 {
		t.Fatalf("metadata %+v", meta)
	}
	plain, err := s.Rows(context.Background(), nil, true, nil, 10, Projection{})
	if err != nil {
		t.Fatal(err)
	}
	owned, err := s.Rows(context.Background(), nil, true, nil, 10, Projection{Owner: true})
	if err != nil {
		t.Fatal(err)
	}
	if plain[0].OwnerID != nil || owned[0].OwnerID == nil || *owned[0].OwnerDisplayName != "name-0" {
		t.Fatal("owner columns must be read only with Projection.Owner")
	}
	if b := plain[1]; b.Size != nil || b.LastModified != nil || b.ETag != nil || *b.StorageClass != "STANDARD" || *b.ChecksumAlgorithm != "CRC32" {
		t.Fatalf("nulls must stay null: %+v", b)
	}
	if c := plain[2]; *c.Size != 2 || !c.LastModified.Equal(time.Date(2026, 1, 1, 0, 0, 2, 0, time.UTC)) || c.ChecksumAlgorithm != nil {
		t.Fatalf("values: %+v", c)
	}

	gcs, meta, err := Open(context.Background(), listGCS(t, t.TempDir()), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer gcs.Close()
	if meta.SchemaID != "swath/gcs-objects/v1" {
		t.Fatalf("schema id %q", meta.SchemaID)
	}
	got, err := gcs.Rows(context.Background(), nil, true, nil, 1, Projection{Owner: true})
	if err != nil || len(got) != 1 || got[0].Size == nil || got[0].ETag == nil || got[0].OwnerID != nil || got[0].ChecksumType != nil {
		t.Fatalf("GCS projection: %+v %v", got, err)
	}
	// The GCS-native columns are read through the same store by name.
	native, err := gcs.(*Sorted).ReadRows(context.Background(), nil, true, nil, 1, []string{"generation", "crc32c"})
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := native[0].Value("generation"); !ok || v.IsNull() {
		t.Fatalf("generation: %v %v", v, ok)
	}
}

// TestReadersBoundConcurrentReads runs many reads against a store with two
// reader slots and requires the peak in flight to stay at two.
func TestReadersBoundConcurrentReads(t *testing.T) {
	dir := t.TempDir()
	keys := fixtureKeys(3000)
	writeJavaEra(t, dir, keys, 2, 700)
	s, _, err := Open(context.Background(), dir, Options{Readers: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			from := []byte(keys[i*150])
			if _, err := s.Rows(context.Background(), from, true, nil, 300, Projection{}); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	c := s.(*Sorted).Counters()
	if peak := c.PeakReaders.Load(); peak < 1 || peak > 2 {
		t.Fatalf("peak readers %d, want 1..2", peak)
	}
	if c.InFlightReaders.Load() != 0 || c.Requests.Load() != 16 || c.PageDecodes.Load() == 0 || c.RowsDecoded.Load() == 0 {
		t.Fatalf("counters: in flight %d, requests %d, pages %d, rows %d", c.InFlightReaders.Load(), c.Requests.Load(), c.PageDecodes.Load(), c.RowsDecoded.Load())
	}
	if DefaultReaders() < 8 || DefaultReaders() > 32 {
		t.Fatalf("default readers %d", DefaultReaders())
	}
}

// TestIdentity requires the fixture identity to survive a copy that keeps
// names, sizes, times and stamps, and to change with a modification time.
func TestIdentity(t *testing.T) {
	keys := fixtureKeys(200)
	a := writeJavaEra(t, filepath.Join(t.TempDir(), "a"), keys, 2, 100)
	b := filepath.Join(t.TempDir(), "b")
	if err := os.MkdirAll(b, 0o755); err != nil {
		t.Fatal(err)
	}
	when := time.Date(2026, 1, 2, 3, 4, 5, 6000, time.UTC)
	for _, name := range []string{"part-00001.parquet", "part-00002.parquet"} {
		data, err := os.ReadFile(filepath.Join(a, name))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(b, name), data, 0o644); err != nil {
			t.Fatal(err)
		}
		for _, dir := range []string{a, b} {
			if err := os.Chtimes(filepath.Join(dir, name), when, when); err != nil {
				t.Fatal(err)
			}
		}
	}
	identity := func(path string) string {
		s, meta, err := Open(context.Background(), path, Options{})
		if err != nil {
			t.Fatal(err)
		}
		_ = s.Close()
		return meta.Identity
	}
	if identity(a) != identity(b) {
		t.Fatal("a copy keeping names, sizes, times and stamps must keep the identity")
	}
	later := when.Add(time.Second)
	if err := os.Chtimes(filepath.Join(b, "part-00002.parquet"), later, later); err != nil {
		t.Fatal(err)
	}
	if identity(a) == identity(b) {
		t.Fatal("a changed modification time must change the identity")
	}
}

// TestClosedStore requires reads after Close to fail.
func TestClosedStore(t *testing.T) {
	dir := writeJavaEra(t, t.TempDir(), fixtureKeys(10), 1, 0)
	s, _, err := Open(context.Background(), dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Rows(context.Background(), nil, true, nil, 1, Projection{}); !errors.Is(err, legacy.ErrClosed) {
		t.Fatalf("read after close: %v", err)
	}
}

// TestClosedStoreWithWarmWindows reads a one-row fixture with limit two,
// which keeps a final window, and requires the same read after Close to
// fail with ErrClosed as the cache-off store's does, the windows and
// anchors released.
func TestClosedStoreWithWarmWindows(t *testing.T) {
	s := openParts(t, Options{}, legacytest.Part{Rows: objects("a")})
	ctx := context.Background()
	for range 2 {
		if rows, err := s.Rows(ctx, nil, true, nil, 2, Projection{}); err != nil || len(rows) != 1 {
			t.Fatalf("before close: %d rows, %v", len(rows), err)
		}
	}
	if stats, _ := s.PrefetchStats(); stats.Windows == 0 {
		t.Fatal("the read kept no window")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if rows, err := s.Rows(ctx, nil, true, nil, 2, Projection{}); !errors.Is(err, legacy.ErrClosed) {
		t.Fatalf("read after close: %d rows, %v", len(rows), err)
	}
	if stats, _ := s.PrefetchStats(); stats.Windows != 0 || stats.Anchors != 0 || stats.Rows != 0 {
		t.Fatalf("after close: %+v held", stats)
	}
}
