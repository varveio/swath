// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/varveio/swath/go/sortedparquet"
	"github.com/varveio/swath/go/sortedparquet/sortedparquettest"
)

// openParts writes parts as one Java-era fixture and opens it.
func openParts(t *testing.T, opts Options, parts ...sortedparquettest.Part) *Sorted {
	t.Helper()
	dir := t.TempDir()
	for i := range parts {
		parts[i].Stamps = sortedparquettest.Stamps(i+1, i == len(parts)-1)
		writePart(t, filepath.Join(dir, partName(i)), parts[i])
	}
	s, _, err := Open(context.Background(), dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s.(*Sorted)
}

func partName(i int) string { return fmt.Sprintf("part-%05d.parquet", i+1) }

// TestRollupMaxIntLimit asks for math.MaxInt entries. The rollup reads one
// entry past the limit to detect truncation; adding it to math.MaxInt
// wrapped negative and the rollup returned nothing.
func TestRollupMaxIntLimit(t *testing.T) {
	s := openParts(t, Options{}, sortedparquettest.Part{Rows: objects("a")})
	got, ok, err := s.DelimitedRollup(context.Background(), nil, true, nil, nil, []byte("/"), math.MaxInt, Projection{}, true)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	want := rangeWalk(storeWalk(t, s, nil), nil, true, nil, math.MaxInt, true)
	if len(want) != 1 {
		t.Fatalf("the range walk returned %d entries", len(want))
	}
	if d := diffEntries(got, want); d != "" {
		t.Fatal(d)
	}
}

// TestDisorderReasonMatchesJava reads a stamped OBJECT-only group holding
// a, c, b, d through Rows and through the delimiter rollup: both fail with
// the replay server's row_group_disorder reason, naming the part, the
// group and the row, and each failure is counted.
func TestDisorderReasonMatchesJava(t *testing.T) {
	s := openParts(t, Options{}, sortedparquettest.Part{Rows: objects("a", "c", "b", "d")})
	check := func(how string, err error) {
		t.Helper()
		var disorder *sortedparquet.DisorderError
		if Refusal(err) != "row_group_disorder" || !errors.As(err, &disorder) || !errors.Is(err, sortedparquet.ErrCorrupt) {
			t.Fatalf("%s: %v (reason %q)", how, err, Refusal(err))
		}
		if filepath.Base(disorder.Part) != "part-00001.parquet" || disorder.RowGroup != 0 || disorder.Row != 2 || string(disorder.Key) != "b" {
			t.Fatalf("%s: located at %s group %d row %d key %q", how, disorder.Part, disorder.RowGroup, disorder.Row, disorder.Key)
		}
		// The replay server's detail text, byte for byte: the key in hex.
		if want := "its keys must be in strictly ascending unsigned order, but row 2 (62) is at or below its predecessor"; disorder.Detail != want {
			t.Fatalf("%s: detail %q, want %q", how, disorder.Detail, want)
		}
	}
	_, err := s.Rows(context.Background(), nil, true, nil, 10, Projection{})
	check("rows", err)
	_, ok, err := s.DelimitedRollup(context.Background(), nil, true, nil, nil, []byte("/"), 10, Projection{}, true)
	if !ok {
		t.Fatal("declined")
	}
	check("rollup", err)
	if n := s.Counters().DisorderRefusals.Load(); n != 2 {
		t.Fatalf("disorder refusals counted %d, want 2", n)
	}
}

// TestRowsEmptyGroupsAndOneRowParts reads a part whose physical row groups
// are [empty, a, empty, m, empty] followed by a one-row part holding z,
// from at, before and after every key, inclusive and exclusive, with and
// without an upper bound, at limits 1 and 2.
func TestRowsEmptyGroupsAndOneRowParts(t *testing.T) {
	s := openParts(t, Options{},
		sortedparquettest.Part{Rows: objects("a", "m"), Groups: []int{0, 1, 0, 1, 0}},
		sortedparquettest.Part{Rows: objects("z")})
	if s.reader.RowGroups() != 3 {
		t.Fatalf("%d non-empty row groups, want 3", s.reader.RowGroups())
	}
	// The writer really leaves the empty groups in the footer.
	probe := filepath.Join(t.TempDir(), "probe.parquet")
	writePart(t, probe, sortedparquettest.Part{Rows: objects("a", "m"), Groups: []int{0, 1, 0, 1, 0}})
	f, err := os.Open(probe)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	info, _ := f.Stat()
	footer, err := sortedparquet.OpenFooter(f, info.Size())
	if err != nil || len(footer.Metadata().RowGroups) != 5 {
		t.Fatalf("physical row groups: %v", err)
	}
	keys := []string{"a", "m", "z"}
	var rows []refRow
	for _, k := range keys {
		rows = append(rows, refRow{key: k})
	}
	points := []string{"", "\x00", "a", "a\x00", "b", "l", "m", "m\x00", "y", "z", "z\x00"}
	for _, f := range append([]string{"<open>"}, points...) {
		var from *string
		if f != "<open>" {
			from = &f
		}
		for _, to := range append([]*string{nil}, ptrs(points)...) {
			for _, inclusive := range []bool{true, false} {
				for _, limit := range []int{1, 2} {
					got, err := s.Rows(context.Background(), bytesOf(from), inclusive, bytesOf(to), limit, Projection{})
					if err != nil {
						t.Fatal(err)
					}
					want := expectKeys(rows, from, inclusive, to, limit)
					if fmt.Sprint(keysOf(got)) != fmt.Sprint(want) {
						t.Fatalf("from %q inclusive %v to %v limit %d: %q, want %q", f, inclusive, deref(to), limit, keysOf(got), want)
					}
				}
			}
		}
	}
}

func ptrs(values []string) []*string {
	out := make([]*string, len(values))
	for i := range values {
		out[i] = &values[i]
	}
	return out
}

func bytesOf(s *string) []byte {
	if s == nil {
		return nil
	}
	return []byte(*s)
}

func deref(s *string) string {
	if s == nil {
		return "<open>"
	}
	return *s
}

// TestRollupNarrowUpperBound rolls up [a/5, a/8) over groups [a/1] and
// [a/9] with prefix a/: no key lies in the range, so neither floor rule
// may produce a common prefix or an object.
func TestRollupNarrowUpperBound(t *testing.T) {
	s := openParts(t, Options{}, sortedparquettest.Part{Rows: objects("a/1", "a/9"), GroupRows: 1})
	for _, suppress := range []bool{true, false} {
		for _, inclusive := range []bool{true, false} {
			got, ok, err := s.DelimitedRollup(context.Background(), []byte("a/5"), inclusive, []byte("a/8"), []byte("a/"), []byte("/"), 10, Projection{}, suppress)
			if err != nil || !ok {
				t.Fatal(ok, err)
			}
			want := rangeWalk(storeWalk(t, s, []byte("a/8")), []byte("a/5"), inclusive, []byte("a/"), 10, suppress)
			if len(got) != 0 || len(want) != 0 {
				t.Fatalf("suppress %v inclusive %v: %s", suppress, inclusive, describe(got, want))
			}
		}
	}
	// The same narrow range one level up: prefix "" rolls a/1 and a/9 up
	// to a/, which the range does not reach either.
	got, _, err := s.DelimitedRollup(context.Background(), []byte("a/5"), true, []byte("a/8"), nil, []byte("/"), 10, Projection{}, false)
	if err != nil {
		t.Fatal(err)
	}
	if want := rangeWalk(storeWalk(t, s, []byte("a/8")), []byte("a/5"), true, nil, 10, false); diffEntries(got, want) != "" {
		t.Fatal(diffEntries(got, want))
	}
}

// TestCancellationAndCloseWithQueuedReaders holds the one reader slot as
// an admitted read would, queues two reads behind it, cancels one and
// closes the store while the other waits: the cancelled read returns its
// context's error, Close does not wait for a read that holds no slot, the
// waiting read then fails with ErrClosed, no permit is left taken and no
// descriptor open. A second store is closed while eight readers page it:
// every read ends with rows or ErrClosed.
func TestCancellationAndCloseWithQueuedReaders(t *testing.T) {
	keys := make([]string, 60)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%03d", i)
	}
	s := openParts(t, Options{Readers: 1}, sortedparquettest.Part{Rows: objects(keys...), GroupRows: 10})
	if err := s.sem.Acquire(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancelled, queued := make(chan error, 1), make(chan error, 1)
	go func() {
		_, err := s.Rows(ctx, nil, true, nil, 5, Projection{})
		cancelled <- err
	}()
	go func() {
		_, err := s.Rows(context.Background(), nil, true, nil, 5, Projection{})
		queued <- err
	}()
	cancel()
	if err := <-cancelled; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled read: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s.sem.Release(1)
	if err := <-queued; !errors.Is(err, sortedparquet.ErrClosed) {
		t.Fatalf("read queued across Close: %v", err)
	}
	if !s.sem.TryAcquire(1) {
		t.Fatal("a reader permit was not released")
	}
	s.sem.Release(1)
	if n := s.Counters().InFlightReaders.Load(); n != 0 {
		t.Fatalf("%d reads in flight", n)
	}
	if open := s.Files().Stats().Open; open != 0 {
		t.Fatalf("%d descriptors open after Close", open)
	}

	busy := openParts(t, Options{Readers: 2}, sortedparquettest.Part{Rows: objects(keys...), GroupRows: 10})
	var wg sync.WaitGroup
	start := make(chan struct{})
	for g := range 8 {
		wg.Go(func() {
			<-start
			for i := 0; ; i++ {
				_, err := busy.Rows(context.Background(), []byte(keys[(g*7+i)%len(keys)]), true, nil, 7, Projection{Owner: g%2 == 0})
				if errors.Is(err, sortedparquet.ErrClosed) {
					return
				}
				if err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	close(start)
	if err := busy.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if open := busy.Files().Stats().Open; open != 0 {
		t.Fatalf("%d descriptors open after Close", open)
	}
}

// TestConcurrentRowsAndRollupsColdIndexes starts range reads and rollups
// with differing projections on the same row groups of a store none has
// read yet, so their first reads load each group's page index
// concurrently, and compares every result with the same call on a store
// read sequentially.
func TestConcurrentRowsAndRollupsColdIndexes(t *testing.T) {
	keys := fixtureKeys(3000)
	part := sortedparquettest.Part{Rows: objects(keys...), GroupRows: 500, PageBytes: 2048}
	cold := openParts(t, Options{Readers: 8}, part)
	warm := openParts(t, Options{Readers: 1}, part)
	type call func(s *Sorted) string
	var calls []call
	for i, from := range []string{"", "b/", "d03/", "d07/s02/", "long/", "root"} {
		proj := Projection{Owner: i%2 == 0}
		calls = append(calls,
			func(s *Sorted) string {
				got, err := s.Rows(context.Background(), []byte(from), true, nil, 40, proj)
				if err != nil {
					return err.Error()
				}
				return fmt.Sprint(keysOf(got), owners(got))
			},
			func(s *Sorted) string {
				got, _, err := s.DelimitedRollup(context.Background(), nil, true, nil, nil, []byte("/"), 30, proj, true)
				if err != nil {
					return err.Error()
				}
				return entriesString(got)
			})
	}
	want := make([]string, len(calls))
	for i, c := range calls {
		want[i] = c(warm)
	}
	got := make([]string, len(calls)*4)
	var wg sync.WaitGroup
	for i := range got {
		wg.Go(func() { got[i] = calls[i%len(calls)](cold) })
	}
	wg.Wait()
	for i := range got {
		if got[i] != want[i%len(calls)] {
			t.Fatalf("call %d: %s\nwant %s", i%len(calls), got[i], want[i%len(calls)])
		}
	}
}

func owners(objects []Object) []string {
	out := make([]string, len(objects))
	for i, o := range objects {
		if o.OwnerID != nil {
			out[i] = *o.OwnerID
		}
	}
	return out
}

// entriesString spells every entry: each common prefix, or each object's
// key and owner.
func entriesString(entries []DelimitedEntry) string {
	var b strings.Builder
	for _, e := range entries {
		if e.Object == nil {
			fmt.Fprintf(&b, "p:%q ", e.CommonPrefix)
			continue
		}
		fmt.Fprintf(&b, "o:%q/%v ", e.Object.Key, owners([]Object{*e.Object}))
	}
	return b.String()
}

// TestPageDisorderReasonMatchesJava reads a row group whose key pages
// descend (the rows written in reverse over many pages): the range and
// the delimiter path both fail with row_group_disorder at row -1, no key,
// and the replay server's detail for a column index whose pages are not
// ascending. Before, it was a generic ErrCorrupt with no reason.
func TestPageDisorderReasonMatchesJava(t *testing.T) {
	keys := make([]string, 300)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%04d", len(keys)-1-i)
	}
	s := openParts(t, Options{}, sortedparquettest.Part{Rows: objects(keys...), PageBytes: 512})
	check := func(how string, err error) {
		t.Helper()
		var disorder *sortedparquet.DisorderError
		if Refusal(err) != "row_group_disorder" || !errors.As(err, &disorder) {
			t.Fatalf("%s: %v (reason %q)", how, err, Refusal(err))
		}
		want := "its keys must be in strictly ascending unsigned order, but the column index reports its pages DESCENDING"
		if disorder.Row != -1 || disorder.Key != nil || disorder.RowGroup != 0 || disorder.Detail != want {
			t.Fatalf("%s: row %d key %q group %d detail %q", how, disorder.Row, disorder.Key, disorder.RowGroup, disorder.Detail)
		}
	}
	_, err := s.Rows(context.Background(), []byte("k0100"), true, nil, 1, Projection{})
	check("rows", err)
	_, _, err = s.DelimitedRollup(context.Background(), nil, true, nil, nil, []byte("/"), 10, Projection{}, true)
	check("rollup", err)
}
