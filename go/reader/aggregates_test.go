// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"slices"
	"testing"

	seekparquet "github.com/varveio/seekparquet/go"
)

// prefixTotals returns the number and total size of the objects whose key
// begins with prefix.
func prefixTotals(objects []Object, prefix []byte) (count, size int64) {
	for _, o := range objects {
		if bytes.HasPrefix(o.Key, prefix) {
			count++
			size += *o.Size
		}
	}
	return count, size
}

func describeAggregate(a PrefixAggregate) string {
	describe := func(v *int64) string {
		if v == nil {
			return "nil"
		}
		return fmt.Sprint(*v)
	}
	return fmt.Sprintf("{count %s, bytes %s}", describe(a.Count), describe(a.Bytes))
}

// randomPrefixes returns "", every "/"-ending prefix of keys, random byte
// prefixes of keys, absent prefixes, and repeats.
func randomPrefixes(r *rand.Rand, keys []string) [][]byte {
	prefixes := [][]byte{nil, []byte("zzz/"), []byte("a/b/zzz"), []byte("\xff")}
	for _, p := range pathsOf(keys) {
		if len(p) > 0 && p[len(p)-1] == '/' {
			prefixes = append(prefixes, []byte(p))
		}
	}
	for range 40 {
		k := keys[r.IntN(len(keys))]
		prefixes = append(prefixes, []byte(k[:1+r.IntN(len(k))]))
	}
	return append(prefixes, prefixes[:8]...)
}

// TestAggregatesMatchOracle (varve TestListAggregatesMatchOracle) requests
// counts and sums of nested, overlapping, repeated and absent prefixes
// over parts cut inside them. Every total present equals the Stream
// oracle's; on a dataset whose parts are all checked every total is
// present; and a count is never absent where the sum is present.
func TestAggregatesMatchOracle(t *testing.T) {
	keys := randomKeys(rand.New(rand.NewPCG(5, 6)), 420)
	dir := writeParts(t, s3Spec(), 7, 40, cut(keyRows(keys), 0, 97, 210, 211, 330, len(keys))...)
	objects, err := stream(t, openDir(t, dir))
	if err != nil {
		t.Fatal(err)
	}
	prefixes := randomPrefixes(rand.New(rand.NewPCG(7, 8)), keys)
	for _, checked := range []bool{false, true} {
		t.Run(fmt.Sprintf("checked %t", checked), func(t *testing.T) {
			d := openDir(t, dir)
			if checked {
				if err := d.CheckSchema(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			got, err := d.Aggregates(context.Background(), prefixes, AggregateOptions{Counts: true, Sums: SumsAlways})
			if err != nil {
				t.Fatal(err)
			}
			present := 0
			for i, p := range prefixes {
				a := got[i]
				count, size := prefixTotals(objects, p)
				switch {
				case a.Count == nil && a.Bytes == nil && !checked:
					continue
				case a.Count == nil || *a.Count != count || a.Bytes == nil || *a.Bytes != size:
					t.Fatalf("prefix %q: %s, want count %d and bytes %d", p, describeAggregate(a), count, size)
				}
				present++
			}
			if present == 0 {
				t.Fatal("no prefix had its totals checked")
			}
		})
	}
}

// TestAggregatesCountsAddNoReads (varve TestListCountsAddNoReads): counts
// decode no value page, and open no part between a prefix's ends.
func TestAggregatesCountsAddNoReads(t *testing.T) {
	keys := randomKeys(rand.New(rand.NewPCG(5, 6)), 420)
	dir := writeParts(t, s3Spec(), 7, 40, cut(keyRows(keys), 0, 97, 210, 211, 330, len(keys))...)
	d, src := openSource(t, dir)
	src.reset()
	got, err := d.Aggregates(context.Background(), randomPrefixes(rand.New(rand.NewPCG(7, 8)), keys), AggregateOptions{Counts: true})
	if err != nil {
		t.Fatal(err)
	}
	opens, reads := src.taken()
	spec, _ := Schema(S3ObjectsV1)
	if n, _ := columnBytes(t, dir, reads, spec.ColumnNames()...); n != 0 || len(reads) == 0 {
		t.Fatalf("counts made %d reads, %d of them of value pages", len(reads), n)
	}
	for _, a := range got {
		if a.Bytes != nil {
			t.Fatalf("counts alone summed: %s", describeAggregate(a))
		}
	}
	// "" spans every part: its count opens the first and no other.
	d, src = openSource(t, dir)
	src.reset()
	if got, err = d.Aggregates(context.Background(), [][]byte{nil}, AggregateOptions{Counts: true}); err != nil {
		t.Fatal(err)
	}
	if opens, _ = src.taken(); len(opens) > 1 || got[0].Count != nil {
		t.Fatalf("the count of every key opened %q: %s", opens, describeAggregate(got[0]))
	}
}

// TestAggregatesSumOverflowLeavesBytesNil (varve
// TestListSumOverflowLeavesBytesNil) sums a prefix whose sizes overflow
// an int64: its count stays exact and its bytes are nil.
func TestAggregatesSumOverflowLeavesBytesNil(t *testing.T) {
	rows := keyRows([]string{"a/0", "a/1", "a/2", "b/0"})
	for i := range 3 {
		rows[i].Size = ptr(int64(math.MaxInt64/2 + 1))
	}
	rows[3].Size = ptr(int64(0))
	d := openDir(t, writeParts(t, s3Spec(), 100, 0, rows[:2], rows[2:]))
	got, err := d.Aggregates(context.Background(), [][]byte{[]byte("a/"), []byte("b/")}, AggregateOptions{Counts: true, Sums: SumsAlways})
	if err != nil {
		t.Fatal(err)
	}
	if a := got[0]; a.Count == nil || *a.Count != 3 || a.Bytes != nil {
		t.Errorf("a/: %s, want count 3 and no bytes", describeAggregate(a))
	}
	if b := got[1]; b.Bytes == nil || *b.Bytes != 0 {
		t.Errorf("b/: %s, want bytes 0", describeAggregate(b))
	}
}

// budgetRows returns child folders r/cNN/ of 1 to 120 objects each, with
// an exact object for every third child, and the children.
func budgetRows() ([]s3Row, [][]byte) {
	var keys []string
	var children [][]byte
	for c := range 40 {
		prefix := fmt.Sprintf("r/c%02d/", c)
		children = append(children, []byte(prefix))
		if c%3 == 0 {
			keys = append(keys, prefix)
		}
		for o := range 1 + (c*37)%120 {
			keys = append(keys, fmt.Sprintf("%s%04d", prefix, o))
		}
	}
	return keyRows(keys), children
}

func budgetDir(t *testing.T) (string, [][]byte) {
	rows, children := budgetRows()
	return writeParts(t, s3Spec(), 16, 50, rows[:len(rows)/2], rows[len(rows)/2:]), children
}

// TestAggregatesSumsPublishNothingOnFailure (varve
// TestListSumsPublishNothingOnFailure) fails or cancels a summed request
// at its first read after the counts and at its last, after earlier sums:
// it returns the failure and no aggregates.
func TestAggregatesSumsPublishNothingOnFailure(t *testing.T) {
	dir, children := budgetDir(t)
	reads := func(sums SumMode) int {
		d, src := openSource(t, dir)
		if _, err := d.Aggregates(context.Background(), children, AggregateOptions{Counts: true, Sums: sums}); err != nil {
			t.Fatal(err)
		}
		_, r := src.taken()
		return len(r)
	}
	counted, summed := reads(SumsOff), reads(SumsAlways)
	if summed-counted < 3 {
		t.Fatalf("sums made %d reads after the counts' %d, want several", summed-counted, counted)
	}
	failing := errors.New("failing read")
	opt := AggregateOptions{Counts: true, Sums: SumsAlways}
	for _, at := range []int{counted + 1, summed} {
		d, src := openSource(t, dir)
		src.failAt, src.failErr = at, failing
		got, err := d.Aggregates(context.Background(), children, opt)
		var refusal *seekparquet.RefusalError
		if !errors.Is(err, failing) || errors.As(err, &refusal) || got != nil {
			t.Errorf("read %d failing: %v, %v; want no aggregates and the read's failure", at, got, err)
		}

		d, src = openSource(t, dir)
		ctx, cancel := context.WithCancel(context.Background())
		src.cancelAt, src.cancel = at, cancel
		got, err = d.Aggregates(ctx, children, opt)
		cancel()
		if !errors.Is(err, context.Canceled) || got != nil {
			t.Errorf("cancelled at read %d: %v, %v; want no aggregates and context.Canceled", at, got, err)
		}
	}
}

// TestAggregatesOffReadNothing (varve TestListAggregatesOffKeepJSON, whose
// JSON encoding of a listing request is varve's): with neither counts nor
// sums requested, every aggregate is nil and nothing is read.
func TestAggregatesOffReadNothing(t *testing.T) {
	dir, children := budgetDir(t)
	d, src := openSource(t, dir)
	src.reset()
	got, err := d.Aggregates(context.Background(), children, AggregateOptions{})
	if err != nil || !reflect.DeepEqual(got, make([]PrefixAggregate, len(children))) {
		t.Fatalf("aggregates off: %v, %v", got, err)
	}
	if _, reads := src.taken(); len(reads) != 0 {
		t.Fatalf("aggregates off made %d reads", len(reads))
	}
}

// TestAggregatesRejectInvalidRequests (varve
// TestListAggregatesRejectInvalidRequests), before any read. varve's
// refusal of aggregates without a delimiter is its listing's.
func TestAggregatesRejectInvalidRequests(t *testing.T) {
	dir, children := budgetDir(t)
	d, src := openSource(t, dir)
	src.reset()
	for _, opt := range []AggregateOptions{
		{SumBudgetBytes: 1},
		{Counts: true, SumBudgetBytes: 1},
		{Counts: true, Sums: SumsAlways, SumBudgetBytes: 1},
		{Sums: SumsWithinBudget, SumBudgetBytes: -1},
		{Sums: SumMode(7)},
	} {
		if got, err := d.Aggregates(context.Background(), children, opt); !errors.Is(err, seekparquet.ErrInvalidRequest) || got != nil {
			t.Errorf("%+v: %v, %v; want an invalid request", opt, got, err)
		}
	}
	if _, reads := src.taken(); len(reads) != 0 {
		t.Fatalf("invalid requests made %d reads", len(reads))
	}
}

// TestAggregatesSumsStayWithinBudget (varve TestListSumsStayWithinBudget)
// requests budgeted sums of 40 children, each on a fresh Dataset, and
// requires them to read no more size-page bytes than the budget, to be
// exact where present, and to sum every child under a budget of exactly
// their projected bytes, which only charging each shared page and
// dictionary once fits; and SumsAlways without counts to sum every child
// and count none.
func TestAggregatesSumsStayWithinBudget(t *testing.T) {
	dir, children := budgetDir(t)
	objects, err := stream(t, openDir(t, dir))
	if err != nil {
		t.Fatal(err)
	}
	var ranges []seekparquet.Range
	for _, c := range children {
		ranges = append(ranges, seekparquet.PrefixRange(c, 0))
	}
	plan, err := openDir(t, dir).SeekParquet().PlanRanges(context.Background(), ranges, []string{"size"})
	if err != nil {
		t.Fatal(err)
	}
	all, exact := plan.ProjectedBytes()
	if !exact || all < 4*minPageBytes {
		t.Fatalf("the children's size pages: %d bytes (exact %t)", all, exact)
	}

	mixed := false
	for _, budget := range []int64{1, all / 4, all / 2, all - 1, all, 1 << 30} {
		d, src := openSource(t, dir)
		got, err := d.Aggregates(context.Background(), children, AggregateOptions{Counts: true, Sums: SumsWithinBudget, SumBudgetBytes: budget})
		if err != nil {
			t.Fatal(err)
		}
		_, reads := src.taken()
		if _, read := columnBytes(t, dir, reads, "size"); read > budget {
			t.Errorf("budget %d: sums read %d bytes of size pages", budget, read)
		}
		summed := 0
		for i, c := range children {
			count, size := prefixTotals(objects, c)
			a := got[i]
			if a.Count == nil || *a.Count != count {
				t.Fatalf("budget %d: %s %s, want count %d", budget, c, describeAggregate(a), count)
			}
			if a.Bytes == nil {
				continue
			}
			if *a.Bytes != size {
				t.Fatalf("budget %d: %s bytes %d, want %d", budget, c, *a.Bytes, size)
			}
			summed++
		}
		switch {
		case budget == 1 && summed != 0:
			t.Errorf("budget 1 summed %d children", summed)
		case budget >= all && summed != len(children):
			t.Errorf("budget %d of %d summed %d of %d children", budget, all, summed, len(children))
		case budget == all-1 && summed == len(children):
			t.Errorf("budget %d, below the %d bytes of every child, summed them all", budget, all)
		case summed > 0 && summed < len(children):
			mixed = true
		}
	}
	if !mixed {
		t.Error("no budget summed some children and skipped others")
	}

	got, err := openDir(t, dir).Aggregates(context.Background(), children, AggregateOptions{Sums: SumsAlways})
	if err != nil {
		t.Fatal(err)
	}
	for i, c := range children {
		if _, size := prefixTotals(objects, c); got[i].Count != nil || got[i].Bytes == nil || *got[i].Bytes != size {
			t.Fatalf("always: %s %s, want bytes %d and no count", c, describeAggregate(got[i]), size)
		}
	}
}

// TestAggregatesCountsUseOnlyVerifiedRowCounts (varve
// TestListCountsUseOnlyVerifiedRowCounts) gives the middle of three
// one-object parts a manifest row count of 100. A count of a/ across that
// part, unchecked, would be 101: it is nil, and nothing is summed; b/ is
// counted and summed; and a read of the part is refused.
func TestAggregatesCountsUseOnlyVerifiedRowCounts(t *testing.T) {
	rows := keyRows([]string{"a/1", "a/2", "b/1"})
	dir := writeParts(t, s3Spec(), 100, 0, rows[:1], rows[1:2], rows[2:])
	editManifest(t, dir, func(m *seekparquet.Manifest) { m.Parts[1].Rows = 100 })
	d := openDir(t, dir)
	prefixes := [][]byte{[]byte("a/"), []byte("b/")}
	for _, sums := range []SumMode{SumsOff, SumsAlways} {
		got, err := d.Aggregates(context.Background(), prefixes, AggregateOptions{Counts: true, Sums: sums})
		if err != nil {
			t.Fatalf("sums %d: %v", sums, err)
		}
		if a := got[0]; a.Count != nil || a.Bytes != nil {
			t.Errorf("sums %d: a/ %s, want none across the unchecked part", sums, describeAggregate(a))
		}
		if b := got[1]; b.Count == nil || *b.Count != 1 || (sums == SumsAlways) != (b.Bytes != nil) || b.Bytes != nil && *b.Bytes != 2 {
			t.Errorf("sums %d: b/ %s, want count 1 and bytes 2 when summed", sums, describeAggregate(b))
		}
	}
	if _, err := d.SeekParquet().Range(context.Background(), seekparquet.PrefixRange([]byte("a/"), 10)); seekparquet.CodeOf(err) == "" {
		t.Errorf("a read of a/: %v, want a refusal", err)
	}
}

// TestObjectSumChecksArithmetic (varve TestRowIntervalCountChecksArithmetic,
// whose row-interval arithmetic is the module's Count now): a size sum
// that overflows an int64 stays overflowed.
func TestObjectSumChecksArithmetic(t *testing.T) {
	var s objectSum
	s.addSize(math.MaxInt64 - 1)
	if s.addSize(1); s.overflow || s.bytes != math.MaxInt64 {
		t.Fatalf("a sum of math.MaxInt64: %+v", s)
	}
	if s.addSize(1); !s.overflow {
		t.Fatalf("a sum past math.MaxInt64: %+v", s)
	}
	if s.addSize(0); !s.overflow {
		t.Fatalf("an overflowed sum recovered: %+v", s)
	}
}

// TestAggregatesOneByteSumBudgetOpensNothingNew (varve
// TestListOneByteSumBudgetOpensNothingNew) requests a/, spanning three
// parts an earlier read checked, and b/, on Datasets with the same
// history: with counts alone, and with a one-byte sum budget, which no
// page fits. The budgeted request opens no part and reads no byte the
// counts alone do not; summing every prefix reads more.
func TestAggregatesOneByteSumBudgetOpensNothingNew(t *testing.T) {
	var keys []string
	for i := range 150 {
		keys = append(keys, fmt.Sprintf("a/%04d", i))
	}
	for i := range 10 {
		keys = append(keys, fmt.Sprintf("b/%04d", i))
	}
	dir := writeParts(t, s3Spec(), 16, 0, cut(keyRows(keys), 0, 50, 100, 150, 160)...)
	prefixes := [][]byte{[]byte("a/"), []byte("b/")}
	request := func(opt AggregateOptions) ([]PrefixAggregate, []string, []sourceRead) {
		d, src := openSource(t, dir)
		if _, err := d.SeekParquet().Range(context.Background(), seekparquet.PrefixRange([]byte("a/"), 1000)); err != nil {
			t.Fatal(err)
		}
		src.reset()
		got, err := d.Aggregates(context.Background(), prefixes, opt)
		if err != nil {
			t.Fatal(err)
		}
		opens, reads := src.taken()
		return got, opens, reads
	}
	_, countedOpens, counted := request(AggregateOptions{Counts: true})
	got, opens, budgeted := request(AggregateOptions{Counts: true, Sums: SumsWithinBudget, SumBudgetBytes: 1})
	_, _, summed := request(AggregateOptions{Counts: true, Sums: SumsAlways})
	for i, want := range []int64{150, 10} {
		if a := got[i]; a.Count == nil || *a.Count != want || a.Bytes != nil {
			t.Fatalf("%s: %s, want count %d and no bytes", prefixes[i], describeAggregate(a), want)
		}
	}
	if !slices.Equal(opens, countedOpens) || !slices.Equal(budgeted, counted) {
		t.Errorf("a one-byte budget opened %q and made %d reads; counts alone opened %q and made %d", opens, len(budgeted), countedOpens, len(counted))
	}
	if n, _ := columnBytes(t, dir, summed, "size"); n == 0 {
		t.Error("summing every prefix read no size page")
	}
}
