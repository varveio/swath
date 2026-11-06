// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package order

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source"
)

// frontier is the step-3 adversarial shape: a slow, dense first range (the
// frontier) and many fast siblings after it, with long keys.
type frontier struct {
	slowPages, siblings, sibPages, rows int
	slowDelay, sibDelay                 time.Duration
}

type fr struct{ i int } // -1: root, 0: the slow range, 1..: siblings

func (f *frontier) Root() fr { return fr{-1} }

func (f *frontier) key(i, page, j int) string {
	return fmt.Sprintf("r%04d/%0200d-%04d-%04d", i, 0, page, j) // about 220-byte keys
}

func (f *frontier) Expand(ctx context.Context, r fr, e source.Emitter[fr, tp]) error {
	if r.i < 0 {
		for i := 0; i <= f.siblings; i++ {
			if err := e.Child(fr{i}); err != nil {
				return err
			}
		}
		return nil
	}
	pages, delay := f.sibPages, f.sibDelay
	if r.i == 0 {
		pages, delay = f.slowPages, f.slowDelay
	}
	for p := range pages {
		time.Sleep(delay)
		rows := make([]schema.Row[tp], f.rows)
		for j := range rows {
			rows[j] = schema.Row[tp]{Key: f.key(r.i, p, j), Data: tp{Size: int64(j), ETag: "e"}}
		}
		if err := e.Rows(rows); err != nil {
			return err
		}
	}
	return nil
}

func (f *frontier) keys() []string {
	var out []string
	for i := 0; i <= f.siblings; i++ {
		pages := f.sibPages
		if i == 0 {
			pages = f.slowPages
		}
		for p := range pages {
			for j := range f.rows {
				out = append(out, f.key(i, p, j))
			}
		}
	}
	return out
}

// TestSpillKeepsWorkersBusy is the step-3 gate: with a reorder budget far
// below one sibling's output, spill lets the siblings list while the slow
// frontier holds the output back, so the run takes about the frontier's
// time instead of frontier + siblings; the output is identical, and rows in
// memory stay within the budget plus one batch per worker.
func TestSpillKeepsWorkersBusy(t *testing.T) {
	f := &frontier{slowPages: 40, siblings: 56, sibPages: 10, rows: 100, slowDelay: 10 * time.Millisecond, sibDelay: 5 * time.Millisecond}
	want := f.keys()
	const workers, budget = 8, 500
	run := func(opt Options) (time.Duration, Stats) {
		t.Helper()
		c := &collect{}
		start := time.Now()
		st, err := Run[fr, tp](context.Background(), f, c, &testSchema, opt)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(c.keys, want) {
			t.Fatalf("%+v: %d keys, want %d", opt, len(c.keys), len(want))
		}
		for i, d := range c.data { // payloads survive the spill codec
			if d != (tp{Size: int64(i % f.rows), ETag: "e"}) {
				t.Fatalf("%+v: row %d payload %+v", opt, i, d)
			}
		}
		return time.Since(start), st
	}
	plain, pst := run(Options{Workers: workers, Budget: budget})
	dir := t.TempDir()
	for _, compress := range []bool{false, true} {
		spilled, st := run(Options{Workers: workers, Budget: budget, SpillDir: dir, SpillCompress: compress})
		t.Logf("compress=%v: no spill %v (max buffered %d), spill %v (max buffered %d, %d rows in %d frames, %d bytes, %d files)",
			compress, plain, pst.MaxBuffered, spilled, st.MaxBuffered, st.SpillRows, st.SpillFrames, st.SpillBytes, st.SpillFiles)
		if st.SpillRows == 0 {
			t.Fatal("nothing spilled")
		}
		if !raceEnabled && spilled > plain*85/100 { // timing only without the race detector
			t.Fatalf("spill did not keep workers busy: %v vs %v without spill", spilled, plain)
		}
		// D1: the budget, plus one publish batch per worker, plus the
		// frontier path (here one range).
		if limit := int64(budget + workers*publishRows + f.rows); st.MaxBuffered > limit {
			t.Fatalf("max buffered %d > %d", st.MaxBuffered, limit)
		}
		if ents, _ := os.ReadDir(dir); len(ents) != 0 {
			t.Fatalf("spill files left: %d", len(ents))
		}
	}
	if limit := int64(budget + workers*publishRows + f.rows); pst.MaxBuffered > limit {
		t.Fatalf("no spill: max buffered %d > %d", pst.MaxBuffered, limit)
	}
	// The slow frontier makes the drainer wait for it, and without spill
	// the siblings wait on the full budget.
	t.Logf("no spill: frontier wait %v, budget wait %v", pst.FrontierWait, pst.BudgetWait)
	if pst.FrontierWait <= 0 || pst.BudgetWait <= 0 {
		t.Fatalf("no spill: frontier wait %v, budget wait %v", pst.FrontierWait, pst.BudgetWait)
	}
}

// TestSpillCleanedOnFailure: a failing run removes its spill directory.
func TestSpillCleanedOnFailure(t *testing.T) {
	f := &frontier{slowPages: 20, siblings: 20, sibPages: 5, rows: 100, slowDelay: 5 * time.Millisecond}
	dir := t.TempDir()
	c := &collect{fail: 2500}
	if _, err := Run[fr, tp](context.Background(), f, c, &testSchema, Options{Workers: 8, Budget: 300, SpillDir: dir}); err == nil {
		t.Fatal("sink failure not reported")
	}
	if ents, _ := os.ReadDir(dir); len(ents) != 0 {
		t.Fatalf("spill directory left after failure: %d entries", len(ents))
	}
}

// TestSpillDiskBudget: spill stays within its disk budget and backs off to
// waiting when the budget is used, with identical output and memory still
// bounded. A cap well below the spill a free run produces must be hit; a
// free-space reserve larger than any disk refuses every frame, so the run
// behaves as without spill.
func TestSpillDiskBudget(t *testing.T) {
	f := &frontier{slowPages: 40, siblings: 56, sibPages: 10, rows: 100, slowDelay: 5 * time.Millisecond}
	want := f.keys()
	const workers, budget = 8, 500
	run := func(opt Options) Stats {
		t.Helper()
		c := &collect{}
		st, err := Run[fr, tp](context.Background(), f, c, &testSchema, opt)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(c.keys, want) {
			t.Fatalf("%d keys, want %d", len(c.keys), len(want))
		}
		if limit := int64(budget + workers*publishRows + f.rows); st.MaxBuffered > limit {
			t.Fatalf("max buffered %d > %d", st.MaxBuffered, limit)
		}
		return st
	}
	dir := t.TempDir()
	free := run(Options{Workers: workers, Budget: budget, SpillDir: dir})
	if free.SpillRefused != 0 || free.SpillPeak == 0 {
		t.Fatalf("no budget: peak %d, refused %d", free.SpillPeak, free.SpillRefused)
	}
	limit := free.SpillPeak / 4
	capped := run(Options{Workers: workers, Budget: budget, SpillDir: dir, SpillMax: limit})
	t.Logf("peak without a cap %d bytes; with cap %d: peak %d, refused %d, %d rows spilled", free.SpillPeak, limit, capped.SpillPeak, capped.SpillRefused, capped.SpillRows)
	// Trailers are always admitted (a few bytes per file).
	if capped.SpillRefused == 0 || capped.SpillPeak > limit+16*capped.SpillFiles {
		t.Fatalf("cap %d: peak %d, refused %d", limit, capped.SpillPeak, capped.SpillRefused)
	}
	none := run(Options{Workers: workers, Budget: budget, SpillDir: dir, SpillReserve: 1 << 62})
	if none.SpillRows != 0 || none.SpillRefused == 0 {
		t.Fatalf("reserve: %d rows spilled, %d refused", none.SpillRows, none.SpillRefused)
	}
	if ents, _ := os.ReadDir(dir); len(ents) != 0 {
		t.Fatalf("spill files left: %d", len(ents))
	}
}

// TestSpillReadFailures: a frame whose length exceeds the limit, and a
// compressed frame that decodes past the decode limit, are refused before
// their memory is allocated; a reader left open by a failure downstream
// of a replay is closed with the spiller.
func TestSpillReadFailures(t *testing.T) {
	newSp := func(compress bool) *spiller[tp] {
		s, err := newSpiller(testSchema.Codec, t.TempDir(), compress, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	s := newSp(false)
	f := s.file()
	if err := os.WriteFile(f.path, []byte{0xff, 0xff, 0xff, 0x7f, 0, 0, 0, 0}, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := s.next(f); !errors.Is(err, errSpillFrame) || f.f != nil {
		t.Fatalf("oversized length: %v (reader open %v)", err, f.f != nil)
	}
	s.close()

	old := maxSpillDecoded
	maxSpillDecoded = 1 << 20
	s = newSp(true)
	maxSpillDecoded = old
	enc, _ := zstd.NewWriter(nil)
	bomb := enc.EncodeAll(make([]byte, 4<<20), nil) // a few hundred bytes that expand to 4 MiB
	f = s.file()
	if err := os.WriteFile(f.path, s.frame('Z', bomb), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := s.next(f); !errors.Is(err, errSpillFrame) || f.f != nil {
		t.Fatalf("decoded past the limit: %v (reader open %v)", err, f.f != nil)
	}
	s.close()

	s = newSp(false)
	f = s.file()
	rows := []schema.Row[tp]{{Key: "a"}, {Key: "b"}}
	if err := s.write(f, rows[:1], false); err != nil {
		t.Fatal(err)
	}
	if err := s.write(f, rows[1:], true); err != nil {
		t.Fatal(err)
	}
	if got, err := s.next(f); err != nil || len(got) != 1 {
		t.Fatalf("replay: %v %v", got, err)
	}
	// The sink fails here; the run ends without reading the trailer.
	if f.f == nil {
		t.Fatal("the reader is not open mid-file")
	}
	s.close()
	if f.f != nil {
		t.Fatal("close left a spill reader open")
	}
}
