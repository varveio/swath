// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package textout

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/klauspost/compress/zstd"

	"github.com/varveio/swath/go/internal/schema"
)

// randomBatches cuts rows into Write calls of 1 to 3 segments of 0 to 3,000
// rows each.
func randomBatches(r *rand.Rand, rows []schema.Row[obj]) [][][]schema.Row[obj] {
	var calls [][][]schema.Row[obj]
	for len(rows) > 0 {
		var call [][]schema.Row[obj]
		for range 1 + r.IntN(3) {
			n := min(len(rows), r.IntN(3001))
			call = append(call, rows[:n])
			rows = rows[n:]
		}
		calls = append(calls, call)
	}
	return calls
}

// balance is an Account that checks every byte charged is released.
type balance struct{ n, peak atomic.Int64 }

func (b *balance) add(d int64) {
	if v := b.n.Add(d); v > b.peak.Load() {
		b.peak.Store(v)
	}
}

// emit writes calls through a sink on cfg and commits; it returns the
// destination's bytes (cfg.Path's or the stream's).
func emit(t *testing.T, cfg Config, calls [][][]schema.Row[obj]) []byte {
	t.Helper()
	var buf bytes.Buffer
	if cfg.Path == "" {
		cfg.Stdout = &buf
	}
	o, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	k, err := NewSink(o, testSchema)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range calls {
		if err := k.Write(context.Background(), c); err != nil {
			t.Fatal(err)
		}
	}
	if err := o.Commit(); err != nil {
		t.Fatal(err)
	}
	if cfg.Path != "" {
		return must(os.ReadFile(cfg.Path))
	}
	return buf.Bytes()
}

// TestParallelFormatEqualsSerial: rows formatted by 2 to 8 goroutines,
// under random batch sizes, in every format, plain and compressed, are
// byte for byte what the order stage formatting them itself writes; every
// byte charged to the budget is released and the row count is exact.
func TestParallelFormatEqualsSerial(t *testing.T) {
	var rows []schema.Row[obj]
	for _, seg := range bigRows(60_000) {
		rows = append(rows, seg...)
	}
	rows = append(rows, schema.Row[obj]{Key: "zz/tab\there\nnl\\\x1b\x7fµ", Data: obj{size: 3}})
	r := rand.New(rand.NewPCG(1, 2))
	dir := t.TempDir()
	for _, format := range []string{TSV, JSONL, Table} {
		want := emit(t, Config{Format: format}, randomBatches(r, rows))
		for _, n := range []int{2, 3, 8} {
			for _, comp := range []string{None, Zstd} {
				var b balance
				path := filepath.Join(dir, fmt.Sprintf("out-%s-%d.%s", format, n, comp))
				got := emit(t, Config{Format: format, Path: path, Compression: comp, Jobs: 2, Formatters: n, Account: b.add},
					randomBatches(r, rows))
				if comp == Zstd {
					d, _ := zstd.NewReader(nil)
					got, _ = d.DecodeAll(got, nil)
					d.Close()
				}
				if !bytes.Equal(got, want) {
					t.Fatalf("%s, %d formatters, %s: %d bytes, want the serial %d", format, n, comp, len(got), len(want))
				}
				if b.n.Load() != 0 || b.peak.Load() == 0 {
					t.Fatalf("%s, %d formatters: %d bytes left charged (peak %d)", format, n, b.n.Load(), b.peak.Load())
				}
			}
		}
	}
}

// TestParallelAbortAndFailure: an output aborted with batches queued and
// formatters busy joins them and the compressor, leaves the destination as
// it was and releases every charged byte; a destination failing
// mid-stream fails a later Write or the commit with its error, with the
// same guarantees.
func TestParallelAbortAndFailure(t *testing.T) {
	segs := bigRows(150_000)
	dir := t.TempDir()
	path := filepath.Join(dir, "keep.tsv.zst")
	if err := os.WriteFile(path, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	base := runtime.NumGoroutine()
	var b balance
	o, err := Open(Config{Format: TSV, Path: path, Compression: Zstd, Jobs: 4, Formatters: 4, Account: b.add})
	if err != nil {
		t.Fatal(err)
	}
	k, err := NewSink(o, testSchema)
	if err != nil {
		t.Fatal(err)
	}
	for _, seg := range segs {
		if err := k.Write(context.Background(), [][]schema.Row[obj]{seg}); err != nil {
			t.Fatal(err)
		}
	}
	o.Abort()
	settled(t, base)
	if got, _ := os.ReadFile(path); string(got) != "old" {
		t.Fatalf("destination replaced: %d bytes", len(got))
	}
	if ents, _ := os.ReadDir(dir); len(ents) != 1 || b.n.Load() != 0 {
		t.Fatalf("left %v, %d bytes charged", ents, b.n.Load())
	}

	fail := &failWriter{limit: 1 << 20}
	var fb balance
	o, err = Open(Config{Format: TSV, Stdout: fail, Formatters: 4, Account: fb.add})
	if err != nil {
		t.Fatal(err)
	}
	k, _ = NewSink(o, testSchema)
	err = nil
	for _, seg := range segs {
		if err = k.Write(context.Background(), [][]schema.Row[obj]{seg}); err != nil {
			break
		}
	}
	if err == nil {
		err = o.Commit()
	} else {
		o.Abort()
	}
	if !errors.Is(err, errDisk) {
		t.Fatalf("err %v, want the writer's", err)
	}
	settled(t, base)
	if fb.n.Load() != 0 {
		t.Fatalf("%d bytes left charged after a failure", fb.n.Load())
	}
}
