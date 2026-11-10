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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/varveio/swath/go/internal/faults"
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
	rows = append(rows, schema.Row[obj]{Key: "zz/tab\there\nnl\\\x1b\x7fµ", Data: obj{size: 3}},
		schema.Row[obj]{Key: "zz/\xff\xfe", Data: obj{size: 4, owner: "o\xc3"}})
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
				if b.n.Load() != 0 || (b.peak.Load() == 0) != (format == Table) { // the table is formatted serially
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

// TestParallelFailureReportedAtOnce: the sequencer's write fails while no
// further batch is sent (the drainer waits for a page). Config.Fail
// receives the failure at once, and Failed keeps it after Abort, so the
// run reports the output's error rather than whatever stops it later.
func TestParallelFailureReportedAtOnce(t *testing.T) {
	got := make(chan error, 2)
	w := &failWriter{}
	w.armed.Store(true)
	o, err := Open(Config{Format: TSV, Stdout: w, Formatters: 2, Fail: func(err error) { got <- err }})
	if err != nil {
		t.Fatal(err)
	}
	k, err := NewSink(o, testSchema)
	if err != nil {
		t.Fatal(err)
	}
	if err := k.Write(context.Background(), bigRows(30_000)[:1]); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-got:
		if !errors.Is(err, errDisk) {
			t.Fatalf("Fail got %v, want the writer's", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the write failure was not reported while no batch followed")
	}
	o.Abort()
	if err := o.Failed(); !errors.Is(err, errDisk) {
		t.Fatalf("Failed after Abort: %v", err)
	}
	if len(got) != 0 {
		t.Fatal("Fail called twice")
	}
}

// TestIdleCompressorFailureReported: a .zst file with two compression
// jobs accepts a few sections, then the disk fails in the compressor's
// background flusher while nothing more is written (the sequencer idle,
// the drainer waiting for a page). Config.Fail receives the disk's own
// error at once, and Failed keeps it after Abort, not the abort's.
func TestIdleCompressorFailureReported(t *testing.T) {
	dir := t.TempDir()
	release := make(chan struct{})
	var writes atomic.Int64
	defer faults.SetIO(func(op, name string, b []byte) (int, error) {
		// The frame header, written by the sequencer's own call, passes;
		// the jobs' output (the flusher's) stalls, then fails: every
		// write is handed on before the failure.
		if op == "write" && strings.Contains(name, "idle.tsv.zst") && writes.Add(1) > 1 {
			<-release
			return 0, errDisk
		}
		return 0, nil
	})()
	got := make(chan error, 4)
	o, err := Open(Config{Format: TSV, Path: filepath.Join(dir, "idle.tsv.zst"), Compression: Zstd, Jobs: 2, Formatters: 2,
		Fail: func(err error) { got <- err }})
	if err != nil {
		t.Fatal(err)
	}
	k, err := NewSink(o, testSchema)
	if err != nil {
		t.Fatal(err)
	}
	// About 5 MB: one 4 MiB section handed to a job (its output stalls
	// at the disk), the rest held in the compressor's buffer; every Write
	// returns.
	wrote := make(chan error, 1)
	go func() {
		for _, seg := range bigRows(42_000) {
			if err := k.Write(context.Background(), [][]schema.Row[obj]{seg}); err != nil {
				wrote <- err
				return
			}
		}
		wrote <- nil
	}()
	select {
	case err := <-wrote:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatal("the writes blocked behind the stalled disk: the test proves nothing")
	}
	for prev := int64(-1); prev != o.in.n.Load(); { // the sequencer has handed everything on and is idle
		prev = o.in.n.Load()
		time.Sleep(50 * time.Millisecond)
	}
	close(release)
	select {
	case err := <-got:
		if !errors.Is(err, errDisk) {
			t.Fatalf("Fail got %v, want the disk's", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the compressor's write failure was not reported while nothing more was written")
	}
	o.Abort()
	if err := o.Failed(); !errors.Is(err, errDisk) {
		t.Fatalf("Failed after Abort: %v, want the disk's error", err)
	}
}
