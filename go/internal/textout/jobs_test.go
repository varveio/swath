// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package textout

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"

	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/schema"
)

// bigRows is n rows shaped as a real listing's (sorted keys sharing
// prefixes, a per-object ETag), in batches of 1,000: about 120 bytes of
// TSV a row, so 200,000 rows are several parallel zstd jobs (4 MiB each).
func bigRows(n int) [][]schema.Row[obj] {
	var segs [][]schema.Row[obj]
	for i := 0; i < n; i += 1000 {
		seg := make([]schema.Row[obj], 0, 1000)
		for j := i; j < min(n, i+1000); j++ {
			key := fmt.Sprintf("v1.0/btc/transactions/date=2024-%02d-%02d/part-%05d.snappy.parquet", 1+j/20000%12, 1+j/1000%28, j)
			h := md5.Sum([]byte(key))
			seg = append(seg, schema.Row[obj]{Key: key, Data: obj{size: int64(j) * 37, mtime: 1_700_000_000_000_000 + int64(j)*1_000_000,
				etag: hex.EncodeToString(h[:]), class: "STANDARD"}})
		}
		segs = append(segs, seg)
	}
	return segs
}

// writeAll writes segs through a new Out on cfg (to w when cfg.Path is
// empty) and commits it.
func writeAll(t *testing.T, cfg Config, w io.Writer, segs [][]schema.Row[obj]) (*Out, error) {
	t.Helper()
	if cfg.Path == "" {
		cfg.Stdout = w
	}
	o, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	k, err := NewSink(o, testSchema)
	if err != nil {
		t.Fatal(err)
	}
	for _, seg := range segs {
		if err := k.Write(context.Background(), [][]schema.Row[obj]{seg}); err != nil {
			o.Abort()
			return o, err
		}
	}
	return o, o.Commit()
}

// settled waits for the goroutine count to fall back to base: the
// compressor's workers and flusher are joined.
func settled(t *testing.T, base int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > base {
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<16)
			t.Fatalf("%d goroutines, %d before: the compressor's are not joined\n%s", runtime.NumGoroutine(), base, buf[:runtime.Stack(buf, true)])
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestCompressedEqualsPlain: a file compressed by one synchronous zstd
// encoder, by parallel jobs, or by gzip decodes to exactly the bytes of the
// uncompressed output, and the report's bytes in and out are the plain and
// the delivered sizes; the jobs run is one frame.
func TestCompressedEqualsPlain(t *testing.T) {
	segs := bigRows(200_000)
	var plain bytes.Buffer
	if _, err := writeAll(t, Config{Format: TSV}, &plain, segs); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	for _, c := range []struct {
		comp string
		jobs int
	}{{Zstd, 1}, {Zstd, 4}, {Gzip, 4}} {
		path := filepath.Join(dir, fmt.Sprintf("out-%d.tsv.%s", c.jobs, c.comp))
		o, err := writeAll(t, Config{Format: TSV, Path: path, Compression: c.comp, Jobs: c.jobs}, nil, segs)
		if err != nil {
			t.Fatal(err)
		}
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		var r io.Reader
		if c.comp == Gzip {
			r, err = gzip.NewReader(f)
		} else {
			var d *zstd.Decoder
			d, err = zstd.NewReader(f)
			r = d
		}
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(r)
		f.Close()
		if err != nil || !bytes.Equal(got, plain.Bytes()) {
			t.Fatalf("%s jobs %d: %d bytes decoded (%v), want the plain %d", c.comp, c.jobs, len(got), err, plain.Len())
		}
		st, fi := o.Stats(), must(os.Stat(path))
		if st.In != int64(plain.Len()) || st.Out != fi.Size() || o.Bytes() != fi.Size() {
			t.Fatalf("%s jobs %d: stats in %d out %d, want %d and %d", c.comp, c.jobs, st.In, st.Out, plain.Len(), fi.Size())
		}
		if want := map[string]int{Gzip: 0, Zstd: c.jobs}[c.comp]; st.Jobs != want {
			t.Fatalf("%s: jobs %d, want %d", c.comp, st.Jobs, want)
		}
		if st.SinkWait <= 0 || st.OutputWait > st.SinkWait {
			t.Fatalf("sink wait %v, output wait %v", st.SinkWait, st.OutputWait)
		}
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// slowWriter sleeps on every write.
type slowWriter struct {
	bytes.Buffer
	d time.Duration
}

func (w *slowWriter) Write(p []byte) (int, error) { time.Sleep(w.d); return w.Buffer.Write(p) }

// failWriter fails every write once armed (or past limit bytes).
type failWriter struct {
	armed atomic.Bool
	n     atomic.Int64
	limit int64
}

var errDisk = errors.New("disk full")

func (w *failWriter) Write(p []byte) (int, error) {
	if w.armed.Load() || (w.limit > 0 && w.n.Load()+int64(len(p)) > w.limit) {
		return 0, errDisk
	}
	w.n.Add(int64(len(p)))
	return len(p), nil
}

// TestSlowAndFailingWriter: parallel jobs behind a slow destination
// deliver every byte in order (one frame, decoding to the plain rows); a
// destination that fails mid-stream fails a later Write or the commit with
// its error, and either way the compressor's goroutines are joined.
func TestSlowAndFailingWriter(t *testing.T) {
	segs := bigRows(100_000)
	var plain bytes.Buffer
	if _, err := writeAll(t, Config{Format: JSONL}, &plain, segs); err != nil {
		t.Fatal(err)
	}
	base := runtime.NumGoroutine()
	slow := &slowWriter{d: 2 * time.Millisecond}
	o, err := writeAll(t, Config{Format: JSONL, Compression: Zstd, Jobs: 4}, slow, segs)
	if err != nil {
		t.Fatal(err)
	}
	d, _ := zstd.NewReader(nil)
	defer d.Close()
	if got, err := d.DecodeAll(slow.Bytes(), nil); err != nil || !bytes.Equal(got, plain.Bytes()) {
		t.Fatalf("slow writer: %d bytes decoded (%v), want %d", len(got), err, plain.Len())
	}
	// OutputWait sums each hand-off's wait, and Windows' clock ticks about
	// every 0.5 ms: short waits read 0 there.
	if st := o.Stats(); st.OutputWait <= 0 && runtime.GOOS != "windows" {
		t.Fatalf("no output wait behind a slow writer: %+v", st)
	}
	settled(t, base)
	for _, jobs := range []int{1, 4} {
		fail := &failWriter{limit: 1 << 20}
		_, err := writeAll(t, Config{Format: JSONL, Compression: Zstd, Jobs: jobs}, fail, segs)
		if !errors.Is(err, errDisk) {
			t.Fatalf("jobs %d: err %v, want the writer's", jobs, err)
		}
		settled(t, base)
	}
}

// TestAbortJoinsAndKeepsDestination: an output aborted mid-stream (the run
// failed or was cancelled) with parallel jobs running joins the
// compressor's goroutines, leaves the destination as it was and removes
// its temporary; a destination write failure at the final flush (the
// commit) surfaces from Commit with the same guarantees.
func TestAbortJoinsAndKeepsDestination(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "keep.tsv.zst")
	if err := os.WriteFile(path, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	check := func(what string) {
		t.Helper()
		if b, _ := os.ReadFile(path); string(b) != "old" {
			t.Fatalf("%s: destination replaced: %q", what, b)
		}
		if ents, _ := os.ReadDir(dir); len(ents) != 1 {
			t.Fatalf("%s: temporaries left: %v", what, ents)
		}
	}
	base := runtime.NumGoroutine()
	o, err := Open(Config{Format: TSV, Path: path, Compression: Zstd, Jobs: 4})
	if err != nil {
		t.Fatal(err)
	}
	k, err := NewSink(o, testSchema)
	if err != nil {
		t.Fatal(err)
	}
	if err := k.Write(context.Background(), bigRows(150_000)); err != nil {
		t.Fatal(err)
	}
	if runtime.NumGoroutine() <= base {
		t.Fatal("no compression jobs running: the test proves nothing")
	}
	o.Abort()
	settled(t, base)
	check("abort")

	var armed atomic.Bool
	defer faults.SetIO(func(op, name string, b []byte) (int, error) {
		if op == "write" && armed.Load() && strings.Contains(name, "keep.tsv.zst") {
			return 0, errDisk
		}
		return 0, nil
	})()
	for _, jobs := range []int{1, 4} {
		o, err := Open(Config{Format: TSV, Path: path, Compression: Zstd, Jobs: jobs})
		if err != nil {
			t.Fatal(err)
		}
		k, err := NewSink(o, testSchema)
		if err != nil {
			t.Fatal(err)
		}
		if err := k.Write(context.Background(), bigRows(1000)); err != nil { // below one block: all of it waits for the commit
			t.Fatal(err)
		}
		armed.Store(true)
		if err := o.Commit(); !errors.Is(err, errDisk) {
			t.Fatalf("jobs %d: commit %v, want the final flush's error", jobs, err)
		}
		armed.Store(false)
		settled(t, base)
		check(fmt.Sprintf("final flush, jobs %d", jobs))
	}
}

// BenchmarkSink is the order stage's cost of handing rows to a TSV file
// compressed with zstd: one synchronous encoder against parallel jobs.
func BenchmarkSink(b *testing.B) {
	segs := bigRows(100_000)
	for _, jobs := range []int{1, 4} {
		b.Run(fmt.Sprintf("jobs=%d", jobs), func(b *testing.B) {
			dir := b.TempDir()
			for i := 0; i < b.N; i++ {
				o, err := Open(Config{Format: TSV, Path: filepath.Join(dir, "out.tsv.zst"), Compression: Zstd, Jobs: jobs})
				if err != nil {
					b.Fatal(err)
				}
				k, _ := NewSink(o, testSchema)
				for _, seg := range segs {
					if err := k.Write(context.Background(), [][]schema.Row[obj]{seg}); err != nil {
						b.Fatal(err)
					}
				}
				if err := o.Commit(); err != nil {
					b.Fatal(err)
				}
				b.SetBytes(o.Stats().In)
			}
		})
	}
}

// slowDiscard discards at 50 MB/s, below the jobs' rate: their queue
// fills.
type slowDiscard struct{}

func (slowDiscard) Write(p []byte) (int, error) {
	time.Sleep(time.Duration(len(p)) * 20 * time.Nanosecond)
	return len(p), nil
}

// TestJobsWithinAdmission: the compressor's memory over a stream of many
// jobs, behind a destination slow enough that its queue stays full, stays
// within the JobBytes per job the caller admits it for: all it allocates
// (buffers reused through its pools, encoders kept) is measured, which
// bounds what it holds live.
func TestJobsWithinAdmission(t *testing.T) {
	if raceEnabled {
		t.Skip("allocation counts under the race detector, which drops pooled buffers")
	}
	segs := bigRows(600_000) // about 75 MB of TSV: 18 jobs of 4 MiB
	for _, jobs := range []int{1, 2, 4, 8} {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		o, err := writeAll(t, Config{Format: TSV, Compression: Zstd, Jobs: jobs}, slowDiscard{}, segs)
		if err != nil {
			t.Fatal(err)
		}
		runtime.ReadMemStats(&after)
		alloc := int64(after.TotalAlloc - before.TotalAlloc)
		t.Logf("jobs %d: %d MiB allocated for %d MiB in (%d MiB admitted)", jobs, alloc>>20, o.Stats().In>>20, int64(jobs)*JobBytes>>20)
		if bound := int64(jobs)*JobBytes + bufferBytes + 4<<20; alloc > bound {
			t.Fatalf("jobs %d allocated %d MiB, over the %d MiB admitted", jobs, alloc>>20, bound>>20)
		}
	}
}
