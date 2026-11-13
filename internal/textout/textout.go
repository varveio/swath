// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package textout writes a listing as text rows of its schema: TSV (the
// key and the schema's columns under one header line of their names),
// JSONL (one object per row) or a table for people, to stdout or to one
// file, optionally gzip or zstd compressed; and swath du's per-prefix
// totals in the same formats. A file is
// written to a sibling temporary and renamed over the destination once
// complete, so a visible file is never a partial one. Text output is
// one-shot: it keeps no journal and cannot be resumed.
package textout

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"

	"github.com/varveio/swath/internal/dataset"
)

// Formats.
const (
	TSV   = "tsv"
	JSONL = "jsonl"
	Table = "table"
)

// Compressions.
const (
	None = "none"
	Gzip = "gzip"
	Zstd = "zstd"
)

// Config is where and how the rows go.
type Config struct {
	Format      string    // TSV, JSONL or Table
	Path        string    // the destination file; "" for Stdout
	Stdout      io.Writer // the stream when Path is ""
	Compression string    // None (""), Gzip or Zstd
	// Columns, if set, are the TSV and JSONL columns by name, in order
	// (the key's is KeyColumn); nil for the key and every schema column.
	Columns []string
	// Loc is the table's time zone (nil: time.Local); TSV and JSONL are
	// UTC.
	Loc *time.Location
	// Formatters is the goroutines formatting rows at once; below 2 the
	// order stage formats them itself. Account, if set, is told the row
	// bytes (schema.RowsBytes) of batches handed to them and not yet
	// written (negative: released), as the order stage's shared budget.
	Formatters int
	Account    func(int64)
	// Jobs is the zstd jobs compressed at once (admitted by the caller
	// against JobBytes each); below 2, one synchronous encoder. Gzip is one
	// stream whatever it is.
	Jobs int
	// Fail, if set, is called with the first write failure when it
	// happens: the formatters' (the sequencer's write to the compressor or
	// the destination) and the destination's own (a parallel compressor's
	// background flusher included). The order stage may be waiting for a
	// page and the sequencer idle, so nothing else would see it; the
	// caller stops the run with it (as parquetout.Options.Fail). It is
	// called once, with whichever is seen first.
	Fail func(error)
}

// JobBytes is the memory budgeted per concurrent zstd job for admission:
// one job's input section (jobSize, four windows: 4 MiB), its output
// buffer (half the input, 2 MiB), the overlap prefix the next job takes
// (an eighth of a window), and the encoder that compresses it (history of
// two windows, 2 MiB, plus its match tables, about 1.3 MiB), with the
// encoder's queue holding up to one more section per job. Measured at 5
// to 13 MiB of allocation per job (klauspost v1.19.1, level default: 1 to
// 8 jobs over 80 MB behind a slow writer, TestJobsWithinAdmission; 2 and 4
// over a 400 MB TSV); rounded up.
const JobBytes = 16 << 20

// zstdWindow is the zstd window. Parallel jobs are sized at four windows
// and each encoder keeps two, so the 8 MiB default would cost about
// 110 MiB a job; on sorted listings matches are between neighbouring
// keys, and a 1 MiB window measured the same ratio (4.42 against 4.42 on
// a 400 MB TSV with per-object ETags) and the same speed. A decoder needs
// less memory too.
const zstdWindow = 1 << 20

// bufferBytes is the row buffer before the compressor or the file: rows
// are handed on in writes of this size.
const bufferBytes = 256 << 10

// Out is one text destination: open, written by a Sink, then committed or
// aborted.
type Out struct {
	cfg   Config
	f     *dataset.Pending // the file's replacement (nil for the stream)
	comp  io.WriteCloser   // the compressor, if any
	count *counter         // bytes delivered to the file or stream
	in    *counter         // bytes handed to the compressor (or the file), and the time it took
	bw    *bufio.Writer
	rows  int64
	flush bool    // flush after every batch: a stream's reader sees rows as they come
	jobs  int     // zstd jobs at once (1: synchronous; 0 without zstd)
	pipe  stopper // the sink's formatters, if parallel: drained by Commit, stopped by Abort
	sink  time.Duration
}

// errAborted is what a write after Abort gets: the compressor's workers
// stop at their next write instead of compressing into a file to be
// removed.
var errAborted = errors.New("text output aborted")

// counter counts the bytes written through it and the time the writes
// took; once stopped, a write fails at once. The compressor's flusher
// writes from its own goroutine: the counts are atomic, read after it is
// joined (Commit, Abort) or live.
type counter struct {
	w       io.Writer
	n, wait atomic.Int64
	stopped atomic.Bool
	// err is the first failure of w itself (never errAborted); fail, if
	// set, is told it once. On the destination this is the one place a
	// parallel compressor's background write failure is seen when it
	// happens: the sequencer may be idle and write nothing more.
	err  atomic.Pointer[error]
	fail func(error)
}

func (c *counter) Write(p []byte) (int, error) {
	if c.stopped.Load() {
		return 0, errAborted
	}
	t := time.Now()
	n, err := c.w.Write(p)
	c.wait.Add(int64(time.Since(t)))
	c.n.Add(int64(n))
	if err != nil && c.err.CompareAndSwap(nil, &err) && c.fail != nil {
		c.fail(err)
	}
	return n, err
}

// failed is w's first failure, if any.
func (c *counter) failed() error {
	if e := c.err.Load(); e != nil {
		return *e
	}
	return nil
}

// Open prepares the destination: for a file, a temporary beside it.
func Open(cfg Config) (*Out, error) {
	switch cfg.Format {
	case TSV, JSONL, Table:
	default:
		return nil, fmt.Errorf("text output: unknown format %q", cfg.Format)
	}
	o := &Out{cfg: cfg}
	if f := cfg.Fail; f != nil {
		var once sync.Once
		o.cfg.Fail = func(err error) { once.Do(func() { f(err) }) }
	}
	var dst io.Writer
	if cfg.Path == "" {
		if cfg.Stdout == nil {
			return nil, errors.New("text output: no stream")
		}
		dst, o.flush = cfg.Stdout, true
	} else {
		if fi, err := os.Stat(cfg.Path); err == nil && fi.IsDir() {
			return nil, fmt.Errorf("text output: %s is a directory", cfg.Path)
		}
		f, err := dataset.NewPending(cfg.Path)
		if err != nil {
			return nil, fmt.Errorf("text output: %w", err)
		}
		o.f, dst = f, f
	}
	o.count = &counter{w: dst, fail: o.cfg.Fail}
	o.in = o.count
	switch cfg.Compression {
	case "", None:
	case Gzip:
		o.comp = gzip.NewWriter(o.count)
	case Zstd:
		z, err := zstd.NewWriter(o.count, zstdOptions(cfg.Jobs)...)
		if err != nil {
			o.Abort()
			return nil, err
		}
		o.comp, o.jobs = z, max(1, cfg.Jobs)
	default:
		o.Abort()
		return nil, fmt.Errorf("text output: unknown compression %q", cfg.Compression)
	}
	if o.comp != nil {
		o.in = &counter{w: o.comp}
	}
	o.bw = bufio.NewWriterSize(o.in, bufferBytes)
	return o, nil
}

// zstdOptions is the encoder for jobs concurrent jobs. In klauspost
// v1.19.1 a stream is compressed in parallel only with concurrent blocks
// and a concurrency above one (NewWriter turns them off otherwise): the
// input is cut into sections of four windows, each compressed by its own
// goroutine with the previous section's tail as its prefix, and written
// in order as one frame. Below two jobs the encoder is synchronous: no
// goroutine, the drainer compresses as it writes.
func zstdOptions(jobs int) []zstd.EOption {
	o := []zstd.EOption{zstd.WithWindowSize(zstdWindow)}
	if jobs < 2 {
		return append(o, zstd.WithEncoderConcurrency(1))
	}
	return append(o, zstd.WithEncoderConcurrency(jobs), zstd.WithConcurrentBlocks(true))
}

// Paths are the paths a file output writes: the file and its temporary
// (none for the stream).
func (o *Out) Paths() []string {
	if o.f == nil {
		return nil
	}
	return []string{o.cfg.Path, o.f.Temp()}
}

// Format is the output's format.
func (o *Out) Format() string { return o.cfg.Format }

// Bytes is the bytes delivered so far (compressed, header included).
func (o *Out) Bytes() int64 { return o.count.n.Load() }

// Rows is the rows written.
func (o *Out) Rows() int64 { return o.rows }

// Stats is what the text sink did, for the run report.
type Stats struct {
	// SinkWait is the time the order stage spent in the sink: rows waiting
	// on the writer (with one formatter, formatting them and OutputWait;
	// with more, handing batches to the formatters' queue).
	SinkWait time.Duration
	// OutputWait is the time spent handing bytes on: to the compressor (a
	// synchronous encoder compressing, or a parallel one's queue full), or
	// to the file or stream when uncompressed; by the order stage with one
	// formatter, by the sequencer with more.
	OutputWait time.Duration
	// In is the bytes formatted (the uncompressed output), Out the bytes
	// delivered to the file or stream.
	In, Out int64
	// Jobs is the zstd jobs compressed at once (0 without zstd);
	// Formatters the goroutines formatting rows (1: the order stage).
	Jobs, Formatters int
}

// Stats reads the sink's statistics; after Commit or Abort they are final.
func (o *Out) Stats() Stats {
	return Stats{SinkWait: o.sink, OutputWait: time.Duration(o.in.wait.Load()), In: o.in.n.Load() + int64(o.bw.Buffered()),
		Out: o.count.n.Load(), Jobs: o.jobs, Formatters: max(1, o.cfg.Formatters)}
}

// endBatch ends one sink batch: a stream is flushed, so its reader sees the
// rows (and a closed reader is noticed) promptly.
func (o *Out) endBatch() error {
	if !o.flush {
		return nil
	}
	if err := o.bw.Flush(); err != nil {
		return err
	}
	if f, ok := o.comp.(interface{ Flush() error }); ok {
		return f.Flush()
	}
	return nil
}

// Commit completes the output: the buffer and the compressor are flushed
// and closed, and a file is synced and renamed over its destination.
func (o *Out) Commit() error {
	var err error
	if o.pipe != nil {
		err = o.pipe.finish(false)
	}
	if err == nil {
		err = o.bw.Flush()
	}
	if o.comp != nil {
		if cerr := o.comp.Close(); err == nil {
			err = cerr
		}
	}
	if o.f == nil {
		return err
	}
	if err != nil {
		o.f.Abort()
		return fmt.Errorf("text output: %w", err)
	}
	// Durable, as a dataset's commit: a visible file is complete.
	if err := o.f.Commit(dataset.Durable); err != nil {
		return fmt.Errorf("text output: %w", err)
	}
	return nil
}

// Failed is the output's first write failure, if any: the destination's
// own (the original error, a background compressor's included), else the
// formatters'. Abort keeps it, so a run stopped by it reports it rather
// than the stop or the abort.
func (o *Out) Failed() error {
	if err := o.count.failed(); err != nil {
		return err
	}
	if o.pipe != nil {
		return o.pipe.failed()
	}
	return nil
}

// Abort drops the output: the compressor is closed (its goroutines
// joined; for a file, its remaining writes refused rather than made), a
// file's temporary is removed and the destination left as it was.
func (o *Out) Abort() {
	if o.pipe != nil {
		_ = o.pipe.finish(true)
	}
	if o.f != nil {
		o.count.stopped.Store(true)
	}
	if o.comp != nil {
		_ = o.comp.Close()
	}
	if o.f != nil {
		o.f.Abort()
	}
}

// appendTime appends epoch microseconds as RFC 3339 in UTC with six
// fraction digits (2026-01-02T03:04:05.000000Z): every time column's form
// in TSV and JSONL, rows' and totals'.
func appendTime(dst []byte, micros int64) []byte {
	return time.UnixMicro(micros).UTC().AppendFormat(dst, "2006-01-02T15:04:05.000000Z07:00")
}

// tableTime is a table's time layout, to the second in the reader's zone.
const tableTime = "2006-01-02 15:04:05"

func appendLocal(dst []byte, micros int64, loc *time.Location) []byte {
	return time.UnixMicro(micros).In(loc).AppendFormat(dst, tableTime)
}

// appendJSONString appends s, which is UTF-8, as a JSON string, escaped as
// jq prints it: \" and \\, \b \f \n \r \t, any other byte below 0x20 and
// 0x7f as \u00xx, everything else as is.
func appendJSONString(dst []byte, s string) []byte {
	const hex = "0123456789abcdef"
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 0x20 && c != 0x7f && c != '\\' && c != '"' {
			continue
		}
		dst = append(dst, s[start:i]...)
		start = i + 1
		switch c {
		case '\\', '"':
			dst = append(dst, '\\', c)
		case '\b':
			dst = append(dst, '\\', 'b')
		case '\f':
			dst = append(dst, '\\', 'f')
		case '\n':
			dst = append(dst, '\\', 'n')
		case '\r':
			dst = append(dst, '\\', 'r')
		case '\t':
			dst = append(dst, '\\', 't')
		default: // below 0x20, or 0x7f
			dst = append(dst, '\\', 'u', '0', '0', hex[c>>4], hex[c&15])
		}
	}
	return append(append(dst, s[start:]...), '"')
}

// appendTSV appends s as a TSV field. The grammar, of rows' and totals'
// fields and of the tables' names: a row is one line, its fields separated
// by tabs. In a field \\ is a backslash, \t a tab, \n a line feed, \r a
// carriage return and \xHH (two lowercase hex digits) the byte HH, written
// for every other byte below 0x20, for DEL (0x7f) and for every byte that
// is not part of valid UTF-8; every other byte stands for itself. A field
// that is exactly \N is null; an empty field is the empty string. Any
// bytes round-trip (UnescapeTSV), and a terminal is never sent a control
// byte.
//
// Bytes that need no escape are appended a run at a time: the row
// formatter runs on the order stage's drainer, so per-byte appends cost
// listing throughput.
func appendTSV(dst []byte, s string) []byte {
	const hex = "0123456789abcdef"
	start := 0
	for i := 0; i < len(s); {
		c := s[i]
		if c >= utf8.RuneSelf {
			if r, n := utf8.DecodeRuneInString(s[i:]); r != utf8.RuneError || n > 1 {
				i += n
				continue
			}
		} else if c >= 0x20 && c != 0x7f && c != '\\' {
			i++
			continue
		}
		dst = append(dst, s[start:i]...)
		switch c {
		case '\\':
			dst = append(dst, '\\', '\\')
		case '\t':
			dst = append(dst, '\\', 't')
		case '\n':
			dst = append(dst, '\\', 'n')
		case '\r':
			dst = append(dst, '\\', 'r')
		default:
			dst = append(dst, '\\', 'x', hex[c>>4], hex[c&15])
		}
		i++
		start = i
	}
	return append(dst, s[start:]...)
}

// UnescapeTSV decodes one TSV field (appendTSV's grammar): its bytes, and
// false for a null (\N). It refuses an escape the grammar does not write.
func UnescapeTSV(field string) (string, bool, error) {
	if field == nullTSV {
		return "", false, nil
	}
	i := strings.IndexByte(field, '\\')
	if i < 0 {
		return field, true, nil
	}
	out := []byte(field[:i])
	for ; i < len(field); i++ {
		c := field[i]
		if c != '\\' {
			out = append(out, c)
			continue
		}
		if i+1 == len(field) {
			return "", false, fmt.Errorf("TSV field %q: a trailing backslash", field)
		}
		i++
		switch field[i] {
		case '\\':
			out = append(out, '\\')
		case 't':
			out = append(out, '\t')
		case 'n':
			out = append(out, '\n')
		case 'r':
			out = append(out, '\r')
		case 'x':
			if i+2 >= len(field) {
				return "", false, fmt.Errorf(`TSV field %q: a short \x escape`, field)
			}
			v, err := strconv.ParseUint(field[i+1:i+3], 16, 8)
			if err != nil {
				return "", false, fmt.Errorf("TSV field %q: %w", field, err)
			}
			out = append(out, byte(v))
			i += 2
		default:
			return "", false, fmt.Errorf(`TSV field %q: unknown escape \%c`, field, field[i])
		}
	}
	return string(out), true, nil
}
