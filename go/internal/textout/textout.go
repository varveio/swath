// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package textout writes a listing as text rows shaped as the S3 API's
// object: JSONL (one ListObjectsV2 Contents object per line), TSV (the
// same fields as columns, under one header line) or the aws s3 ls table,
// to stdout or to one file, optionally gzip or zstd compressed. A file is
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
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"

	"github.com/varveio/swath/go/internal/dataset"
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
}

func (c *counter) Write(p []byte) (int, error) {
	if c.stopped.Load() {
		return 0, errAborted
	}
	t := time.Now()
	n, err := c.w.Write(p)
	c.wait.Add(int64(time.Since(t)))
	c.n.Add(int64(n))
	return n, err
}

// Open prepares the destination: for a file, a temporary beside it.
func Open(cfg Config) (*Out, error) {
	switch cfg.Format {
	case TSV, JSONL, Table:
	default:
		return nil, fmt.Errorf("text output: unknown format %q", cfg.Format)
	}
	o := &Out{cfg: cfg}
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
	o.count = &counter{w: dst}
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

// appendISO appends epoch microseconds as RFC 3339 in UTC the way the
// AWS CLI prints a LastModified: seconds, then .ffffff only when the
// fraction is not zero, then +00:00.
func appendISO(dst []byte, micros int64) []byte {
	t := time.UnixMicro(micros).UTC()
	dst = t.AppendFormat(dst, "2006-01-02T15:04:05")
	if us := t.Nanosecond() / 1000; us != 0 {
		s := strconv.Itoa(us)
		dst = append(dst, ".000000"[:7-len(s)]...)
		dst = append(dst, s...)
	}
	return append(dst, "+00:00"...)
}

// appendJSON appends s as a JSON string, escaped as jq prints it: \" and
// \\, \b \f \n \r \t, any other byte below 0x20 and 0x7f as \u00xx,
// everything else (UTF-8) as is.
func appendJSON(dst []byte, s string) []byte {
	return append(appendEscaped(append(dst, '"'), s, true), '"')
}

// appendEscaped is the one escaping rule of all three formats: JSON's
// string escapes (without the quote escape outside JSON). It is
// reversible, keeps a row on one line and its TSV fields between tabs,
// and never sends a terminal a control byte.
//
// Bytes that need no escape are appended a run at a time: the row
// formatter runs on the order stage's drainer, so per-byte appends cost
// listing throughput (a tenth of a text run's CPU profile before).
func appendEscaped(dst []byte, s string, json bool) []byte {
	const hex = "0123456789abcdef"
	start := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 0x20 && c != 0x7f && c != '\\' && (c != '"' || !json) {
			continue
		}
		dst = append(dst, s[start:i]...)
		start = i + 1
		switch {
		case c == '\\', c == '"' && json:
			dst = append(dst, '\\', c)
		case c == '\b':
			dst = append(dst, '\\', 'b')
		case c == '\f':
			dst = append(dst, '\\', 'f')
		case c == '\n':
			dst = append(dst, '\\', 'n')
		case c == '\r':
			dst = append(dst, '\\', 'r')
		case c == '\t':
			dst = append(dst, '\\', 't')
		default: // below 0x20, or 0x7f
			dst = append(dst, '\\', 'u', '0', '0', hex[c>>4], hex[c&15])
		}
	}
	return append(dst, s[start:]...)
}
