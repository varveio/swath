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
}

// Out is one text destination: open, written by a Sink, then committed or
// aborted.
type Out struct {
	cfg   Config
	f     *dataset.Pending // the file's replacement (nil for the stream)
	comp  io.WriteCloser   // the compressor, if any
	count *counter         // bytes delivered to the file or stream
	bw    *bufio.Writer
	rows  int64
	flush bool // flush after every batch: a stream's reader sees rows as they come
}

// counter counts the bytes written through it.
type counter struct {
	w io.Writer
	n int64
}

func (c *counter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
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
	var w io.Writer = o.count
	switch cfg.Compression {
	case "", None:
	case Gzip:
		o.comp = gzip.NewWriter(o.count)
	case Zstd:
		z, err := zstd.NewWriter(o.count)
		if err != nil {
			o.Abort()
			return nil, err
		}
		o.comp = z
	default:
		o.Abort()
		return nil, fmt.Errorf("text output: unknown compression %q", cfg.Compression)
	}
	if o.comp != nil {
		w = o.comp
	}
	o.bw = bufio.NewWriterSize(w, 256<<10)
	return o, nil
}

// Format is the output's format.
func (o *Out) Format() string { return o.cfg.Format }

// Bytes is the bytes delivered so far (compressed, header included).
func (o *Out) Bytes() int64 { return o.count.n }

// Rows is the rows written.
func (o *Out) Rows() int64 { return o.rows }

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
	err := o.bw.Flush()
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

// Abort drops the output: a file's temporary is removed and the
// destination left as it was.
func (o *Out) Abort() {
	if o.comp != nil && o.f == nil {
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
func appendEscaped(dst []byte, s string, json bool) []byte {
	const hex = "0123456789abcdef"
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
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
		case c < 0x20 || c == 0x7f:
			dst = append(dst, '\\', 'u', '0', '0', hex[c>>4], hex[c&15])
		default:
			dst = append(dst, c)
		}
	}
	return dst
}
