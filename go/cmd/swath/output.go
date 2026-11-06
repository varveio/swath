// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/varveio/swath/go/internal/textout"
)

// textExts are the file extensions that name a text format (before a
// compression extension).
var textExts = map[string]string{".tsv": textout.TSV, ".jsonl": textout.JSONL}

// textDest strips a .gz or .zst extension from path and returns the text
// format its extension then names ("" for none) and the compression.
func textDest(path string) (format, compression string) {
	p := strings.TrimRight(path, `/\`)
	lower := strings.ToLower(p)
	switch {
	case strings.HasSuffix(lower, ".gz"):
		compression, p = textout.Gzip, p[:len(p)-3]
	case strings.HasSuffix(lower, ".zst"):
		compression, p = textout.Zstd, p[:len(p)-4]
	}
	return textExts[strings.ToLower(filepath.Ext(p))], compression
}

// resolveOutput decides what a run writes. The -o name decides alone:
//
//   - no -o, or -o -: rows on stdout, in --format (auto, the default, is
//     the table on a terminal and TSV otherwise); compress a stream with
//     a pipe (| zstd);
//   - -o NAME.tsv or NAME.jsonl, optionally .gz or .zst: that format and
//     compression, one file;
//   - any other -o: the Parquet dataset directory.
//
// --format is for stdout only: a file's name already says its format, so
// a second way to say it could only conflict.
func resolveOutput(o *options, stdoutTTY bool) (*textout.Config, error) {
	toStdout := o.out == "" || o.out == "-"
	switch {
	case o.countOnly && (o.format != "" || o.out != ""):
		return nil, errors.New("--count-only writes nothing: it takes no --output or --format")
	case o.countOnly:
		return nil, nil
	case !toStdout && o.format != "":
		return nil, fmt.Errorf("--format is for stdout: -o %s decides by its name (.tsv or .jsonl, optionally .gz or .zst, is one text file; any other name, the dataset directory)", o.out)
	case !toStdout:
		format, comp := textDest(o.out)
		if format == "" {
			return nil, nil
		}
		return &textout.Config{Format: format, Path: o.out, Compression: comp}, nil
	}
	switch o.format {
	case "", "auto":
		if stdoutTTY {
			return &textout.Config{Format: textout.Table}, nil
		}
		return &textout.Config{Format: textout.TSV}, nil
	case textout.TSV, textout.JSONL, textout.Table:
		return &textout.Config{Format: o.format}, nil
	}
	return nil, fmt.Errorf("--format %q: want auto, table, tsv or jsonl", o.format)
}
