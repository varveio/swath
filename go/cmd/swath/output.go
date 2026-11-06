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
// compression extension), as Java's.
var textExts = map[string]string{".tsv": textout.TSV, ".jsonl": textout.JSONL}

// textDest strips a compression extension from path and returns the text
// format its extension names ("" for none) and the compression it implies.
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

// resolveOutput decides what a run writes, as Java's --format, -o,
// --output-type and --compression resolution, with Go's dataset in place
// of Java's Parquet directory:
//
//   - no -o, or -o -: rows on stdout; --format auto (the default) is an
//     aligned table when stdout is a terminal, TSV otherwise;
//   - -o NAME.tsv or NAME.jsonl (optionally .gz or .zst): that format, one
//     file; --output-type file forces one file without the extension;
//   - any other -o: the Parquet dataset directory (--format parquet, the
//     default there). A text format there needs --output-type file: text
//     directory datasets are not ported.
//
// Compression applies to text only: --compression, or the .gz/.zst
// extension (they must agree).
func resolveOutput(o *options, stdoutTTY bool) (*textout.Config, error) {
	format := o.format
	switch format {
	case "", "auto", "parquet", textout.TSV, textout.JSONL, textout.Table:
	default:
		return nil, fmt.Errorf("--format %q: want auto, table, tsv, jsonl or parquet", format)
	}
	switch o.compression {
	case "", textout.None, textout.Gzip, textout.Zstd:
	default:
		return nil, fmt.Errorf("--compression %q: want none, gzip or zstd", o.compression)
	}
	switch o.outputType {
	case "", "file", "dir", "directory":
	default:
		return nil, fmt.Errorf("--output-type %q: want file or dir", o.outputType)
	}
	auto := func() string {
		if stdoutTTY {
			return textout.Table
		}
		return textout.TSV
	}
	if o.countOnly {
		if format != "" || o.compression != "" || o.outputType != "" || o.out != "" {
			return nil, errors.New("--count-only writes nothing: it takes no --output, --format, --compression or --output-type")
		}
		return nil, nil
	}
	if o.out == "" || o.out == "-" {
		if o.outputType != "" {
			return nil, errors.New("--output-type is meaningless with stdout (no -o, or -o -): there is no file or directory to choose")
		}
		switch format {
		case "parquet":
			return nil, errors.New("--format parquet writes a dataset directory: pass -o DIR")
		case "", "auto":
			format = auto()
		}
		return &textout.Config{Format: format, Compression: o.compression}, nil
	}
	extFormat, extComp := textDest(o.out)
	if o.compression != "" && extComp != "" && o.compression != extComp {
		return nil, fmt.Errorf("--compression %s conflicts with the extension of -o %s (which implies %s)", o.compression, o.out, extComp)
	}
	comp := o.compression
	if comp == "" {
		comp = extComp
	}
	file := extFormat != ""
	switch o.outputType {
	case "file":
		file = true
	case "dir", "directory":
		file = false
	}
	if !file {
		if format != "" && format != "parquet" {
			return nil, fmt.Errorf("-o %s is a dataset directory, and --format %s is text: text directory datasets are not supported; name a .tsv or .jsonl file, or pass --output-type file", o.out, format)
		}
		if comp != "" && comp != textout.None {
			return nil, errors.New("--compression applies to text output only (table, tsv, jsonl); Parquet parts are compressed by the writer")
		}
		return nil, nil
	}
	switch {
	case format == "parquet":
		return nil, fmt.Errorf("--format parquet writes a dataset directory, not one file (-o %s): Java's one-writer .parquet layout is not ported", o.out)
	case extFormat != "" && format != "" && format != "auto" && format != extFormat:
		return nil, fmt.Errorf("--format %s conflicts with the extension of -o %s (which implies %s)", format, o.out, extFormat)
	case extFormat != "":
		format = extFormat
	case format == "":
		return nil, fmt.Errorf("-o %s has no .tsv or .jsonl extension: pass --format", o.out)
	case format == "auto":
		format = auto()
	}
	return &textout.Config{Format: format, Path: o.out, Compression: comp}, nil
}
