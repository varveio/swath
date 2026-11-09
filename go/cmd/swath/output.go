// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"flag"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/varveio/swath/go/internal/registry"
	"github.com/varveio/swath/go/internal/textout"
	"github.com/varveio/swath/go/internal/totals"
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
	case o.countOnly: // swath count writes nothing (placement refuses -o and --format)
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

// resolveTotals decides swath count's per-prefix totals: on with --depth,
// or at depth 0 (the total alone) with --allocated or --count-links-once,
// written to stdout in --format (auto, the default: the table on a
// terminal, TSV otherwise). A source whose keys are bytes cannot be
// written as JSON. --format without them is placement's refusal; the du
// flags on another source than a local one are foreignFlags'.
func resolveTotals(f *flag.FlagSet, o *options, root string, stdoutTTY bool) error {
	f.Visit(func(fl *flag.Flag) { o.depthGiven = o.depthGiven || fl.Name == "depth" })
	if !o.countOnly || !totalsAsked(f) {
		return nil
	}
	if fl := f.Lookup("metadata"); (o.allocated || o.linksOnce) && fl != nil && fl.Value.String() == "names" {
		return errors.New("--allocated and --count-links-once read each entry's stat: not with --metadata names")
	}
	switch {
	case o.depth < 0:
		return fmt.Errorf("--depth %d: negative", o.depth)
	case o.maxPrefixes < 1:
		return fmt.Errorf("--max-prefixes %d: want at least 1", o.maxPrefixes)
	}
	format := o.format
	switch format {
	case "", "auto":
		format = textout.TSV
		if stdoutTTY {
			format = textout.Table
		}
	case textout.TSV, textout.Table:
	case textout.JSONL:
		if pf, err := registry.Lookup(root); err == nil && pf.BinaryKeys {
			return fmt.Errorf("--format jsonl: a %s source's names are bytes, which JSON cannot carry: use tsv or table", schemeName(pf.Scheme))
		}
	default:
		return fmt.Errorf("--format %q: swath count --depth writes auto, table, tsv or jsonl", o.format)
	}
	opt := totals.Options{Depth: o.depth, MaxPrefixes: o.maxPrefixes, LinksOnce: o.linksOnce}
	if o.allocated {
		opt.BytesColumn = totals.AllocatedBytes
	}
	o.totals = &totalsOut{format: format, tally: totals.New(opt)}
	return nil
}
