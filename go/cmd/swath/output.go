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
	case o.countOnly: // swath du and swath diag count write no rows (placement refuses -o; du's --format is resolveTotals')
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

// resolveTotals sets up swath du's per-prefix totals: a tally at --depth
// below the prefix the source names, written to stdout in --format (auto,
// the default: the table on a terminal, TSV otherwise). The du flags on
// another source than a local one are foreignFlags' refusal.
func resolveTotals(f *flag.FlagSet, o *options, root string, stdoutTTY bool) error {
	if !o.du {
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
	case o.maxInodes < 1:
		return fmt.Errorf("--max-inodes %d: want at least 1", o.maxInodes)
	}
	format := o.format
	switch format {
	case "", "auto":
		format = textout.TSV
		if stdoutTTY {
			format = textout.Table
		}
	case textout.TSV, textout.Table, textout.JSONL:
	default:
		return fmt.Errorf("--format %q: want auto, table, tsv or jsonl", o.format)
	}
	origin, err := keyOrigin(root)
	if err != nil {
		return err
	}
	opt := totals.Options{Depth: o.depth, Origin: origin, MaxPrefixes: o.maxPrefixes, MaxInodes: o.maxInodes, LinksOnce: o.linksOnce}
	if o.allocated {
		opt.BytesColumn = totals.AllocatedBytes
	}
	o.totals = &totalsOut{format: format, tally: totals.New(opt)}
	return nil
}

// keyOrigin is the prefix swath du's depth counts from: the key prefix the
// root names, up to its last '/' (s3://b/logs/2026/ and s3://b/logs/2026/x
// count from logs/2026/; s3://b/logs from the bucket's top). A local tree's
// keys are relative to the root, so it counts from "". It is taken from the
// root as given, before an --include narrows the listed prefix.
func keyOrigin(root string) (string, error) {
	pf, err := registry.Lookup(root)
	if err != nil || pf.KeyPrefix == nil {
		return "", nil // an unknown scheme is refused when the run looks it up
	}
	p, err := pf.KeyPrefix(root)
	if err != nil {
		return "", err
	}
	return p[:strings.LastIndexByte(p, '/')+1], nil
}
