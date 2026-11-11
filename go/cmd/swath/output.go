// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/varveio/swath/go/internal/listing"
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

// unwritten are the data-file extensions swath does not write: an -o name
// ending in one is refused rather than taken for a dataset directory.
var unwritten = []string{".csv", ".json", ".ndjson", ".txt", ".bz2", ".xz", ".gz", ".zst"}

// outputForms names what -o takes, for its refusals.
const outputForms = "-o takes a dataset directory (any other name, or one ending in /), a GCS or S3 bucket prefix with --work-dir, NAME.tsv or NAME.jsonl (optionally .gz or .zst), or - for stdout"

// hasScheme reports whether path starts with a URL scheme (s3://).
func hasScheme(path string) bool {
	i := strings.Index(path, "://")
	return i > 0 && !strings.ContainsAny(path[:i], `/\.`)
}

// resolveOutput decides what a run writes. The -o name decides alone:
//
//   - no -o, or -o -: rows on stdout, in --format (auto, the default, is
//     the table on a terminal and TSV otherwise); compress a stream with
//     a pipe (| zstd);
//   - -o NAME.tsv or NAME.jsonl, optionally .gz or .zst: that format and
//     compression, one file;
//   - -o a bucket prefix: the Parquet dataset there, with --work-dir DIR
//     its local work directory (resolveRemote); a store the listing does
//     not write to is refused;
//   - -o NAME/, or another name: the Parquet dataset directory; a name
//     ending in a data-file extension swath does not write (unwritten) is
//     refused.
//
// With -o FILE a --format that agrees with the file's name is accepted,
// one that does not is refused, as is --format with a dataset directory.
// --columns chooses TSV or JSONL columns: refused with the table and with a
// dataset, and auto on a terminal is then TSV.
func resolveOutput(o *options, stdoutTTY bool) (*textout.Config, error) {
	c, err := resolveText(o, stdoutTTY && len(o.columns) == 0)
	switch {
	case err != nil || len(o.columns) == 0:
		return c, err
	case c == nil:
		return nil, fmt.Errorf("--columns: -o %s is a dataset directory, which holds every column; the columns are chosen for TSV and JSONL", registry.Redact(o.out))
	case c.Format == textout.Table:
		return nil, errors.New("--columns: the table's columns are fixed; the columns are chosen for --format tsv or jsonl")
	}
	c.Columns = o.columns
	return c, nil
}

func resolveText(o *options, stdoutTTY bool) (*textout.Config, error) {
	toStdout := o.out == "" || o.out == "-"
	remote := !toStdout && hasScheme(o.out)
	switch {
	case o.countOnly: // swath du and swath diag count write no rows (placement refuses -o; du's --format is resolveTotals')
		return nil, nil
	case !remote && o.workDir != "":
		return nil, errors.New("--work-dir is the work directory of a dataset in a bucket; a dataset directory is its own")
	case !remote && (o.outputCreds != "" || o.outputEndpoint != "" || o.outputProfile != "" || o.outputRegion != ""):
		return nil, errors.New("--output-gcs-credentials-file, --output-s3-profile, --output-s3-region and --output-endpoint-url apply to a dataset in a bucket")
	case remote:
		return nil, resolveRemote(o)
	case !toStdout:
		format, comp := textDest(o.out)
		dir := strings.HasSuffix(o.out, "/") || strings.HasSuffix(o.out, string(filepath.Separator))
		if dir {
			format = ""
		}
		lower := strings.ToLower(o.out)
		for _, ext := range unwritten {
			if format == "" && !dir && strings.HasSuffix(lower, ext) {
				return nil, fmt.Errorf("-o %s: swath does not write %s files; %s", registry.Redact(o.out), ext, outputForms)
			}
		}
		switch {
		case format == "" && o.format != "":
			return nil, fmt.Errorf("--format %s: -o %s is a dataset directory, not text rows", o.format, registry.Redact(o.out))
		case format != "" && o.format != "" && o.format != "auto" && o.format != format:
			return nil, fmt.Errorf("--format %s: -o %s names %s; %s", o.format, registry.Redact(o.out), format, outputForms)
		case format == "":
			return nil, nil
		}
		if parent := filepath.Dir(o.out); !isDir(parent) {
			return nil, fmt.Errorf("-o %s: its directory %s does not exist", o.out, parent)
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

// resolveRemote resolves an -o that is a URL: a bucket prefix the listing
// writes to (listing.ParseDestination, over objfile.ParseLocation: a literal
// prefix, a dataset root, a scheme it writes), the work directory it needs,
// and the refusals of phase one: no replacement, no text format.
func resolveRemote(o *options) error {
	shown := registry.Redact(o.out)
	l, err := listing.ParseDestination(o.out)
	switch {
	case err != nil:
		return fmt.Errorf("-o %s: %w; %s", shown, err, outputForms)
	case o.workDir == "":
		return fmt.Errorf("-o %s: a dataset in a bucket needs --work-dir DIR, the local directory for its lock, journal and spill", shown)
	case hasScheme(o.workDir):
		return fmt.Errorf("--work-dir %s: the work directory is local", registry.Redact(o.workDir))
	case o.overwrite:
		return fmt.Errorf("--overwrite: -o %s: replacing a dataset in a bucket is not supported; write to another prefix", shown)
	case o.format != "":
		return fmt.Errorf("--format %s: -o %s is a dataset, not text rows", o.format, shown)
	}
	r := listing.RemoteOutput{Location: o.out, CredentialsFile: o.outputCreds, Profile: o.outputProfile, Region: o.outputRegion}
	if err := r.Check(); err != nil {
		return fmt.Errorf("-o %s: %w", shown, err)
	}
	o.remote = &l
	return nil
}

// resolveTotals sets up swath du's per-prefix totals: a tally at --depth
// below the prefix the source names, written to stdout in --format (auto,
// the default: the table on a terminal, TSV otherwise). The du flags on
// another source than a local one are foreignFlags' refusal.
func resolveTotals(o *options, root string, stdoutTTY bool) error {
	if !o.du {
		return nil
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
	if o.sortBy != sortName && o.sortBy != sortSize {
		return fmt.Errorf("--sort %q: want name or size", o.sortBy)
	}
	origin, err := keyOrigin(root)
	if err != nil {
		return err
	}
	opt := totals.Options{Depth: o.depth, Origin: origin, MaxPrefixes: o.maxPrefixes, MaxInodes: o.maxInodes, LinksOnce: o.linksOnce}
	if o.allocated {
		opt.BytesColumn = totals.AllocatedBytes
	}
	if pf, err := registry.Lookup(root); err == nil && pf.CheckTotals != nil {
		if err := pf.CheckTotals(o.sources[pf.Scheme], opt); err != nil {
			return err
		}
	}
	o.totals = &totalsOut{format: format, bySize: o.sortBy == sortSize, tally: totals.New(opt)}
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
