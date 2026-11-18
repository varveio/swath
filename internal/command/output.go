// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/varveio/swath/internal/inventory"
	"github.com/varveio/swath/internal/registry"
	"github.com/varveio/swath/internal/textout"
	"github.com/varveio/swath/internal/totals"
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

// checkTextName refuses a file name with a .tsv or .jsonl component that
// does not end there or in .gz or .zst after it (rows.jsonl.zstd,
// rows.tsv.lz4): the name of a text file is an allowlist, never taken for
// a dataset directory. Any case is the lower case's.
func checkTextName(name string) error {
	parts := strings.Split(strings.ToLower(filepath.Base(name)), ".")
	for i := 1; i < len(parts); i++ {
		ext := "." + parts[i]
		if _, ok := textExts[ext]; !ok {
			continue
		}
		tail := parts[i+1:]
		if len(tail) == 0 || len(tail) == 1 && (tail[0] == "gz" || tail[0] == "zst") {
			return nil
		}
		hint := "; " + outputForms
		if len(tail) == 1 && tail[0] == "zstd" {
			hint = fmt.Sprintf(" (did you mean %s?)", registry.Redact(name[:len(name)-len("zstd")]+"zst"))
		}
		return fmt.Errorf("-o %s: a %s file's name ends in %s, %s.gz or %s.zst, and a directory's in /%s", registry.Redact(name), ext, ext, ext, ext, hint)
	}
	return nil
}

// usableDir refuses a dataset directory name that is an existing file or
// device (-o /dev/null), naming what to do instead.
func usableDir(name string) error {
	fi, err := os.Stat(strings.TrimRight(name, `/\`))
	switch {
	case err != nil || fi.IsDir():
		return nil
	case !fi.Mode().IsRegular():
		return fmt.Errorf("-o %s is not a directory, and a dataset is one; rows to stdout need no -o, and swath diag count lists without writing", name)
	}
	return fmt.Errorf("-o %s: %s is a file, and a dataset is a directory; rows go to a file named .tsv or .jsonl", name, strings.TrimRight(name, `/\`))
}

// formatError refuses a --format value, with what was likely meant: the
// lower case, jsonl for json, a dataset for parquet.
func formatError(v string) error {
	err := fmt.Errorf("--format %q: want auto, table, tsv or jsonl", v)
	switch l := strings.ToLower(v); {
	case l == "json" || l == "ndjson":
		return fmt.Errorf("%w (did you mean jsonl?)", err)
	case l == "parquet":
		return fmt.Errorf("%w (a Parquet dataset is swath list -o DIR)", err)
	case l != v && slices.Contains([]string{"auto", textout.Table, textout.TSV, textout.JSONL}, l):
		return fmt.Errorf("%w (did you mean %s?)", err, l)
	}
	return err
}

// unwritten are the data-file extensions swath does not write: an -o name
// ending in one is refused rather than taken for a dataset directory.
var unwritten = []string{".csv", ".json", ".ndjson", ".txt", ".bz2", ".xz", ".gz", ".zst"}

// outputForms names what -o takes, for its refusals.
const outputForms = "-o takes a dataset directory (any other name, or one ending in /), a GCS, S3 or Azure Blob prefix with --work-dir, NAME.tsv or NAME.jsonl (optionally .gz or .zst), or - for stdout"

// checkDatasetName refuses a file-looking dataset name unless a trailing
// separator explicitly selects a directory.
func checkDatasetName(name string) error {
	if strings.HasSuffix(strings.ToLower(name), ".parquet") {
		return fmt.Errorf("%s: a dataset is a directory of parts; use a name without .parquet, or add / to select a directory", registry.Redact(name))
	}
	return nil
}

// schemeTypo refuses a path whose first segment is a provider's scheme and
// a colon (s3:/bkt/x): a URL typed with one slash, which would otherwise be
// a local path. It names the URL meant, and the spelling of the local path.
func schemeTypo(path string) error {
	first, rest, _ := strings.Cut(filepath.ToSlash(path), "/")
	scheme, ok := strings.CutSuffix(first, ":")
	if !ok || hasScheme(path) {
		return nil
	}
	for _, f := range registry.Factories() {
		if f.Scheme != "" && strings.EqualFold(scheme, f.Scheme) {
			return fmt.Errorf("%s is a local path (did you mean %s? for a local directory write ./%s)", registry.Redact(path), registry.Redact(f.Scheme+"://"+strings.TrimLeft(rest, "/")), registry.Redact(path))
		}
	}
	return nil
}

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
//     ending in .parquet or a data-file extension swath does not write
//     (unwritten), or with anything but .gz or .zst after a .tsv or .jsonl
//     (checkTextName), is refused unless it ends in a directory separator;
//     an existing file or device is refused (usableDir).
//
// With -o FILE a --format that agrees with the file's name is accepted,
// one that does not is refused, as is --format with a dataset directory.
// --columns chooses TSV or JSONL columns: refused with the table and with a
// dataset, and auto on a terminal is then TSV. --no-header is TSV's, so
// likewise, and refused with JSONL too.
func resolveOutput(o *options, stdoutTTY bool) (*textout.Config, error) {
	c, err := resolveText(o, stdoutTTY && len(o.columns) == 0 && !o.noHeader)
	switch {
	case err != nil || o.countOnly || c == nil && len(o.columns) == 0 && !o.noHeader:
		return c, err
	case o.noHeader && c == nil:
		return nil, fmt.Errorf("--no-header: -o %s is a dataset directory; the header line is TSV's", registry.Redact(o.out))
	case o.noHeader && c.Format != textout.TSV:
		return nil, errNoHeader(c.Format)
	case len(o.columns) > 0 && c == nil:
		return nil, fmt.Errorf("--columns: -o %s is a dataset directory, which holds every column; the columns are chosen for TSV and JSONL", registry.Redact(o.out))
	case len(o.columns) > 0 && c.Format == textout.Table:
		return nil, errors.New("--columns: the table's columns are fixed; the columns are chosen for --format tsv or jsonl")
	}
	c.Columns, c.NoHeader = o.columns, o.noHeader
	return c, nil
}

// errNoHeader refuses --no-header with a format other than TSV.
func errNoHeader(format string) error {
	name := "the " + format
	if format == textout.JSONL {
		name = "JSONL"
	}
	return fmt.Errorf("--no-header leaves out TSV's header line: not with %s", name)
}

func resolveText(o *options, stdoutTTY bool) (*textout.Config, error) {
	toStdout := o.out == "" || o.out == "-"
	remote := !toStdout && hasScheme(o.out)
	switch {
	case o.countOnly: // swath du and swath diag count write no rows (placement refuses -o; du's --format is resolveTotals')
		return nil, nil
	case !remote && o.workDir != "":
		return nil, errors.New("--work-dir is the work directory of a dataset in a bucket; a dataset directory is its own")
	case !remote && (o.outputCreds != "" || o.outputEndpoint != "" || o.outputProfile != "" || o.outputRegion != "" || o.outputSAS != ""):
		return nil, errors.New("--output-gcs-credentials-file, --output-s3-profile, --output-s3-region, --output-azure-sas-token and --output-endpoint-url apply to a dataset in a bucket")
	case remote:
		return nil, resolveRemote(o)
	case !toStdout:
		if err := schemeTypo(o.out); err != nil {
			return nil, fmt.Errorf("-o %w", err)
		}
		if err := checkDatasetName(o.out); err != nil {
			return nil, fmt.Errorf("-o %w", err)
		}
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
		if !dir {
			if err := checkTextName(o.out); err != nil {
				return nil, err
			}
		}
		switch {
		case format == "" && o.format != "":
			return nil, fmt.Errorf("--format %s: -o %s is a dataset directory, not text rows", o.format, registry.Redact(o.out))
		case format != "" && o.format != "" && o.format != "auto" && o.format != format:
			return nil, fmt.Errorf("--format %s: -o %s names %s; %s", o.format, registry.Redact(o.out), format, outputForms)
		case format == "":
			return nil, usableDir(o.out)
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
	return nil, formatError(o.format)
}

// resolveRemote resolves an -o that is a URL: a bucket prefix the listing
// writes to (inventory.ParseDestination, over objfile.ParseLocation: a literal
// prefix, a dataset root, a scheme it writes), the work directory it needs,
// and the refusals of phase one: no replacement, no text format.
func resolveRemote(o *options) error {
	shown := registry.Redact(o.out)
	l, err := inventory.ParseDestination(o.out)
	if err == nil && l.Prefix != "" {
		if err := checkDatasetName(o.out); err != nil {
			return fmt.Errorf("-o %w", err)
		}
	}
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
	r := inventory.RemoteOutput{Location: o.out, CredentialsFile: o.outputCreds, Profile: o.outputProfile, SAS: o.outputSAS, Region: o.outputRegion}
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
	format, err := totalsFormat(o, stdoutTTY)
	if err != nil {
		return err
	}
	scope, err := keyPrefix(root)
	if err != nil {
		return err
	}
	origin := scope[:strings.LastIndexByte(scope, '/')+1]
	// A local tree's file with several hard links counts once, as du
	// counts it, unless --count-links.
	view := duView(o, format, registry.Redact(root), stdoutTTY)
	opt := totals.Options{Depth: o.depth, Origin: origin, MaxPrefixes: o.maxPrefixes, MaxInodes: o.maxInodes, View: view != nil}
	opt.Primary, opt.Reason = primaryFigure(o)
	if pf, err := registry.Lookup(root); err == nil {
		opt.LinksOnce = pf.Scheme == "" && !o.countLinks
		if pf.CheckTotals != nil {
			if err := pf.CheckTotals(o.sources[pf.Scheme], &opt); err != nil {
				return err
			}
		}
	}
	if opt.Primary == "" {
		opt.Primary, opt.Reason = totals.Size, totals.ReasonObjectStore
	}
	o.totals = &totalsOut{format: format, sortBy: o.sortBy, noHeader: o.noHeader, tally: totals.New(opt), view: view, scope: scope, label: registry.Redact(root)}
	return nil
}

// primaryFigure is the figure swath du puts first and sorts by, as
// --allocated or --apparent-size chose it, and why; "" when neither did:
// the source's default (registry.Factory.CheckTotals, or what a saved
// dataset recorded).
func primaryFigure(o *options) (figure, reason string) {
	switch {
	case o.allocated:
		return totals.AllocatedBytes, totals.ReasonFlag
	case o.apparentSize:
		return totals.Size, totals.ReasonFlag
	}
	return "", ""
}

// totalsFormat is swath du's --format (auto: the table on a terminal, TSV
// otherwise, or TSV with --no-header), its --sort, its primary figure and
// --no-header checked.
func totalsFormat(o *options, stdoutTTY bool) (string, error) {
	format := o.format
	switch format {
	case "", "auto":
		format = textout.TSV
		if stdoutTTY && !o.noHeader {
			format = textout.Table
		}
	case textout.TSV, textout.Table, textout.JSONL:
	default:
		return "", formatError(o.format)
	}
	switch {
	case o.sortBy != sortName && o.sortBy != sortSize:
		return "", fmt.Errorf("--sort %q: want name or size", o.sortBy)
	case o.allocated && o.apparentSize:
		return "", errors.New("--allocated and --apparent-size each choose the figure that comes first: give one")
	case o.noHeader && format != textout.TSV:
		return "", errNoHeader(format)
	}
	return format, nil
}

// keyPrefix is the key prefix the root names: its keys' common start
// (s3://b/logs/2026/0 names logs/2026/0). A local tree's keys are relative
// to the root, so it names "". It is taken from the root as given, before
// an --include narrows the listed prefix.
func keyPrefix(root string) (string, error) {
	pf, err := registry.Lookup(root)
	if err != nil || pf.KeyPrefix == nil {
		return "", nil // an unknown scheme is refused when the run looks it up
	}
	return pf.KeyPrefix(root)
}

// keyOrigin is the prefix swath du's depth counts from: the key prefix the
// root names, up to its last '/' (s3://b/logs/2026/ and s3://b/logs/2026/x
// count from logs/2026/; s3://b/logs from the bucket's top).
func keyOrigin(root string) (string, error) {
	p, err := keyPrefix(root)
	return p[:strings.LastIndexByte(p, '/')+1], err
}
