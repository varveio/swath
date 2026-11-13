// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/pflag"
	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/listing"
	"github.com/varveio/swath/internal/registry"
	"github.com/varveio/swath/internal/totals"
	"github.com/varveio/swath/reader"
)

// swath du --from-dataset DIR [PREFIX] totals a committed dataset's rows
// instead of listing a source: the same tally, filters and rendering as a
// live swath du (registry.DatasetTotals), labelled as saved data on stderr
// and in JSONL rows.

// datasetFlags are the flags swath du takes with --from-dataset: the
// totals', the output's, the filters' and the terminal's that rows already
// listed can answer. Every other flag shapes a listing (a provider's, a
// limit, the run report) and is refused, never ignored.
var datasetFlags = []string{"from-dataset", "depth", "sort", "max-prefixes", "allocated", "count-links-once", "max-inodes", "format",
	"include", "exclude", "include-regex", "exclude-regex", "min-size", "max-size", "modified-since", "modified-until", "storage-class",
	"verbose", "quiet", "color", "log-format", "no-progress", "no-stats"}

// datasetOptions checks a swath du --from-dataset command line: its flags,
// at most one PREFIX, and the options' values.
func datasetOptions(f *pflag.FlagSet, o *options, operands []string) error {
	var err error
	f.Visit(func(fl *pflag.Flag) {
		if err == nil && !slices.Contains(datasetFlags, fl.Name) {
			err = usageError{fmt.Errorf("--%s: not with --from-dataset, which reads rows already listed and lists nothing", fl.Name)}
		}
	})
	switch {
	case err != nil:
		return err
	case hasScheme(o.fromDataset):
		return usageError{fmt.Errorf("--from-dataset %s: a dataset in a bucket is not supported yet; give a local dataset directory", registry.RedactPlace(o.fromDataset))}
	case len(operands) > 1:
		return seeHelp{usageError{fmt.Errorf("unexpected argument %q: --from-dataset takes one PREFIX", registry.Redact(operands[1]))}, cmdDu}
	case len(operands) == 1 && hasScheme(operands[0]):
		return seeHelp{usageError{fmt.Errorf("PREFIX %q: a key prefix within the dataset (logs/2026/), not a URL", registry.Redact(operands[0]))}, cmdDu}
	case len(operands) == 1:
		o.datasetPrefix = operands[0]
	}
	if err := o.term.validate(); err != nil {
		return usageError{err}
	}
	o.filter.Resolve(time.Now())
	if err := o.filter.Validate(); err != nil {
		return usageError{err}
	}
	if _, err := totalsFormat(o, false); err != nil {
		return usageError{err}
	}
	return nil
}

// openSaved opens the committed dataset in dir, refusing anything else
// with the command that reads it.
func openSaved(ctx context.Context, dir string) (*reader.Dataset, error) {
	fi, err := os.Stat(filepath.Join(dir, dataset.ManifestName))
	switch {
	case err == nil && fi.Mode().IsRegular():
		d, err := reader.OpenDir(ctx, dir, seekparquet.OpenOptions{})
		if err != nil {
			return nil, fmt.Errorf("--from-dataset %s: %w", dir, err)
		}
		return d, nil
	case !isDir(dir):
		return nil, usageError{fmt.Errorf("--from-dataset %s: no such directory", dir)}
	case dataset.Committed(dir):
		return nil, usageError{fmt.Errorf("--from-dataset %s: the work directory of a dataset written to a bucket, which --from-dataset does not read yet (swath show %s)", dir, dir)}
	case dataset.Resumable(dir):
		return nil, usageError{fmt.Errorf("--from-dataset %s: an interrupted run, not a committed dataset: swath show %s describes it, swath resume %s finishes it", dir, dir, dir)}
	case isDir(filepath.Join(dir, dataset.DataDir)):
		return nil, usageError{fmt.Errorf("--from-dataset %s: a run that did not commit (no %s): swath show %s describes it", dir, dataset.ManifestName, dir)}
	}
	return nil, usageError{fmt.Errorf("--from-dataset %s: no dataset (no %s): give the directory swath list -o wrote", dir, dataset.ManifestName)}
}

// duFromDataset is swath du --from-dataset DIR [PREFIX]: the totals of the
// dataset's rows under PREFIX, as swath du of its source would print them,
// with depth counted from PREFIX (or the dataset's scope) up to its last
// '/'. Stderr says what the dataset is (eventLog.dataset); a dataset whose
// listing did not complete prints its totals and exits 1 (incomplete), as
// the run that wrote it did.
func duFromDataset(ctx context.Context, o options, stdout io.Writer) error {
	fail := func(err error) error {
		if ctx.Err() != nil {
			err = context.Cause(ctx) // a signal: interrupted, as a listing
		}
		out := classify(err, nil, "")
		return &exitError{error: explain(err), code: out.ExitCode, out: out}
	}
	d, err := openSaved(ctx, o.fromDataset)
	if err != nil {
		return err
	}
	defer d.Close()
	m := d.Metadata()
	if !m.Swath {
		return usageError{fmt.Errorf("--from-dataset %s: its manifest has no swath metadata: not a dataset swath wrote", o.fromDataset)}
	}
	spec, _ := reader.Schema(m.SchemaID) // reader.Open refuses an id swath does not write
	info, _ := registry.SchemaInfo(string(m.SchemaID))
	refuse := func(flag, need string) error {
		return usageError{fmt.Errorf("--%s: the dataset's schema %s has no %s column", flag, m.SchemaID, need)}
	}
	_, hasAllocated := spec.Column(totals.AllocatedBytes)
	_, hasNlink := spec.Column("nlink")
	switch {
	case o.allocated && !hasAllocated:
		return refuse("allocated", totals.AllocatedBytes)
	case o.linksOnce && !hasNlink:
		return refuse("count-links-once", "nlink")
	case len(o.filter.StorageClasses) > 0 && info.StorageClass == "":
		return refuse("storage-class", "storage class")
	}
	scope := ""
	if m.Scope != nil && m.Scope.Prefix != nil {
		scope = *m.Scope.Prefix
	}
	prefix := o.datasetPrefix
	if prefix == "" {
		prefix = scope
	}
	if !strings.HasPrefix(prefix, scope) {
		return usageError{fmt.Errorf("PREFIX %q is not inside the dataset's scope: it holds the keys under %q", prefix, scope)}
	}
	origin := prefix[:strings.LastIndexByte(prefix, '/')+1]
	opt := totals.Options{Depth: o.depth, Origin: origin, MaxPrefixes: o.maxPrefixes, MaxInodes: o.maxInodes, LinksOnce: o.linksOnce}
	if o.allocated {
		opt.BytesColumn = totals.AllocatedBytes
	}
	o.filter.Base = origin
	format, _ := totalsFormat(&o, stdoutTerminal())
	prov := provenanceOf(o.fromDataset, m)
	t := &totalsOut{format: format, bySize: o.sortBy == sortSize, tally: totals.New(opt)}
	if t.dataset, err = json.Marshal(prov); err != nil {
		return err
	}
	l := elog()
	l.dataset(prov, time.Now())
	read, err := registry.DatasetTotals(ctx, d, prefix, &o.filter, t.tally, runtime.GOMAXPROCS(0))
	if err != nil {
		return fail(err)
	}
	l.read(o.fromDataset, read)
	if read.Rows == 0 && o.datasetPrefix != "" {
		l.warn(fmt.Sprintf("no key of the dataset starts with %q", o.datasetPrefix))
	}
	if err := t.write(stdout); brokenPipe(err) {
		return nil // the reader closed stdout: exit 0, as a live swath du
	} else if err != nil {
		return fail(err)
	}
	c := m.Coverage
	switch {
	case c == nil || c.Complete == nil:
		l.warn("the dataset's manifest does not say whether its listing completed: the totals may leave entries out")
	case !*c.Complete:
		why := "its listing did not complete"
		if c.StopReason != nil {
			why += " (" + *c.StopReason + ")"
		}
		msg := "the dataset is a partial listing: " + why + "; the totals count only the rows it holds"
		return &exitError{error: errors.New(msg), code: exitFatal, out: outcome{Class: listing.ClassIncomplete, ExitCode: exitFatal, Message: msg}}
	case c.SkippedEntries != nil && *c.SkippedEntries > 0:
		l.warn(fmt.Sprintf("%s entries could not be read when the dataset was listed: the totals leave them out", count(*c.SkippedEntries)))
	}
	return nil
}

// provenance is where totals read from a dataset came from: the dataset
// directory and its manifest's source and coverage (null where the
// manifest does not say), the JSONL rows' dataset member and the dataset
// event's fields.
type provenance struct {
	Path              string     `json:"path"`
	Source            *string    `json:"source"`
	SchemaID          string     `json:"schema_id"`
	ListingStartedAt  *time.Time `json:"listing_started_at"`
	ListingFinishedAt *time.Time `json:"listing_finished_at"`
	Complete          *bool      `json:"complete"`
	StopReason        *string    `json:"stop_reason"`
	SkippedEntries    *int64     `json:"skipped_entries"`
	Filters           []string   `json:"filters"`
	Exclusions        []string   `json:"exclusions"`
}

func provenanceOf(dir string, m reader.Metadata) provenance {
	p := provenance{Path: dir, SchemaID: string(m.SchemaID)}
	if m.Source != nil && m.Source.URI != nil {
		uri := registry.Redact(*m.Source.URI)
		p.Source = &uri
	}
	if c := m.Coverage; c != nil {
		p.ListingStartedAt, p.ListingFinishedAt, p.Complete, p.StopReason, p.SkippedEntries = c.ListingStartedAt, c.ListingFinishedAt, c.Complete, c.StopReason, c.SkippedEntries
		p.Filters, p.Exclusions = c.Filters, c.Exclusions
	}
	return p
}

// readFields are the read event's: the rows read under the prefix, those
// the filters dropped, the columns read besides the key, and SeekParquet's
// account of planning and scanning (parts opened, ranges read, their
// bytes).
type readFields struct {
	Rows        int64    `json:"rows"`
	Filtered    int64    `json:"filtered"`
	Columns     []string `json:"columns"`
	PartsOpened int64    `json:"parts_opened"`
	SourceReads int64    `json:"source_reads"`
	SourceBytes int64    `json:"source_bytes"`
}

// read prints what was read of the dataset in dir (-v).
func (l *eventLog) read(dir string, r registry.DatasetRead) {
	if !l.shows(lvInfo) {
		return
	}
	st := r.Plan
	st.Add(r.Scan)
	msg := fmt.Sprintf("read %s rows of %s (%s dropped by the filters), the key and %s: %s bytes in %s reads",
		count(r.Rows), dir, count(r.Filtered), strings.Join(r.Columns, ", "), count(st.SourceBytes), count(st.SourceReads))
	l.print(lvInfo, evRead, msg, readFields{r.Rows, r.Filtered, r.Columns, st.PartsOpened, st.SourceReads, st.SourceBytes})
}

// dataset prints what totals read from a dataset are (unless -q): saved
// rows, not a live listing, with the source, when it was listed and how
// far its listing reached. It is kept, never dropped.
func (l *eventLog) dataset(p provenance, now time.Time) {
	if l.quiet > 0 {
		return
	}
	src := "an unrecorded source"
	if p.Source != nil {
		src = *p.Source
	}
	msg := "totals of the dataset " + p.Path + ", not a live listing: " + src
	if at := p.ListingFinishedAt; at != nil {
		msg += " as listed " + at.UTC().Format(time.RFC3339)
		if age := now.Sub(*at); age >= 0 {
			msg += " (" + ago(age) + ")"
		}
	}
	switch c := p.Complete; {
	case c == nil:
		msg += "; coverage unknown"
	case *c:
		msg += "; complete"
	default:
		msg += "; INCOMPLETE"
		if p.StopReason != nil {
			msg += " (" + *p.StopReason + ")"
		}
	}
	if p.SkippedEntries != nil && *p.SkippedEntries > 0 {
		msg += ", " + count(*p.SkippedEntries) + " entries skipped"
	}
	if len(p.Filters) > 0 {
		msg += "; filtered: " + strings.Join(p.Filters, ", ")
	}
	if len(p.Exclusions) > 0 {
		msg += "; excluded: " + strings.Join(p.Exclusions, ", ")
	}
	if l.json {
		noteKept(errOut, l.line(lvInfo, evDataset, msg, p))
		return
	}
	noteKept(errOut, "swath: "+msg+"\n")
}

// savedInventory is the commands that list SOURCE (as given, redacted)
// once into a dataset and total it at the live run's depth and order: the
// hint a large live swath du ends with. DIR is a placeholder.
func savedInventory(root string, o options) []string {
	du := "swath du --from-dataset DIR"
	if o.depth > 0 {
		du += " -d " + strconv.Itoa(o.depth)
	}
	if o.sortBy == sortSize {
		du += " --sort size"
	}
	return []string{"swath list " + shellQuote(registry.Redact(root)) + " -o DIR", du}
}

// shellQuote is s as one POSIX shell word: as it is when it holds only
// characters no shell treats specially, else in single quotes.
func shellQuote(s string) string {
	if s != "" && strings.Trim(s, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789@%+=:,./_-") == "" {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// ago is an age in whole units: seconds, minutes, hours, then days.
func ago(d time.Duration) string {
	switch {
	case d >= 48*time.Hour:
		return fmt.Sprintf("%d days ago", d/(24*time.Hour))
	case d >= 2*time.Hour:
		return fmt.Sprintf("%d hours ago", d/time.Hour)
	case d >= 2*time.Minute:
		return fmt.Sprintf("%d minutes ago", d/time.Minute)
	}
	return fmt.Sprintf("%d seconds ago", d/time.Second)
}
