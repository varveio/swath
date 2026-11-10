// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/spf13/pflag"

	"github.com/varveio/swath/go/internal/listing"
	"github.com/varveio/swath/go/internal/registry"
	"github.com/varveio/swath/go/internal/source"
	"github.com/varveio/swath/go/internal/totals"
)

// The list commands as typed (command.name), which a flag's table entry
// names in source.Flag.Commands.
const (
	cmdList      = "list"
	cmdDu        = "du"
	cmdDiagList  = "diag list"
	cmdDiagCount = "diag count"
)

// writing are the commands that write rows: the output, part, reorder
// and spill flags are theirs.
var writing = []string{cmdList, cmdDiagList}

// appendTo is a repeatable flag's value: each value given is added to *p.
func appendTo(p *[]string) pflag.Value {
	return source.Func(func(v string) error { *p = append(*p, v); return nil })
}

// newFlags is the flag set of swath list (diag false) or swath diag list
// (diag true). Both hold every flag, the diagnostic ones included, so that
// one given to swath list is refused with a message naming swath diag
// (placement) rather than as unknown; the help shows only the command's own.
func newFlags(o *options, diag bool) *pflag.FlagSet {
	f := pflag.NewFlagSet("list", pflag.ContinueOnError)
	o.diag = diag
	registerFlags(f, o, true)
	return f
}

// registerFlags registers the list commands' flag table on f, with the
// diagnostic flags when diag: the command's own flags here, then the
// terminal's, the shared groups' and each provider's (registry).
//
// Names: a flag is bare when it applies to every object store with the
// same meaning and argument (--endpoint-url, --anonymous), or is a general
// or local-tree flag (--workers, --one-file-system); a flag only one
// provider has carries that provider's prefix (--s3-region, --gcs-
// credentials-file, --azure-sas-token). A default-on switch is turned off
// by --no-NAME, and a recorded switch a resume may change has its other
// spelling too (undoFlags).
func registerFlags(f *pflag.FlagSet, o *options, diag bool) {
	f.SortFlags = false
	def := func(fl source.Flag) { source.Define(f, fl) }

	o.depth, o.maxPrefixes, o.maxInodes = 0, totals.DefaultMaxPrefixes, totals.DefaultMaxInodes
	def(source.Flag{Name: "depth", Short: "d", Value: source.Count(&o.depth, 0), Metavar: "N", Group: source.GroupTotals, Commands: []string{cmdDu},
		Usage: "also total each prefix down to N levels below SOURCE (0: the total)",
		Long:  "A prefix ends at a '/'; each includes what is below it, as du."})
	def(source.Flag{Name: "allocated", Value: source.Switch(&o.allocated), Group: source.GroupTotals, Commands: []string{cmdDu},
		Usage: "local: total the files' allocated bytes instead of their apparent size",
		Long:  "st_blocks x 512, as du; a directory's own blocks are not counted."})
	def(source.Flag{Name: "count-links-once", Value: source.Switch(&o.linksOnce), Group: source.GroupTotals, Commands: []string{cmdDu},
		Usage: "local: count a file with several hard links once, as du does",
		Long:  "Under the first of its names in byte order."})
	o.sortBy = sortName
	def(source.Flag{Name: "sort", Value: source.Text(&o.sortBy), Metavar: "name|size", Default: sortName, Group: source.GroupTotals, Commands: []string{cmdDu},
		Usage: "order prefixes by name, or by size, largest first",
		Long:  "By size the prefixes below each parent are ordered, each still above its own; the total keeps its place (last in the table, first in TSV and JSONL), and a prefix of unknown size comes last."})
	def(source.Flag{Name: "max-prefixes", Value: source.Count(&o.maxPrefixes, 1), Metavar: "N", Default: strconv.Itoa(totals.DefaultMaxPrefixes),
		Group: source.GroupTotals, Commands: []string{cmdDu}, Usage: "fail rather than hold more than N distinct prefixes",
		Long: "Raise it for a deep --depth over a wide tree when memory allows (about 150 bytes a prefix); it bounds memory, never the objects counted."})
	def(source.Flag{Name: "max-inodes", Value: source.Count(&o.maxInodes, 1), Metavar: "N", Default: strconv.Itoa(totals.DefaultMaxInodes),
		Group: source.GroupTotals, Commands: []string{cmdDu}, Usage: "local: the most hard-linked files to remember",
		Long: "With --count-links-once. Raise it for a tree of more hard-linked files when memory allows (about 100 bytes each)."})

	def(source.Flag{Name: "output", Short: "o", Shown: source.ShownPlace, Value: source.Text(&o.out), Metavar: "PATH", Group: source.GroupOutput, Commands: writing,
		Usage: "a dataset DIR, a .tsv or .jsonl file (.gz, .zst), or - (stdout)",
		Long:  "A trailing / names a directory whatever its name."})
	def(source.Flag{Name: "format", Value: source.Text(&o.format), Metavar: "auto|table|tsv|jsonl", Group: source.GroupOutput,
		Commands: []string{cmdList, cmdDu, cmdDiagList}, Usage: "text rows: auto is the table on a terminal, TSV otherwise",
		Long: "With -o FILE the file's name says the format; a --format that agrees with it is accepted."})
	def(source.Flag{Name: "columns", Metavar: "NAME[,NAME...]", Group: source.GroupOutput, Commands: writing,
		Usage: "TSV and JSONL: these columns, in order (repeatable)",
		Long:  "By the dataset's column names; a name the source's schema lacks is refused, listing its columns. Not with the table or a dataset; on a terminal the rows are then TSV.",
		Value: source.Func(names(&o.columns))})
	def(source.Flag{Name: "overwrite", Value: source.Switch(&o.overwrite), Group: source.GroupOutput, Commands: writing,
		Usage: "replace a previous dataset in -o"})

	def(source.Flag{Name: "include", Value: appendTo(&o.filter.Include), Metavar: "GLOB", Group: source.GroupFilters,
		Usage: "keep keys this glob matches (repeatable: any of them)",
		Long: "Matched against the key below SOURCE: with a '/' from SOURCE's root (logs/*.gz), without one the last segment at any depth (*.gz). " +
			"* and ? stay within a segment, ** is whole segments, [a-z], [!a-z], {a,b}, \\c. Includes sharing a literal prefix narrow the listed prefix. Filters keep or drop rows; they do not prune the walk."})
	def(source.Flag{Name: "exclude", Value: appendTo(&o.filter.Exclude), Metavar: "GLOB", Group: source.GroupFilters,
		Usage: "drop keys this glob matches (repeatable; beats any include)",
		Long:  "Matched as --include is. dir/ drops a directory's own row, dir/** the rows below it; the walk still lists them."})
	def(source.Flag{Name: "include-regex", Value: appendTo(&o.filter.IncludeRegex), Metavar: "REGEX", Group: source.GroupFilters,
		Usage: "keep keys this RE2 regex matches anywhere in the full key (repeatable)",
		Long:  "The full key, SOURCE's object-store prefix included (a glob matches the key below SOURCE). Any include, glob or regex, keeps a key. ^ with a literal prefix narrows the listed prefix as a glob's does."})
	def(source.Flag{Name: "exclude-regex", Value: appendTo(&o.filter.ExcludeRegex), Metavar: "REGEX", Group: source.GroupFilters,
		Usage: "drop keys this RE2 regex matches anywhere in the full key (repeatable)"})
	def(source.Flag{Name: "min-size", Value: source.Text(&o.filter.MinSize), Metavar: "SIZE", Group: source.GroupFilters,
		Usage: "keep objects (local: entries) of at least this size (1K, 1.5GiB)",
		Long:  "Every SIZE: bytes, or K, M, G, T and KiB, MiB, GiB, TiB (binary) or KB, MB, GB, TB (decimal), any case."})
	def(source.Flag{Name: "max-size", Value: source.Text(&o.filter.MaxSize), Metavar: "SIZE", Group: source.GroupFilters,
		Usage: "keep objects or entries of at most this size"})
	def(source.Flag{Name: "modified-since", Value: source.Text(&o.filter.Since), Metavar: "TIME", Group: source.GroupFilters,
		Usage: "keep objects or entries modified at or after TIME (2026-01-01, 7d)",
		Long:  "An offset or Z; UTC without one. A duration (7d, 12h) is that long before the run started, recorded as the time it names."})
	def(source.Flag{Name: "modified-until", Value: source.Text(&o.filter.Until), Metavar: "TIME", Group: source.GroupFilters,
		Usage: "keep objects or entries modified at or before this time"})
	def(source.Flag{Name: "storage-class", Metavar: "NAME[,NAME...]", Group: source.GroupFilters, Usage: "keep objects in these storage classes (STANDARD,GLACIER; repeatable)",
		Long:  "In any case; an object without one is in none. Azure Blob: the access tier (Hot, Cool, Cold, Archive).",
		Value: source.Func(names(&o.filter.StorageClasses))})

	o.partSize, o.rowBudget, o.partInterval, o.stall = listing.DefaultPartSize, listing.DefaultRowBudget, listing.DefaultPartInterval, 10*time.Minute
	def(source.Flag{Name: "workers", Familiar: []string{"concurrency"}, Short: "j", Value: source.Count(&o.workers, 1), Metavar: "N", Group: source.GroupPerformance,
		Usage: "listing workers (default 64; a local tree: 4 per CPU, at most 64)",
		Long:  "Raise it (128, 256) for a large listing the service does not throttle: listing waits on round trips, not the CPU; lower it when it throttles at the default."})
	def(source.Flag{Name: "encoders", Value: source.Count(&o.encoders, 1), Metavar: "N", Group: source.GroupPerformance, Commands: writing,
		Usage: "Parquet encoders, or a .zst file's compression jobs (default: CPUs)",
		Long:  "Lower it to leave cores to other work; the default follows GOMAXPROCS, the row budget and the descriptor limit. An explicit count is kept, with a warning when they cannot carry it."})
	def(source.Flag{Name: "row-budget", Value: &o.rowBudget, Metavar: "SIZE", Default: sizeText(listing.DefaultRowBudget), Group: source.GroupPerformance, Commands: writing,
		Usage: "rows held between listing and encoding",
		Long:  "Not a cap on resident memory. Raise it when resources.budget_wait_ns is large or spill is heavy and memory allows; lower it on a small host (the peak RSS is a few times it)."})
	def(source.Flag{Name: "spill-budget", Value: &o.spillBudget, Metavar: "SIZE", Group: source.GroupPerformance, Commands: writing,
		Usage: "cap on spill (default: half the output filesystem's free space)",
		Long:  "Set it when the output filesystem is shared or small. Past it, or at the reserve, listing waits instead of spilling."})
	def(source.Flag{Name: "part-size", Value: &o.partSize, Metavar: "SIZE", Default: sizeText(listing.DefaultPartSize), Group: source.GroupPerformance, Commands: writing,
		Usage: "target size of a Parquet part",
		Long:  "Smaller parts for readers that load whole parts, larger for fewer files on a very large listing."})
	def(source.Flag{Name: "part-interval", Value: source.Duration(&o.partInterval), Metavar: "DURATION", Default: source.FormatDuration(listing.DefaultPartInterval),
		Group: source.GroupPerformance, Commands: writing, Usage: "close a part with rows after this long, full or not",
		Long: "0: by size only. Lower it to close parts sooner on a slow listing (it bounds how long rows wait in an open part, not how much a resume repeats: a closed part still publishes in order); 0 for part boundaries independent of timing."})
	def(source.Flag{Name: "part-rotation-max-rows", Value: source.Count64(&o.partMaxRows, 0), Metavar: "N", Group: source.GroupPerformance, Commands: writing,
		Usage: "also close a part at N rows (0: by size and --part-interval only)",
		Long:  "Set it when readers want parts of a bounded row count; it is fixed for the run."})
	def(source.Flag{Name: "seed-from", Shown: source.ShownPlace, Value: source.Text(&o.seedFrom), Metavar: "PATH", Group: source.GroupPerformance,
		Usage: "start from a previous listing's key distribution, not the seed",
		Long:  "A committed dataset of the same source and scope, or a cut file (one key per line, ascending bytewise, optionally TAB and the keys expected up to it); the engine still splits adaptively."})

	def(source.Flag{Name: "max-duration", Value: source.Duration(&o.maxDuration), Metavar: "DURATION", Group: source.GroupLimits,
		Usage: "stop the run after this long (exit 124); 0: no limit",
		Long:  "Give every unattended run one. Only a dataset run of an object store (one with a journal) resumes where it stopped; text output and swath du run again."})
	def(source.Flag{Name: "stall-timeout", Value: source.Duration(&o.stall), Metavar: "DURATION", Default: source.FormatDuration(o.stall), Group: source.GroupLimits,
		Usage: "stop as stuck after this long without progress (exit 75)",
		Long:  "0: never. Raise it for a service that is slow to answer for minutes at a time. Only a dataset run of an object store (one with a journal) resumes where it stopped."})

	def(source.Flag{Name: "report", Shown: source.ShownPlace, Value: source.Text(&o.report), Metavar: "FILE", Group: source.GroupReporting,
		Usage: "write the run report (JSON) to FILE, or - for stdout, at the end",
		Long: "Written on failure too, with the outcome and the attempt id; not for a command line that does not parse (an unknown flag, a bad value). " +
			"A directory, or a name whose directory does not exist, is refused before listing; a report that cannot be written at the end fails the command (exit 1). - is refused where stdout carries rows."})
	o.term.register(f)

	o.spillCompress = true
	if diag {
		// The spill frame format is a runtime choice (and on resume: spill
		// is disposable, so a resume's frames never meet the crashed run's).
		def(source.Flag{Name: "no-spill-compress", Value: source.Off(&o.spillCompress), Commands: []string{cmdDiagList},
			Usage: "write spill frames uncompressed (not zstd level 1)"})
		def(source.Flag{Name: "spill-plain", Value: source.Switch(&o.spillPlain), Commands: []string{cmdDiagList},
			Usage: "write spill frames without delta coding (a comparison arm)"})
		def(source.Flag{Name: "no-journal", Value: source.Switch(&o.noJournal), Commands: []string{cmdDiagList},
			Usage: "publish parts only at the end, without the journal resume needs"})
		def(source.Flag{Name: "no-spill", Value: source.Switch(&o.noSpill), Commands: []string{cmdDiagList},
			Usage: "wait on the row budget instead of spilling rows to disk"})
		def(source.Flag{Name: "spill-reserve", Value: &o.spillReserve, Metavar: "SIZE", Commands: []string{cmdDiagList},
			Usage: "free space spill leaves on its filesystem (0: derived)",
			Long:  "Derived: one part per encoder, at least 1 GiB."})
	}
	registerProviders(f, o, diag)
	undoFlags(f, o)
	f.SetOutput(io.Discard)
}

// registerProviders registers the shared flag groups and each provider's
// flags on f. Each provider owns its flag group; a group several share
// (the store's, the range engine's) is registered once and passed to each
// of them. owners records which providers each provider flag applies to
// (the flags a registration adds), so a flag given for another provider
// is refused rather than ignored (foreignFlags); a flag without a help
// group of its own is shown in its provider's (registry.Factory.Title),
// or with the object stores'.
func registerProviders(f *pflag.FlagSet, o *options, diag bool) {
	o.sources, o.owners = map[string]any{}, map[string][]string{}
	added := func(register func()) []*pflag.Flag {
		before := map[string]bool{}
		f.VisitAll(func(fl *pflag.Flag) { before[fl.Name] = true })
		register()
		var flags []*pflag.Flag
		f.VisitAll(func(fl *pflag.Flag) {
			if !before[fl.Name] {
				flags = append(flags, fl)
			}
		})
		return flags
	}
	shared, groupFlags := source.SharedValues{}, map[*source.FlagGroup][]*pflag.Flag{}
	for _, g := range registry.Groups() {
		groupFlags[g] = added(func() { shared[g] = g.Flags(f, diag) })
		for _, fl := range groupFlags[g] {
			source.SetGroup(fl, source.GroupStores)
		}
	}
	o.allVersions = f.Lookup(source.AllVersionsFlag) // the terminal's nouns (rowNouns)
	for _, p := range registry.Factories() {
		named := source.SharedValues{}
		for _, g := range p.Shared {
			named[g] = shared[g]
			for _, fl := range groupFlags[g] {
				o.owners[fl.Name] = append(o.owners[fl.Name], p.Scheme)
			}
		}
		for _, fl := range added(func() { o.sources[p.Scheme] = p.Flags(f, diag, named) }) {
			o.owners[fl.Name] = append(o.owners[fl.Name], p.Scheme)
			source.SetGroup(fl, p.Title)
		}
	}
	// The totals' du flags read columns only the local schema has
	// (allocated_bytes; nlink, dev and inode), so another source refuses
	// them as it refuses the local provider's own flags.
	for _, n := range localTotalsFlags {
		o.owners[n] = []string{""}
	}
}

// localTotalsFlags are the totals' flags only a local source takes.
var localTotalsFlags = []string{"allocated", "count-links-once", "max-inodes"}

// undoName is the other spelling of switch name: --no-x for --x, --x for
// --no-x.
func undoName(name string) string {
	if n, ok := strings.CutPrefix(name, "no-"); ok {
		return n
	}
	return "no-" + name
}

// undoFlags gives each switch a resume may change (resumeFlags) its other
// spelling, so a resume can undo a switch its run was recorded with
// (--no-anonymous undoes --anonymous). The list commands take it too (a
// resume replays through them); their help leaves it out.
func undoFlags(f *pflag.FlagSet, o *options) {
	for _, n := range resumeFlags {
		fl := f.Lookup(n)
		if fl == nil || fl.Value.Type() != "bool" || f.Lookup(undoName(n)) != nil {
			continue
		}
		d := source.Described(fl)
		u := source.Define(f, source.Flag{Name: undoName(n), Value: inverse{fl.Value}, Group: d.Group, Commands: d.Commands,
			Usage: "undo a recorded --" + n})
		u.Hidden = true
		if own, ok := o.owners[n]; ok {
			o.owners[u.Name] = own
		}
	}
}

// inverse is a switch that sets the opposite of v.
type inverse struct{ v pflag.Value }

func (i inverse) Set(s string) error {
	b, err := strconv.ParseBool(s)
	if err != nil {
		return errors.New("want true or false")
	}
	return i.v.Set(strconv.FormatBool(!b))
}
func (i inverse) String() string { return "" }
func (i inverse) Type() string   { return "bool" }

// mayChange reports whether a resume may give flag name again: a runtime
// or operational flag, or the other spelling of one (undoFlags).
func mayChange(name string) bool {
	return slices.Contains(resumeFlags, name) || slices.Contains(resumeFlags, undoName(name))
}

// names appends a comma list's names to dst; an empty name (an empty
// --columns, --storage-class ,) is refused, never read as no restriction.
func names(dst *[]string) func(string) error {
	return func(v string) error {
		var add []string
		for _, c := range strings.Split(v, ",") {
			if c = strings.TrimSpace(c); c == "" {
				return errors.New("an empty name: want NAME[,NAME...]")
			}
			add = append(add, c)
		}
		*dst = append(*dst, add...)
		return nil
	}
}

// sizeText is n bytes as a user types it: 64MiB, 1GiB, or the bytes.
func sizeText(n int64) string {
	for _, u := range []struct {
		name  string
		shift uint
	}{{"GiB", 30}, {"MiB", 20}, {"KiB", 10}} {
		if n != 0 && n%(1<<u.shift) == 0 {
			return strconv.FormatInt(n>>u.shift, 10) + u.name
		}
	}
	return strconv.FormatInt(n, 10)
}

// seeHelp is a usage error that ends with the help to read: main prints
// "See 'swath CMD --help'." under it.
type seeHelp struct {
	error
	cmd string // "" for the root help
}

func (s seeHelp) Unwrap() error { return s.error }

// see is the line main prints under s.
func (s seeHelp) see() string {
	if s.cmd == "" {
		return "See 'swath --help'."
	}
	return "See 'swath " + s.cmd + " --help'."
}

// parsed is a command line parsed with a flag set (parseFlags).
type parsed struct {
	operands []string // the arguments that are not flags, in order
	// given are the flags given, in order, each in its canonical spelling
	// (--name=value; --name for a switch), as typed: what a command hands
	// on to another (the root's global flags).
	given []string
	// recorded are given as a recorded or reported command line shows
	// them: each value by its flag's table entry (shownValue).
	recorded []string
	longHelp bool // --help (not -h) asked for help
}

// commandLine is a command line as recorded or reported: the command's
// words, the recorded flags, then the operands, redacted as roots, after
// -- when one looks like a flag.
func commandLine(words string, p parsed, operands ...string) []string {
	argv := append(strings.Fields(words), p.recorded...)
	if slices.ContainsFunc(operands, func(o string) bool { return strings.HasPrefix(o, "-") }) {
		argv = append(argv, "--")
	}
	for _, o := range operands {
		argv = append(argv, registry.Redact(o))
	}
	return argv
}

// parseFlags parses args with f, GNU-style: flags before, between and
// after the operands, short flags bundled (-vq) and their values attached
// (-d1), --name value and --name=value, and -- ends the flags for good.
// A long name typed with one dash (-depth) is refused, not read as -d
// epth. accept, if set, may refuse a flag before it is set. A help request
// is pflag.ErrHelp; any other error is a usage error naming the flag and
// pointing at cmd's help, after the --log-format the command line gives
// (as the parser reads it: argFlags) is set on f, so that the refusal is
// reported in it.
func parseFlags(f *pflag.FlagSet, cmd string, args []string, accept func(*pflag.Flag) error) (parsed, error) {
	var p parsed
	flagArgs := args
	if i := slices.Index(args, "--"); i >= 0 {
		flagArgs = args[:i]
	}
	p.longHelp = slices.Contains(flagArgs, "--help")
	// The root's flags end at the command word (dispatch).
	flags, _ := argFlags(f, args, cmd != "")
	usage := func(err error) error {
		if v := askedFormat(flags); v != "" {
			_ = f.Set("log-format", v)
		}
		return seeHelp{usageError{err}, cmd}
	}
	for _, a := range flags {
		if a.dashLong {
			return p, usage(fmt.Errorf("unknown flag -%s (did you mean --%s?)", a.name, a.name))
		}
	}
	f.Usage = func() {}
	err := f.ParseAll(args, func(fl *pflag.Flag, value string) error {
		if accept != nil {
			if err := accept(fl); err != nil {
				return err
			}
		}
		if err := f.Set(fl.Name, value); err != nil {
			return err
		}
		if fl.NoOptDefVal != "" && value == fl.NoOptDefVal {
			p.given = append(p.given, "--"+fl.Name)
			p.recorded = append(p.recorded, "--"+fl.Name)
		} else {
			p.given = append(p.given, "--"+fl.Name+"="+value)
			p.recorded = append(p.recorded, "--"+fl.Name+"="+shownValue(fl, value))
			remember(fl, value)
		}
		return nil
	})
	p.operands = f.Args()
	var ne *pflag.NotExistError
	var ve *pflag.ValueRequiredError
	var ie *pflag.InvalidValueError
	var u usageError
	switch {
	case err == nil:
		return p, nil
	case errors.Is(err, pflag.ErrHelp):
		return p, pflag.ErrHelp
	case errors.As(err, &u):
		return p, usage(u.error)
	case errors.As(err, &ne):
		// The name alone: what follows it may be a value.
		typed := "--" + ne.GetSpecifiedName()
		if ne.GetSpecifiedShortnames() != "" {
			typed = "-" + ne.GetSpecifiedName()
		}
		return p, usage(errors.New("unknown flag " + typed + suggestFlag(f, ne.GetSpecifiedName())))
	case errors.As(err, &ve):
		return p, usage(fmt.Errorf("--%s needs a value (%s)", ve.GetFlag().Name, source.Described(ve.GetFlag()).Metavar))
	case errors.As(err, &ie):
		return p, usage(fmt.Errorf("--%s %q: %w", ie.GetFlag().Name, shownValue(ie.GetFlag(), ie.GetValue()), errors.Unwrap(ie)))
	}
	return p, usage(err)
}

// argFlag is one flag of a command line as a flag set's parser reads it.
type argFlag struct {
	fl       *pflag.Flag // nil: one the set does not define
	name     string      // as typed, without its dashes
	value    string      // its value (a switch's NoOptDefVal when given bare)
	dashLong bool        // a long name typed with one dash (-depth)
}

// argFlags walks args as f's parser reads them: a flag's value is
// consumed, not read as a flag (--include -verbose, --include
// --log-format), the flags end at -- and, unless interspersed, at the
// first operand. A flag f does not define is taken to have no value (the
// parser refuses it there); the walk goes on past it, so that a refusal
// still finds the flags after it (askedFormat).
func argFlags(f *pflag.FlagSet, args []string, interspersed bool) (flags []argFlag, operands []string) {
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--":
			return flags, append(operands, args[i+1:]...)
		case len(a) < 2 || a[0] != '-':
			if !interspersed {
				return flags, append(operands, args[i:]...)
			}
			operands = append(operands, a)
		case a[1] == '-':
			name, v, eq := strings.Cut(a[2:], "=")
			fl := f.Lookup(name)
			switch {
			case fl == nil || eq:
			case fl.NoOptDefVal != "":
				v = fl.NoOptDefVal
			case i+1 < len(args):
				i++
				v = args[i]
			}
			flags = append(flags, argFlag{fl: fl, name: name, value: v})
		default:
			if name, v, _ := strings.Cut(a[1:], "="); len(name) > 1 && f.Lookup(name) != nil {
				flags = append(flags, argFlag{fl: f.Lookup(name), name: name, value: v, dashLong: true})
				continue
			}
			// A bundle (-vd 2): switches, then the first value-taking
			// flag takes the rest of the token, or the next argument.
		bundle:
			for j := 1; j < len(a); j++ {
				fl := f.ShorthandLookup(a[j : j+1])
				switch rest := a[j+1:]; {
				case fl == nil:
					flags = append(flags, argFlag{name: a[j : j+1]})
				case strings.HasPrefix(rest, "=") && len(rest) > 1:
					flags = append(flags, argFlag{fl: fl, name: fl.Name, value: rest[1:]})
				case fl.NoOptDefVal != "":
					flags = append(flags, argFlag{fl: fl, name: fl.Name, value: fl.NoOptDefVal})
					continue
				case rest != "":
					flags = append(flags, argFlag{fl: fl, name: fl.Name, value: rest})
				case i+1 < len(args):
					i++
					flags = append(flags, argFlag{fl: fl, name: fl.Name, value: args[i]})
				default:
					flags = append(flags, argFlag{fl: fl, name: fl.Name})
				}
				break bundle
			}
		}
	}
	return flags, operands
}

// askedFormat is the --log-format flags give, the last one as the parser
// takes it; "" for none, or a value that is not a format.
func askedFormat(flags []argFlag) string {
	v := ""
	for _, a := range flags {
		if a.fl != nil && a.fl.Name == "log-format" && !a.dashLong {
			v = a.value
		}
	}
	if v != logText && v != logJSON {
		return ""
	}
	return v
}

// hidden are the parts of place and secret values given on command lines
// in this process that their shown form leaves out (shownValue): what
// scrub takes out of any message, an error that quotes a value as it was
// used (open PATH: ...) among them.
var hidden struct {
	mu    sync.Mutex
	parts []string // longest first
}

// minHidden is the shortest part scrub replaces: a shorter one would cut
// into ordinary words.
const minHidden = 4

// remember notes the parts of fl's value that its shown form hides.
func remember(fl *pflag.Flag, value string) {
	shown := shownValue(fl, value)
	if shown == value {
		return
	}
	parts := []string{value}
	if source.Described(fl).Shown == source.ShownPlace {
		// A place's userinfo, query and fragment, which an error may
		// quote apart from the rest (a path made absolute).
		if _, rest, ok := strings.Cut(value, "://"); ok {
			authority, _, _ := strings.Cut(rest, "/")
			if i := strings.LastIndex(authority, "@"); i >= 0 {
				parts = append(parts, authority[:i])
			}
		}
		if _, q, ok := strings.Cut(value, "?"); ok {
			q, frag, _ := strings.Cut(q, "#")
			parts = append(parts, q, frag)
		} else if _, frag, ok := strings.Cut(value, "#"); ok {
			parts = append(parts, frag)
		}
	}
	hidden.mu.Lock()
	defer hidden.mu.Unlock()
	for _, p := range parts {
		if len(p) >= minHidden && !slices.Contains(hidden.parts, p) {
			hidden.parts = append(hidden.parts, p)
		}
	}
	slices.SortFunc(hidden.parts, func(a, b string) int { return len(b) - len(a) })
}

// scrub is s without the hidden parts of the place and secret values
// given (remember): the backstop for a message that quotes one as it was
// used, applied where an outcome or an event is settled.
func scrub(s string) string {
	hidden.mu.Lock()
	defer hidden.mu.Unlock()
	for _, p := range hidden.parts {
		s = strings.ReplaceAll(s, p, redacted)
	}
	return s
}

// shownValue is fl's value as a recorded or reported command line, or an
// error, shows it: by its table entry (source.Flag.Shown).
func shownValue(fl *pflag.Flag, value string) string {
	switch source.Described(fl).Shown {
	case source.ShownNever:
		return redacted
	case source.ShownPlace:
		return registry.RedactPlace(value)
	}
	return value
}

// suggestFlag is the " (did you mean --x?)" of an unknown flag name: a
// flag that names it as another tool's spelling (source.Flag.Familiar),
// else the visible flag nearest it within two edits; "" for none.
func suggestFlag(f *pflag.FlagSet, name string) string {
	best, dist := "", 3
	f.VisitAll(func(fl *pflag.Flag) {
		if fl.Hidden {
			return
		}
		if slices.Contains(source.Described(fl).Familiar, name) {
			best, dist = fl.Name, -1
		}
		if d := editDistance(name, fl.Name); d < dist {
			best, dist = fl.Name, d
		}
	})
	if best == "" || len(name) < 2 {
		return ""
	}
	return " (did you mean --" + best + "?)"
}

// editDistance is the Levenshtein distance between a and b, in bytes.
func editDistance(a, b string) int {
	prev := make([]int, len(b)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(a); i++ {
		cur := make([]int, len(b)+1)
		cur[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			cur[j] = min(prev[j]+1, cur[j-1]+1, prev[j-1]+cost)
		}
		prev = cur
	}
	return prev[len(b)]
}
