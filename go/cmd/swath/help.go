// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"sync"

	"github.com/spf13/pflag"
	"golang.org/x/term"

	"github.com/varveio/swath/go/internal/registry"
	"github.com/varveio/swath/go/internal/source"
)

// helpStyle is how help is printed: its width, bold headings and flag
// names, and the long form (--help: each flag's further text under it).
type helpStyle struct {
	width int
	bold  bool
	long  bool
}

// Help is at most maxHelpWidth columns wide: plainHelpWidth when stdout is
// not a terminal, the terminal's width up to the maximum when it is.
const (
	maxHelpWidth   = 100
	plainHelpWidth = 80
)

// docStyle is the help as docs/cli.md holds it: the long form, the widest,
// no styling.
var docStyle = helpStyle{width: maxHelpWidth, long: true}

// styleFor is the help's style on w: on a terminal its width (at most
// maxHelpWidth) and bold under the --color rules (colorEnabled).
func styleFor(w io.Writer, color string, long bool) helpStyle {
	s := helpStyle{width: plainHelpWidth, long: long}
	if f, ok := w.(*os.File); ok && term.IsTerminal(int(f.Fd())) {
		if cols, _, err := term.GetSize(int(f.Fd())); err == nil && cols > 0 {
			s.width = min(cols, maxHelpWidth)
		}
		s.bold = colorEnabled(color, os.LookupEnv, true)
	} else if color == "always" {
		s.bold = true
	}
	return s
}

func (s helpStyle) strong(t string) string {
	if !s.bold {
		return t
	}
	return "\x1b[1m" + t + "\x1b[0m"
}

// heading prints a section heading.
func (s helpStyle) heading(b *strings.Builder, title string) {
	b.WriteString("\n" + s.strong(title+":") + "\n")
}

// para prints text wrapped to the width at indent.
func (s helpStyle) para(b *strings.Builder, indent int, text string) {
	for _, l := range wrap(text, s.width-indent) {
		b.WriteString(strings.Repeat(" ", indent) + l + "\n")
	}
}

// wrap breaks text into lines of at most width columns at spaces (a
// longer word stands alone).
func wrap(text string, width int) []string {
	var lines []string
	line := ""
	for _, w := range strings.Fields(text) {
		switch {
		case line == "":
			line = w
		case len(line)+1+len(w) <= width:
			line += " " + w
		default:
			lines = append(lines, line)
			line = w
		}
	}
	if line != "" {
		lines = append(lines, line)
	}
	return lines
}

// nameColumn is where descriptions start, at most: a flag whose names are
// wider puts its description on the next line.
const nameColumn = 30

// flagNames is fl's left column: its short form, its long name and its
// value's placeholder, and the column's width without styling.
func flagNames(fl *pflag.Flag, s helpStyle) (string, int) {
	d := source.Described(fl)
	short := "    "
	if fl.Shorthand != "" {
		short = s.strong("-"+fl.Shorthand) + ", "
	}
	value := ""
	switch {
	case d.Metavar == "":
	case fl.NoOptDefVal != "":
		value = "[=" + d.Metavar + "]"
	default:
		value = " " + d.Metavar
	}
	plain := "  " + "    " + "--" + fl.Name + value
	return "  " + short + s.strong("--"+fl.Name) + value, len(plain)
}

// writeFlags prints flags in two columns: names, then the one-line
// description and default, and in the long form the flag's further text
// under the description.
func writeFlags(b *strings.Builder, flags []*pflag.Flag, s helpStyle, col int) {
	for _, fl := range flags {
		d := source.Described(fl)
		names, n := flagNames(fl, s)
		desc := d.Usage
		if d.Default != "" {
			desc += " (default " + d.Default + ")"
		}
		lines := wrap(desc, s.width-col)
		if n+2 > col {
			b.WriteString(names + "\n")
		} else if len(lines) > 0 {
			b.WriteString(names + strings.Repeat(" ", col-n) + lines[0] + "\n")
			lines = lines[1:]
		}
		for _, l := range lines {
			b.WriteString(strings.Repeat(" ", col) + l + "\n")
		}
		if s.long && d.Long != "" {
			s.para(b, col+2, d.Long)
		}
	}
}

// column is where the descriptions of flags start: after the widest names
// that fit nameColumn.
func column(flags []*pflag.Flag) int {
	col := 0
	for _, fl := range flags {
		if _, n := flagNames(fl, helpStyle{}); n+2 <= nameColumn {
			col = max(col, n+2)
		}
	}
	if col == 0 {
		return nameColumn
	}
	return col
}

// groupOrder is the help's groups in order: the totals (swath du's), then
// output and filters, the object stores', each provider's (registry.
// Factory.Title: the object stores by scheme descending, so S3, GCS and
// Azure Blob, and the local tree last), then performance, limits,
// reporting and the diagnostics.
func groupOrder() []string {
	order := []string{source.GroupTotals, source.GroupOutput, source.GroupFilters, source.GroupStores}
	fs := registry.Factories()
	for i := len(fs) - 1; i >= 0; i-- {
		order = append(order, fs[i].Title)
	}
	return append(order, source.GroupPerformance, source.GroupLimits, source.GroupReporting, source.GroupDiagnostics)
}

// groupOf is fl's help group: the diagnostics for a flag only swath diag
// takes, else its table entry's.
func groupOf(fl *pflag.Flag) string {
	if diagOnly()[fl.Name] {
		return source.GroupDiagnostics
	}
	return source.Described(fl).Group
}

// shown reports whether cmd's help lists fl: a flag the command takes;
// swath diag list shows its diagnostic flags only (swath list --help has
// the rest), swath diag count every flag it takes.
func shown(fl *pflag.Flag, cmd command) bool {
	cmds := source.Described(fl).Commands
	switch {
	case fl.Hidden, cmds != nil && !slices.Contains(cmds, cmd.name()):
		return false
	case diagOnly()[fl.Name]:
		return cmd.diag
	}
	return !cmd.diag || cmd.count
}

// writeGroups prints the flags of f that keep says, by group.
func writeGroups(b *strings.Builder, f *pflag.FlagSet, s helpStyle, keep func(*pflag.Flag) bool) {
	by := map[string][]*pflag.Flag{}
	var all []*pflag.Flag
	f.VisitAll(func(fl *pflag.Flag) {
		if keep(fl) {
			by[groupOf(fl)] = append(by[groupOf(fl)], fl)
			all = append(all, fl)
		}
	})
	col := column(all)
	for _, g := range groupOrder() {
		if len(by[g]) > 0 {
			s.heading(b, g)
			writeFlags(b, by[g], s, col)
		}
	}
}

// commandHelp is a command's help text before its flags.
type commandHelp struct {
	usage    []string // the usage lines, without "swath "
	about    string
	examples []string
}

func (h commandHelp) write(b *strings.Builder, s helpStyle) {
	for i, u := range h.usage {
		lead := "Usage: "
		if i > 0 {
			lead = "       "
		}
		b.WriteString(s.strong(lead) + "swath " + u + "\n")
	}
	b.WriteString("\n")
	s.para(b, 0, h.about)
	if len(h.examples) > 0 {
		s.heading(b, "Examples")
		for _, e := range h.examples {
			b.WriteString("  " + e + "\n")
		}
	}
}

// examples are the providers' example roots (registry.Factory.Example):
// the object stores' in the help's order (groupOrder), and the local
// tree's.
func examples() (stores []string, local string) {
	fs := registry.Factories()
	for i := len(fs) - 1; i >= 0; i-- {
		switch {
		case fs[i].Example == "":
		case fs[i].Scheme == "":
			local = fs[i].Example
		default:
			stores = append(stores, fs[i].Example)
		}
	}
	return stores, local
}

// helpOf is the help text of a list command.
func helpOf(cmd command) commandHelp {
	src := "SOURCE is " + sourceForms() + "."
	stores, local := examples()
	a, b := stores[0], stores[min(1, len(stores)-1)]
	switch {
	case cmd.count && cmd.diag:
		return commandHelp{usage: []string{"diag count SOURCE [OPTIONS]"},
			about:    "List SOURCE and count its rows and object bytes, writing nothing, to measure the listing: rows are counted as they arrive, never ordered, buffered or spilled. Prints one line of counts and resources. " + src,
			examples: []string{"swath diag count " + a, "swath diag count " + a + " --sequential"}}
	case cmd.diag:
		return commandHelp{usage: []string{"diag list SOURCE [OPTIONS]"},
			about:    "Every swath list option (swath list --help), and the diagnostic ones below: they exist to run an experiment or a comparison, not to operate the tool. " + src,
			examples: []string{"swath diag list " + a + " -o ds --no-seed", "swath diag list " + a + " -o ds --page-size 100"}}
	case cmd.du:
		return commandHelp{usage: []string{"du SOURCE [OPTIONS]"},
			about:    "Total SOURCE's objects (a local tree's files), bytes and newest time, and with -d N each prefix's down to N levels below SOURCE, a parent including its children, as du. On stdout: a table on a terminal (the total last), TSV otherwise or with --format jsonl (the total first, at Depth 0). Bytes are the objects' sizes as listed, not storage or billing. " + src,
			examples: []string{"swath du " + a, "swath du -d 1 " + local, "swath du " + b + " --format jsonl"}}
	}
	return commandHelp{usage: []string{"list SOURCE [OPTIONS]", "ls SOURCE [OPTIONS]"},
		about:    "List SOURCE: into a sorted Parquet dataset with -o DIR, as text rows into -o NAME.tsv or NAME.jsonl (optionally .gz or .zst), or as text rows on stdout without -o. " + src,
		examples: []string{"swath list " + a + " -o logs.parquet", "swath list " + a + " | head", "swath list " + b + " -o rows.jsonl.zst"}}
}

// usage prints a list command's help: its synopsis, examples and its
// flags by group.
func usage(w io.Writer, f *pflag.FlagSet, cmd command, s helpStyle) {
	var b strings.Builder
	helpOf(cmd).write(&b, s)
	writeGroups(&b, f, s, func(fl *pflag.Flag) bool { return shown(fl, cmd) })
	if !cmd.diag {
		b.WriteString("\nswath resume --help lists the options a resume may change.\n")
		if s.long {
			b.WriteString("Profiling: SWATH_CPUPROFILE=FILE and SWATH_MEMPROFILE=FILE.\n")
		}
	}
	fmt.Fprint(w, b.String())
}

// resumeHelp is swath resume's help text before its flags.
var resumeHelp = commandHelp{usage: []string{"resume DIR [OPTIONS]"},
	about:    "Continue the interrupted run in DIR with the command its journal recorded. Only the options below may be given again (the last value wins); anything else is fixed for the run. A switch the run was started with is undone by its other spelling: --no-anonymous undoes --anonymous, --adaptive-limit undoes --no-adaptive-limit.",
	examples: []string{"swath resume logs.parquet", "swath resume logs.parquet --workers 32 --max-duration 1h"}}

// resumeUsage prints swath resume's help: the flags it takes again, the
// other spellings that undo a recorded switch among them.
func resumeUsage(w io.Writer, f *pflag.FlagSet, s helpStyle) {
	var b strings.Builder
	resumeHelp.write(&b, s)
	writeGroups(&b, f, s, func(fl *pflag.Flag) bool { return mayChange(fl.Name) })
	fmt.Fprint(w, b.String())
}

// showHelp is swath show's help text before its flags.
var showHelp = commandHelp{usage: []string{"show DIR [OPTIONS]"},
	about:    "Print a committed dataset's manifest summary, or an interrupted run's recorded command, identity and committed parts.",
	examples: []string{"swath show logs.parquet", "swath show logs.parquet --journal"}}

// diagHelp is swath diag's own help.
var diagHelp = commandHelp{usage: []string{"diag list SOURCE [OPTIONS]", "diag count SOURCE [OPTIONS]"},
	about: "swath list, or a count that writes nothing (a throughput measurement), with the diagnostic options: they exist to run an experiment or a comparison, not to operate the tool. swath diag list --help and swath diag count --help list them."}

// rootCommands are the root help's command lines.
var rootCommands = [][2]string{
	{"list, ls", "list SOURCE into a sorted Parquet dataset, or as text rows"},
	{"du", "total SOURCE's objects and bytes, per prefix down to -d levels"},
	{"resume", "continue an interrupted run"},
	{"show", "show a dataset or an interrupted run"},
	{"diag", "list or count with the diagnostic options (experiments, comparisons)"},
	{"completion", "print a shell completion script (bash, zsh, fish)"},
	{"help", "show a command's help (swath help COMMAND)"},
	{"version", "print the version"},
}

// commandWords are the words dispatch takes as a command.
var commandWords = []string{"list", "ls", "du", "resume", "show", "diag", "completion", "help", "version"}

// rootHelp prints swath's own help.
func rootHelp(w io.Writer, s helpStyle) {
	var b strings.Builder
	s.para(&b, 0, "swath lists object stores and file trees in parallel, into a sorted Parquet dataset, as text rows, or as size totals.")
	b.WriteString("\n" + s.strong("Usage: ") + "swath COMMAND [OPTIONS] ...\n")
	s.heading(&b, "Commands")
	for _, c := range rootCommands {
		b.WriteString("  " + s.strong(c[0]) + strings.Repeat(" ", 12-len(c[0])) + c[1] + "\n")
	}
	s.heading(&b, "Options")
	f := pflag.NewFlagSet("swath", pflag.ContinueOnError)
	f.SortFlags = false
	var t termOptions
	t.register(f)
	var help, version bool
	source.Define(f, source.Flag{Name: "help", Short: "h", Value: source.Switch(&help), Usage: "print help (-h: the short form)"})
	source.Define(f, source.Flag{Name: "version", Short: "V", Value: source.Switch(&version), Usage: "print the version"})
	var keep []*pflag.Flag
	f.VisitAll(func(fl *pflag.Flag) {
		if slices.Contains(globalFlags, fl.Name) || fl.Name == "help" || fl.Name == "version" {
			keep = append(keep, fl)
		}
	})
	writeFlags(&b, keep, s, column(keep))
	b.WriteString("\n")
	s.para(&b, 0, "SOURCE is "+sourceForms()+". The options above may also come before COMMAND.")
	s.heading(&b, "Examples")
	stores, local := examples()
	for _, e := range []string{"swath list " + stores[0] + " -o logs.parquet", "swath list " + stores[0] + " | head", "swath du -d 1 " + local} {
		b.WriteString("  " + e + "\n")
	}
	b.WriteString("\nSee 'swath COMMAND --help'.\n")
	fmt.Fprint(w, b.String())
}

// diagOnly is the flags only swath diag takes: those registered with the
// diagnostic flags and not without (registerFlags; each provider's own
// diagnostic flags included).
var diagOnly = sync.OnceValue(func() map[string]bool {
	names := func(diag bool) map[string]bool {
		f := pflag.NewFlagSet("", pflag.ContinueOnError)
		registerFlags(f, &options{}, diag)
		m := map[string]bool{}
		f.VisitAll(func(fl *pflag.Flag) { m[fl.Name] = true })
		return m
	}
	all, list := names(true), names(false)
	for n := range list {
		delete(all, n)
	}
	return all
})

// placement refuses a flag the command does not take: a totals flag on
// another command than swath du, an output flag on swath du or swath diag
// count (checked first, so the message never names a command that refuses
// the flag too), a diagnostic flag on swath list or swath du.
func placement(f *pflag.FlagSet, cmd command) (err error) {
	f.Visit(func(fl *pflag.Flag) {
		cmds := source.Described(fl).Commands
		taken := cmds == nil || slices.Contains(cmds, cmd.name())
		switch {
		case err != nil:
		case !taken && slices.Equal(cmds, []string{cmdDu}):
			err = fmt.Errorf("--%s has no meaning for swath %s: it shapes swath du's totals", fl.Name, cmd.name())
		case !taken && cmd.count:
			to := "swath list"
			if diagOnly()[fl.Name] {
				to = "swath diag list"
			}
			what := "which writes nothing"
			if cmd.du {
				what = "which writes totals, not rows"
			}
			err = fmt.Errorf("--%s has no meaning for swath %s, %s: %s takes it", fl.Name, cmd.name(), what, to)
		case !cmd.diag && diagOnly()[fl.Name]:
			to := "swath diag " + cmd.name()
			if cmd.du {
				to = "swath diag count or swath diag list" // swath du has no diagnostic form
			}
			err = fmt.Errorf("--%s is a diagnostic flag: give it to %s", fl.Name, to)
		}
	})
	return err
}

// errHelpGroups is TestHelpGroupsCoverFlags's refusal.
var errHelpGroups = errors.New("help groups")

// metavars are the placeholders a flag's value may have, besides an
// enum's choices (a|b|c).
var metavars = []string{"N", "SIZE", "DURATION", "TIME", "PATH", "FILE", "DIR", "URL", "REGEX", "GLOB", "NAME", "COMMAND", "TOKEN"}

// checkHelpGroups reports a flag of f in no help group, one whose
// placeholder is not one of metavars nor an enum, and one whose short help
// line does not fit maxHelpWidth columns.
func checkHelpGroups(f *pflag.FlagSet) error {
	var problems []string
	f.VisitAll(func(fl *pflag.Flag) {
		d := source.Described(fl)
		if !slices.Contains(groupOrder(), groupOf(fl)) {
			problems = append(problems, fmt.Sprintf("--%s is in no help group (%q)", fl.Name, groupOf(fl)))
		}
		if d.Metavar != "" && !slices.Contains(metavars, d.Metavar) && !strings.Contains(d.Metavar, "|") {
			problems = append(problems, fmt.Sprintf("--%s: placeholder %q", fl.Name, d.Metavar))
		}
		line := d.Usage
		if d.Default != "" {
			line += " (default " + d.Default + ")"
		}
		if len(line) > maxHelpWidth-nameColumn {
			problems = append(problems, fmt.Sprintf("--%s: its help line is %d columns, over %d: %s", fl.Name, len(line), maxHelpWidth-nameColumn, line))
		}
	})
	if len(problems) > 0 {
		return fmt.Errorf("%w: %s", errHelpGroups, strings.Join(problems, "; "))
	}
	return nil
}
