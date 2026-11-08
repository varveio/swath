// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
)

// helpFlag is one flag in the help: its name, its stacked forms (-vv,
// -vvv, -qq, shown on its line), and for the performance and limits
// groups when to change it.
type helpFlag struct {
	name    string
	stacked []string
	when    string
}

// helpGroup is a heading of the list commands' help and the flags under it,
// in order.
type helpGroup struct {
	title string
	flags []helpFlag
}

// helpGroups is the list commands' help, every flag of swath diag list in
// exactly one group (TestHelpGroupsCoverFlags); the diagnostic group is
// printed by swath diag list --help only.
var helpGroups = []helpGroup{
	{"Source and output", []helpFlag{
		{name: "o"},
		{name: "overwrite"},
		{name: "format"},
		{name: "region"}, {name: "endpoint"}, {name: "anonymous"}, {name: "path-style"},
		{name: "profile"}, {name: "request-payer"}, {name: "bearer-token-command"}, {name: "bearer-token-refresh-interval"},
		{name: "fips"}, {name: "dualstack"}, {name: "fetch-owner"}, {name: "all-versions"},
		{name: "gcs-endpoint"}, {name: "gcs-anonymous"}, {name: "gcs-credentials"}, {name: "gcs-user-project"},
		{name: "metadata"}, {name: "no-dirs"}, {name: "no-link-target"},
		{name: "include"}, {name: "exclude"}, {name: "min-size"}, {name: "max-size"},
		{name: "modified-since"}, {name: "modified-until"}, {name: "storage-class"},
		{name: "seed-from"},
	}},
	{"Performance", []helpFlag{
		{name: "workers", when: "raise it (128, 256) for a large listing the service does not throttle: listing waits on round trips, not the CPU; lower it when it throttles at the default"},
		{name: "row-budget", when: "raise it when resources.budget_wait_ns is large or spill is heavy and memory allows; lower it on a small host (the peak RSS is a few times it)"},
		{name: "spill-budget", when: "set it when the output filesystem is shared or small: the default takes up to half its free space"},
		{name: "encoders", when: "lower it to leave cores to other work; the default already follows the cores and the row budget"},
		{name: "part-size", when: "smaller parts for readers that load whole parts, larger for fewer files on a very large listing"},
		{name: "part-interval", when: "lower it to close parts sooner on a slow listing (it bounds how long rows wait in an open part, not how much a resume repeats: a closed part still publishes in order); 0 for part boundaries independent of timing"},
		{name: "part-rotation-max-rows", when: "set it when readers want parts of a bounded row count; it is frozen for resume"},
	}},
	{"Limits", []helpFlag{
		{name: "max-duration", when: "give every unattended run one; a stopped object-store run resumes where it stopped"},
		{name: "stall-timeout", when: "raise it for a service that is slow to answer for minutes at a time"},
		{name: "request-rate", when: "set it to stay under a request quota shared with other clients"},
		{name: "limiter", when: "turn it off (with --request-rate) when a run crawls under sustained throttling with the limit near 1"},
		{name: "request-timeout", when: "raise it for a slow S3-compatible store whose pages take longer to answer"},
		{name: "gcs-request-timeout", when: "raise it for a slow GCS endpoint or emulator"},
	}},
	{"Output and reporting", []helpFlag{
		{name: "report"}, {name: "report-format"},
		{name: "progress"}, {name: "no-progress"}, {name: "progress-interval"},
		{name: "stats"}, {name: "no-stats"},
		{name: "v", stacked: []string{"vv", "vvv"}}, {name: "q", stacked: []string{"qq"}},
		{name: "color"},
	}},
	{diagnosticGroup, []helpFlag{
		{name: "seed"}, {name: "structure"}, {name: "slow-split"}, {name: "floors"}, {name: "sequential"}, {name: "max-keys"}, {name: "page-size"},
		{name: "limiter-slow-start"},
		{name: "spill-plain"}, {name: "spill-compress"}, {name: "spill-reserve"}, {name: "no-spill"},
		{name: "no-journal"},
	}},
}

// diagnosticGroup is the heading of the flags only swath diag takes.
const diagnosticGroup = "Diagnostics"

// notCounted are the flags swath count refuses: it lists and counts,
// writing nothing, so it has no output, no parts and no reorder storage.
var notCounted = []string{"o", "format", "overwrite", "part-size", "row-budget", "spill-budget", "encoders",
	"spill-plain", "spill-compress", "spill-reserve", "no-spill", "part-interval", "part-rotation-max-rows", "no-journal"}

// diagOnly is the flags only swath diag takes: those registered with the
// diagnostic flags and not without (registerFlags; each provider's own
// diagnostic flags included).
var diagOnly = sync.OnceValue(func() map[string]bool {
	names := func(diag bool) map[string]bool {
		f := flag.NewFlagSet("", flag.ContinueOnError)
		registerFlags(f, &options{}, diag)
		m := map[string]bool{}
		f.VisitAll(func(fl *flag.Flag) { m[fl.Name] = true })
		return m
	}
	all, list := names(true), names(false)
	for n := range list {
		delete(all, n)
	}
	return all
})

// placement refuses a flag the command does not take: an output flag on
// swath count or swath diag count (checked first, so the message never
// names a command that refuses the flag too), a diagnostic flag on swath
// list or swath count.
func placement(f *flag.FlagSet, cmd command) (err error) {
	f.Visit(func(fl *flag.Flag) {
		switch {
		case err != nil:
		case cmd.count && slices.Contains(notCounted, fl.Name):
			to := "swath list"
			if diagOnly()[fl.Name] {
				to = "swath diag list"
			}
			err = fmt.Errorf("%s has no meaning for swath %s, which writes nothing: %s takes it", dashed(fl.Name), cmd.name(), to)
		case !cmd.diag && diagOnly()[fl.Name]:
			err = fmt.Errorf("%s is a diagnostic flag: give it to swath diag %s", dashed(fl.Name), strings.TrimPrefix(cmd.name(), "diag "))
		}
	})
	return err
}

// usage prints a list command's help: its synopsis and its flags by group,
// generated from the flag set itself (docs/cli.md holds the same text).
func usage(w io.Writer, f *flag.FlagSet, cmd command) {
	switch {
	case cmd.diag:
		fmt.Fprintf(w, "usage: swath %s SOURCE [flags]\n", cmd.name())
		fmt.Fprintf(w, "  every swath %s flag (swath %s --help), and the diagnostic flags below:\n", strings.TrimPrefix(cmd.name(), "diag "), strings.TrimPrefix(cmd.name(), "diag "))
		fmt.Fprintln(w, "  they exist to run an experiment or a comparison, not to operate the tool")
	case cmd.count:
		fmt.Fprintln(w, "usage: swath count SOURCE [flags]")
		fmt.Fprintln(w, "  lists SOURCE and counts its rows and object bytes, writing nothing: rows are counted as they")
		fmt.Fprintln(w, "  arrive, never ordered, buffered or spilled; prints one line, or the run report")
	default:
		fmt.Fprintln(w, "usage: swath list SOURCE [-o DIR|FILE|-] [flags]")
		fmt.Fprintln(w, "  -o DIR writes a sorted Parquet dataset; -o NAME.tsv or NAME.jsonl (optionally .gz or .zst) one text")
		fmt.Fprintln(w, "  file; no -o, or -o -, text rows on stdout")
	}
	fmt.Fprintln(w, "  SOURCE is a local directory, s3://bucket[/prefix] or gs://bucket[/prefix]")
	for _, g := range helpGroups {
		if cmd.diag != (g.title == diagnosticGroup) {
			continue
		}
		var lines []string
		for _, h := range g.flags {
			fl := f.Lookup(h.name)
			if fl == nil || cmd.count && slices.Contains(notCounted, h.name) {
				continue
			}
			lines = append(lines, flagHelp(fl, h))
		}
		if len(lines) > 0 {
			fmt.Fprintf(w, "\n%s:\n%s", g.title, strings.Join(lines, ""))
		}
	}
	if cmd.diag {
		return
	}
	fmt.Fprintln(w, "\nswath resume DIR continues an interrupted run with its recorded command; only")
	fmt.Fprintln(w, strings.Join(runtimeFlags, ", ")+" (runtime) and")
	fmt.Fprintln(w, strings.Join(operationalFlags, ", ")+" (operational) may change.")
	fmt.Fprintln(w, "Profiling: SWATH_CPUPROFILE=FILE and SWATH_MEMPROFILE=FILE.")
}

// flagHelp is one flag's entry: its names and value, then its usage and
// default, then when to change it, indented under it.
func flagHelp(fl *flag.Flag, h helpFlag) string {
	var b strings.Builder
	name, use := flag.UnquoteUsage(fl)
	b.WriteString("  " + dashed(fl.Name))
	for _, a := range h.stacked {
		b.WriteString(", " + dashed(a))
	}
	if name != "" {
		b.WriteString(" " + name)
	}
	b.WriteString("\n        " + use)
	if d := fl.DefValue; d != "" && d != "0" && d != "false" && d != "0s" {
		fmt.Fprintf(&b, " (default %s)", d)
	}
	b.WriteString("\n")
	if h.when != "" {
		b.WriteString("        change it: " + h.when + "\n")
	}
	return b.String()
}

// dashed is a flag name as typed: one dash for a one-letter name.
func dashed(name string) string {
	if len(name) == 1 || name == "vv" || name == "vvv" || name == "qq" {
		return "-" + name
	}
	return "--" + name
}

// errHelpGroups is TestHelpGroupsCoverFlags's refusal.
var errHelpGroups = errors.New("help groups")

// checkHelpGroups reports a flag of f in no help group (nor a stacked
// form), in two, or a group flag f lacks; and that the diagnostic group is
// exactly the diagnostic flags.
func checkHelpGroups(f *flag.FlagSet) error {
	seen := map[string]string{}
	var problems []string
	for _, g := range helpGroups {
		for _, h := range g.flags {
			for _, n := range append([]string{h.name}, h.stacked...) {
				if prev, ok := seen[n]; ok {
					problems = append(problems, fmt.Sprintf("--%s in %s and %s", n, prev, g.title))
				}
				seen[n] = g.title
				if f.Lookup(n) == nil {
					problems = append(problems, fmt.Sprintf("--%s (%s) is not a flag", n, g.title))
				}
			}
			if diagOnly()[h.name] != (g.title == diagnosticGroup) {
				problems = append(problems, fmt.Sprintf("--%s: diagnostic %v, in %s", h.name, diagOnly()[h.name], g.title))
			}
		}
	}
	f.VisitAll(func(fl *flag.Flag) {
		if _, ok := seen[fl.Name]; !ok {
			problems = append(problems, fmt.Sprintf("--%s is in no help group", fl.Name))
		}
	})
	if len(problems) > 0 {
		return fmt.Errorf("%w: %s", errHelpGroups, strings.Join(problems, "; "))
	}
	return nil
}
