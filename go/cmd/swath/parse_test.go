// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/spf13/pflag"

	"github.com/varveio/swath/go/internal/listing"
)

// TestGNUParsing: short flags bundle and take attached values, long flags
// take --name value and --name=value, flags and SOURCE interleave, a long
// name with one dash is refused naming the right spelling, and -- ends the
// flags for good.
func TestGNUParsing(t *testing.T) {
	for _, c := range []struct {
		args   []string
		check  func(o options) bool
		reason string
	}{
		{[]string{"s3://b", "-d1"}, func(o options) bool { return o.depth == 1 }, "-d1"},
		{[]string{"-d", "2", "s3://b"}, func(o options) bool { return o.depth == 2 }, "-d 2"},
		{[]string{"s3://b", "--depth=3"}, func(o options) bool { return o.depth == 3 }, "--depth=3"},
		{[]string{"--depth", "4", "s3://b"}, func(o options) bool { return o.depth == 4 }, "--depth 4"},
		{[]string{"-vq", "s3://b"}, func(o options) bool { return o.term.verbose == 1 && o.term.quiet == 1 }, "-vq"},
		{[]string{"s3://b", "-vvv", "-qq"}, func(o options) bool { return o.term.verbose == 3 && o.term.quiet == 2 }, "-vvv -qq"},
		{[]string{"-j8", "s3://b", "--no-stats"}, func(o options) bool { return o.workers == 8 && o.term.stats.set && !o.term.stats.on }, "-j8 after"},
	} {
		o, root, err := parseList(c.args, io.Discard, command{count: true, du: true})
		if err != nil || root != "s3://b" || !c.check(o) {
			t.Errorf("%s: %v, root %q", c.reason, err, root)
		}
	}
	_, _, err := parseList([]string{"s3://b", "-depth", "1"}, io.Discard, command{count: true, du: true})
	if exitOf(err) != exitUsage || !strings.Contains(err.Error(), "unknown flag -depth (did you mean --depth?)") {
		t.Errorf("-depth: %v", err)
	}
	var out bytes.Buffer
	err = run(context.Background(), []string{"--", ".", "--help"}, &out, false)
	if exitOf(err) != exitUsage || !strings.Contains(err.Error(), `unexpected argument "--help"`) || out.Len() != 0 {
		t.Errorf("list -- . --help: %v, stdout %q", err, out.String())
	}
	// SOURCE after -- may look like a flag.
	o, root, err := parseList([]string{"--workers", "2", "--", "-dir"}, io.Discard, command{diag: true, count: true})
	if err != nil || root != "-dir" || o.workers != 2 {
		t.Errorf("-- -dir: %v %q", err, root)
	}
	// A flag's value, and anything after --, is not read as a flag
	// spelled with one dash.
	for _, args := range [][]string{
		{"s3://b", "--include", "-verbose"},
		{"s3://b", "-v", "--include", "-depth"},
		{"--include=-verbose", "s3://b"},
		{"--include", "x", "--", "-verbose"},
	} {
		o, _, err := parseList(args, io.Discard, command{})
		if err != nil || len(o.filter.Include) != 1 {
			t.Errorf("%q: %v, includes %q", args, err, o.filter.Include)
		}
	}
	if err := dispatch(context.Background(), []string{"list", "--include", "-verbose", "--help"}, io.Discard); !errors.Is(err, pflag.ErrHelp) {
		t.Errorf("the root read list's value as its flag: %v", err)
	}
}

// TestUsageErrors: an unknown flag names the nearest one (or the flag
// another tool's spelling stands for) and the help to read; a value error
// names the flag and the accepted form; a non-positive count is refused.
func TestUsageErrors(t *testing.T) {
	for _, c := range []struct {
		args []string
		want string
	}{
		{[]string{"s3://b", "--workres", "2"}, "unknown flag --workres (did you mean --workers?)"},
		{[]string{"s3://b", "--region", "x"}, "unknown flag --region (did you mean --s3-region?)"},
		{[]string{"s3://b", "--profile", "x"}, "unknown flag --profile (did you mean --s3-profile?)"},
		{[]string{"s3://b", "--request-payer"}, "unknown flag --request-payer (did you mean --s3-requester-pays?)"},
		{[]string{"s3://b", "--requester-pays"}, "unknown flag --requester-pays (did you mean --s3-requester-pays?)"},
		{[]string{"s3://b", "--endpoint", "http://x"}, "unknown flag --endpoint (did you mean --endpoint-url?)"},
		{[]string{"s3://b", "--bogus"}, "unknown flag --bogus"},
		{[]string{"s3://b", "--max-duration", "2x"}, `--max-duration "2x": want a duration such as 90s, 1h30m`},
		{[]string{"s3://b", "--max-duration=-1s"}, `--max-duration "-1s": negative`},
		{[]string{"s3://b", "--workers", "0"}, `--workers "0": want a whole number of at least 1`},
		{[]string{"s3://b", "-j0"}, `--workers "0": want a whole number of at least 1`},
		{[]string{"s3://b", "--encoders", "0"}, `--encoders "0": want a whole number of at least 1`},
		{[]string{"s3://b", "--depth"}, "--depth needs a value (N)"},
		{[]string{"s3://b", "--max-prefixes", "0"}, `--max-prefixes "0": want a whole number of at least 1`},
		{[]string{"s3://b", "--max-keys", "-1"}, "--max-keys"},
	} {
		_, _, err := parseList(c.args, io.Discard, command{count: true, du: true})
		var sh seeHelp
		if exitOf(err) != exitUsage || !strings.Contains(err.Error(), c.want) || !errors.As(err, &sh) || sh.see() != "See 'swath du --help'." {
			t.Errorf("%q: %v, want %q and the help to read", c.args, err, c.want)
		}
	}
}

// TestDispatch: ls is list, swath help COMMAND is COMMAND --help, the
// global flags come before the command word too, and an unknown command
// gets the nearest one, or swath list for a source given without one.
func TestDispatch(t *testing.T) {
	ctx := context.Background()
	var a, b bytes.Buffer
	if err := dispatch(ctx, []string{"help", "list"}, &a); !errors.Is(err, pflag.ErrHelp) {
		t.Fatal(err)
	}
	if err := dispatch(ctx, []string{"ls", "--help"}, &b); !errors.Is(err, pflag.ErrHelp) {
		t.Fatal(err)
	}
	if a.String() != b.String() || !strings.Contains(a.String(), "Usage: swath list SOURCE") {
		t.Fatalf("swath help list and swath ls --help differ:\n%s\n%s", a.String(), b.String())
	}
	src := localSource(t)
	// --color reaches the command (and its check) from before the word.
	if err := dispatch(ctx, []string{"--color", "sometimes", "du", src}, io.Discard); exitOf(err) != exitUsage || !strings.Contains(err.Error(), "--color") {
		t.Errorf("--color before du: %v", err)
	}
	if err := dispatch(ctx, []string{"-q", "--color=never", "du", src, "--no-stats"}, io.Discard); err != nil {
		t.Errorf("-q before du: %v", err)
	}
	if err := dispatch(ctx, []string{"--workers", "2", "du", src}, io.Discard); exitOf(err) != exitUsage || !strings.Contains(err.Error(), "unknown flag --workers") {
		t.Errorf("--workers before du: %v", err)
	}
	for word, want := range map[string]string{
		"lst":    `unknown command "lst" (did you mean "list"?)`,
		"s3://b": `unknown command "s3://b" (did you mean "swath list s3://b"?)`,
		src:      "(did you mean " + strconv.Quote("swath list "+src) + "?)", // quoted: a Windows path's backslashes are escaped
		"zzzzz":  `unknown command "zzzzz"`,
	} {
		err := dispatch(ctx, []string{word}, io.Discard)
		var sh seeHelp
		if exitOf(err) != exitUsage || !strings.Contains(err.Error(), want) || !errors.As(err, &sh) || sh.see() != "See 'swath --help'." {
			t.Errorf("%s: %v", word, err)
		}
	}
	var v bytes.Buffer
	if err := dispatch(ctx, []string{"-V"}, &v); err != nil || !strings.HasPrefix(v.String(), "swath ") {
		t.Errorf("-V: %v %q", err, v.String())
	}
}

// TestShowFlagsAfterDir: swath show takes its flag after DIR too.
func TestShowFlagsAfterDir(t *testing.T) {
	for _, args := range [][]string{{"--help"}, {"x", "--help"}, {"x", "-h"}} {
		var out bytes.Buffer
		if err := show(args, &out); !errors.Is(err, pflag.ErrHelp) || !strings.Contains(out.String(), "--journal") {
			t.Errorf("show %q: %v", args, err)
		}
	}
	if err := show([]string{t.TempDir(), "--journal"}, io.Discard); exitOf(err) == exitUsage {
		t.Errorf("show DIR --journal: a usage error: %v", err)
	}
}

// TestHelpForms: -h is one line per flag, --help adds each flag's further
// text; neither shows =false nor names Java.
func TestHelpForms(t *testing.T) {
	var short, long bytes.Buffer
	if err := dispatch(context.Background(), []string{"list", "-h"}, &short); !errors.Is(err, pflag.ErrHelp) {
		t.Fatal(err)
	}
	if err := dispatch(context.Background(), []string{"list", "--help"}, &long); !errors.Is(err, pflag.ErrHelp) {
		t.Fatal(err)
	}
	const advice = "Raise it (128, 256)"
	if strings.Contains(short.String(), advice) || !strings.Contains(long.String(), advice) {
		t.Fatalf("-h shows the long text, or --help lacks it")
	}
	for _, s := range []string{short.String(), long.String()} {
		for _, bad := range []string{"=false", "Java", "colour"} {
			if strings.Contains(s, bad) {
				t.Errorf("help shows %q", bad)
			}
		}
		for _, l := range strings.Split(s, "\n") {
			if len(l) > plainHelpWidth {
				t.Errorf("help line over %d columns: %q", plainHelpWidth, l)
			}
		}
	}
}

// TestSourceForms: a URL scheme is matched in any case, and an Azure Blob
// service URL is the az:// source it names, its SAS redacted where it is
// recorded or shown.
func TestSourceForms(t *testing.T) {
	for in, want := range map[string]string{
		"S3://bkt/p": "s3://bkt/p",
		"Gs://bkt":   "gs://bkt",
		"https://acct.blob.core.windows.net/cont/logs/": "az://acct/cont/logs/",
		"https://ACCT.blob.core.windows.net/cont":       "az://acct/cont",
	} {
		o, root, err := parseList([]string{in, "-o", filepath.Join(t.TempDir(), "ds")}, io.Discard, command{})
		if err != nil || root != want || o.argv[len(o.argv)-1] != want {
			t.Errorf("%s: %q, %v", in, root, err)
		}
	}
	const sas = "sv=2024-01-01&sig=SECRET"
	o, root, err := parseList([]string{"https://acct.blob.core.windows.net/cont?" + sas, "-o", filepath.Join(t.TempDir(), "ds")}, io.Discard, command{})
	if err != nil || root != "az://acct/cont?"+sas {
		t.Fatalf("SAS URL: %q, %v", root, err)
	}
	if got := strings.Join(redactArgv(o.argv), " "); strings.Contains(got, "SECRET") {
		t.Errorf("recorded %q", got)
	}
	err = dispatch(context.Background(), []string{"https://acct.blob.core.windows.net/cont?" + sas}, io.Discard)
	if err == nil || strings.Contains(err.Error(), "SECRET") {
		t.Errorf("unknown command shows the SAS: %v", err)
	}
	if _, _, err := parseList([]string{"https://example.com/x", "-o", filepath.Join(t.TempDir(), "ds")}, io.Discard, command{}); err != nil {
		t.Logf("another https URL: %v", err) // left to the lookup's refusal when the run starts
	}
}

// TestOutputDestination: -o names a dataset directory, a text file, or
// stdout; a name ending in a data-file extension swath does not write, and
// an object-store URL, are refused; a trailing slash is a directory; a
// --format that agrees with the file's name is accepted.
func TestOutputDestination(t *testing.T) {
	dir := t.TempDir()
	for _, c := range []struct {
		args   []string
		text   string // the text format, "" for a dataset
		refuse string
	}{
		{[]string{"-o", "out.parquet"}, "", ""},
		{[]string{"-o", "out.tsv/"}, "", ""},
		{[]string{"-o", "rows.tsv"}, "tsv", ""},
		{[]string{"-o", "rows.jsonl.zst"}, "jsonl", ""},
		{[]string{"-o", "rows.tsv", "--format", "tsv"}, "tsv", ""},
		{[]string{"-o", "rows.tsv", "--format", "jsonl"}, "", "names tsv"},
		{[]string{"-o", "ds", "--format", "tsv"}, "", "is a dataset directory"},
		{[]string{"-o", "out.csv"}, "", "does not write .csv files"},
		{[]string{"-o", "out.json"}, "", "does not write .json"},
		{[]string{"-o", "out.ndjson"}, "", "does not write"},
		{[]string{"-o", "out.txt"}, "", "does not write"},
		{[]string{"-o", "x.tsv.bz2"}, "", "does not write .bz2"},
		{[]string{"-o", "x.csv.gz"}, "", "does not write .gz"},
		{[]string{"-o", "x.gz"}, "", "does not write .gz"},
		{[]string{"-o", "x.zst"}, "", "does not write .zst"},
		{[]string{"-o", "x.xz"}, "", "does not write .xz"},
		{[]string{"-o", "s3://bkt/out"}, "", "object-store destination"},
		{[]string{"-o", "gs://bkt/out"}, "", "object-store destination"},
		{[]string{"-o", "https://acct.blob.core.windows.net/c"}, "", "object-store destination"},
	} {
		args := append([]string{"s3://bkt"}, c.args...)
		for i, a := range args {
			if i > 0 && args[i-1] == "-o" && !strings.Contains(a, "://") {
				args[i] = filepath.Join(dir, a) + map[bool]string{true: "/"}[strings.HasSuffix(a, "/")]
			}
		}
		o, _, err := parseList(args, io.Discard, command{})
		switch {
		case c.refuse != "":
			if exitOf(err) != exitUsage || !strings.Contains(err.Error(), c.refuse) {
				t.Errorf("%q: %v, want a refusal %q", c.args, err, c.refuse)
			}
		case err != nil:
			t.Errorf("%q: %v", c.args, err)
		case c.text == "" && o.text != nil, c.text != "" && (o.text == nil || o.text.Format != c.text):
			t.Errorf("%q: text %+v, want %q", c.args, o.text, c.text)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "s3:")); err == nil {
		t.Error("-o s3://... made a local directory")
	}
}

// TestRecordedCommandCanonical: the recorded command is every flag in its
// long --name=value form (--name for a switch), in the order given, then
// SOURCE (after -- when it looks like a flag): what swath resume re-parses.
func TestRecordedCommandCanonical(t *testing.T) {
	o, _, err := parseList([]string{"-j4", "s3://b", "-vv", "--s3-region", "x", "-o", "ds", "--no-stats", "--anonymous"}, io.Discard, command{})
	want := []string{"list", "--workers=4", "--verbose", "--verbose", "--s3-region=x", "--output=ds", "--no-stats", "--anonymous", "s3://b"}
	if err != nil || !slices.Equal(o.argv, want) {
		t.Fatalf("%v: %q, want %q", err, o.argv, want)
	}
	o, _, err = parseList([]string{"-o", "ds", "--", "-dir"}, io.Discard, command{diag: true})
	if want := []string{"diag", "list", "--output=ds", "--", "-dir"}; err != nil || !slices.Equal(o.argv, want) {
		t.Fatalf("%v: %q, want %q", err, o.argv, want)
	}
}

// TestRecordedCommandRoundTrip: every value-taking flag's recorded form
// parses back to the same value and the same SOURCE, for an empty value
// (--name=, not --name, which would take the next argument), one that
// starts with a dash and one that holds '='.
func TestRecordedCommandRoundTrip(t *testing.T) {
	accepted := 0
	newFlags(&options{}, true).VisitAll(func(fl *pflag.Flag) {
		if fl.NoOptDefVal != "" {
			return
		}
		for _, v := range []string{"", "-x", "a=b"} {
			f1 := newFlags(&options{}, true)
			p1, err := parseFlags(f1, cmdDiagList, []string{"--" + fl.Name, v, "s3://b"}, nil)
			if err != nil {
				continue // a value this flag refuses
			}
			accepted++
			f2 := newFlags(&options{}, true)
			p2, err := parseFlags(f2, cmdDiagList, append(slices.Clone(p1.given), "s3://b"), nil)
			if err != nil || !slices.Equal(p2.given, p1.given) || !slices.Equal(p2.operands, []string{"s3://b"}) ||
				f2.Lookup(fl.Name).Value.String() != f1.Lookup(fl.Name).Value.String() {
				t.Errorf("--%s %q: recorded %q, parsed back %q %q (%v)", fl.Name, v, p1.given, p2.given, p2.operands, err)
			}
		}
	})
	if accepted == 0 {
		t.Fatal("no flag took any of the values")
	}
}

// TestNegation: a default-on switch is turned off by --no-NAME; a
// resumable switch has its other spelling for a resume to undo it, outside
// the list help; a provider's switch is refused on another provider's
// source in either spelling.
func TestNegation(t *testing.T) {
	if _, _, err := parseList([]string{"s3://b", "-o", "ds", "--no-adaptive-limit", "--adaptive-limit", "--anonymous", "--no-anonymous"}, io.Discard, command{}); err != nil {
		t.Fatal(err)
	}
	f := newFlags(&options{}, true)
	if err := f.Parse([]string{"--anonymous", "--no-anonymous", "--no-adaptive-limit", "--adaptive-limit"}); err != nil {
		t.Fatal(err)
	}
	for n, want := range map[string]string{"anonymous": "false", "no-adaptive-limit": "false"} {
		if got := f.Lookup(n).Value.String(); got != want {
			t.Errorf("--%s then its undo: %s", n, got)
		}
	}
	for _, n := range []string{"no-anonymous", "adaptive-limit", "spill-compress", "no-spill-plain", "no-adaptive-limit-slow-start"} {
		if fl := f.Lookup(n); fl == nil || !fl.Hidden {
			t.Errorf("--%s: no hidden undo spelling", n)
		}
	}
	if f.Lookup("no-seed") == nil || f.Lookup("seed") != nil {
		t.Error("--no-seed, a frozen switch, has an undo spelling")
	}
	for _, args := range [][]string{{"gs://b", "--no-s3-path-style"}, {"gs://b", "--s3-path-style"}, {"az://a/c", "--no-anonymous"}} {
		if _, _, err := parseList(append(args, "-o", "ds"), io.Discard, command{}); args[0] == "az://a/c" && err != nil {
			t.Errorf("%q: %v", args, err)
		} else if args[0] != "az://a/c" && (exitOf(err) != exitUsage || !strings.Contains(err.Error(), "applies to S3 sources, not to GCS sources")) {
			t.Errorf("%q: %v", args, err)
		}
	}
}

// TestResumeFrozenFlag: a resume naming a flag fixed for the run is
// refused naming it and the help to read.
func TestResumeFrozenFlag(t *testing.T) {
	err := resume(context.Background(), []string{t.TempDir(), "--part-size", "1MiB"}, io.Discard)
	if exitOf(err) != exitUsage || !strings.Contains(err.Error(), "--part-size is fixed for the run and cannot change on resume (see 'swath resume --help')") {
		t.Fatalf("%v", err)
	}
}

// TestReportStdout: --report - prints the run report on stdout, and
// nothing else goes there (a dataset run prints no summary line); it is
// refused where stdout carries rows.
func TestReportStdout(t *testing.T) {
	src := localSource(t)
	var out bytes.Buffer
	if err := run(context.Background(), []string{src, "-o", filepath.Join(t.TempDir(), "ds"), "--no-stats", "--report", "-"}, &out, false); err != nil {
		t.Fatal(err)
	}
	var r listing.Report
	if err := json.Unmarshal(out.Bytes(), &r); err != nil || r.Output.RowsEmitted != 2 {
		t.Fatalf("stdout %q: %v", out.String(), err)
	}
	out.Reset()
	if err := run(context.Background(), []string{src, "-o", filepath.Join(t.TempDir(), "ds"), "--no-stats"}, &out, false); err != nil || out.Len() != 0 {
		t.Fatalf("swath list -o wrote %q on stdout (%v)", out.String(), err)
	}
	if err := runDu(context.Background(), []string{src, "--report", "-"}, &out); exitOf(err) != exitUsage || !strings.Contains(err.Error(), "carries the rows") {
		t.Fatalf("du --report -: %v", err)
	}
	// A refusal before the run (an unknown scheme) prints its report too.
	out.Reset()
	err := run(context.Background(), []string{"bad://b", "-o", filepath.Join(t.TempDir(), "ds"), "--report", "-"}, &out, false)
	r = listing.Report{}
	if exitOf(err) != exitUsage || json.Unmarshal(out.Bytes(), &r) != nil || r.Outcome.ExitCode != exitUsage {
		t.Fatalf("unknown scheme, --report -: %v, stdout %q", err, out.String())
	}
}
