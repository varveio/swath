// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/spf13/pflag"

	"github.com/varveio/swath/go/internal/listing"
	"github.com/varveio/swath/go/internal/registry"
	"github.com/varveio/swath/go/internal/source"
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
		{[]string{"s3://b", "--path-style"}, "unknown flag --path-style (did you mean --s3-path-style?)"},
		{[]string{"s3://b", "--fips"}, "unknown flag --fips (did you mean --s3-fips?)"},
		{[]string{"s3://b", "--dualstack"}, "unknown flag --dualstack (did you mean --s3-dualstack?)"},
		{[]string{"s3://b", "--fetch-owner"}, "unknown flag --fetch-owner (did you mean --s3-fetch-owner?)"},
		{[]string{"s3://b", "--bearer-token-command", "x"}, "unknown flag --bearer-token-command (did you mean --s3-bearer-token-command?)"},
		{[]string{"s3://b", "--gcs-credentials", "x"}, "unknown flag --gcs-credentials (did you mean --gcs-credentials-file?)"},
		{[]string{"s3://b", "--gcs-user-project", "x"}, "unknown flag --gcs-user-project (did you mean --gcs-billing-project?)"},
		{[]string{"s3://b", "--azblob-sas", "x"}, "unknown flag --azblob-sas (did you mean --azure-sas-token?)"},
		{[]string{"s3://b", "--no-sign-request"}, "unknown flag --no-sign-request (did you mean --anonymous?)"},
		{[]string{"s3://b", "--one-filesystem"}, "unknown flag --one-filesystem (did you mean --one-file-system?)"},
		{[]string{"s3://b", "--limiter=false"}, "unknown flag --limiter (did you mean --no-adaptive-limit?)"},
		{[]string{"s3://b", "--concurrency", "8"}, "unknown flag --concurrency (did you mean --workers?)"},
		{[]string{"s3://b", "--max-duration", "2x"}, `--max-duration "2x": want a duration such as 90s, 1h30m`},
		{[]string{"s3://b", "--max-duration=-1s"}, `--max-duration "-1s": negative`},
		{[]string{"s3://b", "--workers", "0"}, `--workers "0": want a whole number of at least 1`},
		{[]string{"s3://b", "-j0"}, `--workers "0": want a whole number of at least 1`},
		{[]string{"s3://b", "--encoders", "0"}, `--encoders "0": want a whole number of at least 1`},
		{[]string{"s3://b", "--depth"}, "--depth needs a value (N)"},
		{[]string{"s3://b", "--max-prefixes", "0"}, `--max-prefixes "0": want a whole number of at least 1`},
		{[]string{"s3://b", "--max-keys", "-1"}, "--max-keys"},
		{[]string{"s3://b", "--columns", ""}, `--columns "": an empty name`},
		{[]string{"s3://b", "--columns", ","}, `--columns ",": an empty name`},
		{[]string{"s3://b", "--storage-class", ","}, `--storage-class ",": an empty name`},
		{[]string{"s3://b", "--storage-class=STANDARD,"}, `--storage-class "STANDARD,": an empty name`},
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
// starts with a dash, one that holds '=', and one that looks like a URL
// with credentials (s3://user@host/p?sig=x#f): a flag that does not name a
// place or a secret (source.Flag.Shown) holds data, a filter, recorded
// as given, through the registry's second pass too, so a resume's filters
// equal the run's.
func TestRecordedCommandRoundTrip(t *testing.T) {
	accepted := 0
	newFlags(&options{}, true).VisitAll(func(fl *pflag.Flag) {
		if fl.NoOptDefVal != "" || source.Described(fl).Shown != "" {
			return
		}
		for _, v := range []string{"", "-x", "a=b", "s3://user@host/p?sig=x#f", "a@b", "?x", "#x", "sig=x"} {
			f1 := newFlags(&options{}, true)
			p1, err := parseFlags(f1, cmdDiagList, []string{"--" + fl.Name, v, "s3://b"}, nil)
			if err != nil {
				continue // a value this flag refuses
			}
			accepted++
			if !slices.Equal(p1.recorded, p1.given) || !slices.Equal(registry.RedactArgv(p1.recorded), p1.recorded) {
				t.Errorf("--%s %q: recorded %q, through the registry %q", fl.Name, v, p1.recorded, registry.RedactArgv(p1.recorded))
			}
			f2 := newFlags(&options{}, true)
			p2, err := parseFlags(f2, cmdDiagList, append(slices.Clone(p1.recorded), "s3://b"), nil)
			if err != nil || !slices.Equal(p2.recorded, p1.recorded) || !slices.Equal(p2.operands, []string{"s3://b"}) ||
				f2.Lookup(fl.Name).Value.String() != f1.Lookup(fl.Name).Value.String() {
				t.Errorf("--%s %q: recorded %q, parsed back %q %q (%v)", fl.Name, v, p1.recorded, p2.recorded, p2.operands, err)
			}
		}
	})
	if accepted == 0 {
		t.Fatal("no flag took any of the values")
	}
	o, _, err := parseList([]string{"s3://b", "--include-regex", "s3://user@host/path", "-o", "ds"}, io.Discard, command{})
	if err != nil || !slices.Contains(registry.RedactArgv(o.argv), "--include-regex=s3://user@host/path") {
		t.Errorf("a filter that looks like a URL: %q (%v)", registry.RedactArgv(o.argv), err)
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
	if err := runDu(context.Background(), []string{src, "--report", "-"}, &out); exitOf(err) != exitUsage || !strings.Contains(err.Error(), "carries the rows") || out.Len() != 0 {
		t.Fatalf("du --report -: %v, stdout %q", err, out.String())
	}
	if err := run(context.Background(), []string{src, "--format", "tsv", "--report", "-"}, &out, false); exitOf(err) != exitUsage || out.Len() != 0 {
		t.Fatalf("list to stdout --report -: %v, stdout %q", err, out.String())
	}
	// A refusal before the run (an unknown scheme) prints its report too.
	out.Reset()
	err := run(context.Background(), []string{"bad://b", "-o", filepath.Join(t.TempDir(), "ds"), "--report", "-"}, &out, false)
	r = listing.Report{}
	if exitOf(err) != exitUsage || json.Unmarshal(out.Bytes(), &r) != nil || r.Outcome.ExitCode != exitUsage {
		t.Fatalf("unknown scheme, --report -: %v, stdout %q", err, out.String())
	}
}

// TestResumeDecidesByDisk: swath resume reads what DIR holds, never its
// name: a dataset in a directory named rows.tsv (-o rows.tsv/) is found,
// with or without the slash; a text file's name that is no directory is a
// text run, which keeps no journal.
func TestResumeDecidesByDisk(t *testing.T) {
	captureStderr(t)
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "rows.tsv")
	if err := run(ctx, []string{localSource(t), "-o", dir + string(filepath.Separator), "--no-stats"}, io.Discard, false); err != nil {
		t.Fatal(err)
	}
	for _, d := range []string{dir, dir + string(filepath.Separator)} {
		var out bytes.Buffer
		if err := resume(ctx, []string{d}, &out); err != nil || !strings.Contains(out.String(), "already complete") {
			t.Errorf("resume %s: %v, stdout %q", d, err, out.String())
		}
	}
	if err := resume(ctx, []string{filepath.Join(t.TempDir(), "x.tsv")}, io.Discard); err == nil || !strings.Contains(err.Error(), errText.Error()) {
		t.Errorf("resume of a text file's name: %v", err)
	}
}

// TestGlobalFlagsEverywhere: --log-format, --color, -v and -q are taken
// after the command word by every command, and a command line refused
// before --log-format is read still reports in the format it asks for.
func TestGlobalFlagsEverywhere(t *testing.T) {
	src := localSource(t)
	for _, args := range [][]string{
		{"list", src, "--bogus", "--log-format", "json"},
		{"list", src, "--bogus", "--log-format=json"},
		{"show", filepath.Join(t.TempDir(), "nosuchdir"), "--log-format", "json"},
		{"completion", "nosuchshell", "-q", "--log-format", "json"},
		{"resume", filepath.Join(t.TempDir(), "nosuchdir"), "--bogus", "--log-format", "json"},
		{"lst", "--log-format", "json"},
	} {
		stderr := captureStderr(t)
		t.Cleanup(func() { logs.Store(nil) })
		code := conclude(dispatch(context.Background(), args, io.Discard))
		var ev map[string]any
		if code == exitCompleted || json.Unmarshal(bytes.TrimSpace(stderr.Bytes()), &ev) != nil || ev["event"] != "outcome" {
			t.Errorf("%q: exit %d, stderr %q", args, code, stderr.String())
		}
	}
	// The format is read as the parser reads the flags: a value is a
	// value, -- ends them, the last one wins; every early refusal (a
	// one-dash name, an unknown flag) is reported in it.
	for _, c := range []struct {
		args []string
		json bool
	}{
		{[]string{"--log-format", "text", "list", src, "--include", "--log-format", "json", "--bogus"}, false},
		{[]string{"list", src, "-verbose", "--log-format", "json"}, true},
		{[]string{"list", src, "--include=--log-format", "json", "--bogus"}, false},
		{[]string{"list", src, "--bogus", "--", "--log-format", "json"}, false},
		{[]string{"list", src, "--log-format", "json", "--log-format", "text", "--bogus"}, false},
		{[]string{"list", src, "--log-format", "text", "--log-format=json", "--bogus"}, true},
	} {
		stderr := captureStderr(t)
		t.Cleanup(func() { logs.Store(nil) })
		code := conclude(dispatch(context.Background(), c.args, io.Discard))
		if isJSON := json.Valid(bytes.TrimSpace(stderr.Bytes())); code != exitUsage || isJSON != c.json {
			t.Errorf("%q: exit %d, json %v, stderr %q", c.args, code, isJSON, stderr.String())
		}
	}
	if err := dispatch(context.Background(), []string{"completion", "bash", "--color", "never", "-v"}, io.Discard); err != nil {
		t.Errorf("completion with global flags: %v", err)
	}
	if err := dispatch(context.Background(), []string{"show", t.TempDir(), "--color", "sometimes"}, io.Discard); exitOf(err) != exitUsage || !strings.Contains(err.Error(), "--color") {
		t.Errorf("show --color sometimes: %v", err)
	}
}

// TestShortHelp: -h is the usage, the one-line description, the examples
// and the groups a first run needs (shortGroups), each flag's description
// on one line at 100 columns, then the line naming --help for the rest;
// list -h stays under 60 lines.
func TestShortHelp(t *testing.T) {
	for _, cmd := range []command{{}, {count: true, du: true}, {diag: true, count: true}} {
		var o options
		f := newFlags(&o, cmd.diag)
		var b bytes.Buffer
		usage(&b, f, cmd, helpStyle{width: maxHelpWidth})
		out := b.String()
		lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
		if !cmd.count && len(lines) >= 60 {
			t.Errorf("%s -h: %d lines", cmd.name(), len(lines))
		}
		if !strings.HasSuffix(out, fmt.Sprintf(shortTail, cmd.name())) || strings.Contains(out, source.GroupPerformance+":") {
			t.Errorf("%s -h:\n%s", cmd.name(), out)
		}
		f.VisitAll(func(fl *pflag.Flag) {
			if shown(fl, cmd) && slices.Contains(shortGroups, groupOf(fl)) && !strings.Contains(out, source.Described(fl).Usage) {
				t.Errorf("%s -h: --%s's description wraps or is missing", cmd.name(), fl.Name)
			}
		})
		for _, l := range lines {
			if len(l) > maxHelpWidth {
				t.Errorf("%s -h: line over %d columns: %q", cmd.name(), maxHelpWidth, l)
			}
		}
	}
}

// TestRefusalsSayWhy: a text -o in a directory that does not exist says
// so before listing; show --journal on a committed dataset says a dataset
// keeps no journal.
func TestRefusalsSayWhy(t *testing.T) {
	captureStderr(t)
	src := localSource(t)
	missing := filepath.Join(t.TempDir(), "missing")
	err := run(context.Background(), []string{src, "-o", filepath.Join(missing, "rows.tsv")}, io.Discard, false)
	if exitOf(err) != exitUsage || !strings.Contains(err.Error(), "its directory "+missing+" does not exist") {
		t.Errorf("-o in a missing directory: %v", err)
	}
	ds := filepath.Join(t.TempDir(), "ds")
	if err := run(context.Background(), []string{src, "-o", ds, "--no-stats"}, io.Discard, false); err != nil {
		t.Fatal(err)
	}
	if err := show([]string{ds, "--journal"}, io.Discard); err == nil || !strings.Contains(err.Error(), "is a committed dataset, which keeps no journal") {
		t.Errorf("show --journal on a dataset: %v", err)
	}
}
