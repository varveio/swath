// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/spf13/pflag"

	"github.com/varveio/swath/docs"
	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/filter"
	"github.com/varveio/swath/internal/journal"
	"github.com/varveio/swath/internal/listing"
	"github.com/varveio/swath/internal/source"
)

func TestRun(t *testing.T) {
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "a"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "ds")
	var buf bytes.Buffer
	// root after flags and before them both work
	if err := run(context.Background(), []string{"-o", out, src, "--part-size", "4MiB"}, &buf, false); err != nil {
		t.Fatal(err)
	}
	if buf.Len() != 0 {
		t.Fatalf("a dataset run wrote %q on stdout", buf.String())
	}
	if !dataset.Committed(out) {
		t.Fatal("not committed")
	}
	err := run(context.Background(), []string{src, "-o", out}, &buf, false)
	if err == nil || !strings.Contains(err.Error(), "--overwrite") {
		t.Fatalf("non-empty output: %v", err)
	}
	// The report goes out on failure too, with the outcome.
	buf.Reset()
	if err := run(context.Background(), []string{src, "-o", out, "--report", "-"}, &buf, false); err == nil ||
		!strings.Contains(buf.String(), `"status": "failed"`) || !strings.Contains(buf.String(), "not empty") {
		t.Fatalf("failure report: %v\n%s", err, buf.String())
	}
	if err := run(context.Background(), []string{src, "-o", out, "--overwrite"}, &buf, false); err != nil {
		t.Fatal(err)
	}
	if err := run(context.Background(), []string{src, "-o", src, "--overwrite"}, &buf, false); err == nil || !strings.HasPrefix(err.Error(), "-o:") {
		t.Fatalf("out == root: %v", err)
	}
	if err := runCount(context.Background(), []string{src, "--metadata", "bogus"}, &buf); err == nil {
		t.Fatal("bad --metadata accepted")
	}
}

// TestShowDataset: swath show names a committed dataset's layers, the
// SeekParquet format and draft, the swath schema id and the swath metadata
// version, before its coverage, and counts its parts and rows in English
// (1 part, 1 row).
func TestShowDataset(t *testing.T) {
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "a"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "ds")
	if err := run(context.Background(), []string{"-o", out, src}, io.Discard, false); err != nil {
		t.Fatal(err)
	}
	var shown bytes.Buffer
	if err := show([]string{out}, &shown); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(shown.String(), "\n")
	want := "dataset " + out + ": SeekParquet format 0 draft 1, swath schema swath/local-entries/v1, swath metadata version 1"
	if lines[0] != want || !strings.HasPrefix(lines[3], "coverage complete=true stop=completed") || !strings.HasPrefix(lines[4], "1 part, 1 row, ") {
		t.Fatalf("show:\n%s\nwant first line %q", shown.String(), want)
	}
}

// TestPrintedCommandsQuoted: a command swath prints for the user to run
// (the resume advice after a stop, the command an interrupted run
// recorded) quotes an argument the shell would split or expand, so a
// path with a space can be pasted as printed.
func TestPrintedCommandsQuoted(t *testing.T) {
	for in, want := range map[string]string{"/data/ds": "/data/ds", "--output=/data/ds": "--output=/data/ds", "s3://b/p": "s3://b/p",
		"/data/my ds": "'/data/my ds'", "": "''", "it's": `'it'\''s'`, "a*": "'a*'", "~/ds": "'~/ds'", "$HOME": "'$HOME'"} {
		if got := quoteFor("linux", in); got != want {
			t.Errorf("quoteFor(linux, %q) = %s, want %s", in, got, want)
		}
	}
	// This platform's quoting (TestQuoteFor) for the commands below.
	quoted := func(s string) string {
		if q := shellQuote(s); q != s {
			return q
		}
		t.Fatalf("%q is not quoted", s)
		return ""
	}
	dir := filepath.Join(t.TempDir(), "my inventory")
	r := &listing.Report{Outcome: listing.Outcome{Class: "deadline", ExitCode: exitDeadline, Resumable: true}}
	r.Invocation.Out = dir
	if l := statsLines(r, false, rowNouns(false, false)); l[0] != "INCOMPLETE (deadline) — resume: swath resume "+quoted(dir) {
		t.Fatalf("disposition %q", l[0])
	}
	// A directory named like a flag is an operand only as ./-inv.
	r.Invocation.Out = "-inv"
	if l := statsLines(r, false, rowNouns(false, false)); l[0] != "INCOMPLETE (deadline) — resume: swath resume ./-inv" {
		t.Fatalf("disposition %q", l[0])
	}
	if got := shellOperand("-my inv"); got != quoted("./-my inv") {
		t.Fatalf("shellOperand(-my inv) = %s", got)
	}
	j, err := journal.Create(dir, journal.Identity{Tool: "swath"}, []string{"list", "--output=" + dir, "s3://b"})
	if err != nil {
		t.Fatal(err)
	}
	j.Close()
	var shown bytes.Buffer
	if err := show([]string{dir}, &shown); err != nil {
		t.Fatal(err)
	}
	if want := "\ncommand: swath list " + quoted("--output="+dir) + " s3://b\n"; !strings.Contains(shown.String(), want) {
		t.Fatalf("show:\n%s\nwant %q", shown.String(), want)
	}
}

// TestQuoteFor: a word the shell of the platform would change is quoted
// for it: zsh expands a leading '=' (=ls is ls's path), so it is quoted
// with POSIX single quotes; on Windows, where cmd and PowerShell take
// neither single quotes nor a POSIX escape, in double quotes (a Windows
// path holds no '"'), its backslashes and drive colon plain.
func TestQuoteFor(t *testing.T) {
	for _, c := range []struct{ goos, in, want string }{
		{"linux", "=ls", "'=ls'"},
		{"linux", "--output=/d", "--output=/d"},
		{"darwin", "=x/y", "'=x/y'"},
		{"windows", `C:\data\ds`, `C:\data\ds`},
		{"windows", `C:\my data\ds`, `"C:\my data\ds"`},
		{"windows", "=ls", `"=ls"`},
		{"windows", "", `""`},
		{"windows", "a,b", `"a,b"`},
		{"windows", "%PATH%", `"%PATH%"`},
		{"windows", "--output=C:\\ds", "--output=C:\\ds"},
		{"windows", `C:\my data\`, `"C:\my data\\"`},
		{"windows", `say "hi"`, `"say \"hi\""`},
	} {
		if got := quoteFor(c.goos, c.in); got != c.want {
			t.Errorf("quoteFor(%s, %q) = %s, want %s", c.goos, c.in, got, c.want)
		}
	}
}

func TestVersion(t *testing.T) {
	var b bytes.Buffer
	if err := dispatch(context.Background(), []string{"--version"}, &b); err != nil || !strings.HasPrefix(b.String(), "swath ") {
		t.Fatalf("%v %q", err, b.String())
	}
}

func TestSize(t *testing.T) {
	// One syntax with the size filters (filter.ParseSize): K, M, G, T and
	// KiB... binary, KB... decimal, in any case, exact decimal quantities.
	for in, want := range map[string]int64{"4MiB": 4 << 20, "4MB": 4e6, "4mb": 4e6, "4m": 4 << 20, "123": 123, "2GiB": 2 << 30,
		"1.5g": 3 << 29, "0.5KiB": 512, " 64 MiB ": 64 << 20, "1t": 1 << 40} {
		var s size
		if err := s.Set(in); err != nil || int64(s) != want {
			t.Errorf("%s: %d, %v", in, s, err)
		}
		if f, err := filter.ParseSize(in); err != nil || f != want {
			t.Errorf("%s: the filter parser reads %d, %v", in, f, err)
		}
	}
	var s size
	for _, bad := range []string{"0", "0KiB", "x", "-1", "1.3b", "9223372036854775807GiB", "8589934592GiB", "4PB"} {
		if s.Set(bad) == nil {
			t.Errorf("invalid size %q accepted", bad)
		}
	}
	var z zsize
	if err := z.Set("0"); err != nil || z != 0 {
		t.Errorf("zsize 0: %d, %v", z, err)
	}
	if err := z.Set("2k"); err != nil || z != 2048 {
		t.Errorf("zsize 2k: %d, %v", z, err)
	}
}

// TestDocsCLIFlags keeps each command page's option list (docs/commands/)
// and the diagnostic options (docs/internals/diagnostics.md) generated
// from the flag set, as --help prints them: run with UPDATE=1 to rewrite
// them.
func TestDocsCLIFlags(t *testing.T) {
	groups := func(diag bool, keep func(*pflag.Flag) bool) string {
		var b strings.Builder
		writeGroups(&b, newFlags(&options{}, diag), docStyle, keep)
		return strings.TrimPrefix(b.String(), "\n")
	}
	var global []*pflag.Flag
	newFlags(&options{}, false).VisitAll(func(fl *pflag.Flag) {
		if slices.Contains(globalFlags, fl.Name) {
			global = append(global, fl)
		}
	})
	var root strings.Builder
	writeFlags(&root, global, docStyle, column(global))
	diagnostic := func(fl *pflag.Flag) bool { return groupOf(fl) == source.GroupDiagnostics }
	for _, b := range []struct{ path, marker, text string }{
		{"commands/swath.md", "flags", root.String()},
		{"commands/swath-list.md", "flags", groups(false, func(fl *pflag.Flag) bool { return shown(fl, command{}) })},
		{"commands/swath-du.md", "flags", groups(false, func(fl *pflag.Flag) bool { return shown(fl, command{count: true, du: true}) })},
		{"commands/swath-resume.md", "flags", groups(true, func(fl *pflag.Flag) bool { return mayChange(fl.Name) && !diagnostic(fl) })},
		{"internals/diagnostics.md", "diag-list", groups(true, func(fl *pflag.Flag) bool { return shown(fl, command{diag: true}) })},
		{"internals/diagnostics.md", "diag-count", groups(true, func(fl *pflag.Flag) bool { return shown(fl, command{diag: true, count: true}) && diagnostic(fl) })},
		{"internals/diagnostics.md", "diag-resume", groups(true, func(fl *pflag.Flag) bool { return mayChange(fl.Name) && diagnostic(fl) })},
	} {
		if strings.Contains(b.text, " \t") {
			t.Fatalf("the usage has a space before a tab (git diff --check flags it in docs/%s)", b.path)
		}
		docBlock(t, b.path, b.marker, "text", b.text)
	}
}

// docBlock keeps the block between docs/name's "<!-- marker:begin -->"
// and "<!-- marker:end -->" lines equal to text, fenced as lang: UPDATE=1
// rewrites it, else a stale block fails the test.
func docBlock(t *testing.T, name, marker, lang, text string) {
	t.Helper()
	path := docs.Path(t, name)
	begin, end := "<!-- "+marker+":begin -->\n", "<!-- "+marker+":end -->"
	doc, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	s := string(doc)
	i, j := strings.Index(s, begin), strings.Index(s, end)
	if i < 0 || j < i {
		t.Fatalf("docs/%s lacks the %s markers", name, marker)
	}
	want := s[:i+len(begin)] + "```" + lang + "\n" + text + "```\n" + s[j:]
	if want == s {
		return
	}
	if os.Getenv("UPDATE") != "" {
		if err := os.WriteFile(path, []byte(want), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	t.Errorf("docs/%s: the %s block is stale; run UPDATE=1 go test ./cmd/swath -run '^TestDocs'", name, marker)
}

// TestRefusedRunLeavesOwnerMetadata: a run refused because another process
// owns the output writes neither error.json nor the progress file there.
func TestRefusedRunLeavesOwnerMetadata(t *testing.T) {
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "a"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "ds")
	var buf bytes.Buffer
	if err := run(context.Background(), []string{src, "-o", out}, &buf, false); err != nil {
		t.Fatal(err)
	}
	// Another process now owns the output (say, an overwrite in progress).
	lock, err := journal.TakeLock(out)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release()
	progress := filepath.Join(out, progressName)
	if err := os.WriteFile(progress, []byte("owner"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := run(context.Background(), []string{src, "-o", out, "--overwrite", "--stall-timeout", "40ms"}, &buf, false); !errors.Is(err, journal.ErrLocked) {
		t.Fatalf("competing run: %v", err)
	}
	if _, err := os.Stat(filepath.Join(out, errorName)); err == nil {
		t.Fatal("the refused run wrote error.json into the owner's output")
	}
	if b, _ := os.ReadFile(progress); string(b) != "owner" {
		t.Fatalf("the refused run rewrote the owner's progress: %q", b)
	}
}

// TestDiskFullIsResumable: a full disk loses nothing durable, so with an
// intact journal the outcome is resumable, with the cause named.
func TestDiskFullIsResumable(t *testing.T) {
	dir := t.TempDir()
	j, err := journal.Create(dir, journal.Identity{Tool: "swath"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	j.Close()
	o := classify(fmt.Errorf("journal: %w", syscall.ENOSPC), nil, dir)
	if o.ExitCode != exitDiskFull || o.Class != "disk_full" || !o.Resumable || o.Cause != "no_space" {
		t.Fatalf("journal ENOSPC: %+v", o)
	}
	if o := classify(fmt.Errorf("journal: %w", syscall.ENOSPC), nil, t.TempDir()); o.Resumable {
		t.Fatalf("ENOSPC without a journal is resumable: %+v", o)
	}
}

// TestTerminalCausesOutrankStops: an unsafe output layout and the part
// limit classify fatal, exit 1, not resumable, whatever stop cause was
// selected meanwhile (tenth durability review: a stall, the deadline or a
// signal made an unsafe-layout failure resumable).
func TestTerminalCausesOutrankStops(t *testing.T) {
	dir := t.TempDir()
	j, err := journal.Create(dir, journal.Identity{Tool: "swath"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	j.Close()
	for _, c := range []struct {
		err   error
		cause string
	}{
		{fmt.Errorf("resume: %w", &dataset.LayoutError{Path: "data", Want: "a directory", Got: "a symbolic link"}), "unsafe_layout"},
		{fmt.Errorf("router: %w", dataset.ErrPartLimit), "part_limit"},
	} {
		for _, stop := range []error{nil, &stuckError{stuckNoRows}, errDeadline, &signalError{syscall.SIGTERM}, &signalError{os.Interrupt}} {
			o := classify(c.err, stop, dir)
			if o.Class != "fatal" || o.ExitCode != exitFatal || o.Resumable || o.Cause != c.cause {
				t.Errorf("%v under %v: %+v, want fatal, %d, not resumable, cause %s", c.err, stop, o, exitFatal, c.cause)
			}
		}
	}
}

// TestPartIntervalDefault: --part-interval defaults to Java's rotation
// interval, 30 s (before: 60 s), on swath list (which does not take the
// flag) as on swath diag list and swath resume.
func TestPartIntervalDefault(t *testing.T) {
	for _, diag := range []bool{false, true} {
		var o options
		f := newFlags(&o, diag)
		if err := f.Parse(nil); err != nil {
			t.Fatal(err)
		}
		if o.partInterval != 30*time.Second || f.Lookup("part-interval").DefValue != "30s" {
			t.Fatalf("diag %v: --part-interval default %s", diag, o.partInterval)
		}
	}
	if listing.DefaultPartInterval != 30*time.Second {
		t.Fatalf("listing.DefaultPartInterval %s", listing.DefaultPartInterval)
	}
}

// TestSpillFormatFlags: the default spill frames are delta coded and
// compressed with zstd on every list command, and swath diag list takes
// the frame flags (a comparison arm's).
func TestSpillFormatFlags(t *testing.T) {
	out := t.TempDir()
	for _, diag := range []bool{false, true} {
		for _, c := range []struct {
			args            []string
			plain, compress bool
		}{
			{nil, false, true},
			{[]string{"--no-spill-compress"}, false, false},
			{[]string{"--spill-plain"}, true, true},
			{[]string{"--spill-plain", "--no-spill-compress"}, true, false},
		} {
			if !diag && c.args != nil {
				continue // diagnostic flags: TestMovedFlagsNameDiag
			}
			args := append(append([]string{}, c.args...), "-o", filepath.Join(out, "ds"), "s3://bkt")
			o, _, err := parseList(args, io.Discard, command{diag: diag})
			if err != nil {
				t.Fatalf("diag=%v %v: %v", diag, c.args, err)
			}
			if o.spillPlain != c.plain || o.spillCompress != c.compress {
				t.Fatalf("diag=%v %v: plain %v compress %v, want %v %v", diag, c.args, o.spillPlain, o.spillCompress, c.plain, c.compress)
			}
		}
	}
}
