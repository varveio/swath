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
	"syscall"
	"testing"
	"time"
	"unicode"

	"github.com/spf13/pflag"

	"github.com/varveio/swath/docs"
	"github.com/varveio/swath/internal/cli"
	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/filter"
	"github.com/varveio/swath/internal/journal"
	"github.com/varveio/swath/internal/listing"
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

// TestShowDataset: swath show prints a committed dataset's verdict first,
// then labelled lines in reading order (source, listed, contents with
// counts grouped, coverage, storage last), and clips a long key range to
// the width given. The storage line names the schema; -v adds the
// SeekParquet format id before it and changes nothing else.
func TestShowDataset(t *testing.T) {
	src := t.TempDir()
	long := strings.Repeat("k", 200)
	for _, name := range []string{"a", long} {
		if err := os.WriteFile(filepath.Join(src, name), []byte("12345"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	out := filepath.Join(t.TempDir(), "ds")
	if err := run(context.Background(), []string{"-o", out, src}, io.Discard, false); err != nil {
		t.Fatal(err)
	}
	var shown bytes.Buffer
	if err := show([]string{out}, &shown); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(shown.String(), "\n"), "\n")
	labels := []string{"source", "listed", "contents", "coverage", "storage"}
	if len(lines) != 6 || lines[0] != out+": complete" {
		t.Fatalf("show:\n%s", shown.String())
	}
	for i, l := range labels {
		if !strings.HasPrefix(lines[i+1], "  "+l+" ") {
			t.Fatalf("line %d %q, want the label %s", i+1, lines[i+1], l)
		}
	}
	if !strings.Contains(lines[1], "(whole tree)") || !strings.Contains(lines[3], "2 entries, 10 B; keys \"a\" .. \""+long+"\"") ||
		lines[4] != "  coverage  nothing unreadable; no filters; no exclusions" ||
		!strings.Contains(lines[5], "1 part, ") || !strings.HasSuffix(lines[5], " of Parquet (schema swath/local-entries/v1)") {
		t.Fatalf("show:\n%s", shown.String())
	}
	var m dataset.Manifest
	b, err := os.ReadFile(filepath.Join(out, dataset.ManifestName))
	if err != nil || json.Unmarshal(b, &m) != nil {
		t.Fatal(err)
	}
	var verbose bytes.Buffer
	if err := show([]string{out, "-v"}, &verbose); err != nil {
		t.Fatal(err)
	}
	want := slices.Clone(lines)
	want[5] = strings.Replace(want[5], "(schema ", fmt.Sprintf("(SeekParquet format %d draft %d, schema ", m.FormatVersion, m.DraftRevision), 1)
	if got := strings.Split(strings.TrimSuffix(verbose.String(), "\n"), "\n"); m.DraftRevision == 0 || !slices.Equal(got, want) {
		t.Fatalf("show -v:\n%s\nwant\n%s", verbose.String(), strings.Join(want, "\n"))
	}
	// Clipped to a narrow terminal; left out where even clipped keys
	// would pass its width.
	for width, keys := range map[int]bool{80: true, 50: true, 45: false} {
		shown.Reset()
		writeSummary(&shown, "ds", &m, width, false)
		for _, l := range strings.Split(strings.TrimSuffix(shown.String(), "\n"), "\n") {
			if strings.Contains(l, "contents") && (len(l) > width || strings.Contains(l, "...\"") != keys || strings.Contains(l, "keys") != keys) {
				t.Fatalf("width %d: %q", width, l)
			}
		}
	}
}

// TestShowInterruptedRun: swath show prints an interrupted run's start,
// its command and how far it got; -v adds the journal version and the
// run's frozen identity, a setting without a value (an AWS run's
// endpoint) left out.
func TestShowInterruptedRun(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "run")
	id := journal.Identity{Tool: "swath", SchemaID: "swath/s3-objects/v1", Writer: "swath/parquet-r5", PartSize: 64 << 20, Provider: "s3",
		Source: []journal.Field{{Name: "source", Value: "s3://b/logs/"}, {Name: "endpoint", Value: ""}, {Name: "region", Value: "us-east-1"}, {Name: "fips", Value: "false"}}}
	j, err := journal.Create(dir, id, []string{"list", "--output=" + dir, "s3://b/logs/"})
	if err != nil {
		t.Fatal(err)
	}
	j.Close()
	st, err := journal.Read(dir)
	if err != nil {
		t.Fatal(err)
	}
	started := "  started   " + localTime(st.Header.Created.Time)
	command := "  command   swath list " + shellQuote("--output="+dir) + " s3://b/logs/"
	saved := "  saved     no part; swath resume lists from the start"
	for _, c := range []struct {
		args []string
		want []string
	}{
		{[]string{dir}, []string{dir + ": interrupted run", started, command, saved}},
		{[]string{dir, "-v"}, []string{dir + ": interrupted run", started + " (journal version " + strconv.Itoa(st.Header.Version) + ")", command,
			"  identity  provider s3, schema swath/s3-objects/v1, writer swath/parquet-r5, part size 64.0 MiB, source s3://b/logs/, region us-east-1, fips false", saved}},
	} {
		var shown bytes.Buffer
		if err := show(c.args, &shown); err != nil {
			t.Fatal(err)
		}
		if got := strings.Split(strings.TrimSuffix(shown.String(), "\n"), "\n"); !slices.Equal(got, c.want) {
			t.Fatalf("show %q:\n%s\nwant\n%s", c.args, shown.String(), strings.Join(c.want, "\n"))
		}
	}
}

// TestShowNotADataset: swath show on a missing path says so; on a path
// that is no dataset (a file, a text listing) it says what it does not
// hold, with and without --journal, never an open error ("not a
// directory").
func TestShowNotADataset(t *testing.T) {
	file := filepath.Join(t.TempDir(), "rows.tsv")
	if err := os.WriteFile(file, []byte("key\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(t.TempDir(), "missing")
	for _, args := range [][]string{{file}, {file, "--journal"}, {missing}, {missing, "--journal"}} {
		want := file + " holds "
		if args[0] == missing {
			want = missing + ": no such file or directory"
		}
		if err := show(args, io.Discard); err == nil || !strings.Contains(err.Error(), want) || strings.Contains(err.Error(), "not a directory") {
			t.Fatalf("show %q: %v", args, err)
		}
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
	if l := statsLines(r, false, rowNouns(false, false), false); l[1] != "next: swath resume "+quoted(dir) {
		t.Fatalf("next %q", l)
	}
	// A directory named like a flag is an operand only as ./-inv.
	r.Invocation.Out = "-inv"
	if l := statsLines(r, false, rowNouns(false, false), false); l[1] != "next: swath resume ./-inv" {
		t.Fatalf("next %q", l)
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
	if want := "\n  command   swath list " + quoted("--output="+dir) + " s3://b\n"; !strings.Contains(shown.String(), want) {
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

// TestDocsCLIFlags keeps each command page's options (docs/commands/) and
// the diagnostic options (docs/internals/diagnostics.md) generated from
// the flag set: a page's as Markdown (optionList.markdown), the
// diagnostics' as the text --help prints. UPDATE=1 rewrites them.
func TestDocsCLIFlags(t *testing.T) {
	// grouped is the flags keep says, by help group.
	grouped := func(diag bool, keep func(*pflag.Flag) bool) optionList {
		f := newFlags(&options{}, diag)
		gs, _ := groupFlags(f, keep)
		var b strings.Builder
		writeGroups(&b, f, docStyle, keep)
		return optionList{gs, strings.TrimPrefix(b.String(), "\n")}
	}
	// flat is flags in one list without a heading.
	flat := func(flags []*pflag.Flag) optionList {
		var b strings.Builder
		writeFlags(&b, flags, docStyle, column(flags))
		return optionList{[]flagGroup{{flags: flags}}, b.String()}
	}
	var global []*pflag.Flag
	newFlags(&options{}, false).VisitAll(func(fl *pflag.Flag) {
		if slices.Contains(globalFlags, fl.Name) {
			global = append(global, fl)
		}
	})
	var journal bool
	diagnostic := func(fl *pflag.Flag) bool { return groupOf(fl) == cli.GroupDiagnostics }
	for _, b := range []struct {
		path, marker string
		list         optionList
	}{
		{"commands/swath.md", "flags", flat(global)},
		{"commands/swath-list.md", "flags", grouped(false, func(fl *pflag.Flag) bool { return shown(fl, command{}) })},
		{"commands/swath-du.md", "flags", grouped(false, func(fl *pflag.Flag) bool { return shown(fl, command{count: true, du: true}) })},
		{"commands/swath-resume.md", "flags", grouped(true, func(fl *pflag.Flag) bool { return mayChange(fl.Name) && !diagnostic(fl) })},
		{"commands/swath-show.md", "flags", flat(showFlags(pflag.NewFlagSet("show", pflag.ContinueOnError), &journal))},
		{"internals/diagnostics.md", "diag-list", grouped(true, func(fl *pflag.Flag) bool { return shown(fl, command{diag: true}) })},
		{"internals/diagnostics.md", "diag-count", grouped(true, func(fl *pflag.Flag) bool { return shown(fl, command{diag: true, count: true}) && diagnostic(fl) })},
		{"internals/diagnostics.md", "diag-resume", grouped(true, func(fl *pflag.Flag) bool { return mayChange(fl.Name) && diagnostic(fl) })},
	} {
		if strings.Contains(b.list.text, " \t") {
			t.Fatalf("the usage has a space before a tab (git diff --check flags it in docs/%s)", b.path)
		}
		if strings.HasPrefix(b.path, "commands/") {
			docSection(t, b.path, b.marker, b.list.markdown())
		} else {
			docBlock(t, b.path, b.marker, "text", b.list.text)
		}
	}
}

// optionList is a command's options by help group (one untitled group for
// a flat list), and the text --help prints for them.
type optionList struct {
	groups []flagGroup
	text   string
}

// markdown is a command page's options: a heading per titled group and an
// item per flag, its names linked to its own anchor (option-NAME), its
// help line and default, and its further text.
func (l optionList) markdown() string {
	var b strings.Builder
	for _, g := range l.groups {
		if g.title != "" {
			b.WriteString("### " + g.title + "\n\n")
		}
		for _, fl := range g.flags {
			d := cli.Described(fl)
			names, _ := flagNames(fl, helpStyle{})
			id := "option-" + fl.Name
			// A backslash ending a line is a hard line break.
			fmt.Fprintf(&b, "- <a id=\"%s\"></a>[%s](#%s)\\\n  %s", id, mdCode(strings.TrimSpace(names)), id, mdText(d.Usage))
			if d.Default != "" {
				b.WriteString(" (default " + mdCode(d.Default) + ")")
			}
			b.WriteString("\n\n")
			if d.Long != "" {
				b.WriteString("  " + mdText(d.Long) + "\n\n")
			}
		}
	}
	return b.String()
}

// mdCode is s as a Markdown code span.
func mdCode(s string) string {
	if strings.Contains(s, "`") {
		return "`` " + s + " ``"
	}
	return "`" + s + "`"
}

// mdText is help text as Markdown that renders as the same text: the
// characters that would start emphasis, a link, HTML, an entity or code
// escaped (an underscore inside a word starts nothing: AWS_REGION), and a
// list or heading marker that begins the text.
func mdText(s string) string {
	word := func(i int) bool {
		return i >= 0 && i < len(s) && (unicode.IsLetter(rune(s[i])) || unicode.IsDigit(rune(s[i])))
	}
	var b strings.Builder
	for i, r := range s {
		switch {
		case r == '_' && word(i-1) && word(i+1):
		case strings.ContainsRune("\\`*_[]<>&~", r), i == 0 && strings.ContainsRune("-+#", r) && strings.HasPrefix(s[1:], " "):
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}

// docBlock keeps the block between docs/name's "<!-- marker:begin -->"
// and "<!-- marker:end -->" lines equal to text, fenced as lang (docSection).
func docBlock(t *testing.T, name, marker, lang, text string) {
	t.Helper()
	docSection(t, name, marker, "```"+lang+"\n"+text+"```\n")
}

// docSection keeps what is between docs/name's "<!-- marker:begin -->"
// and "<!-- marker:end -->" lines equal to body: UPDATE=1 rewrites it,
// else a stale section fails the test.
func docSection(t *testing.T, name, marker, body string) {
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
	want := s[:i+len(begin)] + body + s[j:]
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

// TestDiskFullIsResumable: a full disk or a full quota (EDQUOT, exit 1
// before: #518) loses nothing durable, so with an intact journal the
// outcome is exit 74, resumable, with the cause named.
func TestDiskFullIsResumable(t *testing.T) {
	dir := t.TempDir()
	j, err := journal.Create(dir, journal.Identity{Tool: "swath"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	j.Close()
	for _, errno := range []syscall.Errno{syscall.ENOSPC, syscall.EDQUOT} {
		o := classify(fmt.Errorf("journal: %w", errno), nil, dir)
		if o.ExitCode != exitDiskFull || o.Class != "disk_full" || !o.Resumable || o.Cause != "no_space" {
			t.Fatalf("journal %v: %+v", errno, o)
		}
		if o := classify(fmt.Errorf("journal: %w", errno), nil, t.TempDir()); o.Resumable {
			t.Fatalf("%v without a journal is resumable: %+v", errno, o)
		}
	}
}

// TestFatalResumableAsTheJournalSays: a run that stopped on a failure a
// person fixes (credentials, permissions, a failing or read-only disk)
// exits 1, and its outcome is resumable when its journal is intact, as
// swath resume continues it; it said not resumable while resume continued
// it (#517). A failure whose fix changes the recorded command (a missing
// bucket, another region) or that is another process's run is not.
func TestFatalResumableAsTheJournalSays(t *testing.T) {
	dir := t.TempDir()
	j, err := journal.Create(dir, journal.Identity{Tool: "swath"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	j.Close()
	for _, c := range []struct {
		err       error
		cause     string
		resumable bool
	}{
		{&classified{errors.New("list: 401"), "auth"}, "auth", true},
		{&classified{errors.New("list: 403"), "access_denied"}, "access_denied", true},
		{fmt.Errorf("publish: %w", syscall.EIO), "", true},
		{fmt.Errorf("journal: %w", syscall.EROFS), "", true},
		{&classified{errors.New("list: 404"), "not_found"}, "not_found", false},
		{&classified{errors.New("list: 301"), "region_redirect"}, "region_redirect", false},
		{fmt.Errorf("%w (%s)", journal.ErrLocked, dir), "", false},
	} {
		o := classify(c.err, nil, dir)
		if o.Class != "fatal" || o.ExitCode != exitFatal || o.Resumable != c.resumable || o.Cause != c.cause {
			t.Errorf("%v: %+v, want fatal, resumable %v, cause %q", c.err, o, c.resumable, c.cause)
		}
		if o := classify(c.err, nil, t.TempDir()); o.Resumable {
			t.Errorf("%v without a journal is resumable", c.err)
		}
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
