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
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/filter"
	"github.com/varveio/swath/go/internal/journal"
	"github.com/varveio/swath/go/internal/listing"
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
	if !strings.Contains(buf.String(), "rows=1 parts=1") {
		t.Fatalf("summary: %s", buf.String())
	}
	err := run(context.Background(), []string{src, "-o", out}, &buf, false)
	if err == nil || !strings.Contains(err.Error(), "--overwrite") {
		t.Fatalf("non-empty output: %v", err)
	}
	// The report goes out on failure too, with the outcome.
	buf.Reset()
	if err := run(context.Background(), []string{src, "-o", out, "--report-format", "json"}, &buf, false); err == nil ||
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
// version, before its coverage.
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
	if lines[0] != want || !strings.HasPrefix(lines[3], "coverage complete=true stop=completed") {
		t.Fatalf("show:\n%s\nwant first line %q", shown.String(), want)
	}
}

func TestVersion(t *testing.T) {
	var b bytes.Buffer
	if err := dispatch(context.Background(), []string{"--version"}, &b); err != nil || !strings.HasPrefix(b.String(), "swath-go ") {
		t.Fatalf("%v %q", err, b.String())
	}
}

func TestSize(t *testing.T) {
	// One syntax with the size filters (filter.ParseSize): binary units in
	// any case, exact decimal quantities.
	for in, want := range map[string]int64{"4MiB": 4 << 20, "4MB": 4 << 20, "4mb": 4 << 20, "4m": 4 << 20, "123": 123, "2GiB": 2 << 30,
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

// TestCLIDocFlags keeps docs/cli.md's flag list generated from the flag set:
// run with UPDATE=1 to rewrite it.
func TestCLIDocFlags(t *testing.T) {
	var b bytes.Buffer
	for i, cmd := range []command{{}, {count: true, du: true}, {diag: true}, {diag: true, count: true}} {
		if i > 0 {
			b.WriteString("\n")
		}
		fmt.Fprintf(&b, "$ swath %s --help\n", cmd.name())
		usage(&b, newFlags(&options{}, cmd.diag), cmd)
	}
	if strings.Contains(b.String(), " \t") {
		t.Fatal("the usage has a space before a tab (git diff --check flags it in docs/cli.md)")
	}
	const begin, end = "<!-- flags:begin -->\n", "<!-- flags:end -->"
	readme, err := os.ReadFile("../../docs/cli.md")
	if err != nil {
		t.Fatal(err)
	}
	s := string(readme)
	i, j := strings.Index(s, begin), strings.Index(s, end)
	if i < 0 || j < i {
		t.Fatal("docs/cli.md lacks the flags markers")
	}
	want := s[:i+len(begin)] + "```text\n" + b.String() + "```\n" + s[j:]
	if want == s {
		return
	}
	if os.Getenv("UPDATE") != "" {
		if err := os.WriteFile("../../docs/cli.md", []byte(want), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	t.Fatal("docs/cli.md flag list is stale; run UPDATE=1 go test ./cmd/swath")
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
	j, err := journal.Create(dir, journal.Identity{Tool: "swath-go"}, nil)
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
	j, err := journal.Create(dir, journal.Identity{Tool: "swath-go"}, nil)
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
			{[]string{"--spill-compress=false"}, false, false},
			{[]string{"--spill-plain"}, true, true},
			{[]string{"--spill-plain", "--spill-compress=false"}, true, false},
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
