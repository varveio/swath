// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/dataset"
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
	err := run(context.Background(), []string{src, "--output", out}, &buf, false)
	if err == nil || !strings.Contains(err.Error(), "--overwrite") {
		t.Fatalf("non-empty output: %v", err)
	}
	// The report goes out on failure too, with the outcome.
	buf.Reset()
	if err := run(context.Background(), []string{src, "--output", out, "--report-format", "json"}, &buf, false); err == nil ||
		!strings.Contains(buf.String(), `"status": "failed"`) || !strings.Contains(buf.String(), "not empty") {
		t.Fatalf("failure report: %v\n%s", err, buf.String())
	}
	if err := run(context.Background(), []string{src, "--output", out, "--overwrite"}, &buf, false); err != nil {
		t.Fatal(err)
	}
	if err := run(context.Background(), []string{src, "--output", out, "--force"}, &buf, false); err != nil {
		t.Fatalf("--force (Java's alias of --overwrite): %v", err)
	}
	if err := run(context.Background(), []string{src, "--output", src, "--overwrite"}, &buf, false); err == nil || !strings.HasPrefix(err.Error(), "--output:") {
		t.Fatalf("out == root: %v", err)
	}
	if err := run(context.Background(), []string{src, "--count-only", "--metadata", "bogus"}, &buf, false); err == nil {
		t.Fatal("bad --metadata accepted")
	}
}

func TestVersion(t *testing.T) {
	var b bytes.Buffer
	if err := dispatch(context.Background(), []string{"--version"}, &b); err != nil || !strings.HasPrefix(b.String(), "swath-go ") {
		t.Fatalf("%v %q", err, b.String())
	}
}

func TestSize(t *testing.T) {
	for in, want := range map[string]int64{"4MiB": 4 << 20, "4MB": 4_000_000, "123": 123, "2GiB": 2 << 30} {
		var s size
		if err := s.Set(in); err != nil || int64(s) != want {
			t.Errorf("%s: %d, %v", in, s, err)
		}
	}
	var s size
	if s.Set("0") == nil || s.Set("x") == nil || s.Set("9223372036854775807GiB") == nil || s.Set("8589934592GiB") == nil {
		t.Error("invalid sizes accepted")
	}
}

// TestCLIDocFlags keeps docs/cli.md's flag list generated from the flag set:
// run with UPDATE=1 to rewrite it.
func TestCLIDocFlags(t *testing.T) {
	var b bytes.Buffer
	usage(&b, newFlags(&options{}, false), false)
	b.WriteString("\n")
	usage(&b, newFlags(&options{}, true), true)
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
// interval, 30 s (before: 60 s), on swath list and swath resume.
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
