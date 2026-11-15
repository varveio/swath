// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// hostile is a name that forges a swath line and drives a terminal: a
// newline, an ESC sequence, a carriage return, a C1 control, U+2028, a
// bidirectional override and a byte that is not UTF-8.
const hostile = "a\nswath: fake line\x1b[2J\rb\u0085c d‮e\xffz"

// wantEscaped is hostile as escapeControl writes it.
const wantEscaped = `a\nswath: fake line\x1b[2J\rb\x85c\u2028d\u202ee\xffz`

func TestEscapeControl(t *testing.T) {
	for in, want := range map[string]string{
		"plain/key.txt":      "plain/key.txt",
		"ünï/文字/😀 \\ ok":     "ünï/文字/😀 \\ ok",
		"a\nb\tc\rd":         `a\nb\tc\rd`,
		"\x00\x7f\x1b":       `\x00\x7f\x1b`,
		"\u0085\u2028\u2029": "\\x85\\u2028\\u2029",
		"\u202e":             "\\u202e",
		"a\xff\xfeb":         `a\xff\xfeb`,
		"\ufffd":             "\ufffd",
	} {
		if got := escapeControl(in); got != want {
			t.Errorf("escapeControl(%q) = %q, want %q", in, got, want)
		}
		if again := escapeControl(escapeControl(in)); again != escapeControl(in) {
			t.Errorf("escapeControl is not idempotent on %q: %q", in, again)
		}
	}
}

// escapedLine checks s is a single line, and no control byte but its newline.
func escapedLine(t *testing.T, what, s string) {
	t.Helper()
	if strings.Count(s, "\n") != 1 || !strings.HasSuffix(s, "\n") {
		t.Errorf("%s is not one line: %q", what, s)
	}
	for _, r := range strings.TrimSuffix(s, "\n") {
		if unsafeRune(r) {
			t.Errorf("%s has a raw %q: %q", what, r, s)
			break
		}
	}
	if strings.Contains(s, "\nswath:") {
		t.Errorf("%s forges a line: %q", what, s)
	}
}

// TestUntrustedNamesAreEscapedOnStderr: every text sink that carries a
// name from the source (a path, a key, a provider's error, a dataset's
// recorded source) writes one line with visible escapes; JSON keeps the
// string exact.
func TestUntrustedNamesAreEscapedOnStderr(t *testing.T) {
	complete := false
	for _, c := range []struct {
		name string
		emit func(l *eventLog)
	}{
		{"warning", func(l *eventLog) { l.warn("cannot read " + hostile) }},
		{"start", func(l *eventLog) { l.start(startFields{hostile, modeRows, "stdout"}) }},
		{"dataset", func(l *eventLog) {
			s := hostile
			l.dataset(provenance{Path: hostile, Source: &s, Complete: &complete}, time.Now())
		}},
		{"error", func(l *eventLog) { conclude(errors.New("list " + hostile + ": boom")) }},
	} {
		t.Run(c.name, func(t *testing.T) {
			b := captureSyncStderr(t)
			setLog(termOptions{verbose: 1}, "attempt1")
			c.emit(elog())
			out := b.String()
			if !strings.Contains(out, wantEscaped) {
				t.Errorf("text: no escaped name in %q", out)
			}
			for _, line := range strings.SplitAfter(out, "\n") {
				if line != "" && !strings.HasPrefix(line, "swath: ") && !strings.HasPrefix(line, indent) {
					t.Errorf("text: a line that is not swath's: %q", line)
				}
			}
			if strings.ContainsAny(out, "\x1b\r\u0085 ‮\xff") {
				t.Errorf("text: raw control bytes in %q", out)
			}
			b2 := captureSyncStderr(t)
			setLog(termOptions{verbose: 1, logFormat: logJSON}, "attempt1")
			c.emit(elog())
			escapedLine(t, "json", b2.String())
			var ev map[string]any
			if err := json.Unmarshal([]byte(b2.String()), &ev); err != nil {
				t.Fatalf("json: %v: %q", err, b2.String())
			}
			if msg, _ := ev["msg"].(string); !strings.Contains(msg, "a\nswath: fake line\x1b[2J\rb\u0085c d‮e") {
				t.Errorf("json msg is not the exact name: %q", msg)
			}
		})
	}
}

// TestHostileFileNameWarning: a directory whose name forges a swath line
// and cannot be read warns on exactly one stderr line per sample, with the
// name escaped (Linux and the other Unixes; a file system or a platform
// that cannot create the name, or a root who can read anything, skips).
func TestHostileFileNameWarning(t *testing.T) {
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("needs a name with control bytes and a directory permissions deny")
	}
	root := t.TempDir()
	bad := filepath.Join(root, "d\nswath: fake line\x1b[2J\xff")
	if err := os.Mkdir(bad, 0o755); err != nil {
		t.Skipf("cannot create the name: %v", err)
	}
	if err := os.WriteFile(filepath.Join(bad, "f"), nil, 0o644); err != nil {
		t.Skipf("cannot create the name: %v", err)
	}
	if err := os.Chmod(bad, 0); err != nil {
		t.Skipf("cannot deny the directory: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(bad, 0o755) })
	if _, err := os.ReadDir(bad); err == nil {
		t.Skip("permissions do not deny this directory here")
	}
	for _, engine := range []string{"", "portable"} {
		t.Setenv("SWATH_LOCALFS_ENGINE", engine)
		b := captureSyncStderr(t)
		setLog(termOptions{}, "")
		var out strings.Builder
		err := run(context.Background(), []string{root, "--format=tsv", "--no-stats", "--no-progress"}, &out, false)
		conclude(err)
		got := b.String()
		if !strings.Contains(got, `d\nswath: fake line\x1b[2J\xff`) {
			t.Errorf("engine %q: the name is not named escaped: %q", engine, got)
		}
		for _, line := range strings.SplitAfter(got, "\n") {
			if line != "" && !strings.HasPrefix(line, "swath: ") && !strings.HasPrefix(line, indent) {
				t.Errorf("engine %q: a line that is not swath's: %q", engine, line)
			}
		}
		if strings.ContainsAny(got, "\x1b\xff") || strings.Contains(got, "\nswath: fake line") {
			t.Errorf("engine %q: raw bytes on stderr: %q", engine, got)
		}
	}
}
