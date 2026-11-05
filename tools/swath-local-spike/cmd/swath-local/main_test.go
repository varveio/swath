// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRun(t *testing.T) {
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "a"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "ds")
	var buf bytes.Buffer
	// root after flags and before them both work
	if err := run(context.Background(), []string{"-out", out, src, "-part-size", "4MiB"}, &buf); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "rows=1 parts=1") {
		t.Fatalf("summary: %s", buf.String())
	}
	err := run(context.Background(), []string{src, "-out", out}, &buf)
	if err == nil || !strings.Contains(err.Error(), "-overwrite") {
		t.Fatalf("non-empty output: %v", err)
	}
	if err := run(context.Background(), []string{src, "-out", out, "-overwrite"}, &buf); err != nil {
		t.Fatal(err)
	}
	if err := run(context.Background(), []string{src, "-out", src, "-overwrite"}, &buf); err == nil || !strings.HasPrefix(err.Error(), "-out:") {
		t.Fatalf("out == root: %v", err)
	}
	if err := run(context.Background(), []string{src, "-count-only", "-fields", "bogus"}, &buf); err == nil {
		t.Fatal("bad -fields accepted")
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
	if s.Set("0") == nil || s.Set("x") == nil {
		t.Error("invalid sizes accepted")
	}
}

// TestReadmeFlags keeps README.md's flag list generated from the flag set:
// run with UPDATE=1 to rewrite it.
func TestReadmeFlags(t *testing.T) {
	var b bytes.Buffer
	usage(&b, newFlags(&options{}))
	const begin, end = "<!-- flags:begin -->\n", "<!-- flags:end -->"
	readme, err := os.ReadFile("../../README.md")
	if err != nil {
		t.Fatal(err)
	}
	s := string(readme)
	i, j := strings.Index(s, begin), strings.Index(s, end)
	if i < 0 || j < i {
		t.Fatal("README.md lacks the flags markers")
	}
	want := s[:i+len(begin)] + "```text\n" + b.String() + "```\n" + s[j:]
	if want == s {
		return
	}
	if os.Getenv("UPDATE") != "" {
		if err := os.WriteFile("../../README.md", []byte(want), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	t.Fatal("README.md flag list is stale; run UPDATE=1 go test ./cmd/swath-local")
}
