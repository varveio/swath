// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !windows

package listing

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

// TestSeedFromStreamedCutFileBounded: a cut file streamed through a named
// pipe (as a process substitution is) reports size zero to the stat, so
// the 16 MiB budget is enforced while scanning: 200,000 valid ascending
// cuts of about 100 bytes (under the cut-count bound, over the byte one)
// are refused with ErrPrior; a small streamed file seeds the run.
func TestSeedFromStreamedCutFileBounded(t *testing.T) {
	url := fakeS3(t, 300)
	dir := t.TempDir()
	stream := func(name string, lines int) string {
		p := filepath.Join(dir, name)
		if err := syscall.Mkfifo(p, 0o600); err != nil {
			t.Fatal(err)
		}
		go func() {
			f, err := os.OpenFile(p, os.O_WRONLY, 0)
			if err != nil {
				return
			}
			defer f.Close()
			pad := strings.Repeat("x", 88)
			for i := range lines {
				if _, err := fmt.Fprintf(f, "logs/%07d/%s\n", i, pad); err != nil {
					return // the reader stopped at its budget
				}
			}
		}()
		return p
	}
	if _, err := listS3(t, url, "s3://bkt", filepath.Join(dir, "big.out"), stream("big.cuts", 200_000)); !errors.Is(err, ErrPrior) || !strings.Contains(err.Error(), "bytes") {
		t.Fatalf("a streamed cut file over 16 MiB: %v, want ErrPrior naming the byte bound", err)
	}
	if _, err := listS3(t, url, "s3://bkt", filepath.Join(dir, "small.out"), stream("small.cuts", 3)); err != nil {
		t.Fatalf("a small streamed cut file: %v", err)
	}
}
