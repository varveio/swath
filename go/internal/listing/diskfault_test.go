// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/faults"
)

// TestDiskFaultMatrix fails a durable write partway (half its bytes
// written, then ENOSPC) or its fsync (the bytes written, the sync failing
// with EIO), for a part file, a journal record and the manifest: the run
// fails with that error and publishes no manifest, the parts it published
// stay as they were, and a resume completes the dataset of an
// uninterrupted run byte for byte (before, faults were injected only
// before a write: TestENOSPCOnJournalResumes; Sol, sixth review).
func TestDiskFaultMatrix(t *testing.T) {
	url := crashBucket(t)
	ref := filepath.Join(t.TempDir(), "ref")
	if _, err := Run(context.Background(), crashConfig(url, ref, false)); err != nil {
		t.Fatal(err)
	}
	targets := map[string]struct {
		match       func(name string) bool
		write, sync int64 // the occurrence that fails
	}{
		// A part is written in several buffered writes and synced once.
		"part":     {func(n string) bool { return strings.HasSuffix(n, ".parquet") && strings.Contains(n, dataset.PartsDir) }, 40, 3},
		"journal":  {func(n string) bool { return filepath.Base(n) == "journal" }, 4, 4},
		"manifest": {func(n string) bool { return filepath.Base(n) == dataset.ManifestName+".tmp" }, 1, 1},
	}
	for target, tg := range targets {
		for _, op := range []string{"write", "sync"} {
			name := target + " " + op
			want := map[string]error{"write": syscall.ENOSPC, "sync": syscall.EIO}[op]
			out := filepath.Join(t.TempDir(), "ds")
			var n, hit atomic.Int64
			nth := map[string]int64{"write": tg.write, "sync": tg.sync}[op]
			restore := faults.SetIO(func(o, file string, b []byte) (int, error) {
				if o == op && tg.match(file) && n.Add(1) == nth {
					hit.Add(1)
					return len(b) / 2, want
				}
				return 0, nil
			})
			_, err := Run(context.Background(), crashConfig(url, out, false))
			restore()
			if hit.Load() != 1 || !errors.Is(err, want) {
				t.Fatalf("%s: injected %d times, run error %v", name, hit.Load(), err)
			}
			if _, err := os.Stat(filepath.Join(out, dataset.ManifestName)); err == nil {
				t.Fatalf("%s: a manifest after the failure", name)
			}
			kept, _ := filepath.Glob(filepath.Join(out, dataset.DataDir, "*.parquet"))
			before := map[string][]byte{}
			for _, p := range kept {
				before[p], _ = os.ReadFile(p)
			}
			if _, err := Run(context.Background(), crashConfig(url, out, true)); err != nil {
				t.Fatalf("%s: resume: %v", name, err)
			}
			for p, b := range before {
				if got, _ := os.ReadFile(p); !bytes.Equal(got, b) {
					t.Fatalf("%s: published part %s changed", name, filepath.Base(p))
				}
			}
			sameDataset(t, name, ref, out)
			if len(kept) == 0 {
				t.Fatalf("%s: no part was published before the fault", name)
			}
		}
	}
}
