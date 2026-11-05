// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/dataset"
)

func mk(t *testing.T, root string, files ...string) {
	t.Helper()
	for _, f := range files {
		p := filepath.Join(root, filepath.FromSlash(f))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func noScratch(t *testing.T, out string) {
	t.Helper()
	if tmps, _ := filepath.Glob(filepath.Join(out, dataset.DataDir, "*.tmp")); len(tmps) > 0 {
		t.Fatalf("unpublished parts left: %v", tmps)
	}
	if _, err := os.Stat(filepath.Join(out, dataset.SuccessName)); !os.IsNotExist(err) {
		t.Fatal("_SUCCESS present")
	}
}

func TestEndToEnd(t *testing.T) {
	src := t.TempDir()
	mk(t, src, "a/1", "a/2", "b", "z/y/x")
	out := filepath.Join(t.TempDir(), "ds")
	sum, err := Run(context.Background(), Config{Root: src, Out: out})
	if err != nil {
		t.Fatal(err)
	}
	if sum.Rows != 7 || sum.Parts != 1 {
		t.Fatalf("rows %d parts %d", sum.Rows, sum.Parts)
	}
	for _, f := range []string{dataset.ManifestName, dataset.SuccessName, dataset.SummaryName, dataset.SymlinkName, "data/part-00000.parquet"} {
		if _, err := os.Stat(filepath.Join(out, f)); err != nil {
			t.Fatal(err)
		}
	}
}

// An output inside the root is skipped by identity, for both engines, even
// while its parts are being written during the walk.
func TestOutputInsideRootIsSkipped(t *testing.T) {
	for _, portable := range []bool{false, true} {
		src := t.TempDir()
		mk(t, src, "a/1", "a/2", "b", "z/y/x")
		out := filepath.Join(src, "a", "out")
		sum, err := Run(context.Background(), Config{Root: src, Out: out, Source: fsOptions(portable), PartSize: 1})
		if err != nil {
			t.Fatal(err)
		}
		if sum.Rows != 7 || fsStats(sum).Skipped != 1 {
			t.Fatalf("portable=%v: rows %d skipped %d", portable, sum.Rows, fsStats(sum).Skipped)
		}
	}
}

func TestCancellationLeavesNothing(t *testing.T) {
	src := t.TempDir()
	for i := range 20000 {
		mk(t, src, "d"+strconv.Itoa(i%200)+"/f"+strconv.Itoa(i))
	}
	out := filepath.Join(t.TempDir(), "ds")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { // cancel mid-walk: once the first part is being written
		for {
			if _, err := os.Stat(dataset.TempPath(out, 0)); err == nil {
				cancel()
				return
			}
			if ctx.Err() != nil {
				return
			}
			time.Sleep(50 * time.Microsecond)
		}
	}()
	sum, err := Run(ctx, Config{Root: src, Out: out, PartSize: 1, Workers: 1})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if sum.Rows >= 20200 {
		t.Fatalf("walk finished (%d rows) before cancellation took effect", sum.Rows)
	}
	noScratch(t, out)
}

// A sink failure during the walk stops it and leaves nothing published.
func TestEncoderFailureLeavesNothing(t *testing.T) {
	src := t.TempDir()
	mk(t, src, "a", "b", "c")
	out := filepath.Join(t.TempDir(), "ds")
	if err := dataset.Prepare(out, src, "swath-local/v0", false); err != nil {
		t.Fatal(err)
	}
	// Make the part path unwritable: a directory where part 0's file goes.
	if err := os.MkdirAll(filepath.Join(dataset.TempPath(out, 0), "x"), 0o755); err != nil {
		t.Fatal(err)
	}
	_, err := Run(context.Background(), Config{Root: src, Out: out, Replace: true})
	if err == nil {
		t.Fatal("run succeeded")
	}
	if _, err := os.Stat(filepath.Join(out, dataset.SuccessName)); !os.IsNotExist(err) {
		t.Fatal("_SUCCESS present")
	}
}

func TestCountOnly(t *testing.T) {
	src := t.TempDir()
	mk(t, src, "a/1", "b")
	sum, err := Run(context.Background(), Config{Root: src})
	if err != nil {
		t.Fatal(err)
	}
	if sum.Rows != 3 || fsStats(sum).Files != 2 || fsStats(sum).Dirs != 1 {
		t.Fatalf("%+v", sum)
	}
}
