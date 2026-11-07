// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/order"
	"github.com/varveio/swath/go/internal/source/localfs"
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
	if tmps, _ := filepath.Glob(filepath.Join(out, dataset.Workspace, dataset.PartsDir, "*")); len(tmps) > 0 {
		t.Fatalf("unpublished parts left: %v", tmps)
	}
	if dataset.Committed(out) {
		t.Fatal("manifest present")
	}
}

func TestEndToEnd(t *testing.T) {
	src := t.TempDir()
	mk(t, src, "a/1", "a/2", "b", "z/y/x")
	out := filepath.Join(t.TempDir(), "ds")
	sum, err := Run(context.Background(), Config{Source: SourceConfig{URI: src}, Output: OutputConfig{Dir: out}})
	if err != nil {
		t.Fatal(err)
	}
	if sum.Output.RowsListed != 7 || sum.Output.PartsWritten != 1 {
		t.Fatalf("rows %d parts %d", sum.Output.RowsListed, sum.Output.PartsWritten)
	}
	for _, f := range []string{dataset.ManifestName, dataset.ReportName, "data/part-00000.parquet"} {
		if _, err := os.Stat(filepath.Join(out, f)); err != nil {
			t.Fatal(err)
		}
	}
	for _, f := range []string{"_SUCCESS", "symlink.txt"} {
		if _, err := os.Stat(filepath.Join(out, f)); !os.IsNotExist(err) {
			t.Fatalf("%s written: %v", f, err)
		}
	}
	var m dataset.Manifest
	readJSON(t, filepath.Join(out, dataset.ManifestName), &m)
	if m.Format != dataset.Format || m.FormatVersion != dataset.FormatVersion || m.Source.Kind != "local-fs" || m.Scope.Kind != "whole" ||
		!m.Coverage.Complete || m.Coverage.SkippedEntries != 0 || len(m.Parts) != 1 || m.Parts[0].Rows != 7 || m.Parts[0].Path != "data/part-00000.parquet" {
		t.Fatalf("manifest %+v", m)
	}
}

// Keys are bytes: a name that is not UTF-8 is listed, and the manifest
// carries its exact bytes (base64), not a replacement character.
func TestKeysAreBytes(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("NTFS names are UTF-16")
	}
	src := t.TempDir()
	mk(t, src, "a", "z\xff")
	out := filepath.Join(t.TempDir(), "ds")
	if _, err := Run(context.Background(), Config{Source: SourceConfig{URI: src}, Output: OutputConfig{Dir: out}}); err != nil {
		t.Fatal(err)
	}
	var m dataset.Manifest
	readJSON(t, filepath.Join(out, dataset.ManifestName), &m)
	if len(m.Parts) != 1 || m.Parts[0].MinKey != "a" || m.Parts[0].MaxKey != "z\xff" {
		t.Fatalf("parts %+v", m.Parts)
	}
	raw, _ := os.ReadFile(filepath.Join(out, dataset.ManifestName))
	if !strings.Contains(string(raw), `"max_key_base64": "ev8="`) {
		t.Fatalf("manifest bound not base64: %s", raw)
	}
}

// An output inside the root is skipped by identity, for both engines, even
// while its parts are being written during the walk.
func TestOutputInsideRootIsSkipped(t *testing.T) {
	for _, portable := range []bool{false, true} {
		src := t.TempDir()
		mk(t, src, "a/1", "a/2", "b", "z/y/x")
		out := filepath.Join(src, "a", "out")
		sum, err := Run(context.Background(), Config{Source: SourceConfig{URI: src, Options: fsOptions(portable)}, Output: OutputConfig{Dir: out}, Limits: Limits{PartSize: 1}})
		if err != nil {
			t.Fatal(err)
		}
		if sum.Output.RowsListed != 7 || fsStats(sum).Skipped != 1 {
			t.Fatalf("portable=%v: rows %d skipped %d", portable, sum.Output.RowsListed, fsStats(sum).Skipped)
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
	sum, err := Run(ctx, Config{Source: SourceConfig{URI: src}, Output: OutputConfig{Dir: out}, Limits: Limits{PartSize: 1, Workers: 1}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if sum.Output.RowsListed >= 20200 {
		t.Fatalf("walk finished (%d rows) before cancellation took effect", sum.Output.RowsListed)
	}
	noScratch(t, out)
}

// A sink failure during the walk stops it and leaves nothing published.
func TestEncoderFailureLeavesNothing(t *testing.T) {
	src := t.TempDir()
	mk(t, src, "a", "b", "c")
	out := filepath.Join(t.TempDir(), "ds")
	if err := dataset.Prepare(out, src, localfs.Schema.ID, false); err != nil {
		t.Fatal(err)
	}
	// Make the part path unwritable: a directory where part 0's file goes.
	if err := os.MkdirAll(filepath.Join(dataset.TempPath(out, 0), "x"), 0o755); err != nil {
		t.Fatal(err)
	}
	_, err := Run(context.Background(), Config{Source: SourceConfig{URI: src}, Output: OutputConfig{Dir: out, Replace: true}})
	if err == nil {
		t.Fatal("run succeeded")
	}
	if dataset.Committed(out) {
		t.Fatal("manifest present")
	}
}

func TestCountOnly(t *testing.T) {
	src := t.TempDir()
	mk(t, src, "a/1", "b")
	sum, err := Run(context.Background(), Config{Source: SourceConfig{URI: src}})
	if err != nil {
		t.Fatal(err)
	}
	if sum.Output.RowsListed != 3 || fsStats(sum).Files != 2 || fsStats(sum).Dirs != 1 {
		t.Fatalf("%+v", sum)
	}
}

// TestConfigRejectsInvalid: invalid values and combinations are a
// *ConfigError before anything is opened.
func TestConfigRejectsInvalid(t *testing.T) {
	src := t.TempDir()
	out := filepath.Join(t.TempDir(), "ds")
	for name, cfg := range map[string]Config{
		"no source":         {},
		"negative workers":  {Source: SourceConfig{URI: src}, Limits: Limits{Workers: -1}},
		"tiny row budget":   {Source: SourceConfig{URI: src}, Limits: Limits{RowBudget: 1}},
		"resume no output":  {Source: SourceConfig{URI: src}, Output: OutputConfig{Resume: true}},
		"resume replace":    {Source: SourceConfig{URI: src}, Output: OutputConfig{Dir: out, Resume: true, Replace: true}},
		"resume no journal": {Source: SourceConfig{URI: src}, Output: OutputConfig{Dir: out, Resume: true, NoJournal: true}},
		"replace no output": {Source: SourceConfig{URI: src}, Output: OutputConfig{Replace: true}},
	} {
		var ce *ConfigError
		if _, err := Run(context.Background(), cfg); !errors.As(err, &ce) {
			t.Errorf("%s: %v, want a *ConfigError", name, err)
		}
	}
	if _, err := os.Stat(out); !os.IsNotExist(err) {
		t.Fatal("an invalid configuration touched the output")
	}
}

// TestResourceAdmission: encoders are admitted against a writer
// allowance equal to the row budget and the descriptor limit, never below one; the
// default spill reserve is derived from the encoders' parts, and the
// default spill cap is half the free space after it; a negative one is none.
func TestResourceAdmission(t *testing.T) {
	for _, tc := range []struct {
		budget  int64
		workers int
		fds     int64
		want    int
	}{
		{128 << 20, 4, 0, 5},                      // 128 MiB / 24 MiB
		{512 << 20, 4, 0, 21},                     // 512 MiB / 24 MiB
		{512 << 20, 64, 180, 180 - 16 - 128 - 32}, // 4: the descriptors (16 spill: 8 drainer readers, 8 read-ahead reads)
		{512 << 20, 64, 100, 1},                   // the floor
		{minRowBudget, 4, 0, 1},                   // the floor
	} {
		if got := admitEncoders(tc.budget, tc.workers, tc.fds); got != tc.want {
			t.Errorf("admitEncoders(%d, %d, %d) = %d, want %d", tc.budget, tc.workers, tc.fds, got, tc.want)
		}
	}
	// Once listing has ended, each worker's page (1 MiB) joins the
	// allowance: 128 MiB at 64 workers carries 8, at 256 workers 16.
	for _, tc := range []struct {
		workers int
		want    int
	}{{64, 8}, {256, 16}, {4, 5}} {
		if got := admitAfterListing(128<<20, tc.workers, 0); got != tc.want {
			t.Errorf("admitAfterListing(128 MiB, %d) = %d, want %d", tc.workers, got, tc.want)
		}
	}
	// The reserve: one part per encoder, at least 1 GiB (the defaults' 5
	// encoders of 64 MiB parts take the floor); negative is none.
	if a, b := spillReserve(5, DefaultPartSize), spillReserve(16, 256<<20); a != 1<<30 || b != 4<<30 {
		t.Errorf("derived reserves %d, %d", a, b)
	}
	// A product past int64 saturates rather than wrapping.
	if got := spillReserve(1<<20, math.MaxInt64/(1<<19)); got != math.MaxInt64 {
		t.Errorf("an overflowing reserve derived as %d", got)
	}
	for in, want := range map[int64]int64{0: 1 << 30, -1: 0, 7: 7} {
		c := Config{Source: SourceConfig{URI: "s3://b"}, Limits: Limits{Encoders: 2, Spill: SpillLimits{Reserve: in}}}
		if err := c.resolve(); err != nil || c.Limits.Spill.Reserve != want {
			t.Errorf("reserve %d resolved to %d (%v), want %d", in, c.Limits.Spill.Reserve, err, want)
		}
	}
	dir := t.TempDir()
	if got := spillMax(SpillLimits{Max: 5}, dir); got != 5 {
		t.Errorf("explicit cap: %d", got)
	}
	if got := spillMax(SpillLimits{Max: -1}, dir); got != 0 {
		t.Errorf("no cap: %d", got)
	}
	free := order.FreeBytes(dir)
	if got := spillMax(SpillLimits{Reserve: 1 << 20}, filepath.Join(dir, "not", "yet")); free != order.Unlimited && got != (free-(1<<20))/2 && got > free/2 {
		t.Errorf("derived cap %d, free %d", got, free)
	}
}
