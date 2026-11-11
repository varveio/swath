// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/source/keyrange"
	"github.com/varveio/swath/go/internal/source/s3/s3test"
	"github.com/varveio/swath/go/internal/source/scalefake"
	"github.com/varveio/swath/go/internal/textout"
)

// scratchShape is small enough to list in a second and large enough that
// a 128 KiB row budget held behind a slow first range spills.
var scratchShape = scalefake.Shape{Keys: 40_000, Fanout: []int{20}, KeyLen: 60, Seed: 3}

// textRun lists scratchShape (the first 1,000 keys' requests delayed by
// slow) to the text file out under ctx.
func textRun(ctx context.Context, t *testing.T, out string, slow time.Duration) (Report, error) {
	t.Helper()
	ks, err := scalefake.New(scratchShape)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(slowFirst(scalefake.NewServer(ks, scalefake.Options{}), ks.Key(1000), slow, 0))
	t.Cleanup(srv.Close)
	return Run(ctx, Config{
		Source:  SourceConfig{URI: "s3://bkt", Options: &S3Config{Endpoint: srv.URL, PathStyle: true, Anonymous: true, Policy: keyrange.Policy{}}},
		Output:  OutputConfig{Text: &textout.Config{Path: out, Format: textout.TSV, Compression: textout.Zstd}},
		Attempt: &Attempt{ID: "0123456789abcdef0123456789abcdef"},
		Limits:  Limits{Workers: 16, RowBudget: minRowBudget}})
}

// entries is the names in dir.
func entries(t *testing.T, dir string) []string {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range ents {
		names = append(names, e.Name())
	}
	return names
}

// TestTextSpillBesideOutput: a named text output spills in a directory of
// its own beside the file (named for it and the attempt), on its
// filesystem, never under TMPDIR; the report says where; the directory is
// gone once the run has succeeded, and the output's directory holds the
// file alone.
func TestTextSpillBesideOutput(t *testing.T) {
	s3test.NoCredentials(t)
	withMemory(t, memoryLimits{physical: 2 << 20}) // an encoder allowance of the minimum, 128 KiB
	tmp, dir := t.TempDir(), t.TempDir()
	t.Setenv("TMPDIR", tmp)
	out := filepath.Join(dir, "keys.tsv.zst")
	sum, err := textRun(context.Background(), t, out, 500*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	sp := sum.Resources.Spill
	if sp == nil || sp.Rows == 0 {
		t.Fatalf("nothing spilled: the test proves nothing: %+v", sp)
	}
	if want := filepath.Join(dir, ".keys.tsv.zst.swath-01234567-"); !strings.HasPrefix(sp.Dir, want) || filepath.Base(sp.Dir) != "spill" {
		t.Fatalf("spill dir %s, want %s*/spill", sp.Dir, want)
	}
	if got := entries(t, dir); len(got) != 1 || got[0] != "keys.tsv.zst" {
		t.Fatalf("output directory holds %v, want the file alone", got)
	}
	if got := entries(t, tmp); len(got) != 0 {
		t.Fatalf("TMPDIR holds %v", got)
	}
	c, why := sum.Output.Text.Compressor, JobsAllowance
	if runtime.GOMAXPROCS(0) == 1 {
		why = JobsCores // both bind; the cores are named first
	}
	if c == nil || c.Jobs != 1 || c.JobsReason != why || c.BytesOut != sum.Output.Text.Bytes || c.BytesIn <= c.BytesOut {
		t.Fatalf("compressor %+v (bytes %d): a 128 KiB allowance admits one job", c, sum.Output.Text.Bytes)
	}
	if sum.Output.Text.SinkWaitNs <= 0 {
		t.Fatalf("no sink wait: %+v", sum.Output.Text)
	}
}

// TestTextScratchGoneAfterFailure: a run cancelled while its spill is
// live removes its scratch directory and its temporary, and leaves the
// destination as it was.
func TestTextScratchGoneAfterFailure(t *testing.T) {
	s3test.NoCredentials(t)
	dir := t.TempDir()
	t.Setenv("TMPDIR", t.TempDir())
	out := filepath.Join(dir, "keys.tsv.zst")
	if err := os.WriteFile(out, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	seen := make(chan bool, 1)
	go func() { // cancel once the scratch directory holds spill files
		defer cancel()
		for range 10_000 {
			if m, _ := filepath.Glob(filepath.Join(dir, ".keys.tsv.zst.swath-*", "spill", "spill-*", "*")); len(m) > 0 {
				seen <- true
				return
			}
			time.Sleep(time.Millisecond)
		}
		seen <- false
	}()
	_, err := textRun(ctx, t, out, 2*time.Second)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err %v, want the cancellation", err)
	}
	if !<-seen {
		t.Fatal("no spill file appeared before the cancellation: the test proves nothing")
	}
	if got := entries(t, dir); len(got) != 1 || got[0] != "keys.tsv.zst" {
		t.Fatalf("output directory holds %v after a failure", got)
	}
	if b, _ := os.ReadFile(out); string(b) != "old" {
		t.Fatalf("destination replaced on failure: %q", b)
	}
}

// TestTextScratchPerAttempt: two attempts at once, on sibling outputs and
// on the same one, each own a scratch directory of their own; each run
// succeeds and leaves nothing beside its file.
func TestTextScratchPerAttempt(t *testing.T) {
	s3test.NoCredentials(t)
	dir := t.TempDir()
	att := &Attempt{ID: "0123456789abcdef0123456789abcdef"}
	a, err := textScratch(filepath.Join(dir, "a.tsv"), att)
	if err != nil {
		t.Fatal(err)
	}
	b, err := textScratch(filepath.Join(dir, "a.tsv"), att)
	if err != nil {
		t.Fatal(err)
	}
	if a == b || filepath.Dir(a) != dir {
		t.Fatalf("one output's attempts share %s and %s", a, b)
	}
	os.Remove(a)
	os.Remove(b)
	var wg sync.WaitGroup
	sums := make([]Report, 2)
	errs := make([]error, 2)
	for i, name := range []string{"a.tsv.zst", "b.tsv.zst"} {
		wg.Go(func() {
			sums[i], errs[i] = textRun(context.Background(), t, filepath.Join(dir, name), 300*time.Millisecond)
		})
	}
	wg.Wait()
	for i := range sums {
		if errs[i] != nil {
			t.Fatal(errs[i])
		}
		if sums[i].Output.Text.Rows != scratchShape.Keys {
			t.Fatalf("run %d wrote %d rows", i, sums[i].Output.Text.Rows)
		}
	}
	if sums[0].Resources.Spill.Dir == sums[1].Resources.Spill.Dir {
		t.Fatalf("two attempts shared %s", sums[0].Resources.Spill.Dir)
	}
	if got := entries(t, dir); len(got) != 2 {
		t.Fatalf("output directory holds %v", got)
	}
}
