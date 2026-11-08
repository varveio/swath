// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"bytes"
	"context"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/source/keyrange"
	"github.com/varveio/swath/go/internal/source/s3/s3test"
	"github.com/varveio/swath/go/internal/source/scalefake"
	"github.com/varveio/swath/go/internal/textout"
)

// TestTextFormattersShareTheSpillingBudget: four formatters on one 128 KiB
// row budget the order stage shares with them, held behind a slow first
// range so the stage spills. The file is byte for byte the one-formatter
// run's; a write failure mid-stream is what the run reports, and a
// cancellation with spill live is the cancellation; after each, the
// destination is as it was and nothing is left beside it.
func TestTextFormattersShareTheSpillingBudget(t *testing.T) {
	s3test.NoCredentials(t)
	t.Setenv("TMPDIR", t.TempDir())
	run := func(ctx context.Context, out string, encoders int) (Report, error) {
		ks, err := scalefake.New(scratchShape)
		if err != nil {
			t.Fatal(err)
		}
		srv := httptest.NewServer(slowFirst(scalefake.NewServer(ks, scalefake.Options{}), ks.Key(1000), 300*time.Millisecond, 0))
		defer srv.Close()
		return Run(ctx, Config{
			Source:  SourceConfig{URI: "s3://bkt", Options: &S3Config{Endpoint: srv.URL, PathStyle: true, Policy: keyrange.Policy{}}},
			Output:  OutputConfig{Text: &textout.Config{Path: out, Format: textout.JSONL}},
			Attempt: &Attempt{ID: "0123456789abcdef0123456789abcdef"},
			Limits:  Limits{Workers: 16, Encoders: encoders, RowBudget: minRowBudget}})
	}
	dir := t.TempDir()
	one := filepath.Join(dir, "one.jsonl")
	if _, err := run(context.Background(), one, 1); err != nil {
		t.Fatal(err)
	}
	four := filepath.Join(dir, "four.jsonl")
	sum, err := run(context.Background(), four, 4)
	if err != nil {
		t.Fatal(err)
	}
	if sp := sum.Resources.Spill; sp == nil || sp.Rows == 0 || sum.Output.Text.Formatters != 4 {
		t.Fatalf("spill %+v, %d formatters: the test proves nothing", sp, sum.Output.Text.Formatters)
	}
	a, err := os.ReadFile(one)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(four)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) || sum.Output.Text.Rows != int64(scratchShape.Keys) {
		t.Fatalf("four formatters wrote %d bytes (%d rows), one %d", len(b), sum.Output.Text.Rows, len(a))
	}

	failed := filepath.Join(dir, "failed.jsonl")
	if err := os.WriteFile(failed, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	var writes atomic.Int64
	restore := faults.SetIO(func(op, name string, _ []byte) (int, error) {
		if op == "write" && strings.HasPrefix(filepath.Base(name), ".failed.jsonl.") && writes.Add(1) == 3 {
			return 0, syscall.EIO
		}
		return 0, nil
	})
	_, err = run(context.Background(), failed, 4)
	restore()
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("err %v, want the write's EIO", err)
	}
	cancelled := filepath.Join(dir, "cancelled.jsonl")
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer cancel()
		for range 10_000 {
			if m, _ := filepath.Glob(filepath.Join(dir, ".cancelled.jsonl.swath-*", "spill", "spill-*", "*")); len(m) > 0 {
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	if _, err := run(ctx, cancelled, 4); !errors.Is(err, context.Canceled) {
		t.Fatalf("err %v, want the cancellation", err)
	}
	if got := entries(t, dir); !slices.Equal(got, []string{"failed.jsonl", "four.jsonl", "one.jsonl"}) {
		t.Fatalf("the output directory holds %v", got)
	}
	if got, _ := os.ReadFile(failed); string(got) != "old" {
		t.Fatalf("destination replaced on failure: %q", got)
	}
}
