// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/varveio/swath/internal/dataset/datasettest"
	"github.com/varveio/swath/internal/source/gcs/gcstest"
	"github.com/varveio/swath/internal/source/s3/s3test"
)

// TestSpillFormatsIdentical: a sorted dataset listed with spill forced
// (the minimum row budget, the first prefix's requests held up so every
// other range lists ahead of the frontier) is byte-identical to one listed
// without spill, in every frame format: delta+zstd (the default), delta,
// plain and plain+zstd. The report names the format and the logical bytes
// the frames stood for.
func TestSpillFormatsIdentical(t *testing.T) {
	s3test.NoCredentials(t)
	b := s3test.Backend(t, gcstest.Keys(20000))
	url := s3test.Serve(t, slowFirst(s3test.Handler(b), "d01", 150*time.Millisecond, time.Millisecond))
	build := func(spill SpillLimits) (string, *Spill) {
		out := filepath.Join(t.TempDir(), "ds")
		sum, err := Run(context.Background(), Config{Source: SourceConfig{URI: "s3://bkt", Options: &S3Config{Endpoint: url, PathStyle: true, Anonymous: true}},
			Output: OutputConfig{Dir: out, NoJournal: datasettest.NoJournal()}, Limits: Limits{PartSize: 64 << 10, Workers: 16, RowBudget: minRowBudget, Spill: spill, PartInterval: -1}})
		if err != nil {
			t.Fatal(err)
		}
		return out, sum.Resources.Spill
	}
	ref, _ := build(SpillLimits{Off: true})
	for _, c := range []struct {
		spill  SpillLimits
		format string
	}{{SpillLimits{}, "delta+zstd"}, {SpillLimits{Uncompressed: true}, "delta"}, {SpillLimits{Plain: true, Uncompressed: true}, "plain"}, {SpillLimits{Plain: true}, "plain+zstd"}} {
		got, sp := build(c.spill)
		if sp == nil || sp.Rows == 0 || sp.Format != c.format || sp.LogicalBytes <= 0 || sp.DecodeRefused != 0 {
			t.Fatalf("%s: spill %+v", c.format, sp)
		}
		t.Logf("%s: %d rows spilled, %d bytes for %d logical (%.1f B/row)", c.format, sp.Rows, sp.Bytes, sp.LogicalBytes, float64(sp.Bytes)/float64(sp.Rows))
		sameDataset(t, c.format, ref, got)
	}
}
