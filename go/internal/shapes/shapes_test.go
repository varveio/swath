// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package shapes_test

import (
	"bytes"
	"context"
	"math"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/shapes"
	"github.com/varveio/swath/go/internal/source/s3/s3test"
	"github.com/varveio/swath/go/internal/source/scalefake"
)

// small is the default tier's scale: about 200 pages per shape.
const small = 200_000

// TestKeysAreDeterministic: a shape's keys are the same on every call,
// sorted and unique (Keys panics on a key over 1,024 bytes or with a
// newline), and every shape but the tiny ones scales with n.
func TestKeysAreDeterministic(t *testing.T) {
	for _, s := range shapes.All {
		a, b := s.Keys(20_000), s.Keys(20_000)
		if !slices.Equal(a, b) || !slices.IsSorted(a) || len(slices.Compact(slices.Clone(a))) != len(a) {
			t.Fatalf("%s: not deterministic, sorted and unique", s.Name)
		}
		big := len(s.Keys(40_000))
		if grew := float64(big) / float64(len(a)); shapes.Tiny(s.Name) != (grew < 1.5) {
			t.Fatalf("%s: %d keys at 20k, %d at 40k", s.Name, len(a), big)
		}
	}
	if len(shapes.All) != 21 {
		t.Fatalf("%d shapes", len(shapes.All))
	}
}

// TestShapes (synthetic, labelled: every shape at about 200,000 keys, a
// 5 ms fake, the command's engine defaults) lists each shape to Parquet
// twice, at 16 and at 5 workers. Both datasets hold exactly the shape's
// keys and their parts are byte-identical. The 16-worker run is held to
// loose bounds, set to fail on a tenfold regression and never on noise:
// at most 3 list requests per 1,000 keys, a split tree no deeper than
// 6·log2(pages)+16, and at least a quarter of the workers busy on
// average. The tiny shapes, a page or a few, are held instead to at most
// 8 requests per page plus 16 and the depth bound; the mean-active bound
// needs 100 pages or more (maxlen has 40).
//
// It skips under the race detector, which makes it about ten times slower
// (six minutes) and adds nothing: the engine, the order stage and the
// encoders it drives are race-tested by their own packages, the fake by
// scalefake's. CI runs it without -race in a step of its own.
func TestShapes(t *testing.T) {
	if testing.Short() {
		t.Skip("21 listings of up to 200,000 keys")
	}
	if raceOn {
		t.Skip("a measurement, run without -race (CI's shape step)")
	}
	s3test.NoCredentials(t)
	const workers, latency = 16, 5 * time.Millisecond
	for _, s := range shapes.All {
		t.Run(s.Name, func(t *testing.T) {
			keys := s.Keys(small)
			ks, err := scalefake.FromKeys(keys)
			if err != nil {
				t.Fatal(err)
			}
			dir := t.TempDir()
			run := func(w int, out string) shapes.Result {
				t.Helper()
				r, err := shapes.Run(context.Background(), ks, keys, shapes.Arm{Name: s.Name, Shape: s.Name, Scale: small,
					Workers: w, Fake: scalefake.Options{Latency: latency}, Out: filepath.Join(dir, out)})
				if err != nil {
					t.Fatal(err)
				}
				if r.Identical == nil || !*r.Identical || r.Rows != int64(len(keys)) {
					t.Fatalf("%d workers: %d rows of %d keys, identical %v", w, r.Rows, len(keys), r.Identical)
				}
				return r
			}
			r := run(workers, "a")
			run(5, "b")
			sameParts(t, filepath.Join(dir, "a"), filepath.Join(dir, "b"))
			pages := math.Max(1, float64(len(keys))/1000)
			depthBound := int64(6*math.Log2(pages) + 16)
			t.Logf("%s: %d keys, wall %d ms, requests/1k %.2f, depth %d (bound %d), mean active %.1f of %d, splits %d",
				s.Name, len(keys), r.WallMs, r.RequestsPer1k, r.Depth, depthBound, r.MeanActive, workers, r.Splits)
			if r.Depth > depthBound {
				t.Errorf("split.max_depth %d over %d", r.Depth, depthBound)
			}
			if shapes.Tiny(s.Name) {
				if req := r.RequestsPer1k * float64(len(keys)) / 1000; req > 8*pages+16 {
					t.Errorf("%.0f requests for %d keys", req, len(keys))
				}
				return
			}
			if r.RequestsPer1k > 3 {
				t.Errorf("%.2f requests per 1,000 keys, over 3", r.RequestsPer1k)
			}
			if pages >= 100 && r.MeanActive < workers/4 {
				t.Errorf("mean active %.1f, under a quarter of %d workers", r.MeanActive, workers)
			}
		})
	}
}

// sameParts: both datasets have the same parts, byte for byte.
func sameParts(t *testing.T, a, b string) {
	t.Helper()
	pa, _ := filepath.Glob(filepath.Join(a, "data", "*.parquet"))
	pb, _ := filepath.Glob(filepath.Join(b, "data", "*.parquet"))
	if len(pa) == 0 || len(pa) != len(pb) {
		t.Fatalf("%d parts and %d parts", len(pa), len(pb))
	}
	for _, p := range pa {
		x, err1 := os.ReadFile(p)
		y, err2 := os.ReadFile(filepath.Join(b, "data", filepath.Base(p)))
		if err1 != nil || err2 != nil || !bytes.Equal(x, y) {
			t.Fatalf("part %s differs between worker counts (%v, %v)", filepath.Base(p), err1, err2)
		}
	}
}
