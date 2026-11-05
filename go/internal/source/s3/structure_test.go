// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/order"
	"github.com/varveio/swath/go/internal/source/s3/client"
)

func TestStructurePrefixes(t *testing.T) {
	c := "tiff/1950census/A/A-01/f.tif"
	for _, tc := range []struct {
		hi    string
		floor int
		want  []string
	}{
		{"zip/", 0, []string{"", "tiff/"}},
		{"tiff/1950census/B/", 0, []string{"tiff/1950census/", "tiff/1950census/A/"}},
		{"zip/", len("tiff/1950"), []string{"tiff/1950", "tiff/1950census/"}},
		{"tiff/1950census/A/A-01/g", 0, []string{"tiff/1950census/A/A-01/"}},
	} {
		if got := structurePrefixes(c, tc.hi, tc.floor); !slices.Equal(got, tc.want) {
			t.Errorf("hi %q floor %d: %q, want %q", tc.hi, tc.floor, got, tc.want)
		}
	}
	s := structure{bounds: []string{"a/", "b/", "c/", "d/", "e/"}}
	if m, ok := boundary(s, "a/x", "e/"); !ok || m != "c/" {
		t.Fatalf("median of b/ c/ d/: %q %v", m, ok)
	}
	s.capped = true
	if m, _ := boundary(s, "a/x", "e/"); m != "d/" {
		t.Fatalf("capped takes the furthest: %q", m)
	}
	if _, ok := boundary(s, "d/x", "e/"); ok {
		t.Fatal("no boundary inside (d/x, e/)")
	}
	if got := within(s.bounds, "b/", "d/"); !slices.Equal(got, []string{"c/"}) {
		t.Fatalf("within is exclusive: %q", got)
	}
}

// structureGate holds structure probes (delimiter pages of 32) until
// released; pages pass.
type structureGate struct {
	*sorted
	release chan struct{}
}

func (g *structureGate) ListObjectsV2(ctx context.Context, in *client.ListInput) (*client.Page, error) {
	if in.Delimiter != "" && in.MaxKeys == structureKeys {
		select {
		case <-g.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return g.sorted.ListObjectsV2(ctx, in)
}

// TestStructureNeverBlocksASplit: with every structure probe held, the
// listing still completes, splits, and counts its decisions as late.
func TestStructureNeverBlocksASplit(t *testing.T) {
	keys := seedShapes()["deep census"]
	g := &structureGate{sorted: newSorted(keys, false, 1), release: make(chan struct{})}
	g.latency = time.Millisecond
	p, err := New(context.Background(), Options{Bucket: "b", Client: g, Workers: 16, Structure: true})
	if err != nil {
		t.Fatal(err)
	}
	c := &collect{}
	res := make(chan error, 1)
	go func() {
		_, err := order.Run[Range, Object](context.Background(), p, c, &Schema, order.Options{Workers: 16, Budget: 1 << 28})
		res <- err
	}()
	select {
	case err := <-res:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the listing waited on a structure probe")
	}
	close(g.release)
	st := p.Stats()
	if len(c.rows) != len(keys) || st.Get(cSplitAcceptedBounded)+st.Get(cSplitAcceptedFrontier) == 0 || st.Get(cStructureRequests) == 0 {
		t.Fatalf("rows %d of %d, splits %d, structure probes %d", len(c.rows), len(keys),
			st.Get(cSplitAcceptedBounded)+st.Get(cSplitAcceptedFrontier), st.Get(cStructureRequests))
	}
}

// TestStructurePivotsOnFolders: on a deep shared-prefix hierarchy with a
// seed, structure probes supply folder pivots and the empty children fall;
// the output is the keyspace either way.
func TestStructurePivotsOnFolders(t *testing.T) {
	var keys []string
	for i := range 20000 {
		keys = append(keys, fmt.Sprintf("tiff/1950census/43290879-S%02d/43290879-S%02d-%03d/%05d.tif", i%9, i%9, i/9%60, i))
	}
	want := slices.Sorted(slices.Values(keys))
	var empty [2]int64
	for i, on := range []bool{false, true} {
		f := newSorted(keys, false, 1)
		f.latency = 2 * time.Millisecond
		got, st, leaves := runBudget(t, f, Options{Structure: on}, 16, 1<<20)
		if !slices.Equal(got, want) {
			t.Fatalf("structure=%v: %d keys, want %d", on, len(got), len(want))
		}
		checkTiling(t, "structure", leaves)
		empty[i] = st.Get(cChildEmptyLower) + st.Get(cChildEmptyUpper)
		t.Logf("structure=%v splits=%d empty=%d pivot_structure=%d probes=%d late=%d requests=%d",
			on, st.Get(cSplitAcceptedBounded)+st.Get(cSplitAcceptedFrontier), empty[i], st.Get(cSplitPivotStructure),
			st.Get(cStructureRequests), st.Get(cStructureLate), st.Get(cRequests))
		if on && st.Get(cSplitPivotStructure) == 0 {
			t.Fatal("no split pivoted on a folder")
		}
	}
}
