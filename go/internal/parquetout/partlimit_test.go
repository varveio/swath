// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package parquetout

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/varveio/swath/go/internal/dataset"
)

// TestRouterRefusesPartPastLimit: part names have five digits, so the
// router refuses to open part 100,000 (dataset.ErrPartLimit) once parts
// 0 to 99,999 exist, after publishing every earlier part: the journal's
// last record is 99,999. A resume of 99,998 parts stands in for the run
// before, and a one-row cap closes each new part (owner decision,
// 2026-11-06: before, ordinal 100,000 was written as part-100000, a name
// that breaks the five-digit contract).
func TestRouterRefusesPartPastLimit(t *testing.T) {
	const resumed = dataset.MaxParts - 2
	parts := make([]dataset.Part, resumed)
	parts[resumed-1] = dataset.Part{PartInfo: dataset.PartInfo{Rows: 1, MinKey: "a", MaxKey: "a"}}
	var mu sync.Mutex
	var published []int
	opt := Options{Out: t.TempDir(), PartSize: 1 << 20, MaxRows: 1, Encoders: 2,
		Resume: &Resume{Parts: parts, Limit: 1 << 20, Rows: resumed},
		Publish: func(ordinal int, p dataset.Part, limit, rows int64, final bool) (dataset.Part, error) {
			mu.Lock()
			defer mu.Unlock()
			published = append(published, ordinal)
			return p, nil
		}}
	r, err := NewRouter(opt, &testSchema)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Abort()
	var werr error
	for _, k := range []string{"b", "c", "d"} {
		if werr = r.Write(context.Background(), [][]row{{{Key: k, Data: tr{Kind: "FILE"}}}}); werr != nil {
			break
		}
	}
	if !errors.Is(werr, dataset.ErrPartLimit) {
		t.Fatalf("write error %v, want ErrPartLimit", werr)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(published) != 2 || published[0] != dataset.MaxParts-2 || published[1] != dataset.MaxParts-1 {
		t.Fatalf("published ordinals %v, want %d and %d", published, dataset.MaxParts-2, dataset.MaxParts-1)
	}
}
