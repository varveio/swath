// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package parquetout

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/varveio/swath/internal/dataset"
)

// TestRouterRefusesOverlapBeforePublish: parts each ascending on their own
// whose adjacent keys repeat or descend across a forced boundary (one row
// per part) are refused before Publish sees the second one, on a fresh run
// and at a resumed seam; nothing overlapping is ever handed to the
// journal. Before the check, Publish recorded the overlapping part and only
// Close's gate refused the dataset.
func TestRouterRefusesOverlapBeforePublish(t *testing.T) {
	for _, tc := range []struct {
		name    string
		resume  *Resume
		keys    []string
		publish int // parts Publish may see
	}{
		{"repeat", nil, []string{"a", "b", "b", "c"}, 2},
		{"descent", nil, []string{"a", "c", "b", "d"}, 2},
		{"resumed seam repeat", &Resume{Parts: []dataset.Part{{PartInfo: dataset.PartInfo{Rows: 1, MinKey: "k", MaxKey: "m"}}}, Limit: 1, Rows: 1}, []string{"m", "n"}, 0},
		{"resumed seam descent", &Resume{Parts: []dataset.Part{{PartInfo: dataset.PartInfo{Rows: 1, MinKey: "k", MaxKey: "m"}}}, Limit: 1, Rows: 1}, []string{"l", "n"}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var seen []dataset.Part
			opt := Options{Out: t.TempDir(), PartSize: 1, Encoders: 2, Resume: tc.resume,
				Publish: func(ordinal int, p dataset.Part, limit, rows int64, final bool) (dataset.Part, error) {
					mu.Lock()
					defer mu.Unlock()
					seen = append(seen, p)
					return p, nil
				}}
			r, err := NewRouter(opt, &testSchema)
			if err != nil {
				t.Fatal(err)
			}
			var rows []row
			for _, k := range tc.keys {
				rows = append(rows, row{Key: k, Data: tr{Kind: "FILE"}})
			}
			// One row per Write: each row is its own part at PartSize 1.
			var werr error
			for i := range rows {
				if werr = r.Write(context.Background(), [][]row{rows[i : i+1]}); werr != nil {
					break
				}
			}
			_, err = r.Close()
			if werr != nil && err == nil {
				err = werr
			}
			var ie *dataset.InvariantError
			if !errors.As(err, &ie) {
				t.Fatalf("err = %v, want an invariant error", err)
			}
			if len(seen) > tc.publish {
				t.Fatalf("Publish saw %d parts, at most %d allowed (the overlapping part reached the journal): %+v", len(seen), tc.publish, seen)
			}
			for i := 1; i < len(seen); i++ {
				if !(seen[i-1].MaxKey < seen[i].MinKey) {
					t.Fatalf("published parts %d and %d overlap", i-1, i)
				}
			}
		})
	}
}
