// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package order_test

import (
	"math/rand/v2"
	"reflect"
	"testing"

	"github.com/varveio/swath/go/internal/order"
	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source/s3"
)

// TestVersionFramesRoundTrip: S3 versions rows survive every frame format
// exactly, with keys repeated across consecutive rows (a key group: the
// whole key is the shared prefix), groups split across frames, versions
// and delete markers interleaved, literal "null" and repeated version IDs,
// and the absent fields of delete markers.
func TestVersionFramesRoundTrip(t *testing.T) {
	for seed := range uint64(20) {
		r := rand.New(rand.NewPCG(seed, 11))
		var rows []schema.Row[s3.Version]
		var prev s3.Version
		for _, key := range sortedKeys(r, 1+r.IntN(200)) {
			for range 1 + r.IntN(6) {
				v := s3.Version{VersionID: pick(r, "null", hexOf(r, "abcdefgh"), prev.VersionID), IsLatest: r.IntN(2) == 0, DeleteMarker: r.IntN(3) == 0}
				if v.DeleteMarker {
					v.Mtime, v.OwnerID = timeOf(r, prev.Mtime), pick(r, "", prev.OwnerID)
				} else {
					v.Object = s3Object(r, &prev.Object)
				}
				rows = append(rows, schema.Row[s3.Version]{Key: key, Data: v})
				prev = v
			}
		}
		var frames [][]schema.Row[s3.Version]
		for len(rows) > 0 {
			n := 1 + r.IntN(min(len(rows), 50))
			frames, rows = append(frames, rows[:n]), rows[n:]
		}
		for _, f := range []struct{ plain, compress bool }{{false, false}, {false, true}, {true, false}, {true, true}} {
			got, err := order.SpillRoundTrip(&s3.VersionsSchema, t.TempDir(), f.plain, f.compress, frames)
			if err != nil {
				t.Fatalf("seed %d %+v: %v", seed, f, err)
			}
			if !reflect.DeepEqual(got, frames) {
				t.Fatalf("seed %d %+v: rows differ after the round trip", seed, f)
			}
		}
	}
}
