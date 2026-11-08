// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package order_test

import (
	"fmt"
	"github.com/varveio/swath/go/internal/source/gcs"
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

// Deterministic transitions complement the random corpus: false is a
// present boolean, "null" is a present ID, and an older time is a negative
// delta. A frame reset in the middle of a group must preserve all of them.
func TestVersionTransitionFrames(t *testing.T) {
	rows := []s3.VersionRow{
		{Key: "k", Data: s3.Version{Object: s3.Object{Size: 0, Mtime: 100, ETag: "e", StorageClass: "STANDARD", ChecksumAlgorithm: "CRC32C", ChecksumType: "FULL_OBJECT", OwnerID: "owner"}, VersionID: "null", IsLatest: true}},
		{Key: "k", Data: s3.Version{Object: s3.Object{Mtime: 90}, VersionID: "null", DeleteMarker: true}},
		{Key: "k", Data: s3.Version{Object: s3.Object{Size: 9, Mtime: 80, ETag: "old"}, VersionID: "old"}},
	}
	for _, format := range []struct{ plain, compress bool }{{true, false}, {false, false}, {true, true}, {false, true}} {
		t.Run(fmt.Sprint(format), func(t *testing.T) {
			for _, frames := range [][][]s3.VersionRow{{rows}, {rows[:2], rows[2:]}, {rows[:1], rows[1:2], rows[2:]}} {
				got, written, err := order.SpillRoundTripBytes(&s3.VersionsSchema, t.TempDir(), format.plain, format.compress, frames, true)
				if err != nil || written <= 0 || !reflect.DeepEqual(got, frames) {
					t.Fatalf("bytes=%d err=%v got=%+v", written, err, got)
				}
			}
		})
	}
	if got := s3.VersionsSchema.Detach(rows); !reflect.DeepEqual(got, rows) {
		t.Fatal(got)
	}
	for i := range rows {
		p := &rows[i].Data
		wantHeap := int64(len(p.VersionID) + len(p.ETag) + len(p.StorageClass) + len(p.ChecksumAlgorithm) + len(p.ChecksumType) + len(p.OwnerID))
		if s3.VersionsSchema.Heap(p) != wantHeap || s3.VersionsSchema.Bytes(p) != 18+wantHeap {
			t.Fatalf("row%d bytes/heap", i)
		}
	}
}

func TestGenerationTransitionFrames(t *testing.T) {
	rows := []gcs.GenerationRow{
		{Key: "k", Data: gcs.Generation{Object: gcs.Object{Size: 1, Mtime: 100, Generation: 1, ETag: "e", MD5: "m", StorageClass: "STANDARD"}, TimeDeleted: 200}},
		{Key: "k", Data: gcs.Generation{Object: gcs.Object{Size: 0, Mtime: 90, Generation: 2, CRC32C: "c"}, TimeDeleted: 180}},
		{Key: "k", Data: gcs.Generation{Object: gcs.Object{Size: 3, Mtime: 80, Generation: 3}}},
	}
	for _, format := range []struct{ plain, compress bool }{{true, false}, {false, false}, {true, true}, {false, true}} {
		for _, frames := range [][][]gcs.GenerationRow{{rows}, {rows[:2], rows[2:]}, {rows[:1], rows[1:2], rows[2:]}} {
			got, written, err := order.SpillRoundTripBytes(&gcs.GenerationSchema, t.TempDir(), format.plain, format.compress, frames, true)
			if err != nil || written <= 0 || !reflect.DeepEqual(got, frames) {
				t.Fatalf("format=%+v bytes=%d err=%v got=%+v", format, written, err, got)
			}
		}
	}
}
