// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"errors"
	"math/rand/v2"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// TestPackerStopsAtTheArenaLimit packs keys and a text column under a
// 16-byte arena limit, standing in for the uint32 offsets' 4 GiB: the
// row that would take either arena past the limit is refused rather than
// given an offset that wraps, the read is stopped once, nothing more is
// appended, and the packed result is errPackedOverflow.
func TestPackerStopsAtTheArenaLimit(t *testing.T) {
	for _, overflow := range []string{"keys", "text"} {
		t.Run(overflow, func(t *testing.T) {
			stops := 0
			k := s3Objects.packer([]string{"etag"}, Projection{}, 4, 16)
			k.stop = func() { stops++ }
			k.Keys([][]byte{[]byte("aaaa"), []byte("bbbb")})
			k.Values(0, []parquet.Value{parquet.ByteArrayValue([]byte("eeeeeee")), parquet.ByteArrayValue([]byte("fffffff"))})
			if k.overflow {
				t.Fatal("rows within the limit overflowed")
			}
			if overflow == "keys" {
				k.Keys([][]byte{[]byte("cccc"), []byte("dddd"), []byte("gggg")})
			} else {
				k.Keys([][]byte{[]byte("cccc")})
				k.Values(0, []parquet.Value{parquet.ByteArrayValue([]byte("hhh"))})
			}
			k.Missing(0)
			k.Keys([][]byte{[]byte("z")})
			if len(k.out.keys) > 16 || k.texts[0].Len() > 16 {
				t.Fatalf("arenas grew past the limit: keys %d text %d", len(k.out.keys), k.texts[0].Len())
			}
			if !k.overflow || stops != 1 {
				t.Fatalf("overflow %v, stopped %d times; want true, once", k.overflow, stops)
			}
			if _, err := k.packed(); !errors.Is(err, errPackedOverflow) {
				t.Fatalf("packed() = %v, want errPackedOverflow", err)
			}
		})
	}
}

// TestWindowCacheServesArenaOverflowUnpacked lowers the store's arena
// limit to a few keys' bytes, so that most fills (a walk's, a read-ahead,
// a read longer than a window) overflow, and requires every read equal to
// the uncached store's: an overflowing read is served unpacked, never
// with wrapped offsets.
func TestWindowCacheServesArenaOverflowUnpacked(t *testing.T) {
	ctx := context.Background()
	for _, f := range fixtures(t) {
		t.Run(f.name, func(t *testing.T) {
			bare, _, err := Open(ctx, f.path, Options{Prefetch: Prefetch{Off: true}})
			if err != nil {
				t.Fatal(err)
			}
			defer bare.Close()
			opened, _, err := Open(ctx, f.path, Options{Prefetch: Prefetch{WindowRows: 37, MaxWindows: 5}})
			if err != nil {
				t.Fatal(err)
			}
			defer opened.Close()
			cached := opened.(*Sorted)
			rows, _ := reference(t, f.path)
			cached.arenaLimit = 3 * len(rows[len(rows)/2].key)
			r := rand.New(rand.NewPCG(3, uint64(len(f.name))))
			for range 200 {
				from := []byte(rows[r.IntN(len(rows))].key)
				limit := 1 + r.IntN(60)
				for range 4 {
					want, err := bare.Rows(ctx, from, false, nil, limit, Projection{})
					if err != nil {
						t.Fatal(err)
					}
					got, err := cached.Rows(ctx, from, false, nil, limit, Projection{})
					if err != nil {
						t.Fatalf("from %q limit %d: %v", from, limit, err)
					}
					if len(got) != len(want) {
						t.Fatalf("from %q limit %d: %d rows, want %d", from, limit, len(got), len(want))
					}
					for i := range got {
						if !sameFields(&got[i], &want[i]) {
							t.Fatalf("from %q limit %d: row %d %+v, want %+v", from, limit, i, got[i], want[i])
						}
					}
					if len(got) < limit {
						break
					}
					from = got[len(got)-1].Key
				}
			}
		})
	}
}
