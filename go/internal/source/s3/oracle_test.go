// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"testing"

	"github.com/varveio/swath/go/internal/source/keyrange"
	"github.com/varveio/swath/go/internal/source/s3/s3test/sdkoracle"
)

// TestRowsMatchSDK lists through the client and the stock SDK client,
// sequential and parallel.
func TestRowsMatchSDK(t *testing.T) {
	keys := append(testKeys(), "sp ace", "pl+us", "pct%41", "amp&<>")
	url := fakeBucket(t, keys, 0)
	sdk := sdkoracle.New(url)
	for _, seq := range []bool{true, false} {
		want, _ := list(t, Options{Bucket: "bkt", Client: sdk, FetchOwner: true, Workers: 4, Policy: keyrange.Policy{Sequential: seq}}, 4)
		got, _ := list(t, Options{Bucket: "bkt", Endpoint: url, PathStyle: true, Anonymous: true, FetchOwner: true, Workers: 4, Policy: keyrange.Policy{Sequential: seq}}, 4)
		if len(got) != len(keys) || len(got) != len(want) {
			t.Fatalf("rows %d, sdk %d, keys %d", len(got), len(want), len(keys))
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("sequential=%v row %d: %+v vs %+v", seq, i, got[i], want[i])
			}
		}
	}
}
