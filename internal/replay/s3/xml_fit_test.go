// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"runtime"
	"strings"
	"testing"

	"github.com/varveio/swath/internal/replay/output"
	"github.com/varveio/swath/internal/replay/store"
)

// TestOversizedContentsAllocatesWithinTheCap renders an object whose ETag
// is 64 MiB of '&' (320 MiB escaped), and one whose key is 64 MiB of '&'
// (192 MiB percent-encoded), under an 8 KiB response cap, in both key
// encodings and with chunks smaller than and equal to the cap: each
// listing is refused by the cap, and the render allocates a bounded
// amount, not the escaped element in scratch before its fit is checked.
func TestOversizedContentsAllocatesWithinTheCap(t *testing.T) {
	huge := strings.Repeat("&", 64<<20)
	for _, obj := range []store.Object{{Key: []byte("k"), ETag: &huge}, {Key: []byte(huge)}} {
		entries := []Entry{{Object: &obj}}
		for _, enc := range []bool{false, true} {
			for _, chunk := range []int{512, 8 << 10} {
				req := &Request{Bucket: "b", MaxKeys: 1000, EncodingURL: enc}
				budget := output.NewBudget(1<<30, nil)
				o, err := output.NewChunked(budget, 4096, 8<<10, chunk, nil)
				if err != nil {
					t.Fatal(err)
				}
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				RenderList(o, &Result{Request: req, Entries: entries})
				runtime.ReadMemStats(&after)
				if o.Err() == nil {
					t.Fatalf("url=%v chunk %d: a 64 MiB ETag rendered under an 8 KiB cap", enc, chunk)
				}
				o.Close()
				if n := after.TotalAlloc - before.TotalAlloc; n > 1<<20 {
					t.Fatalf("url=%v chunk %d: rendering allocated %d bytes under an 8 KiB cap", enc, chunk, n)
				}
			}
		}
	}
}
