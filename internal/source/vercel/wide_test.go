// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package vercel

import (
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/varveio/swath/internal/schema"
)

func TestWideFoldedWalk(t *testing.T) {
	const direct, folders, deep, limit = 2300, 1123, 32, 1000
	stamp := time.Date(2026, 11, 27, 1, 2, 3, 456789000, time.UTC)
	keys := make([]string, 0, direct+folders+deep+2)
	for i := range direct {
		keys = append(keys, fmt.Sprintf("entry-%04d", i))
	}
	for i := range folders {
		keys = append(keys, fmt.Sprintf("entry-%04d/child", i))
	}
	keys = append(keys, "entry-0999/", "entry-1000/")
	for i := 1; i <= deep; i++ {
		keys = append(keys, "entry-1000/"+strings.Repeat("deep/", i)+"leaf")
	}
	slices.Sort(keys)
	want := make([]schema.Row[Object], len(keys))
	blobs := make(map[string]wireBlob, len(keys))
	for i, k := range keys {
		etag := fmt.Sprintf("fixture-etag-%d", i)
		blobs[k] = wireBlob{k, int64(i), stamp.Format(time.RFC3339Nano), etag}
		want[i] = schema.Row[Object]{Key: k, Data: Object{Size: int64(i), UploadedAt: stamp.UnixMicro(), ETag: etag}}
	}
	for _, workers := range []int{1, 8} {
		t.Run(strconv.Itoa(workers), func(t *testing.T) {
			var mu sync.Mutex
			requests := make(map[string]int)
			pageSizes := make(map[string]int)
			hc := &http.Client{Transport: listTransport(func(r *http.Request) (*http.Response, error) {
				q := r.URL.Query()
				if q.Get("mode") != "folded" || q.Get("limit") != strconv.Itoa(limit) {
					return nil, fmt.Errorf("unrecognized fixture request")
				}
				prefix, cursor := q.Get("prefix"), q.Get("cursor")
				page, err := foldedPage(blobs, prefix, cursor, limit)
				if err != nil {
					return nil, err
				}
				mu.Lock()
				requests[prefix]++
				if prefix == "" {
					pageSizes[cursor] = len(page.Blobs) + len(page.Folders)
				}
				mu.Unlock()
				return response(page)
			})}
			o, got, err := run(t, "", Options{Workers: workers, PageSize: limit, HTTPClient: hc})
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(got, want) {
				t.Fatalf("incorrect inventory: got %d rows, expected %d", len(got), len(want))
			}
			if requests[""] != 4 || pageSizes[""] != 1000 || pageSizes["1000"] != 1000 ||
				pageSizes["2000"] != 1000 || pageSizes["3000"] != 423 {
				t.Fatalf("root pages = %d, root page sizes = %v", requests[""], pageSizes)
			}
			for prefix, count := range requests {
				if prefix != "" && count != 1 {
					t.Fatalf("child prefix %q requested %d times", prefix, count)
				}
			}
			if len(requests) != 1+folders+deep {
				t.Fatalf("visited %d prefixes, expected %d", len(requests), 1+folders+deep)
			}
			report := o.Report()
			if !report.Coverage.Complete ||
				report.Stats.(*Stats).Counters.Value("tree.pages") != 4+folders+deep {
				t.Fatalf("wrong publication coverage or page count: %+v", report)
			}
		})
	}
}
