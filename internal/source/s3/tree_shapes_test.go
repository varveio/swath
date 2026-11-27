// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3_test

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/varveio/swath/internal/bind"
	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/internal/shapes"
	"github.com/varveio/swath/internal/source/s3"
	"github.com/varveio/swath/internal/source/s3/s3test"
)

type rows struct {
	mu   sync.Mutex
	rows []schema.Row[s3.Object]
}

func (c *rows) Write(_ context.Context, segs [][]schema.Row[s3.Object]) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, s := range segs {
		c.rows = append(c.rows, s...)
	}
	return nil
}

// TestTreeShapesOnR2: every shape, at a small scale, lists through the
// folder walk from the R2 fake exactly: the shape's keys (sorted as raw
// bytes by the shape itself) with the payload the fake gives each, at
// pages of 1, 2 and 1,000 keys and 1 and 8 workers.
func TestTreeShapesOnR2(t *testing.T) {
	s3test.NoCredentials(t)
	at := time.Date(2026, 11, 27, 0, 0, 0, 0, time.UTC)
	for _, sh := range shapes.All {
		keys := sh.Keys(200)
		u := s3test.Serve(t, s3test.NewR2(keys))
		for _, page := range []int{1, 2, 1000} {
			if page < 1000 && len(keys) > 2000 {
				continue // small10k: 10,000 keys a page at a time adds nothing but time
			}
			for _, workers := range []int{1, 8} {
				opt := s3.Options{Endpoint: u, PathStyle: true, Anonymous: true, Region: "us-east-1", Listing: s3.ListingTree, Workers: workers}
				opt.Policy.PageSize = page
				inv, err := s3.Open(context.Background(), "s3://"+s3test.Name, opt)
				if err != nil {
					t.Fatal(err)
				}
				l, err := inv.Bound().Prepare(bind.Env{Order: order.Options{Workers: workers, Budget: 64 << 20}})
				if err != nil {
					t.Fatal(err)
				}
				c := &rows{}
				if _, err := l.Run(context.Background(), c); err != nil {
					t.Fatalf("%s page %d workers %d: %v", sh.Name, page, workers, err)
				}
				what := fmt.Sprintf("%s page %d workers %d", sh.Name, page, workers)
				if len(c.rows) != len(keys) {
					t.Fatalf("%s: %d rows, want %d", what, len(c.rows), len(keys))
				}
				for i, k := range keys {
					sum := md5.Sum([]byte(k))
					want := s3.Object{Size: int64(len(k)), ETag: hex.EncodeToString(sum[:]), StorageClass: "STANDARD", Mtime: at.Add(time.Duration(i) * time.Second).UnixMicro()}
					if r := c.rows[i]; r.Key != k || r.Data != want {
						t.Fatalf("%s: row %d is %q %+v, want %q %+v", what, i, r.Key, r.Data, k, want)
					}
				}
			}
		}
		if slices.Equal(s3test.NewR2(keys).Keys(), keys) {
			t.Logf("%s: R2's order is byte order here", sh.Name)
		}
	}
}
