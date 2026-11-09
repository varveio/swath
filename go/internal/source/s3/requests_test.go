// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/varveio/swath/go/internal/order"
	"github.com/varveio/swath/go/internal/source/keyrange"
	"github.com/varveio/swath/go/internal/source/keyrange/keyrangetest"
	"github.com/varveio/swath/go/internal/source/s3/client"
)

// recorder is an S3 API over the range engine's fake listing; it logs
// every request.
type recorder struct {
	*keyrangetest.Sorted
	mu  sync.Mutex
	log []string
}

func (r *recorder) ListObjectsV2(ctx context.Context, in *client.ListInput) (*client.Page, error) {
	r.mu.Lock()
	r.log = append(r.log, fmt.Sprintf("p=%q a=%q d=%q m=%d t=%v", in.Prefix, in.StartAfter, in.Delimiter, in.MaxKeys, in.ContinuationToken != ""))
	r.mu.Unlock()
	out, err := r.List(ctx, &keyrangetest.ListInput{Prefix: in.Prefix, Delimiter: in.Delimiter, StartAfter: in.StartAfter,
		ContinuationToken: in.ContinuationToken, MaxKeys: in.MaxKeys})
	if err != nil {
		return nil, err
	}
	pg := &client.Page{CommonPrefixes: out.CommonPrefixes, IsTruncated: out.IsTruncated, NextContinuationToken: out.NextContinuationToken}
	for _, o := range out.Contents {
		pg.Contents = append(pg.Contents, client.Object{Key: o.Key, Size: o.Size})
	}
	return pg, nil
}

// TestRequests pins what the adapter sends for the engine's queries: the
// scope prefix, a resume's start-after, the continuation chain, 1,000 keys
// a page, the seed's delimiter listing, and a real key's '%' sent as a
// bound. Scheduling (which ranges split
// where) is the engine's and tested there, against the tiling, resume and
// sorted-output laws; the SDK oracle checks the page decoding.
func TestRequests(t *testing.T) {
	var keys []string
	for i := range 2500 {
		keys = append(keys, fmt.Sprintf("d/k%04d", i))
	}
	seq := keyrange.Policy{Sequential: true}
	for _, c := range []struct {
		name  string
		after string
		opt   Options
		want  []string // nil: the seed's shape, below
		rows  int
		// folder: the seed's shape only: a tile's start-after begins with
		// it; percent lists keys in folders p%2F0/ .. p%2F4/ instead.
		folder  string
		percent bool
	}{
		{name: "sequential", opt: Options{Policy: seq}, rows: 2500, want: []string{
			`p="" a="" d="" m=1000 t=false`,
			`p="" a="" d="" m=1000 t=true`,
			`p="" a="" d="" m=1000 t=true`,
		}},
		{name: "prefix", opt: Options{Policy: seq, Prefix: "d/k1"}, rows: 1000, want: []string{
			`p="d/k1" a="" d="" m=1000 t=false`,
		}},
		{name: "resume", opt: Options{Policy: seq}, after: "d/k0999", rows: 1500, want: []string{
			`p="" a="d/k0999" d="" m=1000 t=false`,
			`p="" a="" d="" m=1000 t=true`, // a continuation carries no start-after
		}},
		{name: "seed", opt: Options{Policy: keyrange.Policy{Seed: true}}, rows: 2500, folder: "d/"},
		// A real key's '%' is a usable bound (AcceptBound): the seed's cuts
		// at folders whose names hold it are sent as start-after.
		{name: "seed-percent", opt: Options{Policy: keyrange.Policy{Seed: true}}, rows: 2500, folder: "p%2F", percent: true},
	} {
		t.Run(c.name, func(t *testing.T) {
			ks := keys
			if c.percent {
				ks = nil
				for i := range 2500 {
					ks = append(ks, fmt.Sprintf("p%%2F%d/k%04d", i/500, i))
				}
			}
			rec := &recorder{Sorted: keyrangetest.NewSorted(ks, false, 1)}
			opt := c.opt
			opt.Bucket, opt.Client, opt.Workers = "b", rec, 1
			p, err := New(context.Background(), opt)
			if err != nil {
				t.Fatal(err)
			}
			if c.after != "" {
				if err := p.Restart(c.after); err != nil {
					t.Fatal(err)
				}
			}
			col := &collect{}
			if _, err := order.Run[Range, Object](context.Background(), p, col, &Schema, order.Options{Workers: 1, Budget: 100 * 256}); err != nil {
				t.Fatal(err)
			}
			if len(col.rows) != c.rows {
				t.Fatalf("%d rows, want %d", len(col.rows), c.rows)
			}
			if c.want != nil && !slices.Equal(rec.log, c.want) {
				t.Errorf("requests\n%q\nwant\n%q", rec.log, c.want)
			}
			if c.want != nil {
				return
			}
			// The seed's first wave is the delimiter listing of the scope's
			// top level and, concurrently (either order), its first data
			// page; the tiles then list without the delimiter, from their
			// start-after or on a continuation, never both. Where the seed
			// cuts is policy.
			const top = `p="" a="" d="/" m=1000 t=false`
			if len(rec.log) < 3 || !slices.Contains(rec.log[:2], top) || slices.Contains(rec.log[slices.Index(rec.log, top)+1:], top) {
				t.Fatalf("requests %q: want the seed's delimiter listing of the scope once, in the first wave", rec.log)
			}
			bound := false
			for _, r := range rec.log {
				if r == top {
					continue
				}
				if !strings.Contains(r, `d="" m=1000`) || strings.HasSuffix(r, "t=true") && !strings.Contains(r, `a=""`) {
					t.Errorf("tile request %q: want no delimiter, a full page, no start-after on a continuation", r)
				}
				bound = bound || strings.HasPrefix(r, `p="" a="`+c.folder)
			}
			if !bound {
				t.Errorf("requests %q: want a tile starting after a cut in %q", rec.log, c.folder)
			}
		})
	}
}
