// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"runtime/metrics"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/source/keyrange"
	"github.com/varveio/swath/go/internal/source/s3/s3test"
	"github.com/varveio/swath/go/internal/source/scalefake"
	"github.com/varveio/swath/go/internal/textout"
)

// memShape is goes-like: 400,000 keys of at least 120 bytes in 40
// folders, served as real ListObjectsV2 XML (scalefake) and parsed by the
// production parser, whose page strings are views of one arena per page.
var memShape = scalefake.Shape{Keys: 400_000, Fanout: []int{40}, KeyLen: 120, Seed: 7}

// slowFirst delays every request that starts below the slow key (its
// start-after or continuation key; the scope's start without either) by d,
// the rest by fast: the first range's pages are slow, so every range after
// it finishes ahead of the frontier and waits there.
func slowFirst(h http.Handler, slow string, d, fast time.Duration) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v := r.URL.Query()
		start := v.Get("start-after")
		if tok := v.Get("continuation-token"); tok != "" {
			if raw, err := base64.RawURLEncoding.DecodeString(tok); err == nil && len(raw) > 0 {
				start = max(start, string(raw[1:]))
			}
		}
		wait := fast
		if v.Get("delimiter") == "" && start < slow {
			wait = d
		}
		select {
		case <-time.After(wait):
		case <-r.Context().Done():
			return
		}
		h.ServeHTTP(w, r)
	})
}

func liveHeap() int64 {
	s := []metrics.Sample{{Name: heapLive}}
	metrics.Read(s)
	return int64(s[0].Value.Uint64())
}

// TestRetainedBoundsDoNotPinPages (synthetic, labelled: memShape, the
// first 3,000 keys' requests delayed 3 s and the rest 1 ms, 32 workers,
// a 4 MiB row budget, ordered text output, spill on): while the first range
// is held up, every other range lists, splits and finishes ahead of the
// frontier. A finished range waits there with its bounds, and a lower
// child's lo is the cursor of its parent's last page. Were that a view of
// the page, each waiting range would keep its parent's whole page arena
// alive (about 1,000 × key and ETag), which the budget charges as a node of
// about 150 bytes. The peak live heap must stay within a bound of the
// budget and the pages in flight: measured +17 MiB (+30 MiB under -race)
// against a 44 MiB bound, and +62 MiB (+63 MiB) with the cursor left a view
// of its page.
func TestRetainedBoundsDoNotPinPages(t *testing.T) {
	if testing.Short() {
		t.Skip("a 400,000-key listing")
	}
	s3test.NoCredentials(t)
	t.Setenv("TMPDIR", t.TempDir()) // the text run spills beside its file; nothing may land in the real TMPDIR
	ks, err := scalefake.New(memShape)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(slowFirst(scalefake.NewServer(ks, scalefake.Options{}), ks.Key(3000), 3*time.Second, time.Millisecond))
	t.Cleanup(srv.Close)
	const workers, budget = 32, 4 << 20
	runtime.GC()
	base := liveHeap()
	out := filepath.Join(t.TempDir(), "keys.tsv")
	sum, err := Run(context.Background(), Config{
		Source: SourceConfig{URI: "s3://bkt", Options: &S3Config{Endpoint: srv.URL, PathStyle: true, Policy: keyrange.Policy{}}},
		Output: OutputConfig{Text: &textout.Config{Path: out, Format: textout.TSV}},
		// Four formatters (beyond what the budget admits, as asked): their
		// queued batches are charged to the same budget.
		Limits: Limits{Workers: workers, RowBudget: budget, Encoders: 4}})
	if err != nil {
		t.Fatal(err)
	}
	if sum.Output.Text.Formatters != 4 {
		t.Fatalf("%d formatters, want 4", sum.Output.Text.Formatters)
	}
	res := sum.Resources
	grew := res.PeakHeapLiveBytes - base
	// The pages in flight: a response body and its arena per worker,
	// about 300 KB each at this shape, twice over for the parser's pooled
	// scratch; the budget three times over for row structs, publish
	// batches and the frontier path.
	bound := int64(3*budget + workers*2*512<<10)
	t.Logf("synthetic: %d rows; peak live heap +%d MiB (bound %d MiB, budget %d MiB); peak nodes %d, peak ahead %d rows, frontier stuck %d ms, max buffered %d KiB, spill %d rows",
		sum.Output.RowsEmitted, grew>>20, bound>>20, budget>>20, res.PeakNodes, res.PeakAheadRows, res.FrontierStuckMaxNs/1e6, res.MaxBufferedBytes>>10, res.Spill.Rows)
	if sum.Output.RowsEmitted != memShape.Keys {
		t.Fatalf("listed %d of %d", sum.Output.RowsEmitted, memShape.Keys)
	}
	if res.PeakNodes < 100 || res.Spill.Rows == 0 || res.FrontierStuckMaxNs < int64(time.Second) {
		t.Fatalf("the frontier was not held up long enough to matter: %+v", res)
	}
	if grew > bound {
		t.Fatalf("peak live heap grew by %d MiB, over %d MiB: retained ranges pin their pages", grew>>20, bound>>20)
	}
	if fi, err := os.Stat(out); err != nil || fi.Size() == 0 {
		t.Fatalf("no output: %v", err)
	}
}

// TestCountOnlyHoldsNothing: a dry run over the same held-up frontier
// counts every key without ordering them. Nothing is buffered or spilled
// (no file ever appears under TMPDIR, the report's spill is absent and its
// order-stage peaks zero), the count is the keyspace's, and the live heap
// stays at the pages in flight.
func TestCountOnlyHoldsNothing(t *testing.T) {
	if testing.Short() {
		t.Skip("a 400,000-key listing")
	}
	s3test.NoCredentials(t)
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)
	ks, err := scalefake.New(memShape)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(slowFirst(scalefake.NewServer(ks, scalefake.Options{}), ks.Key(3000), time.Second, time.Millisecond))
	t.Cleanup(srv.Close)
	stop, seen := make(chan struct{}), make(chan int)
	go func() { // anything created under TMPDIR while the run goes
		n := 0
		for {
			if es, _ := os.ReadDir(tmp); len(es) > 0 {
				n = max(n, len(es))
			}
			select {
			case <-stop:
				seen <- n
				return
			case <-time.After(5 * time.Millisecond):
			}
		}
	}()
	const workers = 32
	runtime.GC()
	base := liveHeap()
	sum, err := Run(context.Background(), Config{
		Source: SourceConfig{URI: "s3://bkt", Options: &S3Config{Endpoint: srv.URL, PathStyle: true}},
		Limits: Limits{Workers: workers, RowBudget: minRowBudget}})
	close(stop)
	created := <-seen
	if err != nil {
		t.Fatal(err)
	}
	res := sum.Resources
	t.Logf("synthetic: %d rows; peak live heap +%d MiB; peak nodes %d", sum.Output.RowsEmitted, (res.PeakHeapLiveBytes-base)>>20, res.PeakNodes)
	if sum.Outcome.Status != StatusListed || sum.Output.RowsEmitted != memShape.Keys {
		t.Fatalf("%s: %d rows of %d", sum.Outcome.Status, sum.Output.RowsEmitted, memShape.Keys)
	}
	if created != 0 || res.Spill != nil || res.Features["spill"] || res.Features["order"] {
		t.Fatalf("a dry run spilled or ordered: %d entries under TMPDIR, spill %+v, features %v", created, res.Spill, res.Features)
	}
	if res.MaxBufferedBytes != 0 || res.PeakAheadRows != 0 || res.FrontierStuckMaxNs != 0 || res.PeakMarkers != 0 || res.PeakNodes == 0 {
		t.Fatalf("a dry run reports order-stage use: %+v", res)
	}
	if grew := res.PeakHeapLiveBytes - base; grew > workers*2*512<<10+8<<20 {
		t.Fatalf("peak live heap grew by %d MiB holding only pages in flight", grew>>20)
	}
}
