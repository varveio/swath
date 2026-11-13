// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/internal/dataset/datasettest"
	"github.com/varveio/swath/internal/source/keyrange"
	"github.com/varveio/swath/internal/source/s3/s3test"
)

// TestPriorSlowSplitPercentThrottleCompose: the warm start, the slow-range
// split, '%' bounds and throttling at once. Forty folders whose names hold
// a '%' (S3 accepts a real key's '%' as a bound), seeded from a cut file
// with one cut per folder; one folder of five pages answers in 60 ms a
// page, the others in 2 ms, with slow-split on; every seventh request is a
// 503 SlowDown. The dataset holds every key once, in order, and the paths
// engaged: tiles from the prior, bounds holding '%' sent, throttles met.
// On this small shape the slow-range split itself does not fire (logged;
// its engagement is gated by keyrange TestSlowRangeSplits): slow-split is
// composed here for correctness only.
func TestPriorSlowSplitPercentThrottleCompose(t *testing.T) {
	s3test.NoCredentials(t)
	var keys []string
	for d := range 40 {
		n := 400
		if d == 0 {
			n = 5000
		}
		for i := range n {
			keys = append(keys, fmt.Sprintf("d%%%02d/obj-%05d", d, i))
		}
	}
	slices.Sort(keys)
	h := s3test.Handler(s3test.Backend(t, keys))
	var n, percent atomic.Int64
	url := s3test.Serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Query().Get("start-after"), "%") {
			percent.Add(1)
		}
		if n.Add(1)%7 == 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>SlowDown</Code><Message>slow down</Message></Error>`)
			return
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, r)
		body := rec.Body.Bytes()
		d := 2 * time.Millisecond
		if bytes.Contains(body, []byte("d%00/")) || bytes.Contains(body, []byte("d%2500/")) {
			d = 60 * time.Millisecond // the slow folder's pages
		}
		time.Sleep(d)
		for k, v := range rec.Header() {
			w.Header()[k] = v
		}
		w.WriteHeader(rec.Code)
		w.Write(body)
	}))
	dir := t.TempDir()
	cuts := filepath.Join(dir, "prior.cuts")
	var body []byte
	for i, k := range keys {
		if i == len(keys)-1 || k[:4] != keys[i+1][:4] { // each folder's last key
			body = append(body, k+"\n"...)
		}
	}
	if err := os.WriteFile(cuts, body, 0o644); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "ds")
	sum, err := Run(context.Background(), Config{Source: SourceConfig{URI: "s3://bkt", SeedFrom: cuts,
		Options: &S3Config{Endpoint: url, PathStyle: true, Anonymous: true, Policy: keyrange.Policy{Seed: true, SlowSplit: true}}},
		Output: OutputConfig{Dir: out, NoJournal: datasettest.NoJournal()}, Limits: Limits{PartSize: 16 << 10, Workers: 16}})
	if err != nil {
		t.Fatal(err)
	}
	if got := checkContract(t, out); !slices.Equal(got, keys) {
		t.Fatalf("%d keys, want %d", len(got), len(keys))
	}
	c := s3Stats(sum).Counters
	t.Logf("slow splits %d, throttles %d, bounds with %%: %d, tiles %d", c.Value("split.reason.slow_range"), sum.Requests.Throttles, percent.Load(), c.Value("seed.from.tiles"))
	if percent.Load() == 0 || c.Value("seed.from.tiles") < 2 || c.Value("seed.requests") != 0 || sum.Requests == nil || sum.Requests.Throttles == 0 {
		t.Fatalf("a path did not engage")
	}
}
