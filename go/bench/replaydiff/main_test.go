// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"math/rand/v2"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/replay/server"
	"github.com/varveio/swath/go/internal/replay/store/memstore"
)

func TestBytesComparator(t *testing.T) {
	h := func(kv ...string) http.Header {
		x := http.Header{}
		for i := 0; i < len(kv); i += 2 {
			x[kv[i]] = []string{kv[i+1]}
		}
		return x
	}
	ref := &response{200, h("Content-Type", "application/xml", "x-amz-request-id", "A", "Date", "x", "Server", "Jetty"), []byte("<a>hello</a>")}
	same := &response{200, h("Content-Type", "application/xml", "X-Amz-Request-Id", "A", "Date", "y"), []byte("<a>hello</a>")}
	if d := (bytesComparator{}).compare(ref, same); !d.OK {
		t.Errorf("identical answers differ: %+v", d)
	}
	other := &response{200, h("Content-Type", "text/xml", "x-amz-request-id", "A", "x-swath-replay-error", "r"), []byte("<a>help</a>")}
	d := (bytesComparator{}).compare(ref, other)
	if d.OK || d.Offset != 6 || len(d.HeaderDiffs) != 2 {
		t.Errorf("%+v", d)
	}
}

func TestLiveComparatorMasksTokensAndFormatting(t *testing.T) {
	ref := &response{200, http.Header{"Content-Type": {"application/xml"}, "X-Amz-Request-Id": {"X1"}},
		[]byte("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult><Name>b</Name><NextContinuationToken>1ueG=</NextContinuationToken><KeyCount>1</KeyCount></ListBucketResult>")}
	got := &response{200, http.Header{"Content-Type": {"application/xml"}, "x-amz-request-id": {"S3LISTINGREPLAY"}},
		[]byte(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult><Name>b</Name><NextContinuationToken>v2:AA</NextContinuationToken><KeyCount>1</KeyCount></ListBucketResult>`)}
	if d := (liveComparator{}).compare(ref, got); !d.OK {
		t.Errorf("%+v", d)
	}
	got.Body = []byte(strings.Replace(string(got.Body), "<KeyCount>1", "<KeyCount>2", 1))
	if d := (liveComparator{}).compare(ref, got); d.OK {
		t.Error("a different KeyCount compared equal")
	}
	e1 := &response{400, nil, []byte("<Error><Code>InvalidArgument</Code><Message>S3 words</Message></Error>")}
	e2 := &response{400, nil, []byte("<Error><Code>InvalidArgument</Code><Message>replay words</Message><Resource>/b</Resource></Error>")}
	if d := (liveComparator{}).compare(e1, e2); !d.OK {
		t.Errorf("errors with the same code: %+v", d)
	}
}

func TestMatrixNamesAreUnique(t *testing.T) {
	keys := [][]byte{[]byte("a/b/c"), []byte("a/b/d e"), []byte("a/x"), []byte("b/é"), []byte("c")}
	seen := map[string]bool{}
	for _, r := range s3Matrix(keys, rand.New(rand.NewPCG(1, 2)), 100) {
		if seen[r.Name] {
			t.Errorf("duplicate %q", r.Name)
		}
		seen[r.Name] = true
	}
	if !seen["prefix=a/b/ delimiter=/"] || !seen["start-after=successor a/"] || !seen["encoded key 0"] {
		t.Errorf("missing expected requests: %v", len(seen))
	}
}

// TestDriverFindsNoDifferenceBetweenRollupAndWalk runs the whole request
// set against two Go servers over the same keys, one whose store answers
// the "/" rollup and one that walks ranges: their answers must be
// byte-identical, and the driver must say so.
func TestDriverFindsNoDifferenceBetweenRollupAndWalk(t *testing.T) {
	var keys []string
	for i := range 300 {
		keys = append(keys, fmt.Sprintf("d%d/s%d/k%03d", i%3, i%7, i), fmt.Sprintf("top%02d", i%40), fmt.Sprintf("x/%c y", 'a'+i%26))
	}
	start := func(rollup bool) string {
		srv, err := server.New(server.Config{Bucket: "bkt", Host: "127.0.0.1", MetricsPort: -1, MaxConcurrentRequests: 64,
			ResponseBufferBudget: 1 << 30, MaxResponseBytes: 1 << 26, StopTimeout: time.Second, IdleTimeout: time.Minute,
			WriteTimeout: time.Minute, SlowRequestLog: -1, Store: memstore.Keys(memstore.Options{Rollup: rollup}, dedupe(keys)...), ServingMode: "MEMORY"})
		if err != nil {
			t.Fatal(err)
		}
		if err := srv.Listen(); err != nil {
			t.Fatal(err)
		}
		go srv.Serve()
		t.Cleanup(func() { srv.Shutdown(time.Second) })
		return fmt.Sprintf("http://127.0.0.1:%d", srv.Port())
	}
	client := &http.Client{}
	ref := &target{name: "walk", base: start(false), client: client}
	got := &target{name: "rollup", base: start(true), client: client}
	o := options{bucket: "bkt", parallel: 4, maxPrefixes: 50, maxTop: 10}
	rep := &report{}
	d := &driver{o: o, ref: ref, got: got, comp: bytesComparator{}, rep: rep}
	all, _, err := listAll(ref, "bkt")
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewPCG(9, 9))
	d.runCases("case", s3Matrix(all, rng, o.maxPrefixes))
	d.runWalks(s3Walks(all, []int{1, 7, 1000}, o.maxTop))
	var random []request
	for range 200 {
		random = append(random, randomRequest(all, rng, directoryPrefixes(all)))
	}
	d.runCases("random", random)
	if rep.Failed != 0 || rep.Passed < 500 {
		for _, r := range rep.Results {
			if !r.Pass {
				t.Errorf("%s %q: %+v %s", r.Kind, r.Name, r.diff, r.Error)
				break
			}
		}
		t.Fatalf("%d passed, %d failed", rep.Passed, rep.Failed)
	}
}

func dedupe(keys []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, k := range keys {
		if !seen[k] {
			seen[k] = true
			out = append(out, k)
		}
	}
	return out
}
