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

func TestGCSComparator(t *testing.T) {
	h := func(kv ...string) http.Header {
		x := http.Header{}
		for i := 0; i < len(kv); i += 2 {
			x[kv[i]] = []string{kv[i+1]}
		}
		return x
	}
	body := `{"kind":"storage#objects","items":[{"name":"a"}],"nextPageToken":"gcs1.AA"}`
	ref := &response{200, h("Content-Type", "application/json; charset=utf-8", "Date", "x"), []byte(body)}
	if d := (gcsComparator{}).compare(ref, &response{200, h("Content-Type", "application/json; charset=utf-8"), []byte(body)}); !d.OK {
		t.Errorf("identical answers differ: %+v", d)
	}
	tokens := &response{200, ref.Header, []byte(strings.Replace(body, "gcs1.AA", "gcs1.AB", 1))}
	if d := (gcsComparator{}).compare(ref, tokens); d.OK || !strings.Contains(d.Detail, "tokens differ") {
		t.Errorf("a token difference: %+v", d)
	}
	items := &response{200, ref.Header, []byte(strings.Replace(body, `"a"`, `"b"`, 1))}
	if d := (gcsComparator{}).compare(ref, items); d.OK || d.Detail != "documents differ" {
		t.Errorf("an item difference: %+v", d)
	}
	allow := &response{405, h("Content-Type", "application/json; charset=utf-8", "Allow", "GET"), nil}
	if d := (gcsComparator{}).compare(allow, &response{405, h("Content-Type", "application/json; charset=utf-8"), nil}); d.OK {
		t.Error("a missing Allow compared equal")
	}
}

func TestGCSMatrixNamesAreUnique(t *testing.T) {
	keys := [][]byte{[]byte("a/b/c"), []byte("a/b/d e"), []byte("a/x"), []byte("b/é"), []byte("c")}
	seen := map[string]bool{}
	for _, r := range gcsMatrix(keys, rand.New(rand.NewPCG(1, 2)), 100) {
		if seen[r.Name] {
			t.Errorf("duplicate %q", r.Name)
		}
		seen[r.Name] = true
	}
	for _, name := range []string{"prefix=a/b/ delimiter=/", "prefix a/ startOffset inside a/b/", "escaped key 0", "token page 2 other prefix", "HEAD"} {
		if !seen[name] {
			t.Errorf("missing %q", name)
		}
	}
}

// TestGCSDriverFindsNoDifferenceBetweenRollupAndWalk runs the GCS request
// set against two Go servers over the same keys, one whose store answers
// the "/" rollup and one that walks ranges: the answers must be
// byte-identical, tokens included. A third server over one key fewer
// must be told apart.
func TestGCSDriverFindsNoDifferenceBetweenRollupAndWalk(t *testing.T) {
	var keys []string
	for i := range 300 {
		keys = append(keys, fmt.Sprintf("d%d/s%d/k%03d", i%3, i%7, i), fmt.Sprintf("top%02d", i%40), fmt.Sprintf("x/%c y", 'a'+i%26))
	}
	keys = dedupe(keys)
	start := func(rollup bool, keys []string) string {
		srv, err := server.New(server.Config{Bucket: "bkt", Host: "127.0.0.1", MetricsPort: -1, MaxConcurrentRequests: 64,
			ResponseBufferBudget: 1 << 30, MaxResponseBytes: 1 << 26, StopTimeout: time.Second, IdleTimeout: time.Minute,
			WriteTimeout: time.Minute, SlowRequestLog: -1, Store: memstore.Keys(memstore.Options{Rollup: rollup}, keys...),
			ServingMode: "MEMORY", FixtureIdentity: "f", Protocols: []string{server.ProtocolGCS}})
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
	ref := &target{name: "walk", base: start(false, keys), client: client}
	o := options{bucket: "bkt", parallel: 4, maxPrefixes: 50, maxTop: 10, walkSizes: "1,7,1000", random: 200, seed: 9}
	all, err := gcsListAll(ref, "bkt")
	if err != nil || len(all) != len(keys) {
		t.Fatalf("%d keys, %v", len(all), err)
	}
	run := func(got *target) *report {
		rep := &report{}
		d := &driver{o: o, ref: ref, got: got, comp: gcsComparator{}, rep: rep}
		d.runGCSAll(all, rand.New(rand.NewPCG(9, 9)))
		return rep
	}
	rep := run(&target{name: "rollup", base: start(true, keys), client: client})
	if rep.Failed != 0 || rep.Passed < 700 {
		for _, r := range rep.Results {
			if !r.Pass {
				t.Errorf("%s %q: %+v %s", r.Kind, r.Name, r.diff, r.Error)
				break
			}
		}
		t.Fatalf("%d passed, %d failed", rep.Passed, rep.Failed)
	}
	if rep := run(&target{name: "short", base: start(true, keys[1:]), client: client}); rep.Failed == 0 {
		t.Error("a server missing a key compared equal")
	}
}
