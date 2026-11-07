// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"math/rand/v2"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/replay/server"
	"github.com/varveio/swath/go/internal/replay/store/memstore"
)

func TestAzureComparator(t *testing.T) {
	h := func(id, ver string) http.Header {
		return http.Header{"Content-Type": {"application/xml"}, "X-Ms-Request-Id": {id}, "X-Ms-Version": {ver},
			"Date": {"Sat, 07 Nov 2026 10:00:00 GMT"}}
	}
	body := func(port, marker string) []byte {
		return []byte(azureBOM + `<EnumerationResults ServiceEndpoint="http://127.0.0.1:` + port + `/replay/"><NextMarker>` + marker + `</NextMarker></EnumerationResults>`)
	}
	// Same boundary, different scope digests: equal by effect.
	marker := func(bind byte, boundary string) string {
		raw := append([]byte{0, 0}, bytes.Repeat([]byte{bind}, 16)...)
		return "az1." + base64.RawURLEncoding.EncodeToString(append(raw, boundary...))
	}
	m1, m2 := marker(1, "a"), marker(2, "a")
	ref := &response{200, h("replay-1", "2026-06-06"), body("1", m1)}
	got := &response{200, h("replay-9f", "2026-06-06"), body("2", m2)}
	if d := (azureComparator{}).compare(ref, got); !d.OK {
		t.Errorf("same effect: %+v", d)
	}
	other := &response{200, h("replay-2", "2026-06-06"), body("2", marker(1, "b"))}
	if d := (azureComparator{}).compare(ref, other); d.OK {
		t.Error("a different boundary compared equal")
	}
	if d := (azureComparator{}).compare(ref, &response{200, h("replay-2", "2026-10-06"), body("1", m1)}); d.OK {
		t.Error("a different version echo compared equal")
	}
	if d := (azureComparator{}).compare(ref, &response{200, h("x", "2026-06-06"), body("1", m1)}); d.OK {
		t.Error("a malformed request id compared equal")
	}
	noBOM := &response{200, h("replay-3", "2026-06-06"), body("1", m1)[3:]}
	if d := (azureComparator{}).compare(ref, noBOM); d.OK {
		t.Error("a body without the BOM compared equal")
	}
	seg := &response{500, http.Header{"X-Ms-Request-Id": {"replay-1"}, "X-Swath-Replay-Error": {"too_many_segments"}}, nil}
	if d := (azureComparator{acceptSegments: true}).compare(seg, ref); !d.OK || d.Detail == "" {
		t.Errorf("segment improvement: %+v", d)
	}
	if d := (azureComparator{}).compare(seg, ref); d.OK {
		t.Error("segment improvement accepted without the flag")
	}
}

// TestAzureDriverAgreesWithItself runs the Azure matrix, marker cases and
// walks against two Go servers over the same names, one whose store
// rolls up and one that walks: every answer must agree, and the driver
// must say so.
func TestAzureDriverAgreesWithItself(t *testing.T) {
	var keys []string
	for i := range 200 {
		keys = append(keys, fmt.Sprintf("d%d/s%d/k%03d", i%3, i%7, i), fmt.Sprintf("u/é%d", i%9), fmt.Sprintf("u/\ue000%d", i%5),
			fmt.Sprintf("u/\U00010000%d", i%4))
	}
	start := func(rollup bool) string {
		srv, err := server.New(server.Config{Bucket: "bkt", Host: "127.0.0.1", MetricsPort: -1, MaxConcurrentRequests: 64,
			ResponseBufferBudget: 1 << 30, MaxResponseBytes: 1 << 26, StopTimeout: time.Second, IdleTimeout: time.Minute,
			WriteTimeout: time.Minute, SlowRequestLog: -1, Store: memstore.Keys(memstore.Options{Rollup: rollup}, dedupe(keys)...),
			ServingMode: "MEMORY", FixtureIdentity: "same", Protocols: []string{"s3", "azure"}, AzureAccount: "replay"})
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
	o := options{bucket: "bkt", parallel: 4, maxPrefixes: 30, maxTop: 5, walkSizes: "1,7,5000", random: 100}
	rep := &report{}
	d := &driver{o: o, ref: ref, got: got, rep: rep}
	all, _, err := listAll(ref, "bkt")
	if err != nil {
		t.Fatal(err)
	}
	d.runAzure(all, rand.New(rand.NewPCG(3, 3)))
	if rep.Failed != 0 || rep.Passed < 300 {
		for _, r := range rep.Results {
			if !r.Pass {
				t.Errorf("%s %q: %+v %s", r.Kind, r.Name, r.diff, r.Error)
				break
			}
		}
		t.Fatalf("%d passed, %d failed", rep.Passed, rep.Failed)
	}
	if !strings.Contains(fmt.Sprint(rep.Results[0].Name), "version") {
		t.Errorf("first case %q", rep.Results[0].Name)
	}
}
