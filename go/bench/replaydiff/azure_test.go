// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"regexp"
	"slices"
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
	// A marker element closed by the other marker tag is not normalized
	// into a valid one.
	broken := &response{200, h("replay-4", "2026-06-06"), []byte(strings.Replace(string(body("2", m2)), "</NextMarker>", "</Marker>", 1))}
	if d := (azureComparator{}).compare(ref, broken); d.OK {
		t.Error("a mismatched marker tag compared equal")
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

func azureServer(t *testing.T, rollup bool, keys ...string) string {
	t.Helper()
	srv, err := server.New(server.Config{Bucket: "bkt", Host: "127.0.0.1", MetricsPort: -1, MaxConcurrentRequests: 64,
		ResponseBufferBudget: 1 << 30, MaxResponseBytes: 1 << 26, StopTimeout: time.Second, IdleTimeout: time.Minute,
		WriteTimeout: time.Minute, SlowRequestLog: -1, Store: memstore.Keys(memstore.Options{Rollup: rollup}, keys...),
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

// TestMarkerCasesRunOrSkip pins that every marker case sends its
// follow-up when the fixture can make it meaningful, and is otherwise a
// skip, never a pass: the Azure counterpart of the S3 and GCS
// missing-token tests. Over {a, b, c} the three prefix cases are skipped
// (no directory prefix holds two names, and an absent and an empty prefix
// bind the same scope); over {a} every case is (no marker to resume),
// after its source requests were compared.
func TestMarkerCasesRunOrSkip(t *testing.T) {
	const prefixCases = 3
	for _, c := range []struct {
		keys    []string
		skipped int // -1: every case
	}{{[]string{"a", "b", "c"}, prefixCases}, {[]string{"d/1", "d/2", "e"}, 0},
		{[]string{"c\ufffe/1", "c\ufffe/2", "d/1", "d/2"}, 0}, {[]string{"a"}, -1}} {
		client := &http.Client{}
		rep := &report{}
		d := &driver{o: options{bucket: "bkt"}, ref: &target{name: "a", base: azureServer(t, false, c.keys...), client: client},
			got: &target{name: "b", base: azureServer(t, true, c.keys...), client: client}, comp: azureComparator{}, rep: rep}
		var keys [][]byte
		for _, k := range c.keys {
			keys = append(keys, []byte(k))
		}
		_, marks := azureMatrix(keys, rand.New(rand.NewPCG(1, 1)), 10)
		for _, m := range marks {
			d.record(d.markerCase(m))
		}
		if c.skipped < 0 {
			if len(rep.Skipped) != len(marks) || rep.Passed != 0 || rep.Failed != 0 || rep.Requests != 2*(len(marks)-prefixCases) {
				t.Errorf("%v: %d skipped %d passed %d failed of %d, %d requests", c.keys, len(rep.Skipped), rep.Passed, rep.Failed, len(marks), rep.Requests)
			}
			continue
		}
		run := len(marks) - c.skipped
		if len(rep.Skipped) != c.skipped || rep.Failed != 0 || rep.Passed != run || rep.Requests != 4*run {
			t.Errorf("%v: %d skipped %d passed %d failed, %d requests for %d cases: %v", c.keys, len(rep.Skipped), rep.Passed,
				rep.Failed, rep.Requests, len(marks), rep.Skipped)
		}
	}
}

// TestDroppedPrefixIsAnInvalidMarker pins what the scope-change cases
// require: a marker issued under prefix d/ and sent without it is refused
// as InvalidQueryParameterValue invalid_marker, and a case marked as a
// scope change whose follow-up keeps the scope fails rather than passes.
func TestDroppedPrefixIsAnInvalidMarker(t *testing.T) {
	base := azureServer(t, false, "d/1", "d/2", "e")
	client := &http.Client{}
	tg := &target{name: "a", base: base, client: client}
	h := versionHeader(azureVersions[0])
	src := request{Method: "GET", Path: azurePath, Query: aq("prefix", "d/", "maxresults", "1"), Header: h}
	first, err := tg.do("bkt", src)
	if err != nil {
		t.Fatal(err)
	}
	m := azureNextMarker(first.Body)
	if m == "" {
		t.Fatal("no marker")
	}
	dropped, err := tg.do("bkt", request{Method: "GET", Path: azurePath, Query: aq("maxresults", "3", "marker", m), Header: h})
	if err != nil || !invalidMarker(dropped) {
		t.Fatalf("dropped prefix: %v %d %v", err, dropped.Status, dropped.Header)
	}
	d := &driver{o: options{bucket: "bkt"}, ref: tg, got: &target{name: "b", base: base, client: client}, comp: azureComparator{}, rep: &report{}}
	same := azureMarkerCase{name: "same scope", source: src, refused: true, follow: func(m string) request {
		return request{Method: "GET", Path: azurePath, Query: aq("prefix", "d/", "maxresults", "3", "marker", m), Header: h}
	}}
	if r := d.markerCase(same); r.Pass || !strings.Contains(r.Error, "changed scope was accepted") {
		t.Errorf("%+v", r)
	}
}

// TestProgressMarkerCaseKeepsProgress pins what the "marker progress 1"
// case sends and requires: its follow-up resumes with the source's marker
// with progress 1 (flag and boundary unchanged; the binding too, or the
// route would refuse it), and the route's next marker keeps progress 1,
// which the comparator's marker effect then holds the two servers to.
func TestProgressMarkerCaseKeepsProgress(t *testing.T) {
	tg := &target{name: "a", base: azureServer(t, false, "a", "b", "c"), client: &http.Client{}}
	_, marks := azureMatrix([][]byte{[]byte("a"), []byte("b"), []byte("c")}, rand.New(rand.NewPCG(1, 1)), 10)
	i := slices.IndexFunc(marks, func(m azureMarkerCase) bool { return m.name == "marker progress 1" })
	if i < 0 {
		t.Fatal("no progress case")
	}
	first, err := tg.do("bkt", marks[i].source)
	if err != nil {
		t.Fatal(err)
	}
	m := azureNextMarker(first.Body)
	if got, want := markerEffect(markerWithProgress(m, 1)), strings.Replace(markerEffect(m), "progress=0", "progress=1", 1); m == "" || got != want {
		t.Fatalf("marker %q: progressed effect %s, want %s", m, got, want)
	}
	next, err := tg.do("bkt", marks[i].follow(m))
	if err != nil {
		t.Fatal(err)
	}
	if got := markerEffect(azureNextMarker(next.Body)); next.Status != 200 || !bytes.Contains(next.Body, []byte("<Name>b</Name>")) ||
		got != "az1(flag=0 progress=1 boundary=62)" {
		t.Fatalf("%d next marker %s: %s", next.Status, got, next.Body)
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

// editingProxy serves base's answers with each body passed through edit.
func editingProxy(t *testing.T, base string, edit func(body []byte) []byte) string {
	t.Helper()
	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	p := httputil.NewSingleHostReverseProxy(u)
	p.ModifyResponse = func(resp *http.Response) error {
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return err
		}
		body = edit(body)
		resp.Body = io.NopCloser(bytes.NewReader(body))
		resp.ContentLength = int64(len(body))
		resp.Header.Set("Content-Length", fmt.Sprint(len(body)))
		return nil
	}
	srv := httptest.NewServer(p)
	t.Cleanup(srv.Close)
	return srv.URL
}

// TestAzureS3RoutedCases: the Azure matrix's requests that the S3 route
// answers are judged against the Java server by the S3 divergence rules.
// A reference in the Java server's form (no newline after the XML
// declaration, its own error document and token) passes a Go answer that
// differs from it only there; a Go answer with a changed object key, a
// changed error code or an empty listing fails.
func TestAzureS3RoutedCases(t *testing.T) {
	keys := []string{"d/1", "d/2", "e"}
	var bkeys [][]byte
	for _, k := range keys {
		bkeys = append(bkeys, []byte(k))
	}
	all, _ := azureMatrix(bkeys, rand.New(rand.NewPCG(1, 1)), 10)
	routed := map[string]bool{"wrong account": true, "extra segment": true, "S3 beside Azure": true, "account root": true}
	var cases []request
	for _, c := range all {
		if routed[c.Name] {
			cases = append(cases, c)
		}
	}
	if len(cases) != len(routed) {
		t.Fatalf("%d of the S3-routed cases in the matrix", len(cases))
	}
	javaForm := regexp.MustCompile(`<BucketName>[^<]*</BucketName>|<HostId>[^<]*</HostId>`)
	javaToken := regexp.MustCompile(`<NextContinuationToken>[^<]*<`)
	java := &target{name: "java", client: &http.Client{}, base: editingProxy(t, azureServer(t, false, keys...), func(b []byte) []byte {
		b = bytes.Replace(b, []byte("?>\n"), []byte("?>"), 1)
		b = javaForm.ReplaceAll(b, nil)
		return javaToken.ReplaceAll(b, []byte("<NextContinuationToken>v2:AGQvMg<"))
	})}
	for _, c := range []struct {
		name   string
		edit   func([]byte) []byte
		failed []string
	}{
		{"unchanged", func(b []byte) []byte { return b }, nil},
		{"object key", func(b []byte) []byte { return bytes.Replace(b, []byte("<Key>d/2</Key>"), []byte("<Key>d/3</Key>"), 1) },
			[]string{"S3 beside Azure"}},
		{"error code", func(b []byte) []byte {
			return bytes.Replace(b, []byte("<Code>NoSuchBucket</Code>"), []byte("<Code>NoSuchKey</Code>"), 1)
		}, []string{"account root", "extra segment", "wrong account"}},
		{"empty listing", func(b []byte) []byte {
			if bytes.Contains(b, []byte("<ListBucketResult")) {
				return nil
			}
			return b
		}, []string{"S3 beside Azure"}},
	} {
		got := &target{name: "go", client: &http.Client{}, base: editingProxy(t, azureServer(t, false, keys...), c.edit)}
		rep := &report{}
		d := &driver{o: options{bucket: "bkt", parallel: 1, protocol: "azure"}, ref: java, got: got, comp: azureComparator{}, rep: rep}
		d.runCases("case", cases)
		var failed []string
		for _, r := range rep.Results {
			if !r.Pass {
				failed = append(failed, r.Name)
			}
		}
		slices.Sort(failed)
		if !slices.Equal(failed, c.failed) || rep.Passed != len(cases)-len(c.failed) {
			t.Errorf("%s: failed %q, want %q (%d passed)", c.name, failed, c.failed, rep.Passed)
		}
		if c.failed == nil && len(rep.Divergences) != 3 {
			t.Errorf("%s: documented divergences %q, want the three error documents", c.name, rep.Divergences)
		}
	}
}
