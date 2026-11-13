// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/replay/server"
)

// captureWalk walks a prefix through the fake provider at base as a
// client would, recording each exchange in the -record shape; gzip
// compresses the recorded bodies as a provider does for a client that
// accepts it.
func captureWalk(t *testing.T, base, path, query, tokenParam string, next func([]byte) string, gz bool) []capturedExchange {
	t.Helper()
	var out []capturedExchange
	token := ""
	for {
		q := query
		if token != "" {
			q += "&" + tokenParam + "=" + campaignEscape(token, "")
		}
		resp, err := http.Get(base + path + "?" + q)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var x capturedExchange
		x.Seq = uint64(len(out) + 1)
		x.Request.Method, x.Request.Path, x.Request.Query = "GET", path, q
		x.Request.Headers = []capturedHeader{{"Host", "provider"}, {"Accept-Encoding", "gzip"}, {"Authorization", "{redacted}"}}
		x.Response.Status = resp.StatusCode
		for k, vs := range resp.Header {
			for _, v := range vs {
				if gz && strings.EqualFold(k, "Content-Length") {
					continue
				}
				x.Response.Headers = append(x.Response.Headers, capturedHeader{k, v})
			}
		}
		if gz {
			var b bytes.Buffer
			w := gzip.NewWriter(&b)
			w.Write(body)
			w.Close()
			s := base64.StdEncoding.EncodeToString(b.Bytes())
			x.Response.BodyBase64 = &s
			x.Response.Headers = append(x.Response.Headers, capturedHeader{"Content-Encoding", "gzip"})
		} else {
			s := string(body)
			x.Response.Body = &s
		}
		out = append(out, x)
		if token = next(body); token == "" {
			return out
		}
	}
}

// TestCaptureReplaysAClientStream: a client's walk through the provider,
// captured with the provider's own tokens, replays against the Go server
// with each captured token mapped to the server's own for the same
// point, and passes; a capture whose answer named another object fails
// at that request, and a token no earlier answer issued fails its
// request, the rest of that walk skipped with the reason.
func TestCaptureReplaysAClientStream(t *testing.T) {
	keys := liveKeys()
	for _, c := range []struct {
		protocol, path, query, param string
		next                         func([]byte) string
		re                           *regexp.Regexp
		gz                           bool
	}{
		{"s3", "/bkt", "list-type=2&prefix=data/&max-keys=7&encoding-type=url", "continuation-token", nextToken,
			regexp.MustCompile(`(<NextContinuationToken>)([^<]*)<`), false},
		{"gcs", "/storage/v1/b/bkt/o", "prefix=data/&maxResults=3&delimiter=/", "pageToken", gcsNextToken,
			regexp.MustCompile(`("nextPageToken":")([^"]*)"`), true},
	} {
		protocols := []string{server.ProtocolS3}
		extra := http.Header{"X-Amz-Request-Id": {"R1"}, "Server": {"AmazonS3"}}
		if c.protocol == "gcs" {
			protocols = []string{server.ProtocolGCS}
			extra = http.Header{"X-Guploader-Uploadid": {"U1"}, "Server": {"UploadServer"}}
		}
		f := &fakeProvider{backend: startGoProtocols(t, protocols, keys...), tokenParam: c.param, tokenRe: c.re, extra: extra, tokens: map[string]string{}}
		live := httptest.NewServer(f)
		defer live.Close()
		xs := captureWalk(t, live.URL, c.path, c.query, c.param, c.next, c.gz)
		if len(xs) < 3 {
			t.Fatalf("%s: %d pages", c.protocol, len(xs))
		}
		got := &target{name: "go", base: startGoProtocols(t, protocols, keys...), client: &http.Client{}}
		run := func(xs []capturedExchange) *report {
			rep := &report{}
			replayCapture(&driver{o: options{bucket: "bkt"}, got: got, rep: rep, countPasses: true}, c.protocol, gcsSynthetic, exchangeItems(xs), testLimits)
			return rep
		}
		if rep := run(xs); rep.Failed != 0 || rep.Passed != len(xs) || len(rep.Skipped) != 0 || len(rep.Results) != 0 {
			for _, r := range rep.Results {
				if !r.Pass {
					t.Errorf("%s %q: %s %+v", c.protocol, r.Name, r.Error, r.diff)
				}
			}
			t.Fatalf("%s: %d passed, %d failed, skipped %v", c.protocol, rep.Passed, rep.Failed, rep.Skipped)
		}
		changed := append([]capturedExchange(nil), xs...)
		body := ""
		if xs[1].Response.Body != nil {
			body = *xs[1].Response.Body
		}
		if c.gz {
			raw, _ := base64.StdEncoding.DecodeString(*xs[1].Response.BodyBase64)
			zr, _ := gzip.NewReader(bytes.NewReader(raw))
			b, _ := io.ReadAll(zr)
			body = string(b)
			changed[1].Response.BodyBase64 = nil
			var hs []capturedHeader
			for _, h := range xs[1].Response.Headers {
				if h.Name != "Content-Encoding" {
					hs = append(hs, h)
				}
			}
			changed[1].Response.Headers = hs
		}
		body = strings.Replace(body, "data/", "date/", 1)
		changed[1].Response.Body = &body
		rep := run(changed)
		if rep.Failed != 1 || len(rep.Results) != 1 || !strings.HasPrefix(rep.Results[0].Name, fmt.Sprintf("capture %d ", xs[1].Seq)) || rep.Results[0].Pass {
			t.Errorf("%s: a changed answer: %d failed", c.protocol, rep.Failed)
		}
		// A capture that starts mid-walk, and one with a continuation
		// before the answer that issued its token: the request is a
		// failure, never skipped, and the rest of its walk is skipped
		// naming it.
		orphan := append([]capturedExchange(nil), xs[1:]...)
		swapped := append([]capturedExchange{xs[1], xs[0]}, xs[2:]...)
		for _, cc := range []struct {
			name string
			xs   []capturedExchange
			pass int
		}{{"mid-walk", orphan, 0}, {"out of order", swapped, 1}} {
			rep := run(cc.xs)
			if rep.Failed != 1 || rep.Passed != cc.pass || len(rep.Skipped) != len(cc.xs)-1-cc.pass ||
				!strings.Contains(rep.Results[0].Error, "not issued by an earlier captured answer") ||
				!strings.HasPrefix(rep.Results[0].Name, fmt.Sprintf("capture %d ", xs[1].Seq)) {
				t.Errorf("%s %s: %d failed %d passed, skipped %v, %+v", c.protocol, cc.name, rep.Failed, rep.Passed, rep.Skipped, rep.Results)
			}
			for _, sk := range rep.Skipped {
				if !strings.Contains(sk, fmt.Sprintf("issued by capture %d, whose replay failed", xs[1].Seq)) {
					t.Errorf("%s %s: %s", c.protocol, cc.name, sk)
				}
			}
		}
	}
}

var testLimits = captureLimits{maxBody: 1 << 24, maxDecoded: 1 << 26, maxTokens: 1 << 20}

func captureLine(t *testing.T, x *capturedExchange) []byte {
	t.Helper()
	b, err := json.Marshal(x)
	if err != nil {
		t.Fatal(err)
	}
	return append(b, '\n')
}

// s3CaptureWalk captures a walk of data/ at page size 7 through a fake
// provider in front of a Go server holding keys.
func s3CaptureWalk(t *testing.T, keys []string) []capturedExchange {
	t.Helper()
	f := &fakeProvider{backend: startGoProtocols(t, []string{server.ProtocolS3}, keys...), tokenParam: "continuation-token",
		tokenRe: regexp.MustCompile(`(<NextContinuationToken>)([^<]*)<`), extra: http.Header{"X-Amz-Request-Id": {"R1"}, "Server": {"AmazonS3"}}, tokens: map[string]string{}}
	live := httptest.NewServer(f)
	t.Cleanup(live.Close)
	return captureWalk(t, live.URL, "/bkt", "list-type=2&prefix=data/&max-keys=7", "continuation-token", nextToken, false)
}

// TestCaptureFailedIssuerSkipsItsWalk: where the Go server issues no
// token for a page the provider continued, that page fails and every
// later page of the walk is skipped naming it, not reported as an orphan.
func TestCaptureFailedIssuerSkipsItsWalk(t *testing.T) {
	keys := liveKeys()
	xs := s3CaptureWalk(t, keys)
	got := &target{name: "go", base: startGoProtocols(t, []string{server.ProtocolS3}, keys[:5]...), client: &http.Client{}}
	rep := &report{}
	replayCapture(&driver{o: options{bucket: "bkt"}, got: got, rep: rep}, "s3", gcsSynthetic, exchangeItems(xs[:4]), testLimits)
	if rep.Failed != 1 || len(rep.Skipped) != 3 {
		t.Fatalf("%d failed, skipped %v", rep.Failed, rep.Skipped)
	}
	for _, sk := range rep.Skipped {
		if !strings.Contains(sk, "issued by capture 1, whose replay failed") {
			t.Error(sk)
		}
	}
}

// TestCaptureLimits: a captured body over -capture-max-body (a base64
// body measured by its decoded length, padding not counted), a gzip body
// that decodes past -capture-max-decoded, a line over the line limit, an
// exchange the recorder marked as over its own limit, and a token map
// past -capture-max-tokens each fail explicitly; the exchanges after a
// failing one (but the last) are still replayed.
func TestCaptureLimits(t *testing.T) {
	keys := liveKeys()
	xs := s3CaptureWalk(t, keys)
	got := &target{name: "go", base: startGoProtocols(t, []string{server.ProtocolS3}, keys...), client: &http.Client{}}
	run := func(items func(yield func(captureItem) bool), lim captureLimits) *report {
		rep := &report{}
		replayCapture(&driver{o: options{bucket: "bkt"}, got: got, rep: rep}, "s3", gcsSynthetic, items, lim)
		return rep
	}
	wantErr := func(name string, rep *report, i int, sub string) {
		t.Helper()
		if i >= len(rep.Results) || rep.Results[i].Pass || !strings.Contains(rep.Results[i].Error, sub) {
			t.Errorf("%s: want result %d failing with %q: %+v", name, i, sub, rep.Results)
		}
	}
	first := xs[0]
	body := len(*first.Response.Body)

	lim := testLimits
	lim.maxBody = int64(body - 1)
	rep := run(exchangeItems([]capturedExchange{first}), lim)
	wantErr("body", rep, 0, "-capture-max-body")
	lim.maxBody = int64(body)
	if rep := run(exchangeItems([]capturedExchange{first}), lim); rep.Passed != 1 {
		t.Errorf("a body at the limit: %+v", rep.Results)
	}

	// A base64 body is limited by its decoded length, padding not
	// counted: binary bodies of one, two and three bytes past a multiple
	// of three (two, one and no padding characters), the default limit's
	// size among them, pass at the limit and fail one byte over it.
	for _, n := range []int{1, 2, 3, 4, 5, 6, 16 << 20} {
		bin := bytes.Repeat([]byte{0xff, 0x00, 0x80}, n/3+1)[:n]
		enc := base64.StdEncoding.EncodeToString(bin)
		x := first
		x.Response.Body, x.Response.BodyBase64 = nil, &enc
		lim := testLimits
		lim.maxBody = int64(n)
		if r, err := capturedResponse(&x, lim); err != nil || !bytes.Equal(r.Body, bin) {
			t.Errorf("a %d-byte base64 body (%d characters) at the limit: %v", n, len(enc), err)
		}
		lim.maxBody = int64(n - 1)
		if _, err := capturedResponse(&x, lim); err == nil || !strings.Contains(err.Error(), "-capture-max-body") {
			t.Errorf("a %d-byte base64 body over a limit of %d: %v", n, n-1, err)
		}
	}
	// The listing itself, base64-encoded at its own size, replays.
	b64first := first
	enc := base64.StdEncoding.EncodeToString([]byte(*first.Response.Body))
	b64first.Response.Body, b64first.Response.BodyBase64 = nil, &enc
	lim.maxBody = int64(body)
	if rep := run(exchangeItems([]capturedExchange{b64first}), lim); rep.Passed != 1 {
		t.Errorf("a base64 body (%d bytes, %d characters) at the limit: %+v", body, len(enc), rep.Results)
	}

	var bomb bytes.Buffer
	zw := gzip.NewWriter(&bomb)
	zw.Write(make([]byte, 4<<20))
	zw.Close()
	gz := first
	b64 := base64.StdEncoding.EncodeToString(bomb.Bytes())
	gz.Response.Body, gz.Response.BodyBase64 = nil, &b64
	gz.Response.Headers = append(append([]capturedHeader(nil), first.Response.Headers...), capturedHeader{"Content-Encoding", "gzip"})
	lim = testLimits
	lim.maxDecoded = 1 << 20
	wantErr("decoded", run(exchangeItems([]capturedExchange{gz, first}), lim), 0, "-capture-max-decoded")

	over := first
	over.LimitExceeded = "the answer's body is over 1000 bytes"
	over.Response.Body = nil
	rep = run(exchangeItems([]capturedExchange{over, first}), testLimits)
	wantErr("recorder", rep, 0, "body limit was exceeded")
	if rep.Passed != 1 {
		t.Errorf("after the recorder's limit: %d passed", rep.Passed)
	}

	var file bytes.Buffer
	file.Write(captureLine(t, &first))
	file.WriteString(`{"seq":2,"request":{"path":"` + strings.Repeat("x", 3<<20) + `"}}` + "\n")
	second := first
	second.Seq = 3
	file.Write(captureLine(t, &second))
	rep = run(captureLines(&file, 1<<20), testLimits)
	if rep.Passed != 2 || rep.Failed != 1 {
		t.Errorf("a long line: %+v", rep.Results)
	}
	wantErr("line", rep, 1, "line 2 is over 1048576 bytes")

	lim = testLimits
	lim.maxTokens = 1
	rep = run(exchangeItems(xs), lim)
	if rep.Compared != 2 || rep.Passed != 1 {
		t.Errorf("tokens: %d compared %d passed", rep.Compared, rep.Passed)
	}
	wantErr("tokens", rep, 1, "-capture-max-tokens")
}

// TestCaptureReportIsBounded: a capture full of resets, exchanges over
// the recorder's limit and changed answers keeps every count exact but
// details at most -capture-max-details failures and as many skips, each
// quoting at most a clipped query, and says how many it does not show.
func TestCaptureReportIsBounded(t *testing.T) {
	keys := liveKeys()
	xs := s3CaptureWalk(t, keys)
	got := &target{name: "go", base: startGoProtocols(t, []string{server.ProtocolS3}, keys...), client: &http.Client{}}
	const n = 30
	var stream []capturedExchange
	long := "list-type=2&prefix=" + strings.Repeat("p", 64<<10)
	for i := range n {
		reset := xs[0]
		reset.Response.Status, reset.Response.Body = 0, nil
		reset.Request.Query = long
		over := xs[0]
		over.LimitExceeded, over.Response.Body = "the answer's body is over 1000 bytes", nil
		changed := xs[0]
		body := strings.Replace(*xs[0].Response.Body, "data/", "date/", 1)
		changed.Response.Body = &body
		for j, x := range []capturedExchange{reset, over, changed} {
			x.Seq = uint64(3*i + j + 1)
			stream = append(stream, x)
		}
	}
	for i, x := range xs {
		x.Seq = uint64(3*n + i + 1)
		stream = append(stream, x)
	}
	for _, max := range []int{0, 5} {
		lim := testLimits
		lim.maxDetails = max
		d := &driver{o: options{bucket: "bkt"}, got: got, rep: &report{}, countPasses: true}
		replayCapture(d, "s3", gcsSynthetic, exchangeItems(stream), lim)
		rep := d.rep
		shown, skips := 2*n, n
		if max > 0 {
			shown, skips = max, max
		}
		if rep.Failed != 2*n || rep.Passed != len(xs) || rep.SkippedCount != n ||
			len(rep.Results) != shown || len(rep.Skipped) != skips ||
			rep.FailuresNotShown != 2*n-shown || rep.SkipsNotShown != n-skips {
			t.Fatalf("max %d: %d failed (%d shown, %d not), %d passed, %d skipped (%d shown, %d not)", max, rep.Failed, len(rep.Results),
				rep.FailuresNotShown, rep.Passed, rep.SkippedCount, len(rep.Skipped), rep.SkipsNotShown)
		}
		for _, sk := range rep.Skipped {
			if len(sk) > 2<<10 || !strings.Contains(sk, fmt.Sprintf("(%d bytes)", len(long))) {
				t.Errorf("max %d: a skip of %d bytes: %.200s", max, len(sk), sk)
			}
		}
		text := string(d.summary())
		for _, want := range []string{
			fmt.Sprintf("%d failed, %d skipped", 2*n, n),
			fmt.Sprintf("%d more failures not shown (-capture-max-details %d)\n", 2*n-shown, max),
			fmt.Sprintf("%d more skips not shown (-capture-max-details %d)\n", n-skips, max),
			fmt.Sprintf("(%d failures and %d skips not shown)\n", 2*n-shown, n-skips),
		} {
			if strings.Contains(text, want) != (max > 0 || strings.HasPrefix(want, fmt.Sprint(2*n))) {
				t.Errorf("max %d: the summary has %q: %v\n%s", max, want, strings.Contains(text, want), text)
			}
		}
		if c := strings.Count(text, "\nFAIL "); c != shown {
			t.Errorf("max %d: %d failures in the summary, want %d", max, c, shown)
		}
	}
}

// TestCaptureStreams: the replay reads a capture one exchange at a time:
// the first exchange reaches the Go server before the second line is
// written.
func TestCaptureStreams(t *testing.T) {
	keys := liveKeys()
	xs := s3CaptureWalk(t, keys)
	backend := startGoProtocols(t, []string{server.ProtocolS3}, keys...)
	seen := make(chan struct{}, len(xs))
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- struct{}{}
		resp, err := http.Get(backend + r.RequestURI)
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		defer resp.Body.Close()
		for k, v := range resp.Header {
			w.Header()[k] = v
		}
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
	}))
	defer front.Close()
	pr, pw := io.Pipe()
	go func() {
		defer pw.Close()
		for i := range xs {
			pw.Write(captureLine(t, &xs[i]))
			select {
			case <-seen:
			case <-time.After(5 * time.Second):
				t.Errorf("exchange %d was not replayed before the next line was written", i+1)
				return
			}
		}
	}()
	rep := &report{}
	replayCapture(&driver{o: options{bucket: "bkt"}, got: &target{name: "go", base: front.URL, client: &http.Client{}}, rep: rep},
		"s3", gcsSynthetic, captureLines(pr, 1<<20), testLimits)
	if rep.Passed != len(xs) {
		t.Errorf("%d of %d passed: %+v", rep.Passed, len(xs), rep.Results)
	}
}

// testRecorder stands for bench/faultproxy -record in front of a
// provider: each exchange is kept, in the -record shape, before its
// answer is released; a request's X-Record-* headers are kept as the
// exchange's annotations and not forwarded; and the answer released
// carries the X-Content-Type-Options the proxy sets.
type testRecorder struct {
	next http.Handler
	mu   sync.Mutex
	xs   []capturedExchange
}

func (rec *testRecorder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var x capturedExchange
	for k, vs := range r.Header {
		if strings.HasPrefix(k, "X-Record-") {
			for _, v := range vs {
				x.Annotations = append(x.Annotations, capturedHeader{k, v})
			}
			delete(r.Header, k)
		}
	}
	path, query, _ := strings.Cut(r.RequestURI, "?")
	x.Request.Method, x.Request.Path, x.Request.Query = r.Method, path, query
	rw := httptest.NewRecorder()
	rec.next.ServeHTTP(rw, r)
	x.Response.Status = rw.Code
	for k, vs := range rw.Header() {
		for _, v := range vs {
			x.Response.Headers = append(x.Response.Headers, capturedHeader{k, v})
		}
	}
	body := rw.Body.String()
	x.Response.Body = &body
	rec.mu.Lock()
	x.Seq = uint64(len(rec.xs) + 1)
	rec.xs = append(rec.xs, x)
	rec.mu.Unlock()
	maps.Copy(w.Header(), rw.Header())
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(rw.Code)
	io.WriteString(w, body)
}

// TestLiveMatrixRecordedReplaysOffline: the live matrix (provider cases,
// walks, token cases with altered and invented tokens, the scripted
// matrix) sent once through a recording proxy with -live-record replays
// from the recording alone with the live run's verdicts: no failure the
// live run did not have, each live failure again a failure, and the
// documented differences documented in both. Every exchange is
// accounted for (compared, documented or skipped), and a second replay
// gives the same report. The same recording without its annotations
// fails the requests whose tokens no answer issued.
func TestLiveMatrixRecordedReplaysOffline(t *testing.T) {
	keys := liveKeys()
	for _, protocol := range []string{"s3", "gcs"} {
		rec := &testRecorder{}
		live := runFakeLiveThrough(t, protocol, nil, rec, true, nil)
		if live.Compared < 50 || len(rec.xs) < live.Requests/2 {
			t.Fatalf("%s: live compared %d, %d requests, %d recorded", protocol, live.Compared, live.Requests, len(rec.xs))
		}
		liveFailed := map[string]string{}
		for _, r := range live.Results {
			if !r.Pass {
				liveFailed[r.Name] = r.Detail
			}
		}
		protocols := []string{server.ProtocolS3}
		if protocol == "gcs" {
			protocols = []string{server.ProtocolGCS}
		}
		got := &target{name: "go", base: startGoProtocols(t, protocols, keys...), client: &http.Client{}}
		replay := func(xs []capturedExchange) *report {
			rep := &report{}
			replayCapture(&driver{o: options{bucket: "bkt"}, got: got, rep: rep, countPasses: true}, protocol, gcsSynthetic, exchangeItems(xs), testLimits)
			return rep
		}
		rep := replay(rec.xs)
		if n := rep.Compared + len(rep.Residuals) + rep.SkippedCount; n != len(rec.xs) {
			t.Errorf("%s: %d exchanges, %d accounted for", protocol, len(rec.xs), n)
		}
		caseOf := func(line string) string { return strings.SplitN(strings.SplitN(line, "[", 2)[1], "]", 2)[0] }
		capFailed := map[string]string{}
		for _, r := range rep.Results {
			capFailed[caseOf(r.Name)] = r.Detail
			if r.Error != "" {
				t.Errorf("%s %s: %s", protocol, r.Name, r.Error)
			}
		}
		if fmt.Sprint(capFailed) != fmt.Sprint(liveFailed) {
			t.Errorf("%s: the replay failed %v, the live run %v", protocol, capFailed, liveFailed)
		}
		var liveDoc, capDoc []string
		for _, l := range live.Residuals {
			liveDoc = append(liveDoc, strings.SplitN(l, ":", 2)[0])
		}
		for _, l := range rep.Residuals {
			capDoc = append(capDoc, caseOf(l))
		}
		// The live run fills its list in worker-completion order, the replay in
		// recording order: compare them sorted.
		slices.Sort(liveDoc)
		slices.Sort(capDoc)
		if fmt.Sprint(liveDoc) != fmt.Sprint(capDoc) {
			t.Errorf("%s: documented in the replay %v, in the live run %v", protocol, capDoc, liveDoc)
		}
		// A case the live run skipped was never sent, so never recorded.
		if len(rep.Skipped) != 0 {
			t.Errorf("%s: skipped %v", protocol, rep.Skipped)
		}
		again := replay(rec.xs)
		strip := func(r *report) string { b, _ := json.Marshal(r); return string(b) }
		if strip(again) != strip(rep) {
			t.Errorf("%s: a second replay differs", protocol)
		}
		bare := make([]capturedExchange, len(rec.xs))
		copy(bare, rec.xs)
		for i := range bare {
			bare[i].Annotations = nil
		}
		orphans := 0
		for _, r := range replay(bare).Results {
			if strings.Contains(r.Error, "not issued by an earlier captured answer") {
				orphans++
			}
		}
		if orphans == 0 {
			t.Errorf("%s: without annotations no request's token was unknown", protocol)
		}
		t.Logf("%s: %d exchanges: %d compared (%d passed, %d failed), %d documented, %d skipped; live %d failed, %d documented",
			protocol, len(rec.xs), rep.Compared, rep.Passed, rep.Failed, len(rep.Residuals), rep.SkippedCount, live.Failed, len(live.Residuals))
	}
}

// TestPinnedDifferencesAreDocumentedInBoth: a provider whose later pages
// name another object fails the walks at their second page and the
// token cases; with each of those mismatches pinned for its case, the
// live run documents every one (a walk at the page that differs) and
// fails none, and the replay of its recording documents the same
// exchanges, with the same pins, and fails none.
func TestPinnedDifferencesAreDocumentedInBoth(t *testing.T) {
	edit := func(query string, body []byte) []byte {
		if strings.Contains(query, "continuation-token=") {
			return bytes.Replace(body, []byte("<Key>data/"), []byte("<Key>date/"), 1)
		}
		return body
	}
	first := runFakeLiveThrough(t, "s3", edit, &testRecorder{}, false, nil)
	pins := map[string]pin{}
	walks := 0
	for _, r := range first.Results {
		if !r.Pass && r.diff != nil {
			pins[r.Name] = testPin(t, "s3", r)
			if r.Kind == "walk" {
				walks++
			}
		}
	}
	if walks == 0 || len(pins) != first.Failed {
		t.Fatalf("the edit failed %d cases, %d walks: %v", first.Failed, walks, pins)
	}
	rec := &testRecorder{}
	live := runFakeLiveThrough(t, "s3", edit, rec, false, pins)
	if live.Failed != 0 || len(live.Residuals) != len(pins)+1 { // and s3-07x
		t.Errorf("live: %d failed, documented %q", live.Failed, live.Residuals)
	}
	got := &target{name: "go", base: startGoProtocols(t, []string{server.ProtocolS3}, liveKeys()...), client: &http.Client{}}
	rep := &report{}
	replayCapture(&driver{o: options{bucket: "bkt"}, got: got, rep: rep, countPasses: true, pins: pins}, "s3", gcsSynthetic, exchangeItems(rec.xs), testLimits)
	if rep.Failed != 0 || len(rep.Residuals) != len(live.Residuals) {
		for _, r := range rep.Results {
			t.Errorf("replay: %s %s", r.Name, r.Detail)
		}
		t.Errorf("replay: %d failed, documented %q", rep.Failed, rep.Residuals)
	}
}

// testPin pins a failed result's mismatch for its case as readPins binds
// a pin to the evidence, the shape being the request's own query: a
// matrix case's token and mutation, any token for a walk.
func testPin(t *testing.T, protocol string, r result) pin {
	t.Helper()
	_, q, _ := queryToken(r.Target, tokenParamOf(protocol))
	p := pin{mismatch: r.Detail, shape: regexp.MustCompile("^" + regexp.QuoteMeta(q) + "$"), refStatus: r.diff.Status[0], token: pinAnyToken}
	cases := s3ProviderCases()
	if protocol == "gcs" {
		cases = gcsProviderCases()
	}
	if i := slices.IndexFunc(cases, func(c providerCase) bool { return c.ID == r.Name }); i >= 0 {
		switch c := cases[i]; {
		case c.Mutation == "tampered":
			p.token = pinTampered
		case c.TokenFrom != "":
			p.token, p.mutation = pinSourced, c.Mutation
		case c.Kind != "walk":
			p.token = pinNoToken
		}
	}
	return p
}

// TestCaptureTokenIssuedByTwoScopes: a provider can issue the same token
// to requests of different scope (a GCS token names the object it ends
// at), where the Go server's tokens differ and each is refused with the
// other's scope. Two walks whose first pages issue one token, their
// pages interleaved, each continue with the Go token their own first
// page issued, and pass.
func TestCaptureTokenIssuedByTwoScopes(t *testing.T) {
	keys := liveKeys()
	re := regexp.MustCompile(`("nextPageToken":")([^"]*)"`)
	f := &fakeProvider{backend: startGoProtocols(t, []string{server.ProtocolGCS}, keys...), tokenParam: "pageToken", tokenRe: re,
		extra: http.Header{"X-Guploader-Uploadid": {"U1"}, "Server": {"UploadServer"}}, tokens: map[string]string{}}
	live := httptest.NewServer(f)
	defer live.Close()
	path := "/storage/v1/b/bkt/o"
	a := captureWalk(t, live.URL, path, "prefix=data/small/&maxResults=3", "pageToken", gcsNextToken, false)
	b := captureWalk(t, live.URL, path, "prefix=data/tree/&maxResults=3", "pageToken", gcsNextToken, false)
	same := func(w []capturedExchange) {
		tok := gcsNextToken([]byte(*w[0].Response.Body))
		body := strings.Replace(*w[0].Response.Body, tok, "SAME", 1)
		w[0].Response.Body = &body
		w[0].Response.Headers = slices.DeleteFunc(w[0].Response.Headers, func(h capturedHeader) bool { return h.Name == "Content-Length" })
		w[1].Request.Query = strings.Replace(w[1].Request.Query, "pageToken="+campaignEscape(tok, ""), "pageToken=SAME", 1)
	}
	same(a)
	same(b)
	xs := []capturedExchange{a[0], b[0], a[1], b[1]}
	for i := range xs {
		xs[i].Seq = uint64(i + 1)
	}
	got := &target{name: "go", base: startGoProtocols(t, []string{server.ProtocolGCS}, keys...), client: &http.Client{}}
	rep := &report{}
	replayCapture(&driver{o: options{bucket: "bkt"}, got: got, rep: rep, countPasses: true}, "gcs", gcsSynthetic, exchangeItems(xs), testLimits)
	if rep.Passed != 4 || rep.Failed != 0 {
		for _, r := range rep.Results {
			t.Errorf("%s: %s %s", r.Name, r.Error, r.Detail)
		}
		t.Fatalf("%d passed, %d failed", rep.Passed, rep.Failed)
	}
}

// TestTokenMapLookup: a token issued from several requests resolves, for
// a request annotated with its source walk, to that walk's entry or to
// none, whatever the request's own query issued; for one with no source,
// to the entry of the same query (its own case's where several differ);
// never to the latest: entries that would send different Go tokens fail
// as ambiguous, and a token no matching request issued fails. Re-issuing
// from the same request replaces its entry and adds none.
func TestTokenMapLookup(t *testing.T) {
	tm := &tokenMap{m: map[string][]capturedToken{}}
	tm.set("T", capturedToken{got: "a", query: "q=a", caseID: "walk a"})
	tm.set("T", capturedToken{got: "b", query: "q=b", caseID: "walk b"})
	tm.set("T", capturedToken{got: "c", query: "q=c", caseID: "walk c"})
	tm.set("T", capturedToken{got: "a2", query: "q=a", caseID: "walk a2"})
	tm.set("T", capturedToken{got: "s", query: "q=s", caseID: "walk s1"})
	tm.set("T", capturedToken{got: "s", query: "q=s", caseID: "walk s2"})
	if tm.grows("T", "q=b", "walk b") || !tm.grows("T", "q=b", "walk d") {
		t.Error("grows")
	}
	if tm.set("T", capturedToken{got: "b2", query: "q=b", caseID: "walk b"}); tm.n != 6 {
		t.Errorf("%d entries", tm.n)
	}
	for _, c := range []struct{ query, caseID, from, want string }{
		{"q=b", "walk b", "", "b2"},
		{"q=a", "walk a", "walk c", "c"}, // the source, not the same query
		{"q=x", "case", "walk a", "a"},
		{"q=a", "walk a", "", "a"}, // the own case's among differing entries
		{"q=a", "walk a2", "", "a2"},
		{"q=s", "case", "", "s"},      // entries that agree
		{"q=a", "case", "", ""},       // ambiguous
		{"q=x", "case", "", ""},       // no request of the query issued it
		{"q=x", "case", "walk z", ""}, // the source issued none
		{"q=b", "walk b", "walk z", ""},
	} {
		got, err := tm.lookup("T", c.query, c.caseID, c.from)
		if c.want == "" && err == nil || c.want != "" && (err != nil || got.got != c.want) {
			t.Errorf("lookup %s %s from %q: %q %v", c.query, c.caseID, c.from, got.got, err)
		}
	}
	// Another page size of the same query continues its walk; two such
	// issuers whose Go tokens differ are ambiguous.
	tm.pageParam = "maxResults"
	tm.set("P", capturedToken{got: "p", query: "prefix=p&maxResults=3"})
	if got, err := tm.lookup("P", "prefix=p&maxResults=2", "", ""); err != nil || got.got != "p" {
		t.Errorf("another page size: %q %v", got.got, err)
	}
	if _, err := tm.lookup("P", "prefix=q&maxResults=2", "", ""); err == nil {
		t.Error("another prefix continued the walk")
	}
	tm.set("P", capturedToken{got: "p4", query: "prefix=p&maxResults=4"})
	if got, err := tm.lookup("P", "prefix=p&maxResults=2", "", ""); err == nil {
		t.Errorf("two page sizes with differing Go tokens: %q", got.got)
	}
	if got, err := tm.lookup("P", "prefix=p&maxResults=4", "", ""); err != nil || got.got != "p4" {
		t.Errorf("the exact query: %q %v", got.got, err)
	}
	if _, err := tm.lookup("U", "q=a", "", ""); !errors.Is(err, errNotIssued) {
		t.Errorf("an unknown token: %v", err)
	}
}

// TestCaptureMutationAnnotationIsVerified: an annotation that says a
// request's token is an issued token altered is a claim about the
// captured request, held to it: the alteration of the named token must
// be exactly the token the request carried. A forged flip-decoded
// annotation on a continuation whose captured answer is the provider's
// next page fails the exchange (it would otherwise send an altered Go
// token and document the refusal against a captured 200), as does a
// verbatim annotation on a token an earlier answer issued; the genuine
// annotation, on the token so altered, is replayed and documented.
func TestCaptureMutationAnnotationIsVerified(t *testing.T) {
	keys := liveKeys()
	xs := s3CaptureWalk(t, keys)[:2]
	issued := nextToken([]byte(*xs[0].Response.Body))
	flipped, err := mutateToken(issued, "flip-decoded")
	if err != nil {
		t.Fatal(err)
	}
	annotate := func(token, carried string) capturedExchange {
		x := xs[1]
		x.Annotations = []capturedHeader{{recordCaseHeader, "s3-07x"}, {recordTokenHeader, token}}
		x.Request.Query = strings.Replace(x.Request.Query, "continuation-token="+campaignEscape(issued, ""),
			"continuation-token="+campaignEscape(carried, ""), 1)
		return x
	}
	got := &target{name: "go", base: startGoProtocols(t, []string{server.ProtocolS3}, keys...), client: &http.Client{}}
	for _, c := range []struct {
		name string
		x    capturedExchange
		want string // the failure; "" for documented
	}{
		{"forged flip-decoded", annotate(tokenNote("flip-decoded", issued), issued), "annotation does not match the captured token"},
		{"forged truncate", annotate(tokenNote("truncate", issued), flipped), "annotation does not match the captured token"},
		{"forged verbatim", annotate(tokenVerbatim, issued), "annotation does not match the captured token"},
		{"genuine flip-decoded", annotate(tokenNote("flip-decoded", issued), flipped), ""},
	} {
		rep := &report{}
		replayCapture(&driver{o: options{bucket: "bkt"}, got: got, rep: rep, countPasses: true}, "s3", gcsSynthetic,
			exchangeItems([]capturedExchange{xs[0], c.x}), testLimits)
		switch {
		case c.want == "" && (len(rep.Residuals) != 1 || rep.Failed != 0):
			t.Errorf("%s: documented %q, %d failed %+v", c.name, rep.Residuals, rep.Failed, rep.Results)
		case c.want != "" && (len(rep.Residuals) != 0 || len(rep.Results) != 1 || !strings.Contains(rep.Results[0].Error, c.want)):
			t.Errorf("%s: documented %q, results %+v", c.name, rep.Residuals, rep.Results)
		}
	}
}

// TestCaptureTokenFromNamedWalk: two GCS walks of one prefix, one from a
// startOffset that lands where the other starts, issue the same provider
// token at their first page, where the Go server's tokens differ (each
// bound to its walk's scope). A request in walk B's scope annotated as
// sending walk A's token is replayed with A's Go token, whatever B
// issued: the provider's 200 to it then fails (the Go server refuses A's
// token in B's scope), and a captured scope refusal passes. Before, the
// same-query entry won and B's token turned the scope question into a
// plain continuation.
func TestCaptureTokenFromNamedWalk(t *testing.T) {
	keys := liveKeys()
	re := regexp.MustCompile(`("nextPageToken":")([^"]*)"`)
	f := &fakeProvider{backend: startGoProtocols(t, []string{server.ProtocolGCS}, keys...), tokenParam: "pageToken", tokenRe: re,
		extra: http.Header{"X-Guploader-Uploadid": {"U1"}, "Server": {"UploadServer"}}, tokens: map[string]string{}}
	live := httptest.NewServer(f)
	defer live.Close()
	path := "/storage/v1/b/bkt/o"
	qa, qb := "prefix=data/small/&maxResults=3", "prefix=data/small/&maxResults=3&startOffset=data/small/f00.csv"
	a := captureWalk(t, live.URL, path, qa, "pageToken", gcsNextToken, false)
	b := captureWalk(t, live.URL, path, qb, "pageToken", gcsNextToken, false)
	tokA := gcsNextToken([]byte(*a[0].Response.Body))
	// The provider's answer to A's token in B's scope: the backend's
	// refusal, as a provider that binds its tokens would answer.
	refusal := captureWalk(t, live.URL, path, qb+"&pageToken="+campaignEscape(tokA, ""), "pageToken", gcsNextToken, false)[0]
	if refusal.Response.Status != 400 {
		t.Fatalf("A's token in B's scope: %d %s", refusal.Response.Status, *refusal.Response.Body)
	}
	same := func(x *capturedExchange) {
		tok := gcsNextToken([]byte(*x.Response.Body))
		body := strings.Replace(*x.Response.Body, tok, "SAME", 1)
		x.Response.Body = &body
		x.Response.Headers = slices.DeleteFunc(slices.Clone(x.Response.Headers), func(h capturedHeader) bool { return h.Name == "Content-Length" })
	}
	same(&a[0])
	same(&b[0])
	a[0].Annotations = []capturedHeader{{recordCaseHeader, "walk-a"}}
	b[0].Annotations = []capturedHeader{{recordCaseHeader, "walk-b"}}
	got := &target{name: "go", base: startGoProtocols(t, []string{server.ProtocolGCS}, keys...), client: &http.Client{}}
	for _, c := range []struct {
		name   string
		answer capturedExchange
		pass   bool
	}{{"the provider's page", b[1], false}, {"a scope refusal", refusal, true}} {
		x := c.answer
		x.Request.Query = qb + "&pageToken=SAME"
		x.Annotations = []capturedHeader{{recordCaseHeader, "scope-case"}, {recordFromHeader, "walk-a"}}
		xs := []capturedExchange{a[0], b[0], x}
		for i := range xs {
			xs[i].Seq = uint64(i + 1)
		}
		rep := &report{}
		replayCapture(&driver{o: options{bucket: "bkt"}, got: got, rep: rep, countPasses: true}, "gcs", gcsSynthetic, exchangeItems(xs), testLimits)
		wantFailed := 1
		if c.pass {
			wantFailed = 0
		}
		if rep.Passed != 3-wantFailed || rep.Failed != wantFailed {
			for _, r := range rep.Results {
				t.Errorf("%s: %s: %s %s", c.name, r.Name, r.Error, r.Detail)
			}
			t.Errorf("%s: %d passed, %d failed", c.name, rep.Passed, rep.Failed)
		}
	}
	// A source walk that issued no such token fails the request; so does
	// an unannotated request whose own walk issued none.
	for _, notes := range [][]capturedHeader{{{recordCaseHeader, "scope-case"}, {recordFromHeader, "walk-c"}}, {{recordCaseHeader, "scope-case"}}} {
		x := b[1]
		x.Seq, x.Request.Query, x.Annotations = 3, "prefix=data/&maxResults=3&pageToken=SAME", notes
		rep := &report{}
		replayCapture(&driver{o: options{bucket: "bkt"}, got: got, rep: rep, countPasses: true}, "gcs", gcsSynthetic,
			exchangeItems([]capturedExchange{a[0], b[0], x}), testLimits)
		if rep.Failed != 1 || len(rep.Results) != 1 || rep.Results[0].Error == "" {
			t.Errorf("%v: %d failed, %+v", notes, rep.Failed, rep.Results)
		}
	}
}

// TestPinsBindTheRecordedRequest: a pin documents its mismatch only for a
// request of the shape the evidence captured for its case, answered as
// the evidence's provider answered. Over the Go server, the pinned
// includeTrailingDelimiter=true question refused against a captured 200
// is documented, and so is the pinned reason pair of a refused
// projection; an unrelated request labelled gcs-03p whose refusal gives
// the same message fails.
func TestPinsBindTheRecordedRequest(t *testing.T) {
	pins, err := readPins(writeEvidence(t, "gcs", "probe/r1/", map[string]evidenceCapture{
		"gcs-03p": {query: "prefix=probe/r1/d/&delimiter=/&includeTrailingDelimiter=true", status: 200, body: `{"kind":"storage#objects"}`,
			mismatch: "GCS HTTP status differs: expected=200 actual=400"},
		"gcs-11c": {query: "prefix=probe/r1/s/&projection=bogus", status: 400, body: gcsRefusalBody(400, "invalidParameter"),
			mismatch: "GCS error reason differs: expected=invalidParameter actual=invalid"},
	}))
	if err != nil {
		t.Fatal(err)
	}
	page := `{"kind":"storage#objects"}`
	exchange := func(seq uint64, caseID, query string, status int, body string) capturedExchange {
		var x capturedExchange
		x.Seq, x.Annotations = seq, []capturedHeader{{recordCaseHeader, caseID}}
		x.Request.Method, x.Request.Path, x.Request.Query = "GET", "/storage/v1/b/bkt/o", query
		x.Request.Headers = []capturedHeader{{"Accept", "application/json"}}
		x.Response.Status, x.Response.Body = status, &body
		x.Response.Headers = []capturedHeader{{"Content-Type", "application/json; charset=UTF-8"}}
		return x
	}
	xs := []capturedExchange{
		exchange(1, "gcs-03p", "prefix=data/tree/&delimiter=/&includeTrailingDelimiter=true", 200, page),
		exchange(2, "gcs-11c", "prefix=data/small/&projection=bogus", 400, gcsRefusalBody(400, "invalidParameter")),
		exchange(3, "gcs-03p", "prefix=data/tree/&delimiter=/&projection=bogus", 200, page),
	}
	got := &target{name: "go", base: startGoProtocols(t, []string{server.ProtocolGCS}, liveKeys()...), client: &http.Client{}}
	rep := &report{}
	replayCapture(&driver{o: options{bucket: "bkt"}, got: got, rep: rep, countPasses: true, pins: pins}, "gcs", gcsSynthetic, exchangeItems(xs), testLimits)
	if len(rep.Residuals) != 2 || !strings.HasPrefix(rep.Residuals[0], "capture 1 [gcs-03p]") || !strings.HasPrefix(rep.Residuals[1], "capture 2 [gcs-11c]") {
		t.Errorf("documented %q", rep.Residuals)
	}
	if rep.Failed != 1 || len(rep.Results) != 1 || !strings.HasPrefix(rep.Results[0].Name, "capture 3 [gcs-03p]") ||
		rep.Results[0].Detail != "GCS HTTP status differs: expected=200 actual=400" {
		t.Errorf("%d failed: %+v", rep.Failed, rep.Results)
	}
}

// TestSourcedPinNeedsAnIssuedToken: a pin for a case that sends a token
// its source walk issued applies only where the replay mapped such a
// token. A mid-walk exchange labelled gcs-07i and annotated verbatim
// sends the provider's unmapped token to the Go server, which refuses
// it: the pinned 200 -> 400 must not document that (it would hide the
// missing issuer), so the exchange fails. The same pin documents the
// case's request when its token is the mapped one its source issued.
func TestSourcedPinNeedsAnIssuedToken(t *testing.T) {
	status := "GCS HTTP status differs: expected=200 actual=400"
	pins, err := readPins(writeEvidence(t, "gcs", "probe/r1/", map[string]evidenceCapture{
		"gcs-07i": {query: "prefix=probe/r1/x/&maxResults=2&pageToken=TOK", status: 200, body: `{"kind":"storage#objects"}`, mismatch: status},
	}))
	if err != nil {
		t.Fatal(err)
	}
	keys := liveKeys()
	f := &fakeProvider{backend: startGoProtocols(t, []string{server.ProtocolGCS}, keys...), tokenParam: "pageToken",
		tokenRe: regexp.MustCompile(`("nextPageToken":")([^"]*)"`), extra: http.Header{"X-Guploader-Uploadid": {"U1"}, "Server": {"UploadServer"}},
		tokens: map[string]string{}}
	live := httptest.NewServer(f)
	defer live.Close()
	walk := captureWalk(t, live.URL, "/storage/v1/b/bkt/o", "prefix=data/small/&maxResults=2", "pageToken", gcsNextToken, false)
	tok := gcsNextToken([]byte(*walk[0].Response.Body))
	source := walk[0]
	source.Annotations = []capturedHeader{{recordCaseHeader, "gcs-07q"}}
	sourced := walk[1]
	sourced.Seq, sourced.Request.Query = 2, "prefix=data/tree/&maxResults=2&pageToken="+campaignEscape(tok, "")
	sourced.Annotations = []capturedHeader{{recordCaseHeader, "gcs-07i"}, {recordFromHeader, "gcs-07q"}}
	orphan := sourced
	orphan.Seq, orphan.Annotations = 1, []capturedHeader{{recordCaseHeader, "gcs-07i"}, {recordTokenHeader, tokenVerbatim}}
	got := &target{name: "go", base: startGoProtocols(t, []string{server.ProtocolGCS}, keys...), client: &http.Client{}}
	replay := func(xs ...capturedExchange) *report {
		rep := &report{}
		replayCapture(&driver{o: options{bucket: "bkt"}, got: got, rep: rep, countPasses: true, pins: pins}, "gcs", gcsSynthetic, exchangeItems(xs), testLimits)
		return rep
	}
	if rep := replay(orphan); len(rep.Residuals) != 0 || rep.Failed != 1 || rep.Results[0].Detail != status {
		t.Errorf("verbatim provider token: documented %q, %d failed %+v", rep.Residuals, rep.Failed, rep.Results)
	}
	if rep := replay(source, sourced); len(rep.Residuals) != 1 || !strings.HasPrefix(rep.Residuals[0], "capture 2 [gcs-07i]") || rep.Failed != 0 {
		t.Errorf("mapped token: documented %q, %d failed %+v", rep.Residuals, rep.Failed, rep.Results)
	}
}

// TestCaptureWalkChangesPageSize: a client may continue a walk with
// another page size (the GCS and S3 token contracts accept the same scope
// with another maxResults or max-keys). An unannotated captured walk
// whose pages ask 3, then 2, then 4 names replays with each token mapped
// and passes; two issuers of one token in that scope with different Go
// tokens still fail as ambiguous (TestTokenMapLookup).
func TestCaptureWalkChangesPageSize(t *testing.T) {
	keys := liveKeys()
	for _, c := range []struct {
		protocol, path, query, size, param string
		next                               func([]byte) string
		re                                 *regexp.Regexp
	}{
		{"gcs", "/storage/v1/b/bkt/o", "prefix=data/small/", "maxResults", "pageToken", gcsNextToken, regexp.MustCompile(`("nextPageToken":")([^"]*)"`)},
		{"s3", "/bkt", "list-type=2&prefix=data/small/", "max-keys", "continuation-token", nextToken, regexp.MustCompile(`(<NextContinuationToken>)([^<]*)<`)},
	} {
		protocols, extra := []string{server.ProtocolS3}, http.Header{"X-Amz-Request-Id": {"R1"}, "Server": {"AmazonS3"}}
		if c.protocol == "gcs" {
			protocols, extra = []string{server.ProtocolGCS}, http.Header{"X-Guploader-Uploadid": {"U1"}, "Server": {"UploadServer"}}
		}
		f := &fakeProvider{backend: startGoProtocols(t, protocols, keys...), tokenParam: c.param, tokenRe: c.re, extra: extra, tokens: map[string]string{}}
		live := httptest.NewServer(f)
		defer live.Close()
		var xs []capturedExchange
		token := ""
		for page, size := range []int{3, 2, 4} {
			q := fmt.Sprintf("%s&%s=%d", c.query, c.size, size)
			if token != "" {
				q += "&" + c.param + "=" + campaignEscape(token, "")
			}
			x := captureWalk(t, live.URL, c.path, q, c.param, func([]byte) string { return "" }, false)[0]
			x.Seq = uint64(page + 1)
			xs = append(xs, x)
			if token = c.next([]byte(*x.Response.Body)); token == "" && page < 2 {
				t.Fatalf("%s: page %d issued no token", c.protocol, page+1)
			}
		}
		got := &target{name: "go", base: startGoProtocols(t, protocols, keys...), client: &http.Client{}}
		rep := &report{}
		replayCapture(&driver{o: options{bucket: "bkt"}, got: got, rep: rep, countPasses: true}, c.protocol, gcsSynthetic, exchangeItems(xs), testLimits)
		if rep.Passed != 3 || rep.Failed != 0 {
			for _, r := range rep.Results {
				t.Errorf("%s: %s: %s %+v", c.protocol, r.Name, r.Error, r.diff)
			}
			t.Errorf("%s: %d passed, %d failed", c.protocol, rep.Passed, rep.Failed)
		}
	}
}

// TestCaptureEmptyMutationSource: an X-Record-Token naming an alteration
// of no token ("flip ", "truncate ", "flip-decoded ") is a malformed
// annotation: the exchange fails with a reported reason, the replay does
// not stop, and the exchanges after it are still replayed.
func TestCaptureEmptyMutationSource(t *testing.T) {
	keys := liveKeys()
	xs := s3CaptureWalk(t, keys)[:2]
	got := &target{name: "go", base: startGoProtocols(t, []string{server.ProtocolS3}, keys...), client: &http.Client{}}
	for _, mutation := range []string{"flip", "truncate", "flip-decoded"} {
		bad := xs[1]
		bad.Annotations = []capturedHeader{{recordCaseHeader, "s3-07r"}, {recordTokenHeader, mutation + " "}}
		later := xs[0]
		later.Seq = 3
		rep := &report{}
		replayCapture(&driver{o: options{bucket: "bkt"}, got: got, rep: rep, countPasses: true}, "s3", gcsSynthetic,
			exchangeItems([]capturedExchange{xs[0], bad, later}), testLimits)
		if rep.Failed != 1 || rep.Passed != 2 || len(rep.Results) != 1 || !strings.Contains(rep.Results[0].Error, recordTokenHeader) {
			t.Errorf("%s: %d passed, %d failed, %+v", mutation, rep.Passed, rep.Failed, rep.Results)
		}
	}
	for _, mutation := range []string{"flip", "truncate", "flip-decoded"} {
		if _, err := mutateToken("", mutation); err == nil {
			t.Errorf("%s of an empty token: no error", mutation)
		}
	}
}
