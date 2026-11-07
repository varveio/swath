// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
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
