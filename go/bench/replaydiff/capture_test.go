// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

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
// at that request, and a token no earlier answer issued is skipped (and
// with it the rest of that walk).
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
			replayCapture(&driver{o: options{bucket: "bkt"}, got: got, rep: rep}, c.protocol, gcsSynthetic, xs)
			return rep
		}
		if rep := run(xs); rep.Failed != 0 || rep.Passed != len(xs) || len(rep.Skipped) != 0 {
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
		if rep.Failed != 1 || !strings.HasPrefix(rep.Results[1].Name, fmt.Sprintf("capture %d ", xs[1].Seq)) || rep.Results[1].Pass {
			t.Errorf("%s: a changed answer: %d failed", c.protocol, rep.Failed)
		}
		orphan := append([]capturedExchange(nil), xs[1:]...)
		if rep := run(orphan); len(rep.Skipped) != len(orphan) {
			t.Errorf("%s: a token no answer issued: skipped %v", c.protocol, rep.Skipped)
		}
	}
}
