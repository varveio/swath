// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/varveio/swath/go/internal/source/objhttp/faulttest"
)

// TestRecordWritesEveryExchange: each forwarded exchange is one JSON
// line with the raw target split into path and query, the client's
// headers with credentials redacted, the answer's status, headers and
// body (text when UTF-8, base64 otherwise), and an injected throttle is
// recorded as answered.
func TestRecordWritesEveryExchange(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		if r.URL.Query().Get("prefix") == "bin" {
			w.Write([]byte{0xff, 0x00})
			return
		}
		w.Write([]byte("<ListBucketResult><Name>b</Name></ListBucketResult>"))
	}))
	defer upstream.Close()
	p, err := faulttest.New(upstream.URL, faulttest.Plan{Windows: []faulttest.Window{{N0: 2, N1: 3, Fraction: 1, Fault: faulttest.Throttle}}})
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	rec := newRecorder(p, upstream.URL, &out)
	front := httptest.NewServer(rec)
	defer front.Close()
	for _, q := range []string{"list-type=2&prefix=a%2Fb", "list-type=2&prefix=bin", "list-type=2"} {
		req, _ := http.NewRequest("GET", front.URL+"/b?"+q, nil)
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=secret")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	var got []Exchange
	sc := bufio.NewScanner(&out)
	for sc.Scan() {
		var x Exchange
		if err := json.Unmarshal(sc.Bytes(), &x); err != nil {
			t.Fatal(err)
		}
		got = append(got, x)
	}
	if len(got) != 3 {
		t.Fatalf("%d lines", len(got))
	}
	x := got[0]
	if x.Seq != 1 || x.Request.Method != "GET" || x.Request.Path != "/b" || x.Request.Query != "list-type=2&prefix=a%2Fb" ||
		x.Request.URL != upstream.URL+"/b?list-type=2&prefix=a%2Fb" || x.Response.Status != 200 || x.Response.Body == nil ||
		*x.Response.Body != "<ListBucketResult><Name>b</Name></ListBucketResult>" || x.StartedAt == "" || x.CapturedAt == "" {
		t.Errorf("%+v", x)
	}
	for _, h := range x.Request.Headers {
		if h.Name == "Authorization" && h.Value != "{redacted}" {
			t.Errorf("credential written: %v", h)
		}
	}
	if b := got[1].Response; b.Body != nil || b.BodyBase64 == nil || *b.BodyBase64 != "/wA=" {
		t.Errorf("binary body: %+v", b)
	}
	if got[2].Response.Status != 503 {
		t.Errorf("injected throttle recorded as %d", got[2].Response.Status)
	}
}
