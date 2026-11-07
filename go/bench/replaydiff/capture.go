// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// The capture mode (-capture FILE) replays a real client's request
// stream: the exchanges bench/faultproxy -record wrote while a client
// (the swath lister) listed a live bucket through it. Every captured
// request is sent, in order, to the Go server serving the recording of
// the same bucket, and its answer is compared with the provider's
// captured one under the live comparators' field policy. A captured
// token is never sent: it is mapped by effect to the token the Go server
// issued at the same point (the answer to the request that issued it),
// as the evidence replays align walks.

// capturedExchange is one line of a faultproxy -record capture.
type capturedExchange struct {
	Seq        uint64 `json:"seq"`
	CapturedAt string `json:"captured_at"`
	Request    struct {
		Method  string           `json:"method"`
		Path    string           `json:"path"`
		Query   string           `json:"query"`
		Headers []capturedHeader `json:"headers"`
	} `json:"request"`
	Response struct {
		Status     int              `json:"status"`
		Headers    []capturedHeader `json:"headers"`
		Body       *string          `json:"body"`
		BodyBase64 *string          `json:"body_base64"`
	} `json:"response"`
}

type capturedHeader struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

func readCapture(path string) ([]capturedExchange, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []capturedExchange
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<30)
	for sc.Scan() {
		var x capturedExchange
		if err := json.Unmarshal(sc.Bytes(), &x); err != nil {
			return nil, fmt.Errorf("%s line %d: %w", path, len(out)+1, err)
		}
		out = append(out, x)
	}
	return out, sc.Err()
}

// capturedResponse rebuilds the provider's answer. A body the provider
// sent with a content coding (GCS compresses when the client accepts
// gzip) is decoded and the coding's headers dropped: the comparison is
// of the answer, and the Go server is asked without Accept-Encoding.
func capturedResponse(x *capturedExchange) (*response, error) {
	r := &response{Status: x.Response.Status, Header: http.Header{}}
	for _, h := range x.Response.Headers {
		r.Header[http.CanonicalHeaderKey(h.Name)] = append(r.Header[http.CanonicalHeaderKey(h.Name)], h.Value)
	}
	switch {
	case x.Response.Body != nil:
		r.Body = []byte(*x.Response.Body)
	case x.Response.BodyBase64 != nil:
		b, err := base64.StdEncoding.DecodeString(*x.Response.BodyBase64)
		if err != nil {
			return nil, err
		}
		r.Body = b
	}
	if strings.EqualFold(r.Header.Get("Content-Encoding"), "gzip") {
		zr, err := gzip.NewReader(bytes.NewReader(r.Body))
		if err != nil {
			return nil, err
		}
		if r.Body, err = io.ReadAll(zr); err != nil {
			return nil, err
		}
		r.Header.Del("Content-Encoding")
		r.Header.Del("Content-Length")
	}
	return r, nil
}

// forwardedHeaders are the client's request headers sent on to the Go
// server: all but the connection's own, credentials and the content
// coding the client negotiated.
func forwardedHeaders(hs []capturedHeader) [][2]string {
	var out [][2]string
	for _, h := range hs {
		switch strings.ToLower(h.Name) {
		case "host", "authorization", "accept-encoding", "content-length", "connection", "x-forwarded-for", "x-amz-security-token":
			continue
		}
		out = append(out, [2]string{http.CanonicalHeaderKey(h.Name), h.Value})
	}
	return out
}

// queryToken is the token a raw query carries (decoded) under name, and
// the query with it removed.
func queryToken(query, name string) (token string, rest string, ok bool) {
	var kept []string
	for _, part := range strings.Split(query, "&") {
		n, v, _ := strings.Cut(part, "=")
		if n == name && !ok {
			t, err := url.QueryUnescape(strings.ReplaceAll(v, "+", "%2B"))
			if err != nil {
				t = v
			}
			token, ok = t, true
			continue
		}
		kept = append(kept, part)
	}
	return token, strings.Join(kept, "&"), ok
}

func runCapture(o options) error {
	xs, err := readCapture(o.capture)
	if err != nil {
		return err
	}
	if o.goBin == "" || o.fixture == "" {
		return fmt.Errorf("-go and -fixture are required with -capture")
	}
	protocol := o.protocol
	for _, x := range xs {
		if strings.HasPrefix(x.Request.Path, "/storage/v1/b/") {
			protocol = "gcs"
			break
		}
	}
	goFixture := o.fixture
	if o.goFixture != "" {
		goFixture = o.goFixture
	}
	args := serveArgs(goFixture, o.bucket, o.goArgs)
	if protocol == "gcs" {
		args = append(args, "--protocols", "gcs")
	}
	goSrv, err := startServer(context.Background(), "go", o.goBin, args, filepath.Join(o.logDir, "replaydiff-go.log"))
	if err != nil {
		return err
	}
	defer goSrv.stop()
	got := &target{name: "go", base: goSrv.endpoint, client: liveClient(1)}
	rep := &report{Reference: "capture " + o.capture, Fixture: o.fixture, GoFixture: o.goFixture, Bucket: o.bucket, Started: time.Now()}
	d := &driver{o: o, got: got, rep: rep}
	meta := gcsSynthetic
	if protocol == "gcs" {
		if meta, err = gcsMetadataOf(got, o); err != nil {
			return err
		}
	}
	replayCapture(d, protocol, meta, xs)
	rep.Seconds = time.Since(rep.Started).Seconds()
	return d.write()
}

// replayCapture sends each captured request to d.got in order and
// compares the answers.
func replayCapture(d *driver, protocol string, meta gcsMetadata, xs []capturedExchange) {
	tokenParam, next := "continuation-token", nextToken
	if protocol == "gcs" {
		tokenParam, next = "pageToken", gcsNextToken
	}
	mapped := map[string]string{} // a captured token -> the Go server's for the same point
	for i := range xs {
		x := &xs[i]
		name := fmt.Sprintf("capture %d %s %s?%s", x.Seq, x.Request.Method, x.Request.Path, x.Request.Query)
		if x.Response.Status == 0 {
			d.record(result{Kind: "capture", Name: name, Skipped: "the proxy reset the connection: no answer was captured"})
			continue
		}
		ref, err := capturedResponse(x)
		if err != nil {
			d.record(result{Kind: "capture", Name: name, Error: "captured answer: " + err.Error()})
			continue
		}
		query := x.Request.Query
		refTok, rest, hasTok := queryToken(query, tokenParam)
		gotTok := ""
		if hasTok && refTok != "" {
			t, ok := mapped[refTok]
			if !ok {
				d.record(result{Kind: "capture", Name: name, Skipped: "its token was not issued by an earlier captured answer"})
				continue
			}
			gotTok = t
			query = rest + "&" + tokenParam + "=" + campaignEscape(gotTok, "")
			query = strings.TrimPrefix(query, "&")
		}
		path := x.Request.Path
		res := result{Kind: "capture", Name: name, Method: x.Request.Method, Target: path + "?" + query}
		b, err := d.got.do(d.o.bucket, request{Method: x.Request.Method, Path: path, Query: query, Header: forwardedHeaders(x.Request.Headers)})
		d.count(1)
		if err != nil {
			res.Error = err.Error()
			d.record(res)
			continue
		}
		var df diff
		if protocol == "gcs" {
			df = gcsLive(ref, b, d.o.bucket, gcsFullProjection(query), meta)
		} else {
			df = s3Live(ref, b, d.o.bucket, refTok, gotTok)
		}
		if t := next(ref.Body); t != "" && ref.Status == 200 {
			mapped[t] = next(b.Body)
		}
		res.Pass = df.OK
		if !df.OK {
			df.RefExcerpt, df.GotExcerpt = fmt.Sprintf("%d %.300q", ref.Status, ref.Body), fmt.Sprintf("%d %.300q", b.Status, b.Body)
			res.diff = &df
		}
		d.record(res)
	}
}
