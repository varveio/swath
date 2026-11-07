// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// Exchange is one line of a -record capture: a request the proxy
// forwarded and the answer it returned, in the shape of the provider
// conformance campaign's raw exchanges (request method, URL and headers;
// response status, headers and body) plus the parts a replay needs
// apart (the raw path and query) and the timestamps. A body that is
// valid UTF-8 is written as text, any other as base64. Credentials a
// client sends (Authorization and the signing headers) are never
// written.
type Exchange struct {
	Seq        uint64   `json:"seq"`
	StartedAt  string   `json:"started_at"`
	CapturedAt string   `json:"captured_at"`
	Request    Request  `json:"request"`
	Response   Response `json:"response"`
}

// Request is the forwarded request as the client sent it: Path and Query
// are the raw request target's two halves, URL the target it was
// forwarded to.
type Request struct {
	Method  string   `json:"method"`
	URL     string   `json:"url"`
	Path    string   `json:"path"`
	Query   string   `json:"query"`
	Headers []Header `json:"headers"`
}

// Response is the answer returned to the client. Status 0 is a
// connection the proxy reset instead of answering.
type Response struct {
	Status     int      `json:"status"`
	Headers    []Header `json:"headers"`
	Body       *string  `json:"body,omitempty"`
	BodyBase64 *string  `json:"body_base64,omitempty"`
}

type Header struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// redacted are the request headers whose values are credentials.
var redacted = map[string]bool{"authorization": true, "x-amz-security-token": true, "cookie": true, "proxy-authorization": true}

// recorder writes every exchange through next as one JSON line.
type recorder struct {
	next   http.Handler
	target string
	mu     sync.Mutex
	w      *bufio.Writer
	seq    uint64
	err    error
}

func newRecorder(next http.Handler, target string, w io.Writer) *recorder {
	return &recorder{next: next, target: strings.TrimSuffix(target, "/"), w: bufio.NewWriter(w)}
}

func (rec *recorder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	started := time.Now().UTC()
	cw := &captureWriter{ResponseWriter: w}
	defer func() {
		path, query, _ := strings.Cut(r.RequestURI, "?")
		x := Exchange{StartedAt: started.Format(time.RFC3339Nano), CapturedAt: time.Now().UTC().Format(time.RFC3339Nano),
			Request: Request{Method: r.Method, URL: rec.target + r.RequestURI, Path: path, Query: query, Headers: headers(r.Header, r.Host, true)}}
		if !cw.hijacked {
			x.Response.Status = cw.status
			if x.Response.Status == 0 {
				x.Response.Status = http.StatusOK
			}
			x.Response.Headers = headers(cw.Header(), "", false)
			body := cw.body.Bytes()
			if utf8.Valid(body) {
				s := string(body)
				x.Response.Body = &s
			} else {
				s := base64.StdEncoding.EncodeToString(body)
				x.Response.BodyBase64 = &s
			}
		}
		rec.write(&x)
	}()
	rec.next.ServeHTTP(cw, r)
}

func (rec *recorder) write(x *Exchange) {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	rec.seq++
	x.Seq = rec.seq
	line, err := json.Marshal(x)
	if err == nil {
		line = append(line, '\n')
		_, err = rec.w.Write(line)
	}
	if err == nil {
		err = rec.w.Flush()
	}
	if rec.err == nil {
		rec.err = err
	}
}

// Err is the first write error, if any.
func (rec *recorder) Err() error {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return rec.err
}

// headers lists h by name (sorted; a repeated header's values in
// order), credentials redacted; host, when set, comes first as Host.
func headers(h http.Header, host string, request bool) []Header {
	var out []Header
	if host != "" {
		out = append(out, Header{"Host", host})
	}
	names := make([]string, 0, len(h))
	for k := range h {
		names = append(names, k)
	}
	slices.Sort(names)
	for _, k := range names {
		for _, v := range h[k] {
			if request && redacted[strings.ToLower(k)] {
				v = "{redacted}"
			}
			out = append(out, Header{k, v})
		}
	}
	return out
}

// captureWriter tees the answer written to the client.
type captureWriter struct {
	http.ResponseWriter
	status   int
	body     bytes.Buffer
	hijacked bool
}

func (c *captureWriter) WriteHeader(code int) {
	if c.status == 0 {
		c.status = code
	}
	c.ResponseWriter.WriteHeader(code)
}

func (c *captureWriter) Write(b []byte) (int, error) {
	if c.status == 0 {
		c.status = http.StatusOK
	}
	c.body.Write(b)
	return c.ResponseWriter.Write(b)
}

// Hijack lets the proxy reset the connection; the exchange is recorded
// with status 0.
func (c *captureWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	c.hijacked = true
	return c.ResponseWriter.(http.Hijacker).Hijack()
}
