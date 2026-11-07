// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/varveio/swath/go/internal/source/objhttp/faulttest"
)

// Exchange is one line of a -record capture: a request the proxy
// forwarded and the answer it returned, in the shape of the provider
// conformance campaign's raw exchanges (request method, URL and headers;
// response status, headers and body) plus the parts a replay needs
// apart (the raw path and query) and the timestamps. A body that is
// valid UTF-8 is written as text, any other as base64. Credentials a
// client sends (Authorization and the signing headers) are never
// written. LimitExceeded, when set, says the answer's body was over the
// recording's limit (-record-max-body): the response carries the
// provider's status and headers, no body, and the client was answered
// 502 instead.
type Exchange struct {
	Seq           uint64   `json:"seq"`
	StartedAt     string   `json:"started_at"`
	CapturedAt    string   `json:"captured_at"`
	LimitExceeded string   `json:"limit_exceeded,omitempty"`
	Request       Request  `json:"request"`
	Response      Response `json:"response"`
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

// recorder writes every exchange through next as one JSON line, before
// the client receives any byte of its answer: the answer is held while
// the exchange is serialized and written, then released. A client sends
// the request that carries a page token only after it has the answer that
// issued it, so the issuing exchange always precedes, in the file and in
// sequence numbers, every exchange that uses its token, and a capture is
// replayed in one pass (replaydiff -capture) without reordering. Writing
// before the release costs each answer the time of one buffered write;
// numbering exchanges as they arrive and reordering them in a writer
// instead would hold a window of finished exchanges in memory and still
// need the same rule for the order a client can observe.
type recorder struct {
	next    http.Handler
	target  string
	maxBody int64 // the largest answer body recorded (and released)
	mu      sync.Mutex
	w       *bufio.Writer
	enc     *json.Encoder // into line
	line    bytes.Buffer
	seq     uint64
	err     error
	// exceeded counts the answers over maxBody.
	exceeded int
	// closed refuses new requests once Close began; inflight counts the
	// handlers still to record their exchange (Add only under adm while
	// open, so Close's Wait never races an Add). adm is apart from mu so
	// a stalled write cannot hold Close past its deadline.
	adm      sync.Mutex
	closed   bool
	active   int
	inflight sync.WaitGroup
}

// newRecorder records the exchanges through next to w; an answer whose
// body is over maxBody bytes is recorded without it and answered 502
// (the proxy in front of a provider is given the same bound, so it never
// buffers more: faulttest.Plan.MaxBody).
func newRecorder(next http.Handler, target string, w io.Writer, maxBody int64) *recorder {
	rec := &recorder{next: next, target: strings.TrimSuffix(target, "/"), maxBody: maxBody, w: bufio.NewWriter(w)}
	rec.enc = json.NewEncoder(&rec.line)
	// '<', '>' and '&' as they are: an XML body escaped for HTML would
	// take six bytes for each.
	rec.enc.SetEscapeHTML(false)
	return rec
}

func (rec *recorder) begin() bool {
	rec.adm.Lock()
	defer rec.adm.Unlock()
	if rec.closed {
		return false
	}
	rec.active++
	rec.inflight.Add(1)
	return true
}

func (rec *recorder) end() {
	rec.adm.Lock()
	rec.active--
	rec.adm.Unlock()
	rec.inflight.Done()
}

func (rec *recorder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !rec.begin() {
		http.Error(w, "faultproxy: the recording is closed", http.StatusServiceUnavailable)
		return
	}
	defer rec.end()
	started := time.Now().UTC()
	cw := &captureWriter{ResponseWriter: w, limit: rec.maxBody}
	path, query, _ := strings.Cut(r.RequestURI, "?")
	x := Exchange{StartedAt: started.Format(time.RFC3339Nano),
		Request: Request{Method: r.Method, URL: rec.target + r.RequestURI, Path: path, Query: query, Headers: headers(r.Header, r.Host, true)}}
	defer func() {
		// A handler that panics (http.ErrAbortHandler: the upstream
		// answer broke off) gives the client a closed connection, no
		// answer: recorded as status 0, like a reset.
		if p := recover(); p != nil {
			x.CapturedAt = time.Now().UTC().Format(time.RFC3339Nano)
			rec.write(&x)
			panic(p)
		}
	}()
	rec.next.ServeHTTP(cw, r)
	x.CapturedAt = time.Now().UTC().Format(time.RFC3339Nano)
	if !cw.hijacked {
		x.Response.Status = cw.code()
		x.Response.Headers = headers(cw.Header(), "", false)
		switch {
		case cw.over:
			x.LimitExceeded = fmt.Sprintf("the answer's body is over %d bytes", rec.maxBody)
		case cw.Header().Get(faulttest.BodyLimitHeader) != "":
			x.LimitExceeded = fmt.Sprintf("the provider's answer body is over %s bytes; the proxy answered 502", cw.Header().Get(faulttest.BodyLimitHeader))
		default:
			body := cw.body.Bytes()
			if utf8.Valid(body) {
				s := string(body)
				x.Response.Body = &s
			} else {
				s := base64.StdEncoding.EncodeToString(body)
				x.Response.BodyBase64 = &s
			}
		}
	}
	rec.write(&x)
	if x.LimitExceeded != "" {
		log.Printf("faultproxy: recording exchange %d (%s %s): %s, recorded without its body", x.Seq, r.Method, r.RequestURI, x.LimitExceeded)
	}
	if !cw.hijacked {
		cw.release(rec.maxBody)
	}
}

func (rec *recorder) write(x *Exchange) {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	rec.seq++
	x.Seq = rec.seq
	if x.LimitExceeded != "" {
		rec.exceeded++
	}
	rec.line.Reset()
	err := rec.enc.Encode(x) // one line, '\n' included
	if err == nil {
		_, err = rec.w.Write(rec.line.Bytes())
	}
	if rec.line.Cap() > 1<<20 {
		rec.line = bytes.Buffer{} // a large answer's buffer is not kept
	}
	if err == nil {
		err = rec.w.Flush()
	}
	if rec.err == nil {
		rec.err = err
	}
}

// Close refuses requests from now on, waits until every request in
// flight has recorded its exchange or ctx ends, and returns the first
// write error. A reset is recorded after the client already saw its
// connection close, and a client may give up on an answer it was still
// waiting for, so a shutdown without this wait could lose those
// exchanges.
func (rec *recorder) Close(ctx context.Context) error {
	rec.adm.Lock()
	rec.closed = true
	rec.adm.Unlock()
	done := make(chan struct{})
	go func() {
		rec.inflight.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		rec.adm.Lock()
		n := rec.active
		rec.adm.Unlock()
		return fmt.Errorf("%d exchanges still in flight at shutdown were not recorded: %w", n, ctx.Err())
	}
	if err := rec.Err(); err != nil {
		return err
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if rec.exceeded > 0 {
		return fmt.Errorf("%d answers were over the body limit of %d bytes (-record-max-body): recorded without their bodies, answered 502", rec.exceeded, rec.maxBody)
	}
	return nil
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

// captureWriter holds the answer the handler writes until release: the
// status, the body up to limit bytes (the headers are the client's own
// map, sent with the status); over marks a body that went past limit,
// whose bytes beyond it are dropped.
type captureWriter struct {
	http.ResponseWriter
	limit    int64
	status   int
	body     bytes.Buffer
	over     bool
	hijacked bool
}

func (c *captureWriter) WriteHeader(code int) {
	if c.status == 0 {
		c.status = code
	}
}

func (c *captureWriter) Write(b []byte) (int, error) {
	if c.status == 0 {
		c.status = http.StatusOK
	}
	if c.over || c.limit > 0 && int64(c.body.Len()+len(b)) > c.limit {
		c.over = true
		c.body = bytes.Buffer{}
		return len(b), nil
	}
	return c.body.Write(b)
}

// code is the answer's status, 200 when the handler set none (as
// net/http answers).
func (c *captureWriter) code() int {
	if c.status == 0 {
		return http.StatusOK
	}
	return c.status
}

// release sends the held answer to the client; one whose body went past
// limit is answered 502, marked as the proxy marks its own, never cut.
func (c *captureWriter) release(limit int64) {
	if c.over {
		h := c.Header()
		clear(h)
		h.Set(faulttest.BodyLimitHeader, strconv.FormatInt(limit, 10))
		h.Set("Content-Type", "text/plain; charset=utf-8")
		c.ResponseWriter.WriteHeader(http.StatusBadGateway)
		fmt.Fprintf(c.ResponseWriter, "faultproxy: the answer's body is over %d bytes\n", limit)
		return
	}
	c.ResponseWriter.WriteHeader(c.code())
	_, _ = c.ResponseWriter.Write(c.body.Bytes())
}

// Hijack lets the proxy reset the connection; the exchange is recorded
// with status 0.
func (c *captureWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	c.hijacked = true
	return c.ResponseWriter.(http.Hijacker).Hijack()
}
