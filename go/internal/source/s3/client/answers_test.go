// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/varveio/swath/go/internal/source/objhttp"
	"github.com/varveio/swath/go/internal/source/s3/s3test"
)

// closeCounter is a RoundTripper that counts each original response
// body's Close calls.
type closeCounter struct {
	next   http.RoundTripper
	mu     sync.Mutex
	bodies []*countedBody
}

type countedBody struct {
	io.ReadCloser
	closes atomic.Int32
}

func (b *countedBody) Close() error {
	b.closes.Add(1)
	return b.ReadCloser.Close()
}

func (c *closeCounter) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := c.next.RoundTrip(r)
	if err != nil {
		return nil, err
	}
	b := &countedBody{ReadCloser: resp.Body}
	c.mu.Lock()
	c.bodies = append(c.bodies, b)
	c.mu.Unlock()
	resp.Body = b
	return resp, nil
}

// closedOnce fails unless every body was closed exactly once.
func (c *closeCounter) closedOnce(t *testing.T) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, b := range c.bodies {
		if n := b.closes.Load(); n != 1 {
			t.Errorf("body %d closed %d times", i, n)
		}
	}
}

const maxBody = 16 << 20

// sized is a valid empty ListBucketResult of exactly n bytes (padded with
// a comment).
func sized(n int) string {
	head, tail := `<ListBucketResult><IsTruncated>false</IsTruncated><!--`, `--></ListBucketResult>`
	return head + strings.Repeat("x", n-len(head)-len(tail)) + tail
}

// TestAnswers: for each scripted answer, the client fails with the class,
// the retry decision (exhausted or not), the request counters and the
// answer (*Error: status, code, region, request ID) the table names, or
// returns the page, and every original response body is closed once. The
// client is supplied an HTTP client whose policy follows redirects: it
// follows no 307 or 308 (the Location's server fails the test).
func TestAnswers(t *testing.T) {
	s3test.NoCredentials(t)
	xmlError := func(status int, code string, hdr ...string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			for i := 0; i+1 < len(hdr); i += 2 {
				w.Header().Set(hdr[i], hdr[i+1])
			}
			w.WriteHeader(status)
			fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>m</Message><RequestId>RID</RequestId></Error>`, code)
		}
	}
	body := func(status int, b string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(status)
			io.WriteString(w, b)
		}
	}
	elsewhere := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("redirect followed to %s", r.URL)
		io.WriteString(w, okPage)
	}))
	defer elsewhere.Close()
	redirect := func(status int) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Location", elsewhere.URL+r.URL.RequestURI())
			w.WriteHeader(status)
		}
	}
	cutError := func(status int, code string) http.HandlerFunc { return cutAnswer(t, status, code) }
	// The counters of an answer given once (not retried) and of one
	// retried to exhaustion (3 attempts), with the retried answer's own
	// counter.
	once := objhttp.Counts{Attempts: 1}
	exhausted := func(n objhttp.Counts) objhttp.Counts { n.Attempts, n.Retries = 3, 2; return n }
	throttled, parse, timedOut := exhausted(objhttp.Counts{Throttles: 3}), exhausted(objhttp.Counts{ParseFailures: 3}), exhausted(objhttp.Counts{Timeouts: 3})
	for _, c := range []struct {
		name   string
		h      http.HandlerFunc
		class  objhttp.Class // "" for success
		counts objhttp.Counts
		answer *Error // the *Error returned, if any (Message aside)
	}{
		{"429 SlowDown", xmlError(429, "SlowDown"), objhttp.ClassThrottle, throttled, &Error{Status: 429, Code: "SlowDown", RequestID: "RID"}},
		{"503 SlowDown", xmlError(503, "SlowDown"), objhttp.ClassThrottle, throttled, &Error{Status: 503, Code: "SlowDown", RequestID: "RID"}},
		{"503 no body", body(503, ""), objhttp.ClassThrottle, throttled, &Error{Status: 503, Code: "ServiceUnavailable"}},
		{"500 InternalError", xmlError(500, "InternalError"), objhttp.ClassOther, exhausted(objhttp.Counts{}), &Error{Status: 500, Code: "InternalError", RequestID: "RID"}},
		{"500 html", body(500, "<html>oops"), objhttp.ClassOther, exhausted(objhttp.Counts{}), &Error{Status: 500, Code: "InternalServerError"}},
		{"400 RequestTimeout", xmlError(400, "RequestTimeout"), objhttp.ClassTimeout, timedOut, &Error{Status: 400, Code: "RequestTimeout", RequestID: "RID"}},
		{"408", body(408, ""), objhttp.ClassTimeout, timedOut, &Error{Status: 408, Code: "RequestTimeout"}}, // the code from the status text
		{"403 AccessDenied", xmlError(403, "AccessDenied"), objhttp.ClassAccessDenied, once, &Error{Status: 403, Code: "AccessDenied", RequestID: "RID"}},
		{"403 InvalidAccessKeyId", xmlError(403, "InvalidAccessKeyId"), objhttp.ClassAuth, once, &Error{Status: 403, Code: "InvalidAccessKeyId", RequestID: "RID"}},
		{"404 NoSuchBucket", xmlError(404, "NoSuchBucket"), objhttp.ClassOther, once, &Error{Status: 404, Code: "NoSuchBucket", RequestID: "RID"}},
		{"403 header request ID", xmlError(403, "AccessDenied", "X-Amz-Request-Id", "HID"), objhttp.ClassAccessDenied, once, &Error{Status: 403, Code: "AccessDenied", RequestID: "HID"}},
		{"404 no body", body(404, ""), objhttp.ClassOther, once, &Error{Status: 404, Code: "NotFound"}},
		{"301 PermanentRedirect", xmlError(301, "PermanentRedirect", "X-Amz-Bucket-Region", "eu-west-2"), objhttp.ClassOther, once,
			&Error{Status: 301, Code: "PermanentRedirect", Region: "eu-west-2", RequestID: "RID"}},
		{"307", redirect(307), objhttp.ClassOther, once, &Error{Status: 307, Code: "TemporaryRedirect"}},
		{"308", redirect(308), objhttp.ClassOther, once, &Error{Status: 308, Code: "PermanentRedirect"}}, // the code from the status text
		{"error body cut", body(500, `<Error><Code>InternalError</Code><Mess`), objhttp.ClassOther, exhausted(objhttp.Counts{}), &Error{Status: 500, Code: "InternalServerError"}},
		// A real truncation (the connection closed mid-body): the status,
		// headers and bytes read so far still make the answer.
		{"403 connection cut mid-body", cutError(403, "AccessDenied"), objhttp.ClassAccessDenied, once, &Error{Status: 403, Code: "Forbidden", RequestID: "HID"}},
		{"503 connection cut mid-body", cutError(503, "SlowDown"), objhttp.ClassThrottle, throttled, &Error{Status: 503, Code: "ServiceUnavailable", RequestID: "HID"}},
		{"error body over 64 KiB", body(403, `<Error><Code>AccessDenied</Code><Message>`+strings.Repeat("m", 70<<10)+`</Message></Error>`), objhttp.ClassAccessDenied, once,
			&Error{Status: 403, Code: "Forbidden"}},
		{"200 Error envelope", xmlError(200, "InternalError"), objhttp.ClassParse, parse, nil},
		{"200 wrong root", body(200, `<ListAllMyBucketsResult></ListAllMyBucketsResult>`), objhttp.ClassParse, parse, nil},
		{"200 empty", body(200, ""), objhttp.ClassParse, parse, nil},
		{"200 prolog only", body(200, `<?xml version="1.0" encoding="UTF-8"?>`), objhttp.ClassParse, parse, nil},
		{"200 cut mid-element", body(200, `<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>k</Ke`), objhttp.ClassParse, parse, nil},
		{"200 16 MiB", body(200, sized(maxBody)), "", once, nil},
		{"200 16 MiB + 1", body(200, sized(maxBody+1)), objhttp.ClassOther, once, nil},
		{"201", body(201, okPage), objhttp.ClassOther, once, &Error{Status: 201, Code: "Created"}},
		{"201 SlowDown", xmlError(201, "SlowDown"), objhttp.ClassThrottle, throttled, &Error{Status: 201, Code: "SlowDown", RequestID: "RID"}},
		{"202", body(202, ""), objhttp.ClassOther, once, &Error{Status: 202, Code: "Accepted"}},
		{"204", body(204, ""), objhttp.ClassOther, once, &Error{Status: 204, Code: "NoContent"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			srv := httptest.NewServer(c.h)
			defer srv.Close()
			cfg := testConfig(srv.URL)
			cfg.MaxAttempts = 3

			rt := &closeCounter{next: http.DefaultTransport}
			cl := newTest(t, withHTTP(cfg, rt))
			_, err := cl.ListObjectsV2(context.Background(), &ListInput{Bucket: "b"})
			var ex *objhttp.ExhaustedError
			if got := objhttp.Classify(err); got != c.class || errors.As(err, &ex) != (c.counts.Attempts > 1) || requests(cl.Counters()) != c.counts {
				t.Fatalf("class %q (%v), counters %+v; want %q, %+v", got, err, requests(cl.Counters()), c.class, c.counts)
			}
			var se *Error
			if errors.As(err, &se) != (c.answer != nil) {
				t.Fatalf("answer %#v, want %#v", se, c.answer)
			}
			if se != nil {
				if got := (Error{Status: se.Status, Code: se.Code, Region: se.Region, RequestID: se.RequestID}); got != *c.answer {
					t.Fatalf("answer %#v, want %#v", got, *c.answer)
				}
			}
			rt.closedOnce(t)
		})
	}
}

// cutAnswer writes the status line, X-Amz-Request-Id HID and a
// Content-Length of 1,000, then part of an <Error> body naming code, and
// closes the connection: a real HTTP truncation of an error answer.
func cutAnswer(t *testing.T, status int, code string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		conn, bw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		fmt.Fprintf(bw, "HTTP/1.1 %d %s\r\nX-Amz-Request-Id: HID\r\nContent-Type: application/xml\r\nContent-Length: 1000\r\n\r\n"+
			`<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Mess`, status, http.StatusText(status), code)
		bw.Flush()
	}
}

// TestCutThrottleFeedsLimiter: a 503 whose body is cut by the connection
// closing is still a throttle answer to the limiter: every attempt's
// throttle is recorded, as for a complete 503.
func TestCutThrottleFeedsLimiter(t *testing.T) {
	s3test.NoCredentials(t)
	srv := httptest.NewServer(cutAnswer(t, 503, "SlowDown"))
	defer srv.Close()
	cfg := testConfig(srv.URL)
	cfg.MaxAttempts = 3
	cfg.Limiter = objhttp.NewLimiter(64)
	rt := &closeCounter{next: http.DefaultTransport}
	cl := newTest(t, withHTTP(cfg, rt))
	_, err := cl.ListObjectsV2(context.Background(), &ListInput{Bucket: "b"})
	if objhttp.Classify(err) != objhttp.ClassThrottle {
		t.Fatalf("%v (%q)", err, objhttp.Classify(err))
	}
	n := cl.Counters()
	l := n.Limiter
	if n.Throttles != 3 || l.ThrottlePermille == 0 || l.Decreases+l.FloorThrottles+l.Stale+l.Tolerated != 3 {
		t.Fatalf("counters %+v, limiter %+v: want 3 throttles recorded", requests(n), l)
	}
	rt.closedOnce(t)
}

// withHTTP supplies an HTTP client over rt whose policy follows redirects
// (http.Client's default): the client sets its own on a copy.
func withHTTP(cfg Config, rt http.RoundTripper) Config {
	cfg.HTTPClient = &http.Client{Transport: rt}
	return cfg
}

// TestBearerTokenFailureIsNotAnAttempt: a bearer token command that fails
// is a objhttp.CredentialError (auth) before any request is sent or counted; one
// that works sends "Authorization: Bearer".
func TestBearerTokenFailureIsNotAnAttempt(t *testing.T) {
	s3test.NoCredentials(t)
	var auth atomic.Value
	var sent atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sent.Add(1)
		auth.Store(r.Header.Get("Authorization"))
		io.WriteString(w, okPage)
	}))
	defer srv.Close()
	cfg := testConfig(srv.URL)
	cfg.Anonymous = false
	cfg.Bearer = NewBearerToken("exit 3", 0)
	c, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.ListObjectsV2(context.Background(), &ListInput{Bucket: "b"})
	var ce *objhttp.CredentialError
	if !errors.As(err, &ce) || objhttp.Classify(err) != objhttp.ClassAuth || c.Counters().Attempts != 0 || sent.Load() != 0 {
		t.Fatalf("%v (%q), %+v, %d sent", err, objhttp.Classify(err), c.Counters(), sent.Load())
	}
	cfg.Bearer = NewBearerToken("echo tok", 0)
	if c, err = New(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ListObjectsV2(context.Background(), &ListInput{Bucket: "b"}); err != nil || auth.Load() != "Bearer tok" {
		t.Fatalf("%v, authorization %q", err, auth.Load())
	}
}
