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
	"time"

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
	cut := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "1000")
		io.WriteString(w, okPage[:20])
	}
	stall := func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, okPage[:20])
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}
	const stalled = "200 past the deadline mid-body"
	gzipped := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		io.WriteString(w, "not gzip at all")
	}
	// The counters of an answer given once (not retried) and of one
	// retried to exhaustion (3 attempts), with the retried answer's own
	// counter.
	once := Counters{Attempts: 1}
	exhausted := func(n Counters) Counters { n.Attempts, n.Retries = 3, 2; return n }
	throttled, parse, timedOut := exhausted(Counters{Throttles: 3}), exhausted(Counters{ParseFailures: 3}), exhausted(Counters{Timeouts: 3})
	for _, c := range []struct {
		name   string
		h      http.HandlerFunc
		class  objhttp.Class // "" for success
		counts Counters
		answer *Error // the *Error returned, if any (Message aside)
	}{
		{"429 SlowDown", xmlError(429, "SlowDown"), objhttp.ClassThrottle, throttled, &Error{Status: 429, Code: "SlowDown", RequestID: "RID"}},
		{"503 SlowDown", xmlError(503, "SlowDown"), objhttp.ClassThrottle, throttled, &Error{Status: 503, Code: "SlowDown", RequestID: "RID"}},
		{"503 no body", body(503, ""), objhttp.ClassThrottle, throttled, &Error{Status: 503, Code: "ServiceUnavailable"}},
		{"500 InternalError", xmlError(500, "InternalError"), objhttp.ClassServer, exhausted(Counters{}), &Error{Status: 500, Code: "InternalError", RequestID: "RID"}},
		{"500 html", body(500, "<html>oops"), objhttp.ClassServer, exhausted(Counters{}), &Error{Status: 500, Code: "InternalServerError"}},
		{"400 RequestTimeout", xmlError(400, "RequestTimeout"), objhttp.ClassTimeout, timedOut, &Error{Status: 400, Code: "RequestTimeout", RequestID: "RID"}},
		{"408", body(408, ""), objhttp.ClassTimeout, timedOut, &Error{Status: 408, Code: "RequestTimeout"}}, // the code from the status text
		{"403 AccessDenied", xmlError(403, "AccessDenied"), objhttp.ClassAccessDenied, once, &Error{Status: 403, Code: "AccessDenied", RequestID: "RID"}},
		{"403 InvalidAccessKeyId", xmlError(403, "InvalidAccessKeyId"), objhttp.ClassAuth, once, &Error{Status: 403, Code: "InvalidAccessKeyId", RequestID: "RID"}},
		{"404 NoSuchBucket", xmlError(404, "NoSuchBucket"), objhttp.ClassNoSuchBucket, once, &Error{Status: 404, Code: "NoSuchBucket", RequestID: "RID"}},
		{"404 no body", body(404, ""), objhttp.ClassNoSuchBucket, once, &Error{Status: 404, Code: "NotFound"}},
		{"301 PermanentRedirect", xmlError(301, "PermanentRedirect", "X-Amz-Bucket-Region", "eu-west-2"), objhttp.ClassRedirect, once,
			&Error{Status: 301, Code: "PermanentRedirect", Region: "eu-west-2", RequestID: "RID"}},
		{"307", redirect(307), objhttp.ClassOther, once, &Error{Status: 307, Code: "TemporaryRedirect"}},
		{"308", redirect(308), objhttp.ClassRedirect, once, &Error{Status: 308, Code: "PermanentRedirect"}}, // the code from the status text
		{"error body cut", body(500, `<Error><Code>InternalError</Code><Mess`), objhttp.ClassServer, exhausted(Counters{}), &Error{Status: 500, Code: "InternalServerError"}},
		{"error body over 64 KiB", body(403, `<Error><Code>AccessDenied</Code><Message>`+strings.Repeat("m", 70<<10)+`</Message></Error>`), objhttp.ClassAccessDenied, once,
			&Error{Status: 403, Code: "Forbidden"}},
		{"200 Error envelope", xmlError(200, "InternalError"), objhttp.ClassParse, parse, nil},
		{"200 wrong root", body(200, `<ListAllMyBucketsResult></ListAllMyBucketsResult>`), objhttp.ClassParse, parse, nil},
		{"200 empty", body(200, ""), objhttp.ClassParse, parse, nil},
		{"200 prolog only", body(200, `<?xml version="1.0" encoding="UTF-8"?>`), objhttp.ClassParse, parse, nil},
		{"200 cut mid-element", body(200, `<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>k</Ke`), objhttp.ClassParse, parse, nil},
		{"200 16 MiB", body(200, sized(maxBody)), "", once, nil},
		{"200 16 MiB + 1", body(200, sized(maxBody+1)), objhttp.ClassOther, once, nil},
		{"200 cut mid-stream", cut, objhttp.ClassTransport, exhausted(Counters{}), nil},
		{stalled, stall, objhttp.ClassTimeout, timedOut, nil},
		{"200 malformed gzip", gzipped, objhttp.ClassOther, once, nil},
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
			if c.name == stalled {
				cfg.RequestTimeout = 200 * time.Millisecond // the others keep the default: 16 MiB under -race
			}
			rt := &closeCounter{next: http.DefaultTransport}
			cl := newTest(t, withHTTP(cfg, rt))
			_, err := cl.ListObjectsV2(context.Background(), &ListInput{Bucket: "b"})
			var ex *objhttp.ExhaustedError
			if got := Classify(err); got != c.class || errors.As(err, &ex) != (c.counts.Attempts > 1) || requests(cl.Counters()) != c.counts {
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

// withHTTP supplies an HTTP client over rt whose policy follows redirects
// (http.Client's default): the client sets its own on a copy.
func withHTTP(cfg Config, rt http.RoundTripper) Config {
	cfg.HTTPClient = &http.Client{Transport: rt}
	return cfg
}

// TestCancelAfterHeaders: a caller's cancellation while the body is read
// is cancellation, not an attempt failure, and the body is closed.
func TestCancelAfterHeaders(t *testing.T) {
	s3test.NoCredentials(t)
	release := make(chan struct{})
	defer close(release)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `<ListBucketResult><IsTruncated>false</IsTruncated>`)
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	rt := &closeCounter{next: http.DefaultTransport}
	c := newTest(t, withHTTP(testConfig(srv.URL), rt))
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, err := c.ListObjectsV2(ctx, &ListInput{Bucket: "b"})
	cancel()
	if Classify(err) != objhttp.ClassCanceled || c.Counters().Attempts != 1 {
		t.Fatalf("%v (%q) %+v", err, Classify(err), c.Counters())
	}
	rt.closedOnce(t)
}

// TestBearerTokenFailureIsNotAnAttempt: a bearer token command that fails
// is a CredentialError (auth) before any request is sent or counted; one
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
	if !errors.As(err, &ce) || Classify(err) != objhttp.ClassAuth || c.Counters().Attempts != 0 || sent.Load() != 0 {
		t.Fatalf("%v (%q), %+v, %d sent", err, Classify(err), c.Counters(), sent.Load())
	}
	cfg.Bearer = NewBearerToken("echo tok", 0)
	if c, err = New(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ListObjectsV2(context.Background(), &ListInput{Bucket: "b"}); err != nil || auth.Load() != "Bearer tok" {
		t.Fatalf("%v, authorization %q", err, auth.Load())
	}
}
