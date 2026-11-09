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
)

// closes counts the Close calls on every response body its transport
// returns.
type closes struct {
	mu     sync.Mutex
	bodies []*countedBody
}

type countedBody struct {
	io.ReadCloser
	n atomic.Int32
}

func (b *countedBody) Close() error {
	b.n.Add(1)
	return b.ReadCloser.Close()
}

func (c *closes) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := http.DefaultTransport.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	b := &countedBody{ReadCloser: resp.Body}
	resp.Body = b
	c.mu.Lock()
	c.bodies = append(c.bodies, b)
	c.mu.Unlock()
	return resp, nil
}

func (c *closes) check(t *testing.T) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, b := range c.bodies {
		if n := b.n.Load(); n != 1 {
			t.Errorf("response %d closed %d times", i, n)
		}
	}
}

func jsonError(status int, code int, reason string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
		fmt.Fprintf(w, `{"error":{"code":%d,"message":"m","errors":[{"reason":%q}]}}`, code, reason)
	}
}

func body(status int, s string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
		io.WriteString(w, s)
	}
}

// padded is a valid empty listing of exactly n bytes (trailing spaces).
func padded(n int) string {
	s := `{"kind":"storage#objects"}`
	return s + strings.Repeat(" ", n-len(s))
}

const maxBody = 16 << 20

// answer is the *Error an answer must surface: status, reason, message
// (a redirect's: "redirect to TARGET (not followed)").
type answer struct {
	status          int
	reason, message string
}

// TestAnswers: for each scripted answer, the client ends in the expected
// class with the retry decision, attempts and counters it implies under a
// 3-attempt policy (a retryable class is retried to exhaustion, its
// throttles, timeouts or parse failures counted per attempt), surfaces
// the service's answer as an *Error by the HTTP status sent (the body's
// code ignored; a 400 names its query), reads a page only from a 200
// objects envelope, follows no redirect and closes every original body
// once. The rows are the contract the generated client does not give:
// it reads EOF, null, {} and an error member as an empty page, and any
// 2xx as success.
func TestAnswers(t *testing.T) {
	var target atomic.Int64
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { target.Add(1) }))
	defer other.Close()
	redirect := func(status int) func(http.ResponseWriter, *http.Request) {
		return func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, other.URL+"/storage/v1/b/b/o", status)
		}
	}
	moved := "redirect to " + other.URL + "/storage/v1/b/b/o (not followed)"
	for _, c := range []struct {
		name  string
		serve func(http.ResponseWriter, *http.Request)
		class objhttp.Class // "" for a page
		*answer
	}{
		{"429 rateLimitExceeded", jsonError(429, 429, "rateLimitExceeded"), objhttp.ClassThrottle, &answer{429, "rateLimitExceeded", "m"}},
		{"503", jsonError(503, 503, "backendError"), objhttp.ClassServer, &answer{503, "backendError", "m"}},
		{"500", jsonError(500, 500, "internalError"), objhttp.ClassServer, &answer{500, "internalError", "m"}},
		{"408", jsonError(408, 408, ""), objhttp.ClassTimeout, &answer{408, "", "m"}},
		{"403 forbidden", jsonError(403, 403, "forbidden"), objhttp.ClassAccessDenied, &answer{403, "forbidden", "m"}},
		{"403 rateLimitExceeded", jsonError(403, 403, "rateLimitExceeded"), objhttp.ClassThrottle, &answer{403, "rateLimitExceeded", "m"}},
		{"404", jsonError(404, 404, "notFound"), objhttp.ClassNoSuchBucket, &answer{404, "notFound", "m"}},
		{"400", jsonError(400, 400, "invalid"), objhttp.ClassOther, &answer{400, "invalid", "m"}},
		{"401 not JSON", body(401, "denied"), objhttp.ClassAuth, &answer{401, "", ""}},
		// the HTTP status, not the body's code (googleapi.Error.Code)
		{"503 whose body says 400", jsonError(503, 400, "invalid"), objhttp.ClassServer, &answer{503, "invalid", "m"}},
		{"404 whose body says 429", jsonError(404, 429, "notFound"), objhttp.ClassNoSuchBucket, &answer{404, "notFound", "m"}},
		{"301", redirect(301), objhttp.ClassRedirect, &answer{301, "", moved}},
		{"302", redirect(302), objhttp.ClassRedirect, &answer{302, "", moved}},
		{"307", redirect(307), objhttp.ClassRedirect, &answer{307, "", moved}},
		{"200 error envelope", body(200, `{"error":{"code":503,"message":"backend"}}`), objhttp.ClassParse, nil},
		{"200 error null", body(200, `{"error":null}`), objhttp.ClassParse, nil},
		{"200 null", body(200, `null`), objhttp.ClassParse, nil},
		{"200 spaced null", body(200, " null \n"), objhttp.ClassParse, nil},
		{"200 array", body(200, `[]`), objhttp.ClassParse, nil},
		{"200 string", body(200, `""`), objhttp.ClassParse, nil},
		{"200 empty", body(200, ``), objhttp.ClassParse, nil},
		{"200 trailing JSON", body(200, `{"kind":"storage#objects"} {}`), objhttp.ClassParse, nil},
		{"200 kind", body(200, `{"kind":"storage#objects"}`), "", nil},
		// an objects envelope names its kind (the production selector asks for it)
		{"200 {} without kind", body(200, `{}`), objhttp.ClassParse, nil},
		{"200 kind with error member", body(200, `{"kind":"storage#objects","error":{"code":503}}`), objhttp.ClassParse, nil},
		{"200 kind with error null", body(200, `{"kind":"storage#objects","error":null}`), objhttp.ClassParse, nil},
		{"200 kind with escaped error key", body(200, `{"kind":"storage#objects","error":{}}`), objhttp.ClassParse, nil},
		{"200 kind with ERROR key", body(200, `{"kind":"storage#objects","ERROR":1}`), objhttp.ClassParse, nil},
		{"200 error only in a name", body(200, `{"kind":"storage#objects","items":[{"name":"\"error\""}]}`), "", nil},
		{"200 cut mid-object", body(200, `{"kind":"storage#objects","items":[{"na`), objhttp.ClassParse, nil},
		{"200 body short of its length", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Length", "100")
			io.WriteString(w, `{"kind":"storage#objects",`)
		}, objhttp.ClassTransport, nil},
		{"200 16 MiB", body(200, padded(maxBody)), "", nil},
		{"200 16 MiB + 1", body(200, padded(maxBody+1)), objhttp.ClassOther, nil},
		// any 2xx but 200 is not a listing: an answer, class other
		{"201", body(201, `{"kind":"storage#objects"}`), objhttp.ClassOther, &answer{201, "", ""}},
		{"201 garbled", body(201, `{"kind":`), objhttp.ClassOther, &answer{201, "", ""}},
		{"204", body(204, ``), objhttp.ClassOther, &answer{204, "", ""}},
		{"201 throttle reason", jsonError(201, 201, "rateLimitExceeded"), objhttp.ClassThrottle, &answer{201, "rateLimitExceeded", "m"}},
		{"202 forbidden reason", jsonError(202, 202, "forbidden"), objhttp.ClassOther, &answer{202, "forbidden", "m"}},
		{"202 not JSON", body(202, `accepted`), objhttp.ClassOther, &answer{202, "", ""}},
		{"malformed gzip", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Encoding", "gzip")
			io.WriteString(w, "not gzip at all")
		}, objhttp.ClassOther, nil},
		{"size over int64", body(200, `{"kind":"storage#objects","items":[{"name":"a","size":"9223372036854775808"}]}`), objhttp.ClassParse, nil},
		{"size max uint64", body(200, `{"kind":"storage#objects","items":[{"name":"a","size":"18446744073709551615"}]}`), objhttp.ClassParse, nil},
		{"size not a number", body(200, `{"kind":"storage#objects","items":[{"name":"a","size":"x"}]}`), objhttp.ClassParse, nil},
		{"size unquoted", body(200, `{"kind":"storage#objects","items":[{"name":"a","size":5}]}`), objhttp.ClassParse, nil},
		{"size negative", body(200, `{"kind":"storage#objects","items":[{"name":"a","size":"-1"}]}`), objhttp.ClassParse, nil},
		{"malformed updated", body(200, `{"kind":"storage#objects","items":[{"name":"a","updated":"2026-02-19"}]}`), objhttp.ClassParse, nil},
		{"malformed timeCreated", body(200, `{"kind":"storage#objects","items":[{"name":"a","timeCreated":"yesterday"}]}`), objhttp.ClassParse, nil},
		{"null item", body(200, `{"kind":"storage#objects","items":[null]}`), objhttp.ClassParse, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(c.serve))
			defer srv.Close()
			target.Store(0)
			cl := &closes{}
			cli, err := New(context.Background(), Config{Endpoint: srv.URL + "/storage/v1/", Anonymous: true, MaxAttempts: 3, BaseBackoff: time.Microsecond,
				MaxBackoff: time.Microsecond, RequestTimeout: 5 * time.Second, HTTPClient: &http.Client{Transport: cl}})
			if err != nil {
				t.Fatal(err)
			}
			p, err := cli.List(context.Background(), &ListInput{Bucket: "b", Prefix: "p"})
			cl.check(t)
			if target.Load() != 0 {
				t.Errorf("a redirect was followed")
			}
			if cls := Classify(err); cls != c.class {
				t.Fatalf("class %q, want %q (err %v)", cls, c.class, err)
			}
			n := cli.Counters()
			if c.class == "" {
				if p == nil || n.Attempts != 1 || n.Retries != 0 || n.ParseFailures != 0 {
					t.Fatalf("page %+v, counters %+v", p, n)
				}
				return
			}
			f := objhttp.Normalize(err, Answer)
			var exhausted *ExhaustedError
			want := int64(1)
			if f.Retry {
				want = 3
			}
			if errors.As(err, &exhausted) != f.Retry {
				t.Errorf("retry %v, exhausted %v", f.Retry, err)
			}
			count := func(on bool) int64 {
				if on {
					return want
				}
				return 0
			}
			if n.Attempts != want || n.Retries != want-1 || n.Throttles != count(f.Throttle) || n.Timeouts != count(f.Timeout) ||
				n.ParseFailures != count(c.class == objhttp.ClassParse) {
				t.Errorf("counters %+v, want %d attempts (%+v)", n, want, f)
			}
			if f.Throttle != (c.class == objhttp.ClassThrottle) || f.Timeout != (c.class == objhttp.ClassTimeout) {
				t.Errorf("signals %+v for class %q", f, c.class)
			}
			var e *Error
			if errors.As(err, &e) != (c.answer != nil) {
				t.Fatalf("answer %v, want %+v", err, c.answer)
			}
			if e != nil {
				if e.Status != c.status || e.Reason != c.reason || e.Message != c.message || (e.Query != "") != (e.Status == 400) {
					t.Errorf("answer %+v, want %+v", e, *c.answer)
				}
				if e.Status == 400 && !strings.Contains(e.Query, "prefix=p") {
					t.Errorf("400 query %q", e.Query)
				}
			}
		})
	}
}

// TestCanceledAfterHeaders: the caller's cancellation while the body is
// awaited ends the call as canceled, not retried, the body closed once.
func TestCanceledAfterHeaders(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"kind":"storage#objects",`)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer srv.Close()
	cl := &closes{}
	c, err := New(context.Background(), Config{Endpoint: srv.URL + "/storage/v1/", Anonymous: true, MaxAttempts: 3, RequestTimeout: 5 * time.Second,
		HTTPClient: &http.Client{Transport: cl}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, err = c.List(ctx, &ListInput{Bucket: "b"})
	cancel()
	if Classify(err) != objhttp.ClassCanceled || c.Counters().Attempts != 1 || c.Counters().Retries != 0 {
		t.Fatalf("err %v, counters %+v", err, c.Counters())
	}
	cl.check(t)
}

// TestErrorPageThenValidPage: a 200 carrying an error member beside its
// kind is retried as a parse failure, and the retry's page is the answer.
func TestErrorPageThenValidPage(t *testing.T) {
	var n atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1) == 1 {
			io.WriteString(w, `{"kind":"storage#objects","error":{"code":503,"message":"backend"}}`)
			return
		}
		io.WriteString(w, `{"kind":"storage#objects","items":[{"name":"a","size":"1"}]}`)
	}))
	defer srv.Close()
	c, err := New(context.Background(), Config{Endpoint: srv.URL + "/storage/v1/", Anonymous: true, MaxAttempts: 3,
		BaseBackoff: time.Microsecond, MaxBackoff: time.Microsecond})
	if err != nil {
		t.Fatal(err)
	}
	p, err := c.List(context.Background(), &ListInput{Bucket: "b"})
	if err != nil || len(p.Items) != 1 || p.Items[0].Name != "a" {
		t.Fatalf("page %+v, err %v", p, err)
	}
	if got := c.Counters(); got.Attempts != 2 || got.Retries != 1 || got.ParseFailures != 1 {
		t.Fatalf("counters %+v", got)
	}
}

// TestDeadlineDuringBody: an attempt whose body outlasts the request
// timeout is a timeout, counted and retried.
func TestDeadlineDuringBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"kind":"storage#objects",`)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer srv.Close()
	c, err := New(context.Background(), Config{Endpoint: srv.URL + "/storage/v1/", Anonymous: true, MaxAttempts: 2, RequestTimeout: 100 * time.Millisecond,
		BaseBackoff: time.Microsecond, MaxBackoff: time.Microsecond})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.List(context.Background(), &ListInput{Bucket: "b"})
	got := c.Counters()
	if Classify(err) != objhttp.ClassTimeout || got.Attempts != 2 || got.Retries != 1 || got.Timeouts != 2 || got.ParseFailures != 0 {
		t.Fatalf("err %v (class %q), counters %+v", err, Classify(err), got)
	}
}
