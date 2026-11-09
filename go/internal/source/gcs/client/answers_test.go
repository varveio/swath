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
	"net/url"
	"reflect"
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

// cutError writes the status line, a Content-Length of 1,000 and the start
// of a JSON error body naming reason, then closes the connection: a real
// HTTP truncation of an error answer.
func cutError(status int, reason string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, _ *http.Request) {
		conn, bw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		fmt.Fprintf(bw, "HTTP/1.1 %d %s\r\nContent-Type: application/json\r\nContent-Length: 1000\r\n\r\n"+
			`{"error":{"code":%d,"message":"m","errors":[{"reason":%q`, status, http.StatusText(status), status, reason)
		bw.Flush()
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

// The retry decision a row states: a retried failure takes all 3 attempts
// of TestAnswers' policy and ends exhausted; a final one takes 1.
const (
	retried = true
	final   = false
)

// empty is the page an objects envelope without items or prefixes reads as.
var empty = &Page{Items: []Object{}}

// TestAnswers: for each scripted answer, the client ends in the stated
// class with the stated retry decision under a 3-attempt policy (a
// retried failure is retried to exhaustion, a final one is not), and the
// counters that decision and class give (attempts and retries; throttles,
// timeouts or parse failures counted per attempt of that class); it
// surfaces the service's answer as an *Error by the HTTP status sent (the
// body's code ignored; a 400 names its query, exactly), reads exactly the
// stated page, and only from a 200 objects envelope, follows no redirect
// and closes every original body once. The rows are the contract the
// generated client does not give: it reads EOF, null, {} and an error
// member as an empty page, and any 2xx as success.
func TestAnswers(t *testing.T) {
	var target atomic.Int64
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { target.Add(1) }))
	defer other.Close()
	redirect := func(status int) func(http.ResponseWriter, *http.Request) {
		return func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, other.URL+"/storage/v1/b/b/o", status)
		}
	}
	// esc is a backslash, spelled out so that the escaped fixtures below
	// (\u0065 for e) stay escaped in the source as on the wire
	const esc = `\`
	moved := "redirect to " + other.URL + "/storage/v1/b/b/o (not followed)"
	for _, c := range []struct {
		name  string
		serve func(http.ResponseWriter, *http.Request)
		class objhttp.Class // "" for a page
		retry bool
		page  *Page // the page read, for class ""
		*answer
	}{
		{"429 rateLimitExceeded", jsonError(429, 429, "rateLimitExceeded"), objhttp.ClassThrottle, retried, nil, &answer{429, "rateLimitExceeded", "m"}},
		{"503", jsonError(503, 503, "backendError"), objhttp.ClassOther, retried, nil, &answer{503, "backendError", "m"}},
		{"500", jsonError(500, 500, "internalError"), objhttp.ClassOther, retried, nil, &answer{500, "internalError", "m"}},
		{"408", jsonError(408, 408, ""), objhttp.ClassTimeout, retried, nil, &answer{408, "", "m"}},
		{"403 forbidden", jsonError(403, 403, "forbidden"), objhttp.ClassAccessDenied, final, nil, &answer{403, "forbidden", "m"}},
		{"403 rateLimitExceeded", jsonError(403, 403, "rateLimitExceeded"), objhttp.ClassThrottle, retried, nil, &answer{403, "rateLimitExceeded", "m"}},
		{"404", jsonError(404, 404, "notFound"), objhttp.ClassOther, final, nil, &answer{404, "notFound", "m"}},
		// The Guard keeps a cut error answer's status and the bytes read:
		// the status decides, as for the complete answer (a body cut before
		// its reason gives none).
		{"403 connection cut mid-body", cutError(403, "forbidden"), objhttp.ClassAccessDenied, final, nil, &answer{403, "", ""}},
		{"503 connection cut mid-body", cutError(503, "backendError"), objhttp.ClassOther, retried, nil, &answer{503, "", ""}},
		{"400", jsonError(400, 400, "invalid"), objhttp.ClassOther, final, nil, &answer{400, "invalid", "m"}},
		{"401 not JSON", body(401, "denied"), objhttp.ClassAuth, final, nil, &answer{401, "", ""}},
		// the HTTP status, not the body's code (googleapi.Error.Code)
		{"503 whose body says 400", jsonError(503, 400, "invalid"), objhttp.ClassOther, retried, nil, &answer{503, "invalid", "m"}},
		{"404 whose body says 429", jsonError(404, 429, "notFound"), objhttp.ClassOther, final, nil, &answer{404, "notFound", "m"}},
		{"301", redirect(301), objhttp.ClassOther, final, nil, &answer{301, "", moved}},
		{"302", redirect(302), objhttp.ClassOther, final, nil, &answer{302, "", moved}},
		{"307", redirect(307), objhttp.ClassOther, final, nil, &answer{307, "", moved}},
		{"200 error envelope", body(200, `{"error":{"code":503,"message":"backend"}}`), objhttp.ClassParse, retried, nil, nil},
		{"200 error null", body(200, `{"error":null}`), objhttp.ClassParse, retried, nil, nil},
		{"200 null", body(200, `null`), objhttp.ClassParse, retried, nil, nil},
		{"200 empty", body(200, ``), objhttp.ClassParse, retried, nil, nil},
		{"200 trailing JSON", body(200, `{"kind":"storage#objects"} {}`), objhttp.ClassParse, retried, nil, nil},
		{"200 kind", body(200, `{"kind":"storage#objects"}`), "", final, empty, nil},
		// an objects envelope names its kind (the production selector asks for it)
		{"200 {} without kind", body(200, `{}`), objhttp.ClassParse, retried, nil, nil},
		{"200 kind with error member", body(200, `{"kind":"storage#objects","error":{"code":503}}`), objhttp.ClassParse, retried, nil, nil},
		{"200 kind with error null", body(200, `{"kind":"storage#objects","error":null}`), objhttp.ClassParse, retried, nil, nil},
		// the backslash branch of the pre-filter: no "error" in the bytes,
		// one in the decoded key
		{"200 kind with escaped error key", body(200, `{"kind":"storage#objects","`+esc+`u0065rror":{}}`), objhttp.ClassParse, retried, nil, nil},
		{"200 kind with ERROR key", body(200, `{"kind":"storage#objects","ERROR":1}`), objhttp.ClassParse, retried, nil, nil},
		{"200 error only in a name", body(200, `{"kind":"storage#objects","items":[{"name":"\"error\""}],"prefixes":["error/"],"nextPageToken":"t\"error\""}`), "", final,
			&Page{Items: []Object{{Name: `"error"`}}, Prefixes: []string{"error/"}, NextPageToken: `t"error"`}, nil},
		{"200 escaped name", body(200, `{"kind":"storage#objects","items":[{"name":"a`+esc+`u0062\\\"c","size":"3","generation":"7"}]}`), "", final,
			&Page{Items: []Object{{Name: `ab\"c`, Size: 3, Generation: 7}}}, nil},
		{"200 cut mid-object", body(200, `{"kind":"storage#objects","items":[{"na`), objhttp.ClassParse, retried, nil, nil},
		{"200 16 MiB", body(200, padded(maxBody)), "", final, empty, nil},
		{"200 16 MiB + 1", body(200, padded(maxBody+1)), objhttp.ClassOther, final, nil, nil},
		// any 2xx but 200 is not a listing: an answer, class other
		{"201", body(201, `{"kind":"storage#objects"}`), objhttp.ClassOther, final, nil, &answer{201, "", ""}},
		{"201 garbled", body(201, `{"kind":`), objhttp.ClassOther, final, nil, &answer{201, "", ""}},
		{"204", body(204, ``), objhttp.ClassOther, final, nil, &answer{204, "", ""}},
		{"201 throttle reason", jsonError(201, 201, "rateLimitExceeded"), objhttp.ClassThrottle, retried, nil, &answer{201, "rateLimitExceeded", "m"}},
		{"202 forbidden reason", jsonError(202, 202, "forbidden"), objhttp.ClassOther, final, nil, &answer{202, "forbidden", "m"}},
		{"202 not JSON", body(202, `accepted`), objhttp.ClassOther, final, nil, &answer{202, "", ""}},
		{"size over int64", body(200, `{"kind":"storage#objects","items":[{"name":"a","size":"9223372036854775808"}]}`), objhttp.ClassParse, retried, nil, nil},
		{"size not a number", body(200, `{"kind":"storage#objects","items":[{"name":"a","size":"x"}]}`), objhttp.ClassParse, retried, nil, nil},
		{"size negative", body(200, `{"kind":"storage#objects","items":[{"name":"a","size":"-1"}]}`), objhttp.ClassParse, retried, nil, nil},
		{"malformed updated", body(200, `{"kind":"storage#objects","items":[{"name":"a","updated":"2026-02-19"}]}`), objhttp.ClassParse, retried, nil, nil},
		{"malformed timeCreated", body(200, `{"kind":"storage#objects","items":[{"name":"a","timeCreated":"yesterday"}]}`), objhttp.ClassParse, retried, nil, nil},
		{"null item", body(200, `{"kind":"storage#objects","items":[null]}`), objhttp.ClassParse, retried, nil, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(c.serve))
			defer srv.Close()
			target.Store(0)
			cl := &closes{}
			cli, err := New(context.Background(), Config{Endpoint: srv.URL + "/storage/v1/", Anonymous: true, Retry: objhttp.Retry{MaxAttempts: 3, BaseBackoff: time.Microsecond, MaxBackoff: time.Microsecond},
				RequestTimeout: 5 * time.Second, HTTPClient: &http.Client{Transport: cl}})
			if err != nil {
				t.Fatal(err)
			}
			p, err := cli.List(context.Background(), &ListInput{Bucket: "b", Prefix: "p"})
			cl.check(t)
			if target.Load() != 0 {
				t.Errorf("a redirect was followed")
			}
			if cls := objhttp.Classify(err); cls != c.class {
				t.Fatalf("class %q, want %q (err %v)", cls, c.class, err)
			}
			if !reflect.DeepEqual(p, c.page) {
				t.Errorf("page %+v, want %+v", p, c.page)
			}
			attempts := int64(1)
			if c.retry {
				attempts = 3
			}
			per := func(class objhttp.Class) int64 {
				if c.class == class {
					return attempts
				}
				return 0
			}
			want := objhttp.Counts{Attempts: attempts, Retries: attempts - 1, Throttles: per(objhttp.ClassThrottle),
				Timeouts: per(objhttp.ClassTimeout), ParseFailures: per(objhttp.ClassParse)}
			if n := cli.Counters(); n.Attempts != want.Attempts || n.Retries != want.Retries || n.Throttles != want.Throttles ||
				n.Timeouts != want.Timeouts || n.ParseFailures != want.ParseFailures {
				t.Errorf("counters %+v, want %+v", n, want)
			}
			var exhausted *objhttp.ExhaustedError
			if errors.As(err, &exhausted) != c.retry {
				t.Errorf("retry %v, error %v", c.retry, err)
			}
			var e *Error
			if errors.As(err, &e) != (c.answer != nil) {
				t.Fatalf("answer %v, want %+v", err, c.answer)
			}
			if e == nil {
				return
			}
			if e.Status != c.status || e.Reason != c.reason || e.Message != c.message {
				t.Errorf("answer %+v, want %+v", e, *c.answer)
			}
			var query url.Values
			if c.status == http.StatusBadRequest {
				// the request's own query, as the client sent it
				query = url.Values{"alt": {"json"}, "prettyPrint": {"false"}, "projection": {"noAcl"}, "prefix": {"p"}}
			}
			got, perr := url.ParseQuery(e.Query)
			if perr != nil || (query == nil) != (e.Query == "") || query != nil && !reflect.DeepEqual(got, query) {
				t.Errorf("query %q, want %v", e.Query, query)
			}
		})
	}
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
	c, err := New(context.Background(), Config{Endpoint: srv.URL + "/storage/v1/", Anonymous: true, Retry: objhttp.Retry{MaxAttempts: 3, BaseBackoff: time.Microsecond, MaxBackoff: time.Microsecond}})
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
