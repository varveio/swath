// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/source/objhttp"
)

// echoError writes an Azure error answer that quotes the request's query
// (SAS included, as sent and decoded) in its message, as some answers do.
func echoError(w http.ResponseWriter, r *http.Request, status int, code string) {
	if code != "" {
		w.Header().Set(headerErrorCode, code)
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	quote := ""
	if r != nil {
		// as sent and decoded: the signature both ways
		dec, _ := url.QueryUnescape(r.URL.RawQuery)
		quote = r.URL.RawQuery + " " + dec
	}
	fmt.Fprintf(w, "\xef\xbb\xbf<?xml version=\"1.0\" encoding=\"utf-8\"?><Error><Code>%s</Code><Message>m ", code)
	xml.EscapeText(w, []byte(quote))
	fmt.Fprint(w, "</Message></Error>")
}

// closeCount counts the response bodies its transport hands out and every
// Close call on each of them (a second Close is counted, not absorbed).
type closeCount struct {
	next   http.RoundTripper
	mu     sync.Mutex
	closes []*atomic.Int64 // one per body handed out, in order
}

func (c *closeCount) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := c.next.RoundTrip(req)
	if err == nil {
		n := new(atomic.Int64)
		c.mu.Lock()
		c.closes = append(c.closes, n)
		c.mu.Unlock()
		resp.Body = &counted{ReadCloser: resp.Body, n: n}
	}
	return resp, err
}

// closedOnce is "" when want bodies were handed out and each was closed
// exactly once, else the number of Close calls on each body.
func (c *closeCount) closedOnce(want int) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	counts := make([]int64, len(c.closes))
	bad := len(c.closes) != want
	for i, n := range c.closes {
		counts[i] = n.Load()
		bad = bad || counts[i] != 1
	}
	if bad {
		return fmt.Sprintf("%d bodies (want %d), closes per body %v", len(counts), want, counts)
	}
	return ""
}

// counted counts every Close call, then forwards it.
type counted struct {
	io.ReadCloser
	n *atomic.Int64
}

func (c *counted) Close() error {
	c.n.Add(1)
	return c.ReadCloser.Close()
}

// setBodyCap lowers the success body cap of every pooled Guard.
func setBodyCap(c *Client, n int64) {
	for _, g := range c.guards {
		g.MaxBody = n
	}
}

// blobs is the page page(next, names...) decodes to.
func blobs(next string, names ...string) *ListOutput {
	out := &ListOutput{Items: []Item{}, NextMarker: next}
	for _, n := range names {
		out.Items = append(out.Items, Item{Name: n, Size: 5, LastModified: 1792603625000000, CreationTime: 1792603624000000,
			ETag: "0x8DF2F9886625F76", ContentMD5: "2KAlPH8W1jwF0SkOJToFzg==", ContentType: "application/octet-stream",
			BlobType: "BlockBlob", AccessTier: "Hot"})
	}
	return out
}

// outcome is what one List did.
type outcome struct {
	Class   objhttp.Class
	Requeue bool
	Err     error // the class-relevant sentinel the error wraps
	Answer  *Error
	Page    *ListOutput
	// Attempts, Retries, Throttles, Timeouts, ParseFailures
	Counts   [5]int64
	Capped   int64 // retry_after_capped
	Requests int64 // as the server counted them
}

// TestScriptedAnswers sends scripted answers to the client (through a
// supplied HTTP client that counts bodies and closes, with a SAS whose
// signature the answers quote) and holds it to the expected class, retry
// decision, *Error (status, code, message, query, wait), page, counters and
// requests; every body closed exactly once; the signature in no error. The body cap
// is a plain failure: a body at the cap is a page, one byte past it fails
// the request without another one, the input page size unchanged.
func TestScriptedAnswers(t *testing.T) {
	big := page("", "p/0", "p/1", "p/2", "p/3")
	echo := func(status int, code string, wait time.Duration) *Error {
		return &Error{Status: status, Code: code, Message: "m (echo)", Wait: wait}
	}
	redirected := func(status int) *Error { return &Error{Status: status, Message: "redirect not followed"} }
	parse := func(attempts int64) outcome {
		return outcome{Class: objhttp.ClassParse, Requeue: true, Err: objhttp.ErrSyntax, Counts: [5]int64{attempts, attempts - 1, 0, 0, attempts}, Requests: attempts}
	}
	invalid := outcome{Class: objhttp.ClassOther, Err: ErrInvalidPage, Counts: [5]int64{1}, Requests: 1}
	cases := []struct {
		name     string
		attempts int
		in       ListInput
		cap      int64 // the body cap, 0: MaxBody
		handler  func(n int, w http.ResponseWriter, r *http.Request)
		want     outcome
	}{
		{"429 with Retry-After, then a page", 3, ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) {
			if n == 1 {
				w.Header().Set("Retry-After", "0")
				echoError(w, r, 429, "")
				return
			}
			fmt.Fprint(w, page("", "a", "b"))
		}, outcome{Page: blobs("", "a", "b"), Counts: [5]int64{2, 1, 1, 0, 0}, Requests: 2}},
		{"503 ServerBusy past the timeout exhausted", 2, ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Retry-After", "3600")
			echoError(w, r, 503, "ServerBusy")
		}, outcome{Class: objhttp.ClassThrottle, Requeue: true, Answer: echo(503, "ServerBusy", 2*time.Second), Counts: [5]int64{2, 1, 2, 0, 0}, Capped: 2, Requests: 2}},
		{"500 InternalError exhausted", 2, ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) { echoError(w, r, 500, "InternalError") },
			outcome{Class: objhttp.ClassOther, Requeue: true, Answer: echo(500, "InternalError", 0), Counts: [5]int64{2, 1, 0, 0, 0}, Requests: 2}},
		{"500 OperationTimedOut exhausted", 2, ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) {
			echoError(w, r, 500, "OperationTimedOut")
		}, outcome{Class: objhttp.ClassTimeout, Requeue: true, Answer: echo(500, "OperationTimedOut", 0), Counts: [5]int64{2, 1, 0, 2, 0}, Requests: 2}},
		{"500 other code not retried", 3, ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) { echoError(w, r, 500, "Unknown") },
			outcome{Class: objhttp.ClassOther, Answer: echo(500, "Unknown", 0), Counts: [5]int64{1}, Requests: 1}},
		{"500 without a code header", 3, ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(500)
			fmt.Fprint(w, "\xef\xbb\xbf<?xml version=\"1.0\"?><Error><Code>InternalError</Code><Message>x</Message></Error>")
		}, outcome{Class: objhttp.ClassOther, Requeue: true, Answer: &Error{Status: 500, Code: "InternalError", Message: "x"}, Counts: [5]int64{3, 2, 0, 0, 0}, Requests: 3}},
		{"403", 3, ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) { echoError(w, r, 403, "AuthorizationFailure") },
			outcome{Class: objhttp.ClassAccessDenied, Answer: echo(403, "AuthorizationFailure", 0), Counts: [5]int64{1}, Requests: 1}},
		{"401 with a challenge, not re-sent", 3, ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) {
			w.Header().Set("WWW-Authenticate", `Bearer authorization_uri=https://login.example/t, resource_id=https://storage.azure.com`)
			echoError(w, r, 401, "InvalidAuthenticationInfo")
		}, outcome{Class: objhttp.ClassAuth, Answer: echo(401, "InvalidAuthenticationInfo", 0), Counts: [5]int64{1}, Requests: 1}},
		{"404", 3, ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) { echoError(w, r, 404, "ContainerNotFound") },
			outcome{Class: objhttp.ClassOther, Answer: echo(404, "ContainerNotFound", 0), Counts: [5]int64{1}, Requests: 1}},
		{"400 names the query", 3, ListInput{Prefix: "p q+", StartFrom: "s", Marker: "m"}, 0, func(n int, w http.ResponseWriter, r *http.Request) {
			echoError(w, r, 400, "InvalidQueryParameterValue")
		}, outcome{Class: objhttp.ClassOther, Answer: &Error{Status: 400, Code: "InvalidQueryParameterValue", Message: "m (echo)",
			Query: "comp=list&marker=m&prefix=p%20q%2B&restype=container&startFrom=s"}, Counts: [5]int64{1}, Requests: 1}},
		{"408 then a page", 3, ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) {
			if n == 1 {
				echoError(w, r, 408, "")
				return
			}
			fmt.Fprint(w, page("", "a"))
		}, outcome{Page: blobs("", "a"), Counts: [5]int64{2, 1, 0, 1, 0}, Requests: 2}},
		{"301 not followed", 3, ListInput{}, 0, redirect(301), outcome{Class: objhttp.ClassOther, Answer: redirected(301), Counts: [5]int64{1}, Requests: 1}},
		{"302 not followed", 3, ListInput{}, 0, redirect(302), outcome{Class: objhttp.ClassOther, Answer: redirected(302), Counts: [5]int64{1}, Requests: 1}},
		{"307 not followed", 3, ListInput{}, 0, redirect(307), outcome{Class: objhttp.ClassOther, Answer: redirected(307), Counts: [5]int64{1}, Requests: 1}},
		{"308 not followed", 3, ListInput{}, 0, redirect(308), outcome{Class: objhttp.ClassOther, Answer: redirected(308), Counts: [5]int64{1}, Requests: 1}},
		{"308 malformed Location", 3, ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Location", "http://[::1%zz/?"+r.URL.RawQuery)
			w.WriteHeader(308)
		}, outcome{Class: objhttp.ClassOther, Answer: redirected(308), Counts: [5]int64{1}, Requests: 1}},
		{"200 error envelope", 2, ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, "\xef\xbb\xbf<?xml version=\"1.0\"?><Error><Code>InternalError</Code><Message>x</Message></Error>")
		}, parse(2)},
		{"200 wrong root", 2, ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, "<ListBucketResult><Blobs/></ListBucketResult>")
		}, parse(2)},
		{"200 without Blobs", 2, ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, "<EnumerationResults><NextMarker/></EnumerationResults>")
		}, parse(2)},
		{"200 empty body", 2, ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) {}, parse(2)},
		{"200 BOM only", 2, ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "\xef\xbb\xbf") }, parse(2)},
		{"200 cut in an element", 2, ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, listHead+"<Blob><Na") }, parse(2)},
		{"200 connection cut", 2, ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", "100000")
			fmt.Fprint(w, listHead)
			w.(http.Flusher).Flush()
			conn, _, _ := w.(http.Hijacker).Hijack()
			conn.Close()
		}, outcome{Class: objhttp.ClassOther, Requeue: true, Counts: [5]int64{2, 1, 0, 0, 0}, Requests: 2}},
		{"200 out of order", 2, ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, page("", "b", "a")) }, invalid},
		{"200 repeated marker", 2, ListInput{Marker: "m1"}, 0, func(n int, w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, page("m1", "a")) }, invalid},
		{"200 Encoded name not UTF-8", 2, ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, listHead+`<Blob><Name Encoded="true">a%FF</Name></Blob>`+listTail)
		}, invalid},
		{"200 bad Content-Length", 2, ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, listHead+`<Blob><Name>a</Name><Properties><Content-Length>x</Content-Length></Properties></Blob>`+listTail)
		}, parse(2)},
		{"200 bad Last-Modified", 2, ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, listHead+`<Blob><Name>a</Name><Properties><Last-Modified>yesterday</Last-Modified></Properties></Blob>`+listTail)
		}, parse(2)},
		{"200 hierarchy", 2, ListInput{Delimiter: "/"}, 0, func(n int, w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, listHead+`<BlobPrefix><Name>a/</Name></BlobPrefix><Blob><Name>a0</Name></Blob><BlobPrefix><Name>b/</Name></BlobPrefix>`+
				`<Blob><Name>c</Name></Blob><BlobPrefix><Name>`+"\U00010000/"+`</Name></BlobPrefix><Blob><Name>`+""+`</Name></Blob>`+
				`</Blobs><NextMarker>next</NextMarker></EnumerationResults>`)
		}, outcome{Page: &ListOutput{Items: []Item{{Name: "a/", Prefix: true}, {Name: "a0"}, {Name: "b/", Prefix: true}, {Name: "c"},
			{Name: "\U00010000/", Prefix: true}, {Name: ""}}, NextMarker: "next"}, Counts: [5]int64{1}, Requests: 1}},
		{"201", 3, ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(201)
			fmt.Fprint(w, page(""))
		}, outcome{Class: objhttp.ClassOther, Answer: &Error{Status: 201}, Counts: [5]int64{1}, Requests: 1}},
		{"204", 3, ListInput{}, 0, func(n int, w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) },
			outcome{Class: objhttp.ClassOther, Answer: &Error{Status: 204}, Counts: [5]int64{1}, Requests: 1}},
		{"at the cap", 3, ListInput{MaxResults: 4}, int64(len(big)), func(n int, w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, sized(r))
		}, outcome{Page: blobs("", "p/0", "p/1", "p/2", "p/3"), Counts: [5]int64{1}, Requests: 1}},
		{"a byte past the cap, a terminal failure", 3, ListInput{MaxResults: 4}, int64(len(big)) - 1, func(n int, w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, sized(r))
		}, outcome{Class: objhttp.ClassOther, Err: ErrBodyCap, Counts: [5]int64{1}, Requests: 1}},
	}
	var scrubbed atomic.Int64
	t.Cleanup(func() {
		if scrubbed.Load() == 0 {
			t.Error("no echoing answer's message reached the client")
		}
	})
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var mu sync.Mutex
			n := 0
			var maxresults []string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				n++
				k := n
				maxresults = append(maxresults, r.URL.Query().Get("maxresults"))
				mu.Unlock()
				c.handler(k, w, r)
			}))
			cc := &closeCount{next: http.DefaultTransport}
			cl, err := New(context.Background(), Config{Endpoint: srv.URL, Account: "acct", SAS: testSAS, MaxAttempts: c.attempts, RequestTimeout: 2 * time.Second,
				BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond, HTTPClient: &http.Client{Transport: cc}})
			if err != nil {
				t.Fatal(err)
			}
			if c.cap > 0 {
				setBodyCap(cl, c.cap)
			}
			in := c.in
			in.Container = "c"
			p, err := cl.List(context.Background(), &in)
			srv.Close()
			n0 := cl.Counters()
			got := outcome{Page: p, Counts: [5]int64{n0.Attempts, n0.Retries, n0.Throttles, n0.Timeouts, n0.ParseFailures},
				Capped: cl.RetryAfterCapped(), Requests: int64(n)}
			if n0.InflightMax != 1 {
				t.Errorf("inflight max %d", n0.InflightMax)
			}
			if err != nil {
				got.Class, got.Requeue = Classify(err), objhttp.Requeue(err)
				if leak := sasLeak(err.Error()); leak != "" {
					t.Errorf("the SAS's %q in the error: %v", leak, err)
				}
				if e, ok := errors.AsType[*Error](err); ok {
					got.Answer = e
					if strings.HasPrefix(e.Message, "m ") {
						// an echoing answer: its message kept, every SAS
						// parameter redacted wherever the SDK sorted it,
						// the request's own parameters kept
						scrubbed.Add(1)
						if sasLeak(e.Message) != "" || !strings.Contains(e.Message, "sig=REDACTED") || !strings.Contains(e.Message, "comp=list&") {
							t.Errorf("message %q", e.Message)
						}
						cp := *e
						cp.Message = "m (echo)"
						got.Answer = &cp
					}
				}
				for _, s := range []error{objhttp.ErrSyntax, ErrInvalidPage, ErrBodyCap} {
					if errors.Is(err, s) {
						got.Err = s
					}
				}
			}
			// every body the transport handed out (one per request the
			// server answered) closed exactly once
			if msg := cc.closedOnce(n); msg != "" {
				t.Error(msg)
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("%v\n got  %+v\n want %+v", err, got, c.want)
				if got.Answer != nil && c.want.Answer != nil {
					t.Errorf("answers: %+v / %+v", *got.Answer, *c.want.Answer)
				}
			}
			if c.cap > 0 && (in.MaxResults != c.in.MaxResults || maxresults[0] != strconv.Itoa(c.in.MaxResults)) {
				t.Errorf("page size changed: input %d, sent %q", in.MaxResults, maxresults)
			}
		})
	}
}

// redirect answers status with a Location that carries the request's
// query (the SAS).
func redirect(status int) func(int, http.ResponseWriter, *http.Request) {
	return func(n int, w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "http://elsewhere.example/acct/c?"+r.URL.RawQuery)
		w.WriteHeader(status)
	}
}

// sized is a page of maxresults (4 when absent) blobs p/0..: a larger
// page is a longer body.
func sized(r *http.Request) string {
	n, err := strconv.Atoi(r.URL.Query().Get("maxresults"))
	if err != nil || n > 4 {
		n = 4
	}
	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprintf("p/%d", i)
	}
	next := ""
	if n < 4 {
		next = fmt.Sprintf("m%d", n)
	}
	return page(next, names...)
}

// TestCancelAfterHeaders: a caller's cancellation after the answer's
// headers, mid-body, ends List as cancelled, not retried, with the body
// closed.
func TestCancelAfterHeaders(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, listHead)
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(srv.Close)
	cc := &closeCount{next: http.DefaultTransport}
	c, err := New(context.Background(), Config{Endpoint: srv.URL, Account: "acct", Anonymous: true, MaxAttempts: 3, HTTPClient: &http.Client{Transport: cc}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cc.next = headersThen(http.DefaultTransport, cancel)
	_, err = c.List(ctx, &ListInput{Container: "c"})
	if Classify(err) != objhttp.ClassOther || !errors.Is(err, context.Canceled) {
		t.Errorf("%v (class %q)", err, Classify(err))
	}
	if msg := cc.closedOnce(1); msg != "" {
		t.Error(msg)
	}
}

// headersThen calls f once the response headers are in.
func headersThen(next http.RoundTripper, f func()) http.RoundTripper {
	return rtFunc(func(req *http.Request) (*http.Response, error) {
		resp, err := next.RoundTrip(req)
		f()
		return resp, err
	})
}

type rtFunc func(*http.Request) (*http.Response, error)

func (f rtFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }
