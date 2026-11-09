// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package sdkclient

import (
	"context"
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

	"github.com/varveio/swath/go/internal/source/gcs/client"
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

func (c *closes) check(t *testing.T, who string) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, b := range c.bodies {
		if n := b.n.Load(); n != 1 {
			t.Errorf("%s: response %d closed %d times", who, i, n)
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

// TestAnswersMatchHandRolled: for each scripted answer, both clients end
// in the same class, retry decision and counters under the same retry
// policy (3 attempts), and close every original body once; the
// differences are deliberate and listed (sdkClass).
func TestAnswersMatchHandRolled(t *testing.T) {
	var target atomic.Int64
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { target.Add(1) }))
	defer other.Close()
	redirect := func(status int) func(http.ResponseWriter, *http.Request) {
		return func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, other.URL+"/storage/v1/b/b/o", status) }
	}
	for _, c := range []struct {
		name  string
		serve func(http.ResponseWriter, *http.Request)
		class objhttp.Class // the hand-rolled client's; "" for a page
		// sdkClass, when set, is the SDK client's deliberate difference
		sdkClass *objhttp.Class
	}{
		{"429 rateLimitExceeded", jsonError(429, 429, "rateLimitExceeded"), objhttp.ClassThrottle, nil},
		{"503", jsonError(503, 503, "backendError"), objhttp.ClassServer, nil},
		{"500", jsonError(500, 500, "internalError"), objhttp.ClassServer, nil},
		{"408", jsonError(408, 408, ""), objhttp.ClassTimeout, nil},
		{"403 forbidden", jsonError(403, 403, "forbidden"), objhttp.ClassAccessDenied, nil},
		{"403 rateLimitExceeded", jsonError(403, 403, "rateLimitExceeded"), objhttp.ClassThrottle, nil},
		{"404", jsonError(404, 404, "notFound"), objhttp.ClassNoSuchBucket, nil},
		{"400", jsonError(400, 400, "invalid"), objhttp.ClassOther, nil},
		{"401 not JSON", body(401, "denied"), objhttp.ClassAuth, nil},
		// the HTTP status, not the body's code (googleapi.Error.Code)
		{"503 whose body says 400", jsonError(503, 400, "invalid"), objhttp.ClassServer, nil},
		{"404 whose body says 429", jsonError(404, 429, "notFound"), objhttp.ClassNoSuchBucket, nil},
		{"301", redirect(301), objhttp.ClassRedirect, nil},
		{"302", redirect(302), objhttp.ClassRedirect, nil},
		{"307", redirect(307), objhttp.ClassRedirect, nil},
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
		{"200 {} without kind", body(200, `{}`), "", ptr(objhttp.ClassParse)},
		// the decoder ignores an error member beside the kind
		{"200 kind with error member", body(200, `{"kind":"storage#objects","error":{"code":503}}`), objhttp.ClassParse, ptr("")},
		{"200 cut mid-object", body(200, `{"kind":"storage#objects","items":[{"na`), objhttp.ClassParse, nil},
		{"200 body short of its length", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Length", "100")
			io.WriteString(w, `{"kind":"storage#objects",`)
		}, objhttp.ClassTransport, nil},
		{"200 16 MiB", body(200, padded(maxBody)), "", nil},
		{"200 16 MiB + 1", body(200, padded(maxBody+1)), objhttp.ClassOther, nil},
		// any 2xx but 200 is not a listing: the hand-rolled client reads it
		// as an error answer (other), the SDK client as a malformed one
		{"201", body(201, `{"kind":"storage#objects"}`), objhttp.ClassOther, ptr(objhttp.ClassParse)},
		{"204", body(204, ``), objhttp.ClassOther, ptr(objhttp.ClassParse)},
		{"size over int64", body(200, `{"kind":"storage#objects","items":[{"name":"a","size":"9223372036854775808"}]}`), objhttp.ClassParse, nil},
		{"size max uint64", body(200, `{"kind":"storage#objects","items":[{"name":"a","size":"18446744073709551615"}]}`), objhttp.ClassParse, nil},
		{"size not a number", body(200, `{"kind":"storage#objects","items":[{"name":"a","size":"x"}]}`), objhttp.ClassParse, nil},
		{"size unquoted", body(200, `{"kind":"storage#objects","items":[{"name":"a","size":5}]}`), objhttp.ClassParse, nil},
		// a negative size is no uint64
		{"size negative", body(200, `{"kind":"storage#objects","items":[{"name":"a","size":"-1"}]}`), "", ptr(objhttp.ClassParse)},
		{"malformed updated", body(200, `{"kind":"storage#objects","items":[{"name":"a","updated":"2026-02-19"}]}`), objhttp.ClassParse, nil},
		{"malformed timeCreated", body(200, `{"kind":"storage#objects","items":[{"name":"a","timeCreated":"yesterday"}]}`), objhttp.ClassParse, nil},
		{"null item", body(200, `{"kind":"storage#objects","items":[null]}`), "", ptr(objhttp.ClassParse)},
	} {
		t.Run(c.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(c.serve))
			defer srv.Close()
			target.Store(0)
			type result struct {
				err    error
				counts client.Counters
				page   *client.Page
			}
			var got []result
			for _, sdk := range []bool{false, true} {
				cl := &closes{}
				cfg := client.Config{Endpoint: srv.URL + "/storage/v1/", Anonymous: true, MaxAttempts: 3, BaseBackoff: time.Microsecond,
					MaxBackoff: time.Microsecond, RequestTimeout: 5 * time.Second, HTTPClient: &http.Client{Transport: cl}}
				hand, s := pair(t, cfg)
				c := hand
				who := "hand-rolled"
				if sdk {
					c, who = s, "sdk"
				}
				p, err := c.List(context.Background(), &client.ListInput{Bucket: "b", Prefix: "p"})
				got = append(got, result{err, c.Counters(), p})
				cl.check(t, who)
			}
			want := []objhttp.Class{c.class, c.class}
			if c.sdkClass != nil {
				want[1] = *c.sdkClass
			}
			for i, r := range got {
				if cls := client.Classify(r.err); cls != want[i] {
					t.Errorf("client %d: class %q, want %q (err %v)", i, cls, want[i], r.err)
				}
			}
			if target.Load() != 0 {
				t.Errorf("a redirect was followed")
			}
			if c.sdkClass != nil {
				return
			}
			h, s := got[0], got[1]
			if (h.err == nil) != (s.err == nil) {
				t.Fatalf("hand-rolled %v, sdk %v", h.err, s.err)
			}
			if h.err != nil {
				var he, se *objhttp.ExhaustedError
				hf, sf := objhttp.Normalize(h.err, client.Answer), objhttp.Normalize(s.err, client.Answer)
				if errors.As(h.err, &he) != errors.As(s.err, &se) || hf != sf {
					t.Errorf("hand-rolled %+v (%v), sdk %+v (%v)", hf, h.err, sf, s.err)
				}
			}
			hc, sc := h.counts, s.counts
			if hc.Attempts != sc.Attempts || hc.Retries != sc.Retries || hc.Throttles != sc.Throttles || hc.Timeouts != sc.Timeouts || hc.ParseFailures != sc.ParseFailures {
				t.Errorf("counters: hand-rolled %+v, sdk %+v", hc, sc)
			}
			if !reflect.DeepEqual(h.page, s.page) {
				t.Errorf("pages: hand-rolled %+v, sdk %+v", h.page, s.page)
			}
			var ha, sa *client.Error
			if errors.As(h.err, &ha) != errors.As(s.err, &sa) {
				t.Fatalf("answers: %v, %v", h.err, s.err)
			}
			if ha != nil {
				hq, _ := url.ParseQuery(ha.Query)
				sq, _ := url.ParseQuery(sa.Query)
				sq.Del("alt")
				sq.Del("prettyPrint")
				ha.Query, sa.Query = "", ""
				if *ha != *sa || !reflect.DeepEqual(hq, sq) || (ha.Status == 400) != (len(hq) > 0) {
					t.Errorf("answers: hand-rolled %+v %v, sdk %+v %v", ha, hq, sa, sq)
				}
			}
		})
	}
}

func ptr(c objhttp.Class) *objhttp.Class { return &c }

// TestCanceledAfterHeaders: the caller's cancellation while the body is
// awaited ends the call as canceled, not retried, the body closed once.
func TestCanceledAfterHeaders(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"kind":"storage#objects",`)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer srv.Close()
	for _, sdk := range []bool{false, true} {
		cl := &closes{}
		hand, s := pair(t, client.Config{Endpoint: srv.URL + "/storage/v1/", Anonymous: true, MaxAttempts: 3, RequestTimeout: 5 * time.Second,
			HTTPClient: &http.Client{Transport: cl}})
		c := hand
		if sdk {
			c = s
		}
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		_, err := c.List(ctx, &client.ListInput{Bucket: "b"})
		cancel()
		if client.Classify(err) != objhttp.ClassCanceled || c.Counters().Attempts != 1 || c.Counters().Retries != 0 {
			t.Fatalf("sdk %v: err %v, counters %+v", sdk, err, c.Counters())
		}
		cl.check(t, strconv.FormatBool(sdk))
	}
}
