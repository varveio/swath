// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objhttp

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
)

// closeCounter wraps Next so every response body it returns counts its Close.
type closeCounter struct {
	next   http.RoundTripper
	closed atomic.Int32
}

func (c *closeCounter) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := c.next.RoundTrip(req)
	if err == nil {
		resp.Body = &countingBody{ReadCloser: resp.Body, n: &c.closed}
	}
	return resp, err
}

type countingBody struct {
	io.ReadCloser
	n *atomic.Int32
}

func (b *countingBody) Close() error { b.n.Add(1); return b.ReadCloser.Close() }

func serve(t *testing.T, status, size int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
		_, _ = w.Write(bytes.Repeat([]byte("x"), size))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func get(t *testing.T, g *Guard, url string) (*http.Response, []byte, error) {
	t.Helper()
	resp, err := (&http.Client{Transport: g}).Get(url)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, b, nil
}

func TestGuardCap(t *testing.T) {
	const max = 100
	for _, tc := range []struct {
		size int
		fail bool
	}{{max - 1, false}, {max, false}, {max + 1, true}, {10 * max, true}} {
		t.Run(fmt.Sprint(tc.size), func(t *testing.T) {
			srv := serve(t, 200, tc.size)
			resp, body, err := get(t, &Guard{MaxBody: max}, srv.URL)
			if !tc.fail {
				if err != nil || len(body) != tc.size || resp.ContentLength != int64(tc.size) {
					t.Fatalf("err %v, %d bytes, ContentLength %d", err, len(body), resp.ContentLength)
				}
				return
			}
			var ge *GuardError
			if !errors.As(err, &ge) {
				t.Fatalf("err %v is not a *GuardError", err)
			}
			if Cause(err) != ge.Err {
				t.Errorf("Cause %v, want %v", Cause(err), ge.Err)
			}
			if errors.Is(Cause(err), ErrSyntax) || errors.Is(err, ErrSyntax) {
				t.Errorf("an over-cap body must not be ErrSyntax: %v", err)
			}
			if want := fmt.Sprintf("response larger than %d bytes", max); Cause(err).Error() != want {
				t.Errorf("cause %q, want %q", Cause(err), want)
			}
		})
	}
}

func TestGuardDefaults(t *testing.T) {
	srv := serve(t, 500, 200<<10)
	_, body, err := get(t, &Guard{}, srv.URL)
	if err != nil || len(body) != defaultMaxError {
		t.Fatalf("err %v, %d bytes, want %d", err, len(body), defaultMaxError)
	}
}

func TestGuardValidateRejection(t *testing.T) {
	cause := fmt.Errorf("%w: wrong root", ErrSyntax)
	srv := serve(t, 200, 10)
	_, _, err := get(t, &Guard{Validate: func(*http.Request, int, []byte) error { return cause }}, srv.URL)
	if !errors.Is(err, ErrSyntax) {
		t.Fatalf("err %v is not ErrSyntax", err)
	}
	var ue *url.Error
	if !errors.As(err, &ue) {
		t.Fatalf("err %v is not a *url.Error", err)
	}
	if !Transport(err) {
		t.Error("Transport(url.Error) is expected to be true: that is why Cause exists")
	}
	if got := Cause(err); got != cause || Transport(got) {
		t.Errorf("Cause %v, want the validate error, not a transport failure", got)
	}
	if c := Classify(Cause(err), genericAnswer); c != ClassParse {
		t.Errorf("Classify %q, want %q", c, ClassParse)
	}
	if Cause(cause) != cause || Cause(nil) != nil {
		t.Error("Cause must return an error without a GuardError unchanged")
	}
}

func TestGuardClosesOriginal(t *testing.T) {
	reject := func(*http.Request, int, []byte) error { return errors.New("no") }
	for _, tc := range []struct {
		name    string
		status  int
		size    int
		g       Guard
		wantErr bool
	}{
		{"2xx ok", 200, 10, Guard{MaxBody: 100}, false},
		{"2xx over cap", 200, 101, Guard{MaxBody: 100}, true},
		{"validate reject", 200, 10, Guard{MaxBody: 100, Validate: reject}, true},
		{"non-2xx", 404, 10, Guard{MaxError: 100}, false},
		{"non-2xx truncated", 500, 1000, Guard{MaxError: 100}, false},
		{"3xx", 302, 10, Guard{MaxError: 100, Validate: reject}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := serve(t, tc.status, tc.size)
			cc := &closeCounter{next: http.DefaultTransport}
			g := tc.g
			g.Next = cc
			client := &http.Client{Transport: &g, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
			resp, err := client.Get(srv.URL)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err %v, wantErr %v", err, tc.wantErr)
			}
			if resp != nil {
				resp.Body.Close()
			}
			if n := cc.closed.Load(); n != 1 {
				t.Errorf("original closed %d times, want 1", n)
			}
		})
	}
}

func TestGuardNon2xxTruncates(t *testing.T) {
	const max = 1 << 10
	srv := serve(t, 500, 200<<10)
	called := false
	g := &Guard{MaxError: max, Validate: func(*http.Request, int, []byte) error { called = true; return errors.New("no") }}
	resp, body, err := get(t, g, srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 500 || len(body) != max || resp.ContentLength != max || resp.Header.Get("Content-Length") != fmt.Sprint(max) {
		t.Errorf("status %d, %d bytes, ContentLength %d, header %q", resp.StatusCode, len(body), resp.ContentLength, resp.Header.Get("Content-Length"))
	}
	if called {
		t.Error("Validate ran on a non-2xx response")
	}
}

func TestGuardCapture(t *testing.T) {
	srv := serve(t, 200, 10)
	for _, tc := range []struct {
		name    string
		replace []byte
		want    string
	}{
		{"body", nil, strings.Repeat("x", 10)},
		{"replace", []byte("<r/>"), "<r/>"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var validated []byte
			g := &Guard{Validate: func(_ *http.Request, _ int, b []byte) error { validated = b; return nil }}
			c := &Capture{Replace: tc.replace}
			req, _ := http.NewRequestWithContext(WithCapture(t.Context(), c), http.MethodGet, srv.URL, nil)
			resp, err := (&http.Client{Transport: g}).Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			got, _ := io.ReadAll(resp.Body)
			if string(got) != tc.want || resp.ContentLength != int64(len(tc.want)) {
				t.Errorf("upward %q (ContentLength %d), want %q", got, resp.ContentLength, tc.want)
			}
			if !bytes.Equal(c.Body, validated) || string(c.Body) != strings.Repeat("x", 10) {
				t.Errorf("Capture.Body %q, validated %q", c.Body, validated)
			}
		})
	}
	// no Capture: unaffected
	_, body, err := get(t, &Guard{}, srv.URL)
	if err != nil || len(body) != 10 {
		t.Errorf("without Capture: err %v, %d bytes", err, len(body))
	}
}

func TestGuardHeader(t *testing.T) {
	var seen atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen.Store(r.Header.Get("Authorization"))
	}))
	defer srv.Close()
	g := &Guard{Header: func(r *http.Request) error { r.Header.Set("Authorization", "Bearer t"); return nil }}
	req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
	req.Header.Set("X-Keep", "1")
	resp, err := g.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if seen.Load() != "Bearer t" {
		t.Errorf("server saw Authorization %q", seen.Load())
	}
	if len(req.Header) != 1 || req.Header.Get("Authorization") != "" {
		t.Errorf("the caller's request header changed: %v", req.Header)
	}

	boom := errors.New("token unavailable")
	g = &Guard{Header: func(*http.Request) error { return boom }}
	_, err = (&http.Client{Transport: g}).Get(srv.URL)
	var ge *GuardError
	if !errors.As(err, &ge) || !errors.Is(err, boom) || Cause(err) != boom {
		t.Errorf("err %v, want a GuardError around %v", err, boom)
	}
}

func TestGuardTransportErrorPassesThrough(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()
	_, err := (&Guard{}).RoundTrip(mustReq(t, url))
	if err == nil {
		t.Fatal("expected a transport error")
	}
	var ge *GuardError
	if errors.As(err, &ge) {
		t.Errorf("transport error was wrapped: %v", err)
	}
	if !Transport(err) {
		t.Errorf("Transport(%v) = false", err)
	}
	if Cause(err) != err {
		t.Error("Cause changed a transport error")
	}
}

func TestGuardNoContentGoesToValidate(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	defer srv.Close()
	var status, n = -1, -1
	validate := func(_ *http.Request, s int, b []byte) error {
		status, n = s, len(b)
		return fmt.Errorf("%w: empty answer", ErrSyntax)
	}
	_, _, err := get(t, &Guard{Validate: validate}, srv.URL)
	if status != 204 || n != 0 {
		t.Errorf("Validate saw status %d, %d bytes", status, n)
	}
	if !errors.Is(err, ErrSyntax) || errors.As(err, new(*GuardError)) == false {
		t.Errorf("err %v, want a GuardError around ErrSyntax", err)
	}
	// a Validate that accepts lets the 204 through
	resp, body, err := get(t, &Guard{Validate: func(*http.Request, int, []byte) error { return nil }}, srv.URL)
	if err != nil || resp.StatusCode != 204 || len(body) != 0 {
		t.Errorf("err %v, status %d, %d bytes", err, resp.StatusCode, len(body))
	}
}

func mustReq(t *testing.T, u string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		t.Fatal(err)
	}
	return req
}

// TestGuardReadErrorIsGuardError: a failed body read is a *GuardError, so
// Cause strips the url.Error (a net.Error) around it: a cut body is still
// a transport failure, any other read failure is not.
func TestGuardReadErrorIsGuardError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/gzip" {
			w.Header().Set("Content-Encoding", "gzip")
			io.WriteString(w, "not gzip at all")
			return
		}
		w.Header().Set("Content-Length", "100")
		io.WriteString(w, "short")
	}))
	defer srv.Close()
	for path, transport := range map[string]bool{"/cut": true, "/gzip": false} {
		_, err := (&http.Client{Transport: &Guard{}}).Get(srv.URL + path)
		var ge *GuardError
		if !errors.As(err, &ge) {
			t.Fatalf("%s: err %v is not a GuardError", path, err)
		}
		if got := Transport(Cause(err)); got != transport {
			t.Errorf("%s: Transport(Cause(%v)) = %v, want %v", path, err, got, transport)
		}
	}
}
