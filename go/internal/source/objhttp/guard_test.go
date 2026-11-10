// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objhttp

import (
	"bytes"
	"context"
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
	const limit = 100
	for _, status := range []int{200, 201, 299, 403, 503} {
		for _, size := range []int{limit - 1, limit, limit + 1, 10 * limit} {
			for _, declared := range []bool{false, true} {
				t.Run(fmt.Sprintf("status=%d/size=%d/declared=%v", status, size, declared), func(t *testing.T) {
					original := strings.NewReader(strings.Repeat("x", size))
					var closed atomic.Int32
					validated := false
					length := int64(-1)
					if declared {
						length = int64(size)
					}
					g := &Guard{MaxBody: limit, MaxError: limit,
						Next: roundTripFunc(func(*http.Request) (*http.Response, error) {
							return &http.Response{StatusCode: status, ContentLength: length, Header: make(http.Header), Body: &countingBody{ReadCloser: io.NopCloser(original), n: &closed}}, nil
						}),
						Validate: func(_ *http.Request, gotStatus int, _ http.Header, body []byte) error {
							validated = true
							if gotStatus != status || len(body) != size {
								t.Errorf("validated status %d, %d bytes", gotStatus, len(body))
							}
							return nil
						},
					}
					resp, err := g.RoundTrip(mustReq(t, "http://example.test/list"))
					success := status/100 == 2
					tooLarge := success && size > limit
					if closed.Load() != 1 || size-original.Len() != min(size, limit+1) {
						t.Fatalf("closed %d times, read %d bytes", closed.Load(), size-original.Len())
					}
					if validated != (success && !tooLarge) {
						t.Errorf("Validate called: %v", validated)
					}
					if tooLarge {
						var ge *GuardError
						var tl *TooLargeError
						if resp != nil || !errors.As(err, &ge) || !errors.As(err, &tl) || tl.Limit != limit || Cause(err) != ge.Err || errors.Is(err, ErrSyntax) || Transport(Cause(err)) {
							t.Fatalf("response %v, error %v: want terminal Guard cap error", resp, err)
						}
						if want := fmt.Sprintf("response larger than %d bytes", limit); Cause(err).Error() != want {
							t.Errorf("cause %q, want %q", Cause(err), want)
						}
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					defer resp.Body.Close()
					body, err := io.ReadAll(resp.Body)
					want := min(size, limit)
					if err != nil || len(body) != want || resp.StatusCode != status || resp.ContentLength != int64(want) || resp.Header.Get("Content-Length") != fmt.Sprint(want) {
						t.Fatalf("error %v, body %d bytes, response %+v", err, len(body), resp)
					}
				})
			}
		}
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
	_, _, err := get(t, &Guard{Validate: func(*http.Request, int, http.Header, []byte) error { return cause }}, srv.URL)
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
	if c := Classify(err); c != ClassParse {
		t.Errorf("Classify %q, want %q", c, ClassParse)
	}
	if Cause(cause) != cause || Cause(nil) != nil {
		t.Error("Cause must return an error without a GuardError unchanged")
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
	validate := func(_ *http.Request, s int, _ http.Header, b []byte) error {
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
	resp, body, err := get(t, &Guard{Validate: func(*http.Request, int, http.Header, []byte) error { return nil }}, srv.URL)
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

// cutServer answers with status, a header and a body that declares 1,000
// bytes, then closes the connection after part: a real HTTP truncation.
func cutServer(t *testing.T, status int, part string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, bw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		fmt.Fprintf(bw, "HTTP/1.1 %d %s\r\nContent-Length: 1000\r\nX-Request-Id: req-1\r\nContent-Type: application/xml\r\n\r\n%s",
			status, http.StatusText(status), part)
		bw.Flush()
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestGuardCutErrorBodyKeepsAnswer: a non-2xx answer whose body read fails
// goes up as that answer (status, headers, the bounded bytes read so far),
// never as a *GuardError; a cut 2xx body is a *GuardError around a
// transport failure. The original body is closed once either way.
func TestGuardCutErrorBodyKeepsAnswer(t *testing.T) {
	const part = "<Error><Code>AccessDenied</Code>"
	reject := func(*http.Request, int, http.Header, []byte) error { return errors.New("no") }
	for _, status := range []int{301, 403, 404, 500, 503} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			cc := &closeCounter{next: http.DefaultTransport}
			client := &http.Client{Transport: &Guard{Next: cc, Validate: reject},
				CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
			resp, err := client.Get(cutServer(t, status, part).URL)
			if err != nil {
				t.Fatalf("err %v, want the %d answer", err, status)
			}
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != status || resp.Header.Get("X-Request-Id") != "req-1" || resp.Header.Get("Content-Type") != "application/xml" {
				t.Errorf("status %d, header %v", resp.StatusCode, resp.Header)
			}
			if string(body) != part || resp.ContentLength != int64(len(part)) || resp.Header.Get("Content-Length") != fmt.Sprint(len(part)) {
				t.Errorf("body %q, ContentLength %d, header %q", body, resp.ContentLength, resp.Header.Get("Content-Length"))
			}
			if n := cc.closed.Load(); n != 1 {
				t.Errorf("original closed %d times, want 1", n)
			}
		})
	}
	t.Run("bounded", func(t *testing.T) {
		resp, body, err := get(t, &Guard{MaxError: 10}, cutServer(t, 503, part).URL)
		if err != nil || resp.StatusCode != 503 || string(body) != part[:10] {
			t.Fatalf("err %v, body %q", err, body)
		}
	})
	t.Run("200", func(t *testing.T) {
		cc := &closeCounter{next: http.DefaultTransport}
		_, _, err := get(t, &Guard{Next: cc}, cutServer(t, 200, part).URL)
		var ge *GuardError
		if !errors.As(err, &ge) || !Transport(Cause(err)) {
			t.Errorf("err %v, want a GuardError around a transport failure", err)
		}
		if n := cc.closed.Load(); n != 1 {
			t.Errorf("original closed %d times, want 1", n)
		}
	})
}

// Status describes a response handed upward, never headers whose body or
// validation later failed. URL is already an independent copy at send time.
func TestCaptureDelivery(t *testing.T) {
	for _, tc := range []struct {
		name    string
		status  int
		readErr error
		reject  bool
		cap     int64
		replace []byte
		wantErr bool
	}{
		{name: "success", status: 200},
		{name: "replacement", status: 200, replace: []byte("replacement")},
		{name: "empty success", status: 204},
		{name: "cut success", status: 200, readErr: io.ErrUnexpectedEOF, wantErr: true},
		{name: "body deadline", status: 200, readErr: context.DeadlineExceeded, wantErr: true},
		{name: "validation refused", status: 200, reject: true, wantErr: true},
		{name: "success over cap", status: 200, cap: 3, wantErr: true},
		{name: "error answer", status: 403},
		{name: "cut error answer", status: 503, readErr: io.ErrUnexpectedEOF},
		{name: "truncated error answer", status: 503, cap: 3},
		{name: "redirect", status: 307},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &Capture{Replace: tc.replace}
			unpublished := func() {
				t.Helper()
				if c.Status != 0 || c.Header != nil || c.Body != nil {
					t.Fatalf("response published before delivery: %+v", c)
				}
			}
			wire := "body"
			if tc.status == 204 {
				wire = ""
			}
			var closed atomic.Int32
			body := &captureBody{Reader: strings.NewReader(wire), err: tc.readErr, check: unpublished, closed: &closed}
			header := http.Header{"X-Test": {"value"}, "Content-Length": {"99"}}
			g := &Guard{MaxBody: tc.cap, MaxError: tc.cap,
				Next: roundTripFunc(func(req *http.Request) (*http.Response, error) {
					unpublished()
					if c.URL == nil || c.URL == req.URL || c.URL.String() != "https://example.test/list?q=sent" {
						t.Fatalf("URL not copied before send: %v", c.URL)
					}
					req.URL.RawQuery = "q=changed"
					return &http.Response{StatusCode: tc.status, Header: header, Body: body, ContentLength: -1}, nil
				}),
				Validate: func(*http.Request, int, http.Header, []byte) error {
					unpublished()
					if tc.reject {
						return ErrSyntax
					}
					return nil
				},
			}
			req, _ := http.NewRequestWithContext(WithCapture(t.Context(), c), http.MethodGet, "https://example.test/list?q=sent", nil)
			resp, err := g.RoundTrip(req)
			if (err != nil) != tc.wantErr || closed.Load() != 1 {
				t.Fatalf("error %v, original body closed %d times", err, closed.Load())
			}
			if c.URL.RawQuery != "q=sent" {
				t.Fatal("captured URL changed with the request")
			}
			if tc.wantErr {
				unpublished()
				if resp != nil {
					t.Fatal("response delivered on failure")
				}
				return
			}
			defer resp.Body.Close()
			got, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			want := wire
			if tc.cap != 0 {
				want = want[:tc.cap]
			}
			if tc.replace != nil {
				want = string(tc.replace)
			}
			if string(got) != want || c.Status != tc.status || c.Header.Get("X-Test") != "value" || c.Header.Get("Content-Length") != fmt.Sprint(len(want)) {
				t.Fatalf("delivered %q, capture %+v", got, c)
			}
			if tc.status/100 == 2 && string(c.Body) != wire || tc.status/100 != 2 && c.Body != nil {
				t.Fatalf("captured body %q", c.Body)
			}
			c.Header.Set("X-Test", "delivered")
			if header.Get("X-Test") != "value" || header.Get("Content-Length") != "99" {
				t.Fatal("original headers mutated")
			}
		})
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type captureBody struct {
	io.Reader
	err    error
	check  func()
	closed *atomic.Int32
}

func (b *captureBody) Read(p []byte) (int, error) {
	b.check()
	n, err := b.Reader.Read(p)
	if err == io.EOF && b.err != nil {
		return n, b.err
	}
	return n, err
}

func (b *captureBody) Close() error { b.closed.Add(1); return nil }

func TestCaptureWithoutAnswer(t *testing.T) {
	for _, headerFailure := range []bool{false, true} {
		t.Run(fmt.Sprint(headerFailure), func(t *testing.T) {
			c := &Capture{}
			boom := errors.New("no answer")
			g := &Guard{Next: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.Body != nil {
					_ = req.Body.Close()
				}
				return nil, boom
			})}
			var closed atomic.Int32
			if headerFailure {
				g.Header = func(*http.Request) error { return boom }
			}
			req, _ := http.NewRequestWithContext(WithCapture(t.Context(), c), http.MethodPost, "https://example.test/", nil)
			req.Body = &countingBody{ReadCloser: io.NopCloser(strings.NewReader("request")), n: &closed}
			resp, err := g.RoundTrip(req)
			if !errors.Is(err, boom) || resp != nil || c.URL == nil || c.Status != 0 || c.Header != nil || c.Body != nil {
				t.Fatalf("response %v, error %v, capture %+v", resp, err, c)
			}
			if closed.Load() != 1 {
				t.Fatalf("request body closed %d times", closed.Load())
			}
		})
	}
}
