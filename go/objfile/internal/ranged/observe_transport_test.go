// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package ranged

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/varveio/swath/go/objfile"
	"github.com/varveio/swath/go/objfile/internal/objfiletest"
)

// httpGetter is a Getter of the object at url through client, as a store
// package's is: the Range asked, the version from ETag, the start and
// total from Content-Range.
func httpGetter(client *http.Client, url string) Getter {
	return func(ctx context.Context, off, n int64, _ Pin) (Response, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return Response{}, err
		}
		if n >= 0 {
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", off, off+n-1))
		}
		res, err := client.Do(req)
		if err != nil {
			return Response{}, err
		}
		if res.StatusCode/100 != 2 {
			res.Body.Close()
			return Response{}, fmt.Errorf("status %s", res.Status)
		}
		r := Response{Body: res.Body, Pin: Pin{ETag: res.Header.Get("ETag")}, Length: res.ContentLength}
		r.Start, r.Total = Stated(res.Header.Get("Content-Range"))
		return r, nil
	}
}

// objectTransport returns the same wire response as an object store, without
// a listener, so validation and panic tests exercise HTTP observation in isolation.
func objectTransport(noETag bool) http.RoundTripper {
	return rtFunc(func(req *http.Request) (*http.Response, error) {
		h := make(http.Header)
		if !noETag {
			h.Set("ETag", "e")
		}
		a, b := 0, 999
		status := http.StatusOK
		if _, err := fmt.Sscanf(req.Header.Get("Range"), "bytes=%d-%d", &a, &b); err == nil {
			h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/1000", a, b))
			status = http.StatusPartialContent
		}
		return &http.Response{StatusCode: status, Status: fmt.Sprint(status), Header: h, ContentLength: int64(b - a + 1), Body: io.NopCloser(strings.NewReader(strings.Repeat("x", b-a+1))), Request: req}, nil
	})
}

func observed(obs objfile.Observer) context.Context {
	return objfile.WithObserver(context.Background(), obs)
}

// TestObservedLateFailure: a transport-observed attempt is completed only
// once the Object's checks of it are done, so a failure found after its
// body ended (no identity evidence, a checksum failing at the body's end,
// a metadata answer that does not parse) is its result.
func TestObservedLateFailure(t *testing.T) {
	client := &http.Client{Transport: Observe(objectTransport(false))}
	t.Run("NoIdentity", func(t *testing.T) {
		client := &http.Client{Transport: Observe(objectTransport(true))}
		obs := &objfiletest.Observer{}
		f := NewSized("b/obj", 1000, httpGetter(client, "http://store/b/obj"), nil, true)
		if _, err := f.WithContext(observed(obs)).ReadAt(make([]byte, 10), 0); err == nil {
			t.Fatal("a response naming no version was accepted")
		}
		_, ends := obs.Events()
		if len(ends) != 1 || ends[0].Err == nil || !strings.Contains(ends[0].Err.Error(), "no version") {
			t.Fatalf("results %+v; want the rejection", ends)
		}
	})
	t.Run("ChecksumAtEnd", func(t *testing.T) {
		crc := errors.New("crc32c mismatch")
		get := httpGetter(client, "http://store/b/obj")
		failAtEnd := func(ctx context.Context, off, n int64, pin Pin) (Response, error) {
			res, err := get(ctx, off, n, pin)
			if err == nil {
				res.Body = struct {
					io.Reader
					io.Closer
				}{&endErr{r: res.Body, err: crc}, res.Body}
			}
			return res, err
		}
		obs := &objfiletest.Observer{}
		if _, _, err := ReadObject(observed(obs), "b/obj", 1000, true, failAtEnd); !errors.Is(err, crc) {
			t.Fatalf("ReadObject = %v, want the checksum failure", err)
		}
		_, ends := obs.Events()
		if len(ends) != 1 || !errors.Is(ends[0].Err, crc) || ends[0].Bytes != 1000 {
			t.Fatalf("results %+v; want 1000 bytes and the checksum failure", ends)
		}
	})
	t.Run("MalformedMetadata", func(t *testing.T) {
		bad := errors.New("malformed metadata")
		obs := &objfiletest.Observer{}
		_, _, err := Metadata(observed(obs), "b/obj", true, func(ctx context.Context) (int64, Pin, error) {
			req, _ := http.NewRequestWithContext(ctx, http.MethodHead, "http://store/b/obj", nil)
			res, err := client.Do(req)
			if err != nil {
				return 0, Pin{}, err
			}
			res.Body.Close()
			return 0, Pin{}, bad // the SDK's parse of the answer fails
		})
		_, ends := obs.Events()
		if !errors.Is(err, bad) || len(ends) != 1 || !errors.Is(ends[0].Err, bad) {
			t.Fatalf("Metadata = %v, results %+v; want the parse failure", err, ends)
		}
	})
}

type endErr struct {
	r   io.Reader
	err error
}

func (e *endErr) Read(p []byte) (int, error) {
	n, err := e.r.Read(p)
	if err == io.EOF {
		err = e.err
	}
	return n, err
}

// TestObservedTransportResend: Go's transport sends a replayable GET again
// when a reused connection closes before its answer, inside one RoundTrip;
// each send is an attempt.
func TestObservedTransportResend(t *testing.T) {
	var conns, requests atomic.Int32
	transport := &http.Transport{DialContext: func(context.Context, string, string) (net.Conn, error) {
		client, server := net.Pipe()
		n := conns.Add(1)
		go func() {
			defer server.Close()
			br := bufio.NewReader(server)
			for i := 0; ; i++ {
				req, err := http.ReadRequest(br)
				if err != nil {
					return
				}
				io.Copy(io.Discard, req.Body)
				req.Body.Close()
				requests.Add(1)
				if n == 1 && i == 1 {
					return
				} // received on the reused connection, no response
				fmt.Fprint(server, "HTTP/1.1 206 Partial Content\r\nETag: e\r\nContent-Range: bytes 0-9/1000\r\nContent-Length: 10\r\n\r\n0123456789")
			}
		}()
		return client, nil
	}}
	t.Cleanup(transport.CloseIdleConnections)
	client := &http.Client{Transport: Observe(transport)}
	get := httpGetter(client, "http://store/b/obj")
	f := New("b/obj", 1000, Pin{ETag: "e"}, get, true)
	if _, err := f.ReadAt(make([]byte, 10), 0); err != nil { // the connection, made idle
		t.Fatal(err)
	}
	obs := &objfiletest.Observer{}
	if _, err := f.WithContext(observed(obs)).ReadAt(make([]byte, 10), 0); err != nil {
		t.Fatal(err)
	}
	reqs, ends := obs.Events()
	if conns.Load() != 2 || requests.Load() != 3 || len(reqs) != 2 || reqs[0].Retry || !reqs[1].Retry || len(ends) != 2 || ends[0].Err == nil || ends[1].Err != nil || ends[1].Bytes != 10 {
		t.Fatalf("%d connections; observed %+v, %+v; want the unanswered send and its resend", conns.Load(), reqs, ends)
	}
}

// panicking runs f and returns what it panicked with.
func panicking(f func()) (p any) {
	defer func() { p = recover() }()
	f()
	return nil
}

// TestObservedPanics: a request whose getter, stat, transport or body read
// panics is ended once, as unwound, its body closed, and the panic goes on.
func TestObservedPanics(t *testing.T) {
	for _, transport := range []bool{false, true} {
		for _, where := range []string{"Getter", "Stat", "Transport", "ScalarBody", "WholeGetter", "WholeBody"} {
			t.Run(fmt.Sprintf("transport=%v/%s", transport, where), func(t *testing.T) {
				var closed atomic.Bool
				var base http.RoundTripper = rtFunc(func(req *http.Request) (*http.Response, error) {
					res, err := objectTransport(false).RoundTrip(req)
					res.Body = &closeTracked{ReadCloser: res.Body, closed: &closed}
					return res, err
				})
				if where == "Transport" {
					base = rtFunc(func(req *http.Request) (*http.Response, error) { panic("boom") })
				}
				client := &http.Client{Transport: base}
				if transport {
					client.Transport = Observe(base)
				}
				get := httpGetter(client, "http://store/b/obj")
				wrapped := func(ctx context.Context, off, n int64, pin Pin) (Response, error) {
					if !transport && (where == "Getter" || where == "WholeGetter") {
						panic("boom")
					}
					res, err := get(ctx, off, n, pin)
					if err != nil {
						return res, err
					}
					switch where {
					case "Getter", "WholeGetter":
						panic("boom")
					case "ScalarBody", "WholeBody":
						res.Body = &panicBody{rc: res.Body, closed: &closed}
					}
					return res, nil
				}
				obs := &closeFirst{closed: &closed}
				ctx := observed(obs)
				p := panicking(func() {
					switch where {
					case "Stat":
						Metadata(ctx, "b/obj", transport, func(ctx context.Context) (int64, Pin, error) {
							if !transport {
								panic("boom")
							}
							get(ctx, 0, 1, Pin{})
							panic("boom")
						})
					case "WholeGetter", "WholeBody":
						ReadObject(ctx, "b/obj", 1000, transport, wrapped)
					default:
						New("b/obj", 1000, Pin{ETag: "e"}, wrapped, transport).WithContext(ctx).ReadAt(make([]byte, 10), 0)
					}
				})
				if p != "boom" {
					t.Fatalf("panic = %v, want boom", p)
				}
				reqs, ends := obs.Events()
				if len(reqs) != 1 || len(ends) != 1 || ends[0].Err == nil || !strings.Contains(ends[0].Err.Error(), "unwound") {
					t.Fatalf("observed %+v, %+v; want exactly one failed completion", reqs, ends)
				}
				if (where == "ScalarBody" || where == "WholeBody" || transport && where != "Transport") && (!closed.Load() || obs.early.Load()) {
					t.Fatal("panicking body's read did not close it")
				}
			})
		}
	}
}

type rtFunc func(*http.Request) (*http.Response, error)

func (f rtFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type panicBody struct {
	rc     io.ReadCloser
	closed *atomic.Bool
}

func (b *panicBody) Read([]byte) (int, error) { panic("boom") }
func (b *panicBody) Close() error             { b.closed.Store(true); return b.rc.Close() }

// closeFirst is an Observer whose requests' ends check that body was
// closed first.
type closeFirst struct {
	objfiletest.Observer
	closed *atomic.Bool
	early  atomic.Bool
}

func (c *closeFirst) Begin(q objfile.Request) func(objfile.RequestResult) {
	done := c.Observer.Begin(q)
	return func(r objfile.RequestResult) {
		if !c.closed.Load() {
			c.early.Store(true)
		}
		done(r)
	}
}

// TestObservedRejectionAfterClose: a response refused unread is closed
// before its request's end is reported.
func TestObservedRejectionAfterClose(t *testing.T) {
	s := newStore(1000)
	var closed atomic.Bool
	obs := &closeFirst{closed: &closed}
	f := NewSized("obj", 999, func(ctx context.Context, off, n int64, pin Pin) (Response, error) {
		res, err := s.get(ctx, off, n, pin)
		res.Body = &closeTracked{ReadCloser: res.Body, closed: &closed}
		return res, err
	}, s.stat, false)
	var sm *objfile.SizeMismatchError
	if _, err := f.WithContext(observed(obs)).ReadAt(make([]byte, 10), 0); !errors.As(err, &sm) {
		t.Fatalf("read of another size = %v", err)
	}
	if obs.early.Load() || !closed.Load() {
		t.Fatal("the request's end was reported before its body was closed")
	}
}

// TestReadAtNoAllocs: a read with no Observer allocates nothing.
func TestReadAtNoAllocs(t *testing.T) {
	data := make([]byte, 1<<16)
	pin := Pin{ETag: "e"}
	var body memBody
	get := func(_ context.Context, off, n int64, _ Pin) (Response, error) {
		body.Reset(data[off : off+n])
		return Response{Body: &body, Pin: pin, Start: off, Total: int64(len(data)), Length: n}, nil
	}
	f := New("obj", int64(len(data)), pin, get, false)
	r := f.WithContext(context.Background())
	p := make([]byte, 4096)
	if n := testing.AllocsPerRun(100, func() {
		if _, err := f.ReadAt(p, 8192); err != nil {
			t.Fatal(err)
		}
		if _, err := r.ReadAt(p, 100); err != nil {
			t.Fatal(err)
		}
	}); n != 0 {
		t.Fatalf("%v allocations a read without an Observer, want 0", n)
	}
}

type closeTracked struct {
	io.ReadCloser
	closed *atomic.Bool
}

func (b *closeTracked) Close() error { b.closed.Store(true); return b.ReadCloser.Close() }

// TestObservedWholeCleanup checks both successful consumption and unread
// rejection: completion sees the body closed and retains the validation error.
func TestObservedWholeCleanup(t *testing.T) {
	for _, reject := range []bool{false, true} {
		t.Run(fmt.Sprint(reject), func(t *testing.T) {
			var closed atomic.Bool
			obs := &closeFirst{closed: &closed}
			get := func(context.Context, int64, int64, Pin) (Response, error) {
				pin := Pin{ETag: "e"}
				if reject {
					pin = Pin{}
				}
				return Response{Body: &closeTracked{ReadCloser: io.NopCloser(strings.NewReader("abc")), closed: &closed}, Pin: pin, Length: 3, Total: 3}, nil
			}
			_, _, err := ReadObject(observed(obs), "obj", 3, false, get)
			if (err != nil) != reject {
				t.Fatalf("ReadObject = %v", err)
			}
			reqs, ends := obs.Events()
			if len(reqs) != 1 || len(ends) != 1 || (ends[0].Err != nil) != reject || obs.early.Load() || !closed.Load() {
				t.Fatalf("observed %+v, %+v, early %v", reqs, ends, obs.early.Load())
			}
		})
	}
}
