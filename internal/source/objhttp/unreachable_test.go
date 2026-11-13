// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objhttp

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/varveio/swath/internal/source"
)

// TestCertificateFinal: a refused certificate is final at once, never
// retried, so the unreachable fail-fast needs no grace for it.
func TestCertificateFinal(t *testing.T) {
	err := &url.Error{Op: "Get", URL: "https://h/", Err: &net.OpError{Op: "dial", Net: "tcp", Err: &tls.CertificateVerificationError{Err: errors.New("x509: unknown authority")}}}
	if f := Normalize(err); f.Transient {
		t.Fatal("a refused certificate is retried")
	}
}

// closedPort is a local address nothing listens on.
func closedPort(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	host := ln.Addr().String()
	ln.Close()
	return host
}

// guardedGet is an attempt through a Guard: a GET whose body is read.
func guardedGet(c *http.Client, u string) func(ctx context.Context) (int, error) {
	return func(ctx context.Context) (int, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return 0, err
		}
		resp, err := c.Do(req)
		if err != nil {
			return 0, err
		}
		defer resp.Body.Close()
		_, err = io.Copy(io.Discard, resp.Body)
		return resp.StatusCode, err
	}
}

// TestUnreachableRunWide: the fail-fast is the run's, not a client's. With
// nothing answered, a client meeting refused connections stops as
// unreachable once the grace has passed; once any client has had an
// answer, a client made later that meets refused connections (a
// destination, a token endpoint first used hours in) is retried to
// exhaustion as before, never stopped as unreachable.
func TestUnreachableRunWide(t *testing.T) {
	g := source.UnreachableGrace
	source.UnreachableGrace = 50 * time.Millisecond
	t.Cleanup(func() { source.UnreachableGrace = g })
	hc := &http.Client{Transport: &Guard{}}
	dead := "http://" + closedPort(t) + "/"
	r := Retry{MaxAttempts: 30, BaseBackoff: 10 * time.Millisecond, MaxBackoff: 20 * time.Millisecond}

	_, disarm := source.ArmUnreachable(0)
	var lone Counters
	_, err := Do(context.Background(), r, &lone, guardedGet(hc, dead))
	if !errors.Is(err, source.ErrUnreachable) || Classify(err) != ClassUnreachable || lone.Load().UnreachableStops != 1 {
		t.Fatalf("nothing answered: %v (class %q)", err, Classify(err))
	}
	disarm()

	_, disarm = source.ArmUnreachable(0)
	defer disarm()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, "ok") }))
	defer srv.Close()
	var first, later Counters
	if _, err := Do(context.Background(), r, &first, guardedGet(hc, srv.URL)); err != nil {
		t.Fatal(err)
	}
	r.MaxAttempts = 8 // past the grace: 7 backoffs of 10 to 20 ms
	_, err = Do(context.Background(), r, &later, guardedGet(hc, dead))
	var ex *ExhaustedError
	if !errors.As(err, &ex) || errors.Is(err, source.ErrUnreachable) || later.Load().UnreachableStops != 0 {
		t.Fatalf("a later client after an answer: %v", err)
	}
}

// TestUnreachableSlowAnswer: an answer's headers turn the fail-fast off at
// once, while its body is still arriving: a slow first page is an answer,
// and connections refused meanwhile do not stop the run.
func TestUnreachableSlowAnswer(t *testing.T) {
	_, disarm := source.ArmUnreachable(0)
	defer disarm()
	source.ConnectFailed(time.Now()) // another worker's refused connection
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "2")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-release // the body comes late
		io.WriteString(w, "ok")
	}))
	defer srv.Close()
	defer close(release)
	hc := &http.Client{Transport: &Guard{}}
	done := make(chan error, 1)
	go func() {
		_, err := Do(context.Background(), Retry{MaxAttempts: 1}, &Counters{}, guardedGet(hc, srv.URL))
		done <- err
	}()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, armed := source.UnreachableArmed(); !armed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("still armed with the answer's headers in")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-done:
		t.Fatalf("the attempt ended before its body: %v", err)
	default:
	}
	if expired, _ := source.ConnectFailed(time.Now().Add(time.Hour)); expired {
		t.Fatal("expired while an answer's body was arriving")
	}
}
