// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/varveio/swath/go/internal/source/objhttp"
)

// TestPoolSize: a built client has one transport (and one generated
// service) per 16 workers, at least 1, at most 8.
func TestPoolSize(t *testing.T) {
	for workers, want := range map[int]int{-1: 1, 0: 1, 1: 1, 16: 1, 17: 2, 64: 4, 128: 8, 129: 8, 1024: 8} {
		c, err := New(context.Background(), Config{Anonymous: true, Workers: workers})
		if err != nil {
			t.Fatal(err)
		}
		if got := c.Transports(); got != want || len(c.svc) != want {
			t.Errorf("%d workers: %d transports, want %d", workers, got, want)
		}
	}
}

// TestTransportPoolSpreadsConnections lists over HTTP/2 through a TLS
// server that records each request's connection: one HTTP/2 connection
// carries every request of a transport, so the distinct connections are
// the pool's transports, and round-robin gives each the same share. With
// one worker the pool is a single transport on one connection, as before.
func TestTransportPoolSpreadsConnections(t *testing.T) {
	for _, workers := range []int{1, 16, 64, 128} {
		var mu sync.Mutex
		perConn := map[string]int{}
		srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.ProtoMajor != 2 {
				t.Errorf("request over %s, want HTTP/2", r.Proto)
			}
			mu.Lock()
			perConn[r.RemoteAddr]++
			mu.Unlock()
			w.Write([]byte(`{"kind":"storage#objects"}`))
		}))
		srv.EnableHTTP2 = true
		srv.StartTLS()
		pool := objhttp.Pool(workers, nil, nil)
		roots := srv.Client().Transport.(*http.Transport).TLSClientConfig.RootCAs
		for _, hc := range pool {
			hc.Transport.(*http.Transport).TLSClientConfig.RootCAs = roots
		}
		c, err := newClient(context.Background(), Config{Endpoint: srv.URL + "/storage/v1/", Anonymous: true, Workers: workers}, librarySearch{}, pool)
		if err != nil {
			t.Fatal(err)
		}
		n := c.Transports()
		if n != len(pool) {
			t.Fatalf("%d workers: %d services, want %d", workers, n, len(pool))
		}
		list := func() {
			if _, err := c.List(context.Background(), &ListInput{Bucket: "b"}); err != nil {
				t.Error(err)
			}
		}
		// One request per transport opens its connection; then a burst of
		// concurrent requests rides on those connections.
		for range n {
			list()
		}
		const per = 8
		var wg sync.WaitGroup
		for range n * per {
			wg.Go(list)
		}
		wg.Wait()
		srv.Close()
		if len(perConn) != n {
			t.Fatalf("%d workers: %d connections %v, want %d", workers, len(perConn), perConn, n)
		}
		for addr, k := range perConn {
			if k != per+1 {
				t.Errorf("%d workers: connection %s carried %d requests, want %d", workers, addr, k, per+1)
			}
		}
	}
}

// TestSuppliedClientIsOnePool: a supplied HTTP client is used alone.
func TestSuppliedClientIsOnePool(t *testing.T) {
	c, err := New(context.Background(), Config{Anonymous: true, Workers: 128, HTTPClient: &http.Client{}})
	if err != nil {
		t.Fatal(err)
	}
	if n := c.Transports(); n != 1 || len(c.svc) != 1 {
		t.Fatalf("%d transports, %d services with a supplied client, want 1", n, len(c.svc))
	}
}
