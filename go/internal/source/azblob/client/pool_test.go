// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// TestPoolSize: a built client has one transport per 16 workers, at least
// 1, at most 8.
func TestPoolSize(t *testing.T) {
	for workers, want := range map[int]int{-1: 1, 0: 1, 1: 1, 16: 1, 17: 2, 64: 4, 128: 8, 129: 8, 1024: 8} {
		if got := newTestClient(t, Config{Anonymous: true, Workers: workers}).Transports(); got != want {
			t.Errorf("%d workers: %d transports, want %d", workers, got, want)
		}
	}
}

// TestTransportPoolSpreadsConnections lists over HTTP/2 through a TLS
// server that records each request's connection: the distinct connections
// are the pool's transports, and round-robin gives each the same share.
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
			fmt.Fprint(w, page(""))
		}))
		srv.EnableHTTP2 = true
		srv.StartTLS()
		c := newTestClient(t, Config{Endpoint: srv.URL, Anonymous: true, Workers: workers})
		roots := srv.Client().Transport.(*http.Transport).TLSClientConfig.RootCAs
		for _, hc := range c.http {
			hc.Transport.(noLocation).next.(*http.Transport).TLSClientConfig.RootCAs = roots
		}
		n := c.Transports()
		list := func() {
			if _, err := c.List(context.Background(), &ListInput{Container: "c"}); err != nil {
				t.Error(err)
			}
		}
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
	c := newTestClient(t, Config{Anonymous: true, Workers: 128, HTTPClient: &http.Client{}})
	if n := c.Transports(); n != 1 {
		t.Fatalf("%d transports with a supplied client, want 1", n)
	}
}
