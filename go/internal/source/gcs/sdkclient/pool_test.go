// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package sdkclient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/varveio/swath/go/internal/source/gcs/client"
)

// TestTransportPoolSpreadsConnections: the SDK client's services ride the
// core's transport pool (client.PoolSize transports, one HTTP/2
// connection each) and take it round-robin, as the hand-rolled client
// does.
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
		c, err := New(context.Background(), client.Config{Endpoint: srv.URL + "/storage/v1/", Anonymous: true, Workers: workers})
		if err != nil {
			t.Fatal(err)
		}
		roots := srv.Client().Transport.(*http.Transport).TLSClientConfig.RootCAs
		for _, hc := range c.Pool() {
			hc.Transport.(*http.Transport).TLSClientConfig.RootCAs = roots
		}
		n := c.Transports()
		if n != client.PoolSize(workers) || len(c.svc) != n {
			t.Fatalf("%d workers: %d transports, %d services, want %d", workers, n, len(c.svc), client.PoolSize(workers))
		}
		list := func() {
			if _, err := c.List(context.Background(), &client.ListInput{Bucket: "b"}); err != nil {
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
	c, err := New(context.Background(), client.Config{Anonymous: true, Workers: 128, HTTPClient: &http.Client{}})
	if err != nil {
		t.Fatal(err)
	}
	if c.Transports() != 1 || len(c.svc) != 1 {
		t.Fatalf("%d transports, %d services with a supplied client, want 1", c.Transports(), len(c.svc))
	}
}
