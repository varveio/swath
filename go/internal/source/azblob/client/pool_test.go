// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestServicePool(t *testing.T) {
	for _, supplied := range []*http.Client{nil, {}} {
		c := newTestClient(t, Config{Anonymous: true, Workers: 64, HTTPClient: supplied})
		want := 4
		if supplied != nil {
			want = 1
		}
		if c.Transports() != want || len(c.svc) != want {
			t.Fatalf("%d transports, %d services, want %d", c.Transports(), len(c.svc), want)
		}
	}
}

func TestServicePoolRoundRobin(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, page("")) }))
	defer srv.Close()
	c := newTestClient(t, Config{Endpoint: srv.URL, Anonymous: true, Workers: 64})
	seen := make(chan int, len(c.http)*2)
	for i, hc := range c.http {
		tr := hc.Transport.(noLocation).next.(*http.Transport)
		t.Cleanup(tr.CloseIdleConnections)
		tr.Proxy = func(*http.Request) (*url.URL, error) { seen <- i; return nil, nil }
	}
	for i := range len(c.http) * 2 {
		if _, err := c.List(context.Background(), &ListInput{Container: "c"}); err != nil {
			t.Fatal(err)
		}
		select {
		case got := <-seen:
			if got != i%len(c.http) {
				t.Fatalf("request %d used service %d", i, got)
			}
		default:
			t.Fatal("request bypassed the pool")
		}
	}
}
