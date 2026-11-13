// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"net/http"
	"net/url"
	"testing"

	"golang.org/x/oauth2/google"

	"github.com/varveio/swath/internal/source/objhttp"
)

func TestServicePool(t *testing.T) {
	for _, supplied := range []*http.Client{nil, {}} {
		c, err := New(context.Background(), Config{Anonymous: true, Workers: 64, HTTPClient: supplied})
		if err != nil {
			t.Fatal(err)
		}
		want := 4
		if supplied != nil {
			want = 1
		}
		if c.Transports() != want || len(c.svc) != want {
			t.Fatalf("%d transports, %d services, want %d", c.Transports(), len(c.svc), want)
		}
	}
}

// Observe actual requests through each service, so rotating only pick's
// index while using one service would fail too.
func TestServicePoolRoundRobin(t *testing.T) {
	srv := newRecorder(t)
	pool := objhttp.Pool(64, nil, nil, nil)
	seen := make(chan int, len(pool)*2)
	for i, hc := range pool {
		tr := hc.Transport.(*http.Transport)
		t.Cleanup(tr.CloseIdleConnections)
		tr.Proxy = func(*http.Request) (*url.URL, error) { seen <- i; return nil, nil }
	}
	c, err := newClient(context.Background(), Config{Endpoint: srv.URL + "/storage/v1/", Anonymous: true}, google.FindDefaultCredentials, pool)
	if err != nil {
		t.Fatal(err)
	}
	for i := range len(pool) * 2 {
		if _, err := c.List(context.Background(), &ListInput{Bucket: "b"}); err != nil {
			t.Fatal(err)
		}
		select {
		case got := <-seen:
			if got != i%len(pool) {
				t.Fatalf("request %d used service %d", i, got)
			}
		default:
			t.Fatal("request bypassed the pool")
		}
	}
}
