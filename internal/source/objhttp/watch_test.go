// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objhttp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/internal/source"
)

// TestWatch: the watch sees each round trip through a Guard (method, URL,
// status) and each failed attempt of Do, retried or the last.
func TestWatch(t *testing.T) {
	var n atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte("ok"))
	}))
	t.Cleanup(srv.Close)
	var mu sync.Mutex
	var seen []source.Exchange
	var failures []source.Failed
	source.SetWatch(&source.Watch{
		Exchange: func(e source.Exchange) { mu.Lock(); seen = append(seen, e); mu.Unlock() },
		Failed:   func(f source.Failed) { mu.Lock(); failures = append(failures, f); mu.Unlock() },
	})
	t.Cleanup(func() { source.SetWatch(nil) })
	hc := &http.Client{Transport: &Guard{}}
	var c Counters
	r := Retry{BaseBackoff: time.Millisecond}
	r.Defaults()
	_, err := Do(context.Background(), r, &c, func(ctx context.Context) (int, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/b?prefix=a", nil)
		if err != nil {
			return 0, err
		}
		resp, err := hc.Do(req)
		if err != nil {
			return 0, err
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return 0, &Failure{Transient: true, Status: resp.StatusCode, Class: ClassOther, Cause: http.ErrHandlerTimeout}
		}
		return resp.StatusCode, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 2 || seen[0].Status != 503 || seen[1].Status != 200 || seen[0].Method != http.MethodGet || seen[0].URL.Path != "/b" || seen[1].Latency < 0 { // 0 on a coarse clock (Windows)
		t.Fatalf("exchanges %+v", seen)
	}
	if len(failures) != 1 || failures[0].Attempt != 1 || !failures[0].Retry || failures[0].Wait <= 0 || failures[0].Status != 503 || failures[0].Class != string(ClassOther) ||
		failures[0].URL == nil || failures[0].URL.Path != "/b" || failures[0].URL.Query().Get("prefix") != "a" {
		t.Fatalf("failures %+v", failures)
	}
}
