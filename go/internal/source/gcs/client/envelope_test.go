// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/source/objhttp"
)

// TestInvalidSuccessEnvelopeIsRetried: a 200 whose body is `null`, another
// JSON value, an error envelope or an object without the objects kind is
// not an empty final page: it is retried like a garbled body (counted as a
// parse failure) until a real page comes; an envelope naming its kind and
// nothing else is a legitimate empty listing.
func TestInvalidSuccessEnvelopeIsRetried(t *testing.T) {
	bad := []string{`null`, ` null `, `[]`, `"x"`, `{"error":{"code":503,"message":"backend"}}`, `{}`}
	var n atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		i := int(n.Add(1)) - 1
		if r.URL.Query().Get("prefix") == "empty" {
			fmt.Fprint(w, `{"kind":"storage#objects"}`)
			return
		}
		if i < len(bad) {
			fmt.Fprint(w, bad[i])
			return
		}
		fmt.Fprint(w, `{"kind":"storage#objects","items":[{"name":"a","size":"1","updated":"2026-01-01T00:00:00Z"}]}`)
	}))
	defer srv.Close()
	c, err := New(context.Background(), Config{Endpoint: srv.URL + "/storage/v1/", Anonymous: true, Retry: objhttp.Retry{MaxAttempts: 9, BaseBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond}})
	if err != nil {
		t.Fatal(err)
	}
	p, err := c.List(context.Background(), &ListInput{Bucket: "b"})
	if err != nil || len(p.Items) != 1 || p.Items[0].Name != "a" {
		t.Fatalf("page %+v, err %v", p, err)
	}
	if got := c.Counters(); got.ParseFailures != int64(len(bad)) || got.Retries != int64(len(bad)) {
		t.Fatalf("counters %+v, want %d parse failures retried", got, len(bad))
	}
	p, err = c.List(context.Background(), &ListInput{Bucket: "b", Prefix: "empty"})
	if err != nil || len(p.Items) != 0 || p.NextPageToken != "" {
		t.Fatalf("empty listing: %+v, %v", p, err)
	}
}
