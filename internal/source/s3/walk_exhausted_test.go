// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/internal/live"
	"github.com/varveio/swath/internal/source/objhttp"
	"github.com/varveio/swath/internal/source/s3/client"
	"github.com/varveio/swath/internal/source/s3/s3test"
	"github.com/varveio/swath/reader/listing"
)

// discard is a live.Sink that keeps nothing.
type discard struct{}

func (discard) Entries(context.Context, []listing.Entry) error { return nil }
func (discard) Unread(listing.Unread)                          {}

// TestWalkFailsWhenRetriesAreExhausted (#716): a live walk against an
// endpoint that stops answering ends once its page's client retries are
// exhausted, with listing.ErrUnavailable naming the attempt timeout, and
// never asks the page again; before, the engine requeued the page for as
// long as the walk's context lived, so a caller without a deadline waited
// forever. The endpoint holds every request until the client's own
// attempt timeout cancels it; the test's deadline only bounds a walk that
// does not end.
func TestWalkFailsWhenRetriesAreExhausted(t *testing.T) {
	s3test.NoCredentials(t)
	const attempts = 2
	for _, workers := range []int{1, 0} {
		var held atomic.Int64
		ep := s3test.Serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			held.Add(1)
			<-r.Context().Done() // silent until the client gives up on the attempt
		}))
		cl, err := client.New(context.Background(), client.Config{Endpoint: ep, PathStyle: true, Anonymous: true, Region: "us-east-1",
			RequestTimeout: 50 * time.Millisecond,
			Retry:          objhttp.Retry{MaxAttempts: attempts, BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond}})
		if err != nil {
			t.Fatal(err)
		}
		l := &Live{bucket: "bkt", client: cl, location: "test"}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err = l.Walk(ctx, live.Walk{Prefix: []byte("a/"), Workers: workers}, discard{})
		bounded := ctx.Err() != nil
		cancel()
		var ex *objhttp.ExhaustedError
		switch {
		case bounded:
			t.Fatalf("workers %d: the walk ran until the test's deadline (%v) after %d attempts", workers, err, held.Load())
		case !errors.Is(err, listing.ErrUnavailable) || !errors.Is(err, objhttp.ErrTimeout) || !errors.As(err, &ex) || ex.Attempts != attempts:
			t.Fatalf("workers %d: %v, want ErrUnavailable: the attempt timeout after %d attempts", workers, err, attempts)
		case held.Load() != attempts:
			t.Fatalf("workers %d: the endpoint held %d requests, want %d: the exhausted page was asked again", workers, held.Load(), attempts)
		}
	}
}
