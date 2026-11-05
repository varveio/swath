// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/order"
	"github.com/varveio/swath/go/internal/source/keyrange"
	"github.com/varveio/swath/go/internal/source/s3/client"
)

// flaky fails its first n calls with err, then serves the stub.
type flaky struct {
	stub
	n   atomic.Int64
	err error
}

func (f *flaky) ListObjectsV2(ctx context.Context, in *client.ListInput) (*client.Page, error) {
	if f.n.Add(-1) >= 0 {
		return nil, f.err
	}
	return f.stub.ListObjectsV2(ctx, in)
}

// TestRetryExhaustionNeverFailsTheRun: a page whose client retries are
// exhausted on a transient failure is asked again after a backoff, until
// it succeeds; a permanent failure stops the run at once.
func TestRetryExhaustionNeverFailsTheRun(t *testing.T) {
	defer keyrange.SetRequeueBackoff(time.Millisecond, 4*time.Millisecond)()
	page := &client.Page{Contents: []client.Object{{Key: "a"}, {Key: "b"}}}
	exhausted := &client.ExhaustedError{Attempts: 9, Err: &client.Error{Status: 503, Code: "SlowDown"}}
	f := &flaky{stub: stub{pages: []*client.Page{page}}, err: exhausted}
	f.n.Store(5)
	rows, st := list(t, Options{Bucket: "b", Client: f, Sequential: true}, 1)
	if len(rows) != 2 || st.Counters.Value("requeues") != 5 {
		t.Fatalf("%d rows, %d requeues", len(rows), st.Counters.Value("requeues"))
	}
	denied := &flaky{stub: stub{pages: []*client.Page{page}}, err: &client.Error{Status: 403, Code: "AccessDenied"}}
	denied.n.Store(1)
	p, err := New(context.Background(), Options{Bucket: "b", Client: denied, Sequential: true})
	if err != nil {
		t.Fatal(err)
	}
	_, err = order.Run[Range, Object](context.Background(), p, &collect{}, &Schema, order.Options{Workers: 1, Budget: 1 << 20})
	if client.Classify(err) != client.ClassAccessDenied {
		t.Fatalf("access denied: %v (%s)", err, client.Classify(err))
	}
}

// TestClassify covers every class.
func TestClassify(t *testing.T) {
	se := func(status int, code string) error { return &client.Error{Status: status, Code: code} }
	for want, err := range map[client.Class]error{
		client.ClassAuth:         se(403, "InvalidAccessKeyId"),
		client.ClassAccessDenied: se(403, "AccessDenied"),
		client.ClassNoSuchBucket: se(404, "NoSuchBucket"),
		client.ClassRedirect:     se(301, "PermanentRedirect"),
		client.ClassThrottle:     &client.ExhaustedError{Attempts: 9, Err: se(503, "SlowDown")},
		client.ClassTimeout:      se(400, "RequestTimeout"),
		client.ClassServer:       se(500, "InternalError"),
		client.ClassTransport:    &client.ExhaustedError{Attempts: 9, Err: errors.Join(errors.New("read: connection reset by peer"))},
		client.ClassParse:        client.ErrSyntax,
		client.ClassInvalidPage:  client.ErrInvalidPage,
		client.ClassCanceled:     context.Canceled,
		client.ClassOther:        errors.New("something else"),
	} {
		if got := client.Classify(err); got != want {
			t.Errorf("%v: %s, want %s", err, got, want)
		}
		if transient := want == client.ClassThrottle || want == client.ClassTimeout || want == client.ClassServer ||
			want == client.ClassTransport || want == client.ClassParse; want.Transient() != transient {
			t.Errorf("%s: transient %v", want, want.Transient())
		}
	}
}
