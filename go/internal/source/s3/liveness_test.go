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
	"github.com/varveio/swath/go/internal/source/objhttp"
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
	exhausted := &objhttp.ExhaustedError{Attempts: 9, Err: objhttp.Normalize(&client.Error{Status: 503, Code: "SlowDown"})}
	f := &flaky{stub: stub{pages: []*client.Page{page}}, err: exhausted}
	f.n.Store(5)
	rows, st := list(t, Options{Bucket: "b", Client: f, Policy: keyrange.Policy{Sequential: true}}, 1)
	if len(rows) != 2 || st.Counters.Value("requeues") != 5 {
		t.Fatalf("%d rows, %d requeues", len(rows), st.Counters.Value("requeues"))
	}
	denied := &flaky{stub: stub{pages: []*client.Page{page}}, err: &client.Error{Status: 403, Code: "AccessDenied"}}
	denied.n.Store(1)
	p, err := New(context.Background(), Options{Bucket: "b", Client: denied, Policy: keyrange.Policy{Sequential: true}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = order.Run[Range, Object](context.Background(), p, &collect{}, &Schema, order.Options{Workers: 1, Budget: 1 << 20})
	if objhttp.Classify(err) != objhttp.ClassAccessDenied {
		t.Fatalf("access denied: %v (%s)", err, objhttp.Classify(err))
	}
}

// TestClassify: a connection reset known only by its text is transient.
// The status and code classes are TestAnswers' and the shared contract's.
func TestClassify(t *testing.T) {
	err := errors.Join(errors.New("read: connection reset by peer"))
	if f := objhttp.Normalize(err); objhttp.Classify(f) != objhttp.ClassOther || !f.Transient {
		t.Errorf("%v: failure %+v, want other, transient", err, f)
	}
}
