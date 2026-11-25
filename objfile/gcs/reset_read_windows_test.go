// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs_test

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"os"
	"sync/atomic"
	"syscall"
	"testing"

	"cloud.google.com/go/storage"

	"github.com/varveio/swath/objfile"
	"github.com/varveio/swath/objfile/gcs"
)

// resetFirst fails the first request of every two with a connection the
// peer reset or aborted, worded as Windows words it ("An existing
// connection was forcibly closed by the remote host"), and passes the next
// on.
type resetFirst struct {
	errno syscall.Errno
	n     atomic.Int64
	next  http.RoundTripper
}

func (tr *resetFirst) RoundTrip(req *http.Request) (*http.Response, error) {
	if tr.n.Add(1)%2 == 1 {
		if req.Body != nil {
			req.Body.Close()
		}
		return nil, &net.OpError{Op: "read", Net: "tcp", Err: os.NewSyscallError("wsarecv", tr.errno)}
	}
	return tr.next.RoundTrip(req)
}

// TestReadResetRetriedOnWindows: a read whose connection the peer reset or
// aborted is retried, on a client with an attempt allowance (a
// destination's, remote.OpenDestination) and on one without (a store's,
// remote.Open): the object's metadata, a range of it and the whole object
// are each read, the reset request asked again. The SDK's policy
// (storage.ShouldRetry) matches only "connection reset" in the message, so
// before, each read failed at the first reset, as uploads did before
// TestResetRetriedOnWindows.
func TestReadResetRetriedOnWindows(t *testing.T) {
	data := []byte("0123456789abcdef")
	for _, errno := range []syscall.Errno{syscall.WSAECONNRESET, syscall.WSAECONNABORTED} {
		for _, bounded := range []bool{true, false} {
			tr := &resetFirst{errno: errno, next: http.DefaultTransport}
			f, c := clientOf(t, func(h http.Handler) http.Handler { return h }, tr)
			if bounded {
				c.SetRetry(storage.WithMaxAttempts(gcs.DefaultMaxAttempts))
			}
			f.Put("obj", data)
			ctx := context.Background()
			op := gcs.New(c, bucket)
			obj, err := op.Open(ctx, "obj")
			if err != nil {
				t.Fatalf("%v bounded=%v: open: %v", errno, bounded, err)
			}
			buf := make([]byte, 6)
			if _, err := obj.ReadAt(buf, 4); err != nil || string(buf) != "456789" {
				t.Fatalf("%v bounded=%v: range %q, %v", errno, bounded, buf, err)
			}
			whole, _, err := objfile.ReadObject(ctx, op, "obj", 1<<10)
			if err != nil || !bytes.Equal(whole, data) {
				t.Fatalf("%v bounded=%v: whole %q, %v", errno, bounded, whole, err)
			}
			if n := tr.n.Load(); n < 6 {
				t.Fatalf("%v bounded=%v: %d requests, want each of 3 reset once and asked again", errno, bounded, n)
			}
		}
	}
}
