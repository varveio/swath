// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package ranged

import (
	"bytes"
	"context"
	"io"
	"testing"
)

// memBody is a reusable in-memory response body.
type memBody struct{ bytes.Reader }

func (*memBody) Close() error { return nil }

// BenchmarkReadAt measures the client-side cost of one pinned ReadAt of a
// 4 KiB page (a typical Parquet page), the store being an in-memory getter
// that allocates nothing itself: what remains is the Object's own work.
func BenchmarkReadAt(b *testing.B) {
	data := bytes.Repeat([]byte("0123456789abcdef"), 1<<12)
	pin := Pin{ETag: "e"}
	var body memBody
	get := func(_ context.Context, off, n int64, _ Pin) (Response, error) {
		body.Reset(data[off : off+n])
		return Response{Body: &body, Pin: pin, Start: off, Total: int64(len(data)), Length: n}, nil
	}
	f := New("obj", int64(len(data)), pin, get)
	p := make([]byte, 4096)
	b.SetBytes(int64(len(p)))
	b.ReportAllocs()
	for b.Loop() {
		if _, err := f.ReadAt(p, 8192); err != nil && err != io.EOF {
			b.Fatal(err)
		}
	}
}
