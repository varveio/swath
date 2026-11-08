// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package ranged

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/varveio/swath/go/objfile"
)

// cut is a body that yields at most n bytes, then fails.
type cut struct {
	r io.Reader
	n int
}

func (c *cut) Read(p []byte) (int, error) {
	if c.n == 0 {
		return 0, io.ErrUnexpectedEOF
	}
	p = p[:min(len(p), c.n)]
	n, err := c.r.Read(p)
	c.n -= n
	return n, err
}

func (c *cut) Close() error { return nil }

// TestBodyRetry: a body cut short is requested again from where it stopped,
// at most BodyRetries times.
func TestBodyRetry(t *testing.T) {
	data := bytes.Repeat([]byte("0123456789"), 10)
	pin := Pin{ETag: "e"}
	for _, cuts := range []int{0, 1, BodyRetries, BodyRetries + 1} {
		var offs []int64
		get := func(_ context.Context, off, n int64, _ Pin) (Response, error) {
			offs = append(offs, off)
			body := &cut{r: bytes.NewReader(data[off : off+n]), n: int(n)}
			if len(offs) <= cuts {
				body.n = 7
			}
			return Response{Body: body, Pin: pin, Total: int64(len(data)), Length: n}, nil
		}
		f := New("obj", int64(len(data)), pin, get)
		p := make([]byte, 50)
		n, err := f.ReadAt(p, 10)
		if cuts > BodyRetries {
			if !errors.Is(err, io.ErrUnexpectedEOF) || n != 7*(BodyRetries+1) {
				t.Errorf("%d cuts: ReadAt = %d, %v; want %d, ErrUnexpectedEOF", cuts, n, err, 7*(BodyRetries+1))
			}
			continue
		}
		if err != nil || n != 50 || !bytes.Equal(p, data[10:60]) {
			t.Errorf("%d cuts: ReadAt = %d, %v", cuts, n, err)
		}
		for i, off := range offs {
			if off != 10+int64(7*i) {
				t.Errorf("%d cuts: request %d at %d, want %d", cuts, i, off, 10+7*i)
			}
		}
	}
}

// TestPinCheck: an unpinned file takes the first response's pin; a response
// naming another version or size fails with ErrObjectChanged.
func TestPinCheck(t *testing.T) {
	served := Pin{ETag: "a"}
	total := int64(10)
	get := func(_ context.Context, off, n int64, _ Pin) (Response, error) {
		return Response{Body: io.NopCloser(bytes.NewReader(make([]byte, n))), Pin: served, Total: total, Length: n}, nil
	}
	f := New("obj", 10, Pin{}, get)
	if _, err := f.ReadAt(make([]byte, 2), 0); err != nil || f.Identity() != "a" {
		t.Fatalf("first read: %v, Identity %q", err, f.Identity())
	}
	served = Pin{ETag: "b"}
	if _, err := f.ReadAt(make([]byte, 2), 0); !errors.Is(err, objfile.ErrObjectChanged) {
		t.Fatalf("read of another ETag: %v", err)
	}
	served, total = Pin{ETag: "a"}, 11
	if _, err := f.ReadAt(make([]byte, 2), 0); !errors.Is(err, objfile.ErrObjectChanged) {
		t.Fatalf("read of another size: %v", err)
	}
}
