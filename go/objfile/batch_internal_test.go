// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objfile

import (
	"context"
	"math"
	"testing"
)

// TestAddBatchBytes: a batch's byte total is checked without overflow: a
// length that would wrap the sum is over the bound, not a small total.
func TestAddBatchBytes(t *testing.T) {
	for _, c := range []struct {
		total, n int64
		ok       bool
	}{
		{0, MaxBatchBytes, true}, {1, MaxBatchBytes, false}, {1, math.MaxInt64, false},
		{MaxBatchBytes, 0, true}, {MaxBatchBytes, 1, false}, {0, math.MaxInt64, false},
	} {
		got, ok := addBatchBytes(c.total, c.n)
		if ok != c.ok || ok && got != c.total+c.n || !ok && got != c.total {
			t.Errorf("addBatchBytes(%d, %d) = %d, %v; want ok=%v", c.total, c.n, got, ok, c.ok)
		}
	}
}

// WithBatchWait installs a scheduler handshake for external composition tests.
func WithBatchWait(ctx context.Context, wait func(int)) context.Context {
	return context.WithValue(ctx, batchWaitKey{}, wait)
}

// WithAdmissionWait installs a handshake at Limit admission for external tests.
func WithAdmissionWait(ctx context.Context, wait func()) context.Context {
	return context.WithValue(ctx, admissionWaitKey{}, wait)
}
