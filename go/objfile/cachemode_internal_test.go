// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objfile

import (
	"context"
	"errors"
	"io/fs"
	"testing"
)

func TestCacheMode(t *testing.T) {
	ctx := context.Background()
	if m, err := cacheMode(ctx); m != CacheDefault || err != nil {
		t.Fatalf("no mode: %v, %v", m, err)
	}
	if m, err := cacheMode(WithCacheMode(ctx, CacheBypass)); m != CacheBypass || err != nil {
		t.Fatalf("bypass: %v, %v", m, err)
	}
	if m := CacheModeFromContext(WithCacheMode(WithCacheMode(ctx, CacheBypass), CacheDefault)); m != CacheDefault {
		t.Fatalf("the innermost mode is not the one set last: %v", m)
	}
	if _, err := cacheMode(WithCacheMode(ctx, 7)); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("unknown mode: %v, want fs.ErrInvalid", err)
	}
}
