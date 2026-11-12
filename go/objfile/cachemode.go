// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objfile

import (
	"context"
	"fmt"
	"io/fs"
)

// CacheMode controls byte-cache use for an operation.
type CacheMode uint8

const (
	// CacheDefault permits byte-cache lookup and admission.
	CacheDefault CacheMode = iota

	// CacheBypass disables lookup, admission, and joining in-flight
	// cache fetches. Identity verification and Pin remain in effect.
	CacheBypass
)

type cacheModeKey struct{}

// WithCacheMode sets the operation's byte-cache mode. Context-bearing
// operations of a Cache's Opener and Objects reject an unknown mode before
// I/O, with an error wrapping fs.ErrInvalid. A plain ReadAt, bound to
// context.Background, is CacheDefault; WithContext(ctx) binds ctx's mode.
func WithCacheMode(ctx context.Context, mode CacheMode) context.Context {
	return context.WithValue(ctx, cacheModeKey{}, mode)
}

// CacheModeFromContext returns ctx's mode, or CacheDefault.
func CacheModeFromContext(ctx context.Context) CacheMode {
	m, _ := ctx.Value(cacheModeKey{}).(CacheMode)
	return m
}

// cacheMode returns ctx's mode, or an error wrapping fs.ErrInvalid for one
// this package does not know.
func cacheMode(ctx context.Context) (CacheMode, error) {
	switch m := CacheModeFromContext(ctx); m {
	case CacheDefault, CacheBypass:
		return m, nil
	default:
		return m, fmt.Errorf("objfile: cache mode %d: %w", m, fs.ErrInvalid)
	}
}
