// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package gcstest serves a GCS JSON API bucket for tests (test support;
// nothing in the tool imports it): Strict, the objects.list fake the GCS
// provider's own protocol tests run against, and Bucket and Handler over
// it for run-level and command-level tests.
package gcstest

import (
	"fmt"
	"net/http"
	"testing"
	"time"
)

// Keys are n names spread over directories, as the run-level tests use.
func Keys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("d%02d/sub%d/obj+%06d", i%9, i%4, i)
	}
	return keys
}

// Bucket serves Handler(keys, delay) and returns its endpoint
// (…/storage/v1/), closed at the test's end.
func Bucket(t testing.TB, keys []string, delay time.Duration) string {
	t.Helper()
	f := NewStrict(keys)
	f.Delay, f.Lean = delay, true
	return f.Serve(t)
}

// Path is the endpoint's path on the server.
const Path = "/storage/v1/"

// Handler is the strict fake over keys with lean objects (Strict.Lean),
// each answer after delay, for a test that wraps it (faults, latency)
// before serving it.
func Handler(keys []string, delay time.Duration) http.Handler {
	f := NewStrict(keys)
	f.Delay, f.Lean = delay, true
	return f
}
