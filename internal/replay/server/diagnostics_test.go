// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"regexp"
	"strings"
	"testing"

	"github.com/varveio/swath/internal/replay/store/memstore"
)

// TestSlowRequestLogLine pins the slow-request line's fields and their
// spelling, the replay wire contract's (the Java server logs the same
// message after its logger's prefix): the shape, the cost in milliseconds
// as a Java double, the range parameters as sent ("-" when absent),
// whether a continuation token was sent, max-keys, and the result's size
// and truncation. An error answer is not logged.
func TestSlowRequestLogLine(t *testing.T) {
	cfg := config(memstore.Keys(memstore.Options{Rollup: true}, "logs/a/1", "logs/b/1", "logs/c", "logs/d/1", "logs/e/1"))
	cfg.SlowRequestLog = 0
	var log strings.Builder
	cfg.Log = &syncWriter{w: &log}
	s := start(t, cfg)
	get(t, s, "/bucket?list-type=2&delimiter=/&prefix=logs/&start-after=logs/a&max-keys=3")
	get(t, s, "/bucket?list-type=2&max-keys=x")
	want := regexp.MustCompile(`^replay slow request shape=STRUCTURE_PROBE ms=[0-9]+\.[0-9]+(E-[0-9]+)? prefix=logs/ delimiter=/ ` +
		`start_after=logs/a continuation=none max_keys=3 entries=3 truncated=true\n$`)
	if got := log.String(); !want.MatchString(got) {
		t.Errorf("slow-request log:\n%q", got)
	}
}
