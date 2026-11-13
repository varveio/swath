// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/varveio/swath/internal/replay/legacy/legacytest"
	"github.com/varveio/swath/internal/replay/server"
	"github.com/varveio/swath/internal/replay/store"
)

// TestAzureSmallCapsAgainstJava is a differential test of the order
// between response caps and fixture refusals: the Java server and the Go
// route serve one stamped sorted part holding rows the profile cannot
// render (a valid 1,024-byte name with a year-10000 time, a negative size,
// a control character, a time before year 1) beside renderable ones, under
// per-response caps of 256 bytes, 4 KiB and the default, and every answer
// must agree (status, reason, error code, body). It needs the Java
// launcher: SWATH_REPLAY_JAVA=PATH (with JAVA_HOME set as the launcher
// needs it); without it the test skips.
func TestAzureSmallCapsAgainstJava(t *testing.T) {
	java := os.Getenv("SWATH_REPLAY_JAVA")
	if java == "" {
		t.Skip("SWATH_REPLAY_JAVA is not set: the Java launcher is not available")
	}
	dir := t.TempDir()
	// The rows go into an unsorted capture part, and the Java tool sorts
	// them into the fixture both servers serve, so the fixture's page
	// index is the one the Java server's rollup expects.
	capture, fixture := filepath.Join(dir, "capture"), filepath.Join(dir, "fixture")
	if err := os.MkdirAll(capture, 0o755); err != nil {
		t.Fatal(err)
	}
	row := func(key string, size, micros int64) legacytest.Row {
		r := legacytest.Object(key, 1)
		r.Size, r.LastModified = &size, &micros
		return r
	}
	const ok = 1_700_000_000_000_000
	rows := []legacytest.Row{
		row(strings.Repeat("q", 1024), 1, 253402300800000000),
		row("neg", -1, ok),
		row("c\x01", 1, ok),
		row("y0", 1, -62135596801000000),
		row("ok/1", 7, ok),
		row("ok/2", 8, ok),
		row("z"+strings.Repeat("Z", 300), 9, ok),
	}
	if err := legacytest.Write(filepath.Join(capture, "part-00000.parquet"), legacytest.Part{Rows: rows}); err != nil {
		t.Fatal(err)
	}
	sortCtx, cancelSort := context.WithTimeout(context.Background(), javaReady)
	defer cancelSort()
	if out, err := runBounded(sortCtx, java, "sort-fixture", "--capture", capture, "--output", fixture); err != nil {
		t.Fatalf("java sort-fixture: %v\n%s", err, out)
	}
	client := &http.Client{Timeout: javaRequests}
	for _, maxBytes := range []int{256, 4096, server.DefaultMaxResponseBytes} {
		t.Run(fmt.Sprint(maxBytes), func(t *testing.T) {
			testSmallCap(t, java, dir, fixture, client, maxBytes)
		})
	}
}

// testSmallCap compares the two servers under one cap. Both servers are
// stopped in cleanups, on every path out of the subtest.
func testSmallCap(t *testing.T, java, dir, fixture string, client *http.Client, maxBytes int) {
	args := fmt.Sprintf("--parquet-connections 2 --protocols azure --azure-account replay --max-response-bytes %d", maxBytes)
	jp := startJava(t, java, serveArgs(fixture, "bkt", args), filepath.Join(dir, fmt.Sprintf("java-%d.log", maxBytes)))
	st, _, err := store.Open(context.Background(), fixture, store.Options{Readers: 2})
	if err != nil {
		t.Fatal(err)
	}
	srv, err := server.New(server.Config{Bucket: "bkt", Host: "127.0.0.1", MetricsPort: -1, MaxConcurrentRequests: 16,
		ResponseBufferBudget: server.DefaultResponseBufferBudget, MaxResponseBytes: maxBytes, StopTimeout: time.Second,
		IdleTimeout: time.Minute, WriteTimeout: time.Minute, SlowRequestLog: -1, Store: st, ServingMode: "SORTED",
		FixtureIdentity: "caps", Protocols: []string{"azure"}, AzureAccount: "replay"})
	if err != nil {
		st.Close()
		t.Fatal(err)
	}
	if err := srv.Listen(); err != nil {
		st.Close()
		t.Fatal(err)
	}
	go srv.Serve()
	t.Cleanup(func() { srv.Shutdown(time.Second) })
	ref := &target{name: "java", base: jp.endpoint, client: client}
	got := &target{name: "go", base: fmt.Sprintf("http://127.0.0.1:%d", srv.Port()), client: client}
	compared := 0
	for _, prefix := range []string{"", "q", "neg", "c", "y", "ok/", "z"} {
		for _, size := range []string{"1", "2", "5000"} {
			for _, delimiter := range []string{"", "/"} {
				r := request{Method: "GET", Path: azurePath, Query: aq("prefix", prefix, "maxresults", size, "delimiter", delimiter),
					Header: versionHeader(azureVersions[0])}
				a, err := ref.do("bkt", r)
				if err != nil {
					t.Fatal(err)
				}
				b, err := got.do("bkt", r)
				if err != nil {
					t.Fatal(err)
				}
				compared++
				if d := (azureComparator{}).compare(a, b); !d.OK {
					t.Errorf("cap %d %s: %d %s | %d %s: %+v", maxBytes, r.Query, a.Status, values(a.Header, "x-swath-replay-error"),
						b.Status, values(b.Header, "x-swath-replay-error"), d)
				}
			}
		}
	}
	t.Logf("cap %d: %d compared", maxBytes, compared)
}
