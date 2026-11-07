// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/replay/server"
	"github.com/varveio/swath/go/internal/replay/store"
	"github.com/varveio/swath/go/sortedparquet/sortedparquettest"
)

// TestTestWriterPartAgainstJava serves a Java-era fixture written by
// sortedparquettest.Write (two stamped parts, several row groups of one
// and of many pages) from the Java server and the Go route and requires
// byte-identical S3 answers to delimiter and plain listings. The Java
// server's rollup refuses a row group whose key column index does not
// declare ASCENDING (fixture_disordered), which the test writer once left
// to parquet-go's default. It needs the Java launcher:
// SWATH_REPLAY_JAVA=PATH (with JAVA_HOME set as the launcher needs it);
// without it the test skips.
func TestTestWriterPartAgainstJava(t *testing.T) {
	java := os.Getenv("SWATH_REPLAY_JAVA")
	if java == "" {
		t.Skip("SWATH_REPLAY_JAVA is not set: the Java launcher is not available")
	}
	dir := t.TempDir()
	fixture := filepath.Join(dir, "fixture")
	if err := os.MkdirAll(fixture, 0o755); err != nil {
		t.Fatal(err)
	}
	var keys []string
	for d := range 6 {
		for i := range 70 {
			keys = append(keys, fmt.Sprintf("d%d/s%d/obj%03d", d, i%4, i))
		}
	}
	keys = append(keys, "top-a", "top-b")
	slices.Sort(keys) // byte order, the order both servers keep
	rows := make([]sortedparquettest.Row, len(keys))
	for i, k := range keys {
		rows[i] = sortedparquettest.Object(k, i)
	}
	half := len(rows) / 2
	parts := []sortedparquettest.Part{
		{Rows: rows[:half], GroupRows: 60, PageBytes: 512, Stamps: sortedparquettest.Stamps(1, false)},
		{Rows: rows[half:], GroupRows: 1000, Stamps: sortedparquettest.Stamps(2, true)}, // one group, one page
	}
	for i, p := range parts {
		if err := sortedparquettest.Write(filepath.Join(fixture, fmt.Sprintf("part-%05d.parquet", i+1)), p); err != nil {
			t.Fatal(err)
		}
	}
	jp := startJava(t, java, serveArgs(fixture, "bkt", "--parquet-connections 2 --protocols s3"), filepath.Join(dir, "java.log"))
	st, _, err := store.Open(context.Background(), fixture, store.Options{Readers: 2})
	if err != nil {
		t.Fatal(err)
	}
	srv, err := server.New(server.Config{Bucket: "bkt", Host: "127.0.0.1", MetricsPort: -1, MaxConcurrentRequests: 16,
		ResponseBufferBudget: server.DefaultResponseBufferBudget, MaxResponseBytes: server.DefaultMaxResponseBytes, StopTimeout: time.Second,
		IdleTimeout: time.Minute, WriteTimeout: time.Minute, SlowRequestLog: -1, Store: st, ServingMode: "SORTED",
		FixtureIdentity: "test-writer", Protocols: []string{"s3"}})
	if err == nil {
		err = srv.Listen()
	}
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	defer srv.Shutdown(time.Second)
	client := &http.Client{}
	ref := &target{name: "java", base: jp.endpoint, client: client}
	got := &target{name: "go", base: fmt.Sprintf("http://127.0.0.1:%d", srv.Port()), client: client}
	compared := 0
	for _, prefix := range []string{"", "d0/", "d3/", "d3/s2/", "d5/s1/", "top"} {
		for _, maxKeys := range []string{"1", "3", "1000"} {
			for _, delimiter := range []string{"/", ""} {
				r := request{Name: "test writer", Method: "GET", Query: q("list-type", "2", "prefix", prefix, "delimiter", delimiter, "max-keys", maxKeys)}
				a, err := ref.do("bkt", r)
				if err != nil {
					t.Fatal(err)
				}
				b, err := got.do("bkt", r)
				if err != nil {
					t.Fatal(err)
				}
				compared++
				if a.Status != http.StatusOK {
					t.Fatalf("java %s: %d %s %s", r.Query, a.Status, values(a.Header, "x-swath-replay-error"), a.Body)
				}
				if d := (bytesComparator{}).compare(a, b); !d.OK {
					t.Errorf("%s: %+v", r.Query, d)
				}
			}
		}
	}
	t.Logf("%d requests compared", compared)
}
