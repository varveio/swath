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
	"strings"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/replay/legacy/legacytest"
	"github.com/varveio/swath/go/internal/replay/server"
	"github.com/varveio/swath/go/internal/replay/store"
)

// TestTestWriterPartAgainstJava serves a Java-era fixture written by
// legacytest.Write (two stamped parts, several row groups of one
// and of many pages) from the Java server and the Go route and requires
// the same S3 answers to delimiter and plain listings, judged as the
// driver judges every S3 request against the Java server (compareS3):
// the Java answer with the newline live S3 writes after the XML
// declaration, token values masked, and no other difference, documented
// or not. The fixture's rows carry owners, so the same listings with
// fetch-owner=true are compared as well: each must pass only as the
// documented divergence of Owner's place (written before StorageClass, as
// live S3 does); with fetch-owner=TRUE, only as the divergence of the
// value (the Java server renders owners for it, live S3 does not). The
// Java server's rollup refuses a row group whose key column index does not
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
	rows := make([]legacytest.Row, len(keys))
	for i, k := range keys {
		rows[i] = legacytest.Object(k, i)
	}
	half := len(rows) / 2
	parts := []legacytest.Part{
		{Rows: rows[:half], GroupRows: 60, PageBytes: 512, Stamps: legacytest.Stamps(1, false)},
		{Rows: rows[half:], GroupRows: 1000, Stamps: legacytest.Stamps(2, true)}, // one group, one page
	}
	for i, p := range parts {
		if err := legacytest.Write(filepath.Join(fixture, fmt.Sprintf("part-%05d.parquet", i+1)), p); err != nil {
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
	client := &http.Client{Timeout: javaRequests}
	ref := &target{name: "java", base: jp.endpoint, client: client}
	got := &target{name: "go", base: fmt.Sprintf("http://127.0.0.1:%d", srv.Port()), client: client}
	rep := &report{}
	d := &driver{o: options{bucket: "bkt"}, ref: ref, got: got, rep: rep}
	compared, owned := 0, map[string]string{}
	for _, prefix := range []string{"", "d0/", "d3/", "d3/s2/", "d5/s1/", "top"} {
		for _, maxKeys := range []string{"1", "3", "1000"} {
			for _, params := range [][]string{{"delimiter", "/"}, {"delimiter", ""}, {"delimiter", "/", "fetch-owner", "true"}, {"fetch-owner", "true"}, {"fetch-owner", "TRUE"}} {
				query := q(append([]string{"list-type", "2", "prefix", prefix, "max-keys", maxKeys}, params...)...)
				r := request{Name: "test writer " + query, Method: "GET", Query: query}
				a, df, err := d.compareOne(r)
				if err != nil {
					t.Fatal(err)
				}
				compared++
				if a.Status != http.StatusOK {
					t.Fatalf("java %s: %d %s %s", r.Query, a.Status, values(a.Header, "x-swath-replay-error"), a.Body)
				}
				if !df.OK {
					t.Errorf("%s: %+v", r.Query, df)
				}
				if strings.Contains(string(a.Body), "<Owner>") {
					owned[r.Name] = "(as live S3: Owner before StorageClass)"
					if strings.Contains(query, "fetch-owner=TRUE") {
						owned[r.Name] = "(as live S3: Owner only for fetch-owner=true exactly)"
					}
				}
			}
		}
	}
	for _, l := range rep.Divergences {
		name, _, _ := strings.Cut(l, ": java ")
		if reason := owned[name]; reason == "" || !strings.HasSuffix(l, reason) {
			t.Errorf("not the owner divergence alone: %s", l)
		}
		delete(owned, name)
	}
	for name := range owned {
		t.Errorf("%s: an owner listing not judged as the documented divergence", name)
	}
	t.Logf("%d requests compared (%d with token bytes differing)", compared, len(rep.TokenBytes))
}
