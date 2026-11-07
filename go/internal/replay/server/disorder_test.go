// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/replay/metrics"
	"github.com/varveio/swath/go/internal/replay/store"
	"github.com/varveio/swath/go/internal/replay/store/memstore"
	"github.com/varveio/swath/go/sortedparquet"
	"github.com/varveio/swath/go/sortedparquet/sortedparquettest"
)

// TestDisorderedFixtureNamesOnlyThePart: a fixture disordered inside a row
// group fails the request with the replay wire contract's envelope (500
// InternalError, x-swath-replay-error: fixture_disordered, the reason, the
// row group and the part's file name in the message), and the server's
// directory layout reaches the log only.
func TestDisorderedFixtureNamesOnlyThePart(t *testing.T) {
	disorder := &sortedparquet.DisorderError{Part: "/srv/captures/some-bucket/part-00001.parquet", RowGroup: 7, Row: 42,
		Detail: "its keys must be in strictly ascending unsigned order"}
	st := &hookStore{Store: memstore.Keys(memstore.Options{}, "a"), hook: func(context.Context) error { return disorder }}
	cfg := config(st)
	var log strings.Builder
	cfg.Log = &syncWriter{w: &log}
	s := start(t, cfg)
	r := get(t, s, "/bucket?list-type=2&delimiter=%2F")
	const want = `<?xml version="1.0" encoding="UTF-8"?><Error><Code>InternalError</Code>` +
		`<Message>row group 7 of part-00001.parquet is not sorted (reason=row_group_disorder): its keys must be in strictly ascending unsigned order</Message>` +
		`<Resource>/bucket</Resource><RequestId>S3LISTINGREPLAY</RequestId></Error>`
	if r.status != 500 || r.body != want || r.header.Get("x-swath-replay-error") != "fixture_disordered" {
		t.Errorf("%d %v\n%s", r.status, r.header, r.body)
	}
	if !strings.Contains(log.String(), "/srv/captures/some-bucket/part-00001.parquet") {
		t.Errorf("log lacks the path: %s", log.String())
	}
	if count(s, metrics.HTTPErrors) != 1 {
		t.Errorf("meters: %s", s.reg.AppendMeters(nil))
	}
}

// TestDisorderedPartOverHTTP serves a stamped part whose one row group
// holds a, c, b, d through the real store: the read that meets the
// disorder answers fixture_disordered with the complete Java message.
func TestDisorderedPartOverHTTP(t *testing.T) {
	dir := t.TempDir()
	var rows []sortedparquettest.Row
	for i, k := range []string{"a", "c", "b", "d"} {
		rows = append(rows, sortedparquettest.Object(k, i))
	}
	if err := sortedparquettest.Write(filepath.Join(dir, "part-00001.parquet"),
		sortedparquettest.Part{Rows: rows, Stamps: sortedparquettest.Stamps(1, true)}); err != nil {
		t.Fatal(err)
	}
	st, _, err := store.Open(context.Background(), dir, store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	s := start(t, config(st))
	r := get(t, s, "/bucket?list-type=2&max-keys=1000")
	// The whole answer, the store's detail included, is the Java server's
	// for the same part: the row in its group and the key in hex.
	const want = `<?xml version="1.0" encoding="UTF-8"?><Error><Code>InternalError</Code>` +
		`<Message>row group 0 of part-00001.parquet is not sorted (reason=row_group_disorder): ` +
		`its keys must be in strictly ascending unsigned order, but row 2 (62) is at or below its predecessor</Message>` +
		`<Resource>/bucket</Resource><RequestId>S3LISTINGREPLAY</RequestId></Error>`
	if r.status != 500 || r.header.Get("x-swath-replay-error") != "fixture_disordered" || r.body != want {
		t.Errorf("%d %v\n%s", r.status, r.header, r.body)
	}
}
