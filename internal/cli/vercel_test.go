// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/spf13/pflag"
	seekparquet "github.com/varveio/seekparquet/go"
	"github.com/varveio/swath/internal/filter"
	"github.com/varveio/swath/internal/inventory"
	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/registry"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/vercel"
	"github.com/varveio/swath/internal/totals"
	"github.com/varveio/swath/reader"
)

func TestVercelFlags(t *testing.T) {
	t.Setenv(vercel.TokenEnv, "vercel_blob_rw_test_secret")
	for _, args := range [][]string{
		{"--anonymous"}, {"--endpoint-url=http://example.com"}, {"--all-versions"},
		{"--sequential"}, {"--no-seed"}, {"--structure=off"}, {"--slow-split"}, {"--max-keys=1"},
		{"--seed-from-tile-pages=1"}, {"--seed-from-window-rows=1"}, {"--trace=/tmp/unused"},
	} {
		src, err := openArgs(t, "vercel://store_test", true, nil, args...)
		if src != nil || !errors.Is(err, source.ErrUnsupported) {
			t.Errorf("%v: source %T error %v", args, src, err)
		}
	}
	root := "vercel://store_test"
	f := factory(t, root)
	fs := pflag.NewFlagSet("vercel", pflag.ContinueOnError)
	options := f.RegisterFlags(fs, true)
	if fs.Lookup("s3-listing") != nil || fs.Lookup("listing") != nil {
		t.Fatal("Vercel registers a listing mode flag")
	}
	if err := fs.Parse([]string{"--page-size=2", "--request-rate=5"}); err != nil {
		t.Fatal(err)
	}
	if f.Workers == nil || f.Workers() != 4 {
		t.Fatal("Vercel factory should default to four workers")
	}
	for _, tc := range []struct{ workers, limit int }{{0, 4}, {f.Workers(), 4}, {8, 8}} {
		src, err := f.Open(t.Context(), registry.Request{Root: root, Options: options, Workers: tc.workers})
		if err != nil {
			t.Fatal(err)
		}
		if got := src.(source.Requested).Requests().Limit; got != int64(tc.limit) {
			t.Fatalf("requested %d workers, limiter = %d, want %d", tc.workers, got, tc.limit)
		}
		if cp := src.(source.Capable).Capabilities(); cp.Resume || cp.Seek != source.SeekTokenOnly {
			t.Fatalf("capabilities %+v", cp)
		}
		if loc := src.Locator(); loc.Kind != "vercel" || loc.Container != "store_test" {
			t.Fatalf("locator %+v", loc)
		}
		if _, ok := src.(source.Resumable); ok {
			t.Fatal("token-only provider advertised restart")
		}
	}
}

type vercelTransport func(*http.Request) (*http.Response, error)

func (f vercelTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// TestVercelDatasetTotals exercises the registered source, publication,
// typed reader and live/saved totals against independently specified rows.
func TestVercelDatasetTotals(t *testing.T) {
	ctx := context.Background()
	opt := &vercel.Options{Token: "vercel_blob_rw_test_secret", HTTPClient: &http.Client{Transport: vercelTransport(func(r *http.Request) (*http.Response, error) {
		body := `{"blobs":[{"pathname":"b","size":7,"uploadedAt":"2026-01-01T00:00:00Z","etag":"e2","url":"https://test.private.blob.vercel-storage.com/b"},{"pathname":"a","size":3,"uploadedAt":"2026-01-01T00:00:01Z","etag":"e1","url":"https://test.private.blob.vercel-storage.com/a"}],"folders":[],"hasMore":false}`
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
	})}}
	dir := filepath.Join(t.TempDir(), "dataset")
	if _, err := inventory.Run(ctx, inventory.Config{Source: inventory.SourceConfig{URI: "vercel://store_test", Options: opt}, Output: inventory.OutputConfig{Dir: dir}}); err != nil {
		t.Fatal(err)
	}
	d, err := reader.OpenDir(ctx, dir, seekparquet.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err := d.CheckSchema(ctx); err != nil {
		t.Fatal(err)
	}
	spec, _ := reader.Schema(reader.VercelObjectsV1)
	plan, err := d.SeekParquet().Plan(ctx, []seekparquet.Selection{{}}, spec.ColumnNames(), nil)
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	var sizes, modified []int64
	var etags []string
	if err := plan.Scan(ctx, nil, func(batch *seekparquet.Batch) error {
		for i := range batch.Len() {
			keys = append(keys, string(batch.Key(i)))
			sizes = append(sizes, batch.Column(0).Value(i).Int64())
			modified = append(modified, batch.Column(1).Value(i).Int64())
			etags = append(etags, string(batch.Column(2).Value(i).Bytes()))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(keys, []string{"a", "b"}) || !reflect.DeepEqual(sizes, []int64{3, 7}) ||
		!reflect.DeepEqual(modified, []int64{1767225601000000, 1767225600000000}) || !reflect.DeepEqual(etags, []string{"e1", "e2"}) {
		t.Fatalf("rows keys=%q sizes=%v modified=%v etags=%q", keys, sizes, modified, etags)
	}
	for _, tc := range []struct {
		name                  string
		filter                filter.Spec
		objects, size, newest int64
	}{
		{name: "all", objects: 2, size: 10, newest: 1767225601000000},
		{name: "since", filter: filter.Spec{Since: "2026-01-01T00:00:01Z"}, objects: 1, size: 3, newest: 1767225601000000},
		{name: "until", filter: filter.Spec{Until: "2026-01-01T00:00:00Z"}, objects: 1, size: 7, newest: 1767225600000000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			live, saved := totals.New(totals.Options{}), totals.New(totals.Options{})
			src, err := openVercel(ctx, registry.Request{Root: "vercel://store_test", Options: opt, Workers: 2})
			if err != nil {
				t.Fatal(err)
			}
			var filtered atomic.Int64
			if _, _, err := src.Run(ctx, registry.Env{Count: true, Totals: live, Filter: &tc.filter, Filtered: &filtered, Order: order.Options{Workers: 2}}); err != nil {
				t.Fatal(err)
			}
			read, err := registry.DatasetTotals(ctx, d, "", false, &tc.filter, saved, 2)
			if err != nil {
				t.Fatal(err)
			}
			if want := 2 - tc.objects; filtered.Load() != want || read.Filtered != want {
				t.Fatalf("live filtered %d saved filtered %d, want %d", filtered.Load(), read.Filtered, want)
			}
			got, want := saved.Result(), live.Result()
			if !reflect.DeepEqual(got, want) || len(got) != 1 || got[0].Objects != tc.objects ||
				got[0].Size != tc.size || !got[0].HasNewest || got[0].Newest != tc.newest {
				t.Fatalf("saved %+v live %+v, want %d objects, %d bytes, newest %d", got, want, tc.objects, tc.size, tc.newest)
			}
		})
	}
}

func TestVercelFilterAndTLS(t *testing.T) {
	f := factory(t, "vercel://store_test")
	if err := f.CheckFilter(nil, &filter.Spec{StorageClasses: []string{"STANDARD"}}); err == nil {
		t.Fatal("accepted unavailable storage class")
	}
	for _, spec := range []*filter.Spec{
		{Since: "2026-01-01"}, {Until: "2026-01-01"}, {MinSize: "1", Include: []string{"a*"}},
	} {
		if err := f.CheckFilter(nil, spec); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv(vercel.TokenEnv, "vercel_blob_rw_test_secret")
	var codes []string
	src, err := openArgs(t, "vercel://store_test", false, func(code, msg string) { codes = append(codes, code) }, "--no-verify-ssl")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(codes, []string{source.WarnTLSUnverified}) {
		t.Fatalf("warnings %q", codes)
	}
	if !src.(source.Reporter).Report().Stats.(*vercel.Stats).NoVerifySSL {
		t.Fatal("TLS override absent from report")
	}
}
