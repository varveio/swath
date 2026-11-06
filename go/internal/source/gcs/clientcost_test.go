// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package gcs

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"sort"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"google.golang.org/api/option"
	storage "google.golang.org/api/storage/v1"

	"github.com/varveio/swath/go/internal/order"
	"github.com/varveio/swath/go/internal/source/gcs/client"
	"github.com/varveio/swath/go/internal/source/gcs/gcstest"
)

// sdkAPI is the API over Google's generated JSON client (test only): the
// same objects.list call, its page converted to the client's Page.
type sdkAPI struct{ svc *storage.Service }

func (s sdkAPI) List(ctx context.Context, in *client.ListInput) (*client.Page, error) {
	call := s.svc.Objects.List(in.Bucket).Prefix(in.Prefix).Delimiter(in.Delimiter).StartOffset(in.StartOffset).
		EndOffset(in.EndOffset).PageToken(in.PageToken).Context(ctx)
	if in.MaxResults > 0 {
		call = call.MaxResults(int64(in.MaxResults))
	}
	r, err := call.Do()
	if err != nil {
		return nil, err
	}
	p := &client.Page{Prefixes: r.Prefixes, NextPageToken: r.NextPageToken, Items: make([]client.Object, len(r.Items))}
	for i, o := range r.Items {
		up, err := time.Parse(time.RFC3339Nano, o.Updated)
		if err != nil {
			return nil, err
		}
		p.Items[i] = client.Object{Name: o.Name, Size: int64(o.Size), Updated: up.UnixMicro(), ETag: o.Etag, Generation: o.Generation,
			Metageneration: o.Metageneration, StorageClass: o.StorageClass, MD5: o.Md5Hash, CRC32C: o.Crc32c, ContentType: o.ContentType}
	}
	return p, nil
}

type countRows struct{ n atomic.Int64 }

func (c *countRows) Write(_ context.Context, segs [][]Row) error {
	for _, s := range segs {
		c.n.Add(int64(len(s)))
	}
	return nil
}

func processCPU() time.Duration {
	var ru syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

// TestListClientCost measures the hand-rolled objects.list client against
// Google's generated JSON client on the same in-process fake bucket
// (gcstest, full responses): process CPU per listed object and allocations
// per page, listing every page in sequence ("list loop") and through the
// range engine at one worker ("engine"). A baseline that only reads each
// page's body (same requests) is subtracted, so the fake server's own cost
// and the HTTP exchange do not count as client cost. Opt-in measurement
// (SWATH_CLIENT_COST=N objects, e.g. 1000000), not a gate: it logs the
// medians of three interleaved repetitions.
func TestListClientCost(t *testing.T) {
	n := 0
	if v := os.Getenv("SWATH_CLIENT_COST"); v != "" {
		n = atoiOr(v, 0)
	}
	if n == 0 {
		t.Skip("measurement; set SWATH_CLIENT_COST=1000000")
	}
	srv := httptest.NewServer(gcstest.Handler(gcstest.Keys(n), 0))
	t.Cleanup(srv.Close)
	ep := srv.URL + gcstest.Path
	ctx := context.Background()
	ours, err := client.New(ctx, client.Config{Endpoint: ep, Anonymous: true})
	if err != nil {
		t.Fatal(err)
	}
	svc, err := storage.NewService(ctx, option.WithEndpoint(ep), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	apis := map[string]API{"ours": ours, "sdk": sdkAPI{svc}}
	loop := func(api API) (objects, pages int) {
		in := client.ListInput{Bucket: "bkt"}
		for {
			p, err := api.List(ctx, &in)
			if err != nil {
				t.Fatal(err)
			}
			objects, pages = objects+len(p.Items), pages+1
			if p.NextPageToken == "" {
				return
			}
			in.PageToken = p.NextPageToken
		}
	}
	// The baseline reads the pages the hand-rolled client was given, so
	// the server does the same work.
	var tokens []string
	{
		in := client.ListInput{Bucket: "bkt"}
		for {
			p, err := ours.List(ctx, &in)
			if err != nil {
				t.Fatal(err)
			}
			if p.NextPageToken == "" {
				break
			}
			tokens = append(tokens, p.NextPageToken)
			in.PageToken = p.NextPageToken
		}
	}
	baseline := func() (int, int) {
		for _, tok := range append([]string{""}, tokens...) {
			u := ep + "b/bkt/o?projection=noAcl"
			if tok != "" {
				u += "&pageToken=" + tok
			}
			resp, err := http.Get(u)
			if err != nil {
				t.Fatal(err)
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		return n, len(tokens) + 1
	}
	engine := func(api API) (int, int) {
		p, err := New(ctx, Options{Bucket: "bkt", Endpoint: ep, Anonymous: true, Workers: 1, Client: api})
		if err != nil {
			t.Fatal(err)
		}
		c := &countRows{}
		if _, err := order.Run[Range, Object](ctx, p, c, &Schema, order.Options{Workers: 1, Budget: 64 << 20}); err != nil {
			t.Fatal(err)
		}
		return int(c.n.Load()), len(tokens) + 1
	}
	type sample struct{ cpuPerObj, allocsPerPage, bytesPerPage float64 }
	measure := func(f func() (int, int)) sample {
		runtime.GC()
		var m0, m1 runtime.MemStats
		runtime.ReadMemStats(&m0)
		c0 := processCPU()
		objs, pages := f()
		c1 := processCPU()
		runtime.ReadMemStats(&m1)
		if objs != n {
			t.Fatalf("%d objects, want %d", objs, n)
		}
		return sample{float64((c1 - c0).Nanoseconds()) / 1e3 / float64(objs), float64(m1.Mallocs-m0.Mallocs) / float64(pages),
			float64(m1.TotalAlloc-m0.TotalAlloc) / float64(pages)}
	}
	arms := []struct {
		name string
		f    func() (int, int)
	}{
		{"baseline (read bodies)", baseline},
		{"ours, list loop", func() (int, int) { return loop(apis["ours"]) }},
		{"sdk, list loop", func() (int, int) { return loop(apis["sdk"]) }},
		{"ours, engine", func() (int, int) { return engine(apis["ours"]) }},
		{"sdk, engine", func() (int, int) { return engine(apis["sdk"]) }},
	}
	got := map[string][]sample{}
	for range 3 {
		for _, a := range arms {
			got[a.name] = append(got[a.name], measure(a.f))
		}
	}
	median := func(s []sample, f func(sample) float64) float64 {
		v := make([]float64, len(s))
		for i := range s {
			v[i] = f(s[i])
		}
		sort.Float64s(v)
		return v[len(v)/2]
	}
	base := got[arms[0].name]
	bc, ba, bb := median(base, func(s sample) float64 { return s.cpuPerObj }), median(base, func(s sample) float64 { return s.allocsPerPage }),
		median(base, func(s sample) float64 { return s.bytesPerPage })
	t.Logf("%d objects, %d pages, GOMAXPROCS %d", n, len(tokens)+1, runtime.GOMAXPROCS(0))
	for _, a := range arms {
		c, al, by := median(got[a.name], func(s sample) float64 { return s.cpuPerObj }), median(got[a.name], func(s sample) float64 { return s.allocsPerPage }),
			median(got[a.name], func(s sample) float64 { return s.bytesPerPage })
		t.Logf("%-24s process CPU %.2f µs/object (over baseline %.2f), allocs/page %.0f (over baseline %.0f), bytes/page %.0f KiB (over baseline %.0f KiB)",
			a.name, c, c-bc, al, al-ba, by/1024, (by-bb)/1024)
	}
}

func atoiOr(s string, d int) int {
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return d
		}
		n = n*10 + int(c-'0')
	}
	return n
}
