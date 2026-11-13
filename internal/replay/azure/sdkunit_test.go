// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azure_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"

	"github.com/varveio/swath/internal/replay/server"
	"github.com/varveio/swath/internal/replay/store"
	"github.com/varveio/swath/internal/replay/store/memstore"
)

// sdkServer serves names on the Azure route alone, container
// replay-bucket in account replay, and returns its ServiceEndpoint.
func sdkServer(t *testing.T, rollup bool, objs []store.Object) string {
	t.Helper()
	slices.SortFunc(objs, func(a, b store.Object) int { return compareBytes(a.Key, b.Key) })
	st, err := memstore.New(objs, memstore.Options{Rollup: rollup})
	if err != nil {
		t.Fatal(err)
	}
	srv, err := server.New(server.Config{Bucket: replayBucket, Host: "127.0.0.1", MetricsPort: -1, MaxConcurrentRequests: 16,
		ResponseBufferBudget: 256 << 20, MaxResponseBytes: 64 << 20, StopTimeout: time.Second, IdleTimeout: time.Minute,
		WriteTimeout: time.Minute, SlowRequestLog: -1, Store: st, ServingMode: "MEMORY", FixtureIdentity: "sdk",
		Protocols: []string{"azure"}, AzureAccount: replayAccount})
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	t.Cleanup(func() { srv.Shutdown(time.Second) })
	return fmt.Sprintf("http://127.0.0.1:%d/%s/", srv.Port(), replayAccount)
}

func compareBytes(a, b []byte) int { return slices.Compare(a, b) }

func object(name string, size int64, sec int64) store.Object {
	tm := time.Unix(sec, 0).UTC()
	return store.Object{Key: []byte(name), Size: &size, LastModified: &tm}
}

// TestSDKDecodesEncodedNamesAndProperties lists names XML cannot carry
// (U+FFFE, U+FFFF, which the route sends Encoded) and ordinary names with
// the official SDK's flat and hierarchy paginators on the local account
// path, two results per page: every name, prefix, size and time decodes
// back, in UTF-16 order, and every call went to /replay/replay-bucket.
func TestSDKDecodesEncodedNamesAndProperties(t *testing.T) {
	names := []string{"a/\ufffe", "a/\uffff/x", "a/b c&<>", "a/\ue000", "a/\U00010000", "a/z/1", "a/z/2", "b"}
	var objs []store.Object
	for i, n := range names {
		objs = append(objs, object(n, int64(10+i), 1_700_000_000+int64(i)))
	}
	for _, rollup := range []bool{false, true} {
		endpoint := sdkServer(t, rollup, objs)
		tr := &sdkTransport{version: "2026-10-06"}
		cc := sdkClient(endpoint, tr, -1)
		flat, err := sdkWalk(cc, sdkOptions{Walk: "flat", Prefix: "a/", PageSize: 2, MaxPages: 64})
		if err != nil {
			t.Fatal(err)
		}
		want := []string{"a/b c&<>", "a/z/1", "a/z/2", "a/\U00010000", "a/\ue000", "a/\ufffe", "a/\uffff/x"}
		var got []string
		for i, it := range flat {
			got = append(got, it.name)
			idx := slices.Index(names, it.name)
			if it.prefix || it.size != int64(10+idx) || !it.lastModified.Equal(time.Unix(1_700_000_000+int64(idx), 0)) {
				t.Errorf("flat item %d: %+v", i, it)
			}
		}
		if !slices.Equal(got, want) {
			t.Errorf("flat %q, want %q", got, want)
		}
		hier, err := sdkWalk(cc, sdkOptions{Walk: "hierarchy", Prefix: "a/", Delimiter: "/", PageSize: 2, MaxPages: 64})
		if err != nil {
			t.Fatal(err)
		}
		var prefixes, blobs []string
		for _, it := range hier {
			if it.prefix {
				prefixes = append(prefixes, it.name)
			} else {
				blobs = append(blobs, it.name)
			}
		}
		if !slices.Equal(prefixes, []string{"a/z/", "a/\uffff/"}) || !slices.Equal(blobs, []string{"a/b c&<>", "a/\U00010000", "a/\ue000", "a/\ufffe"}) {
			t.Errorf("hierarchy prefixes %q blobs %q", prefixes, blobs)
		}
		for _, s := range tr.steps {
			if s.request.path != "/"+replayAccount+"/"+replayBucket || s.exchange.status != 200 {
				t.Errorf("call %s %d", s.request.path, s.exchange.status)
			}
		}
		if len(tr.steps) < 6 {
			t.Errorf("%d calls: the paginators did not page", len(tr.steps))
		}
	}
}

// TestSDKConsumesFiveThousandPages pages 5,001 names with the SDK's
// default page size: two calls, 5,000 then 1.
func TestSDKConsumesFiveThousandPages(t *testing.T) {
	var objs []store.Object
	for i := range 5001 {
		objs = append(objs, object("k/"+strconv.Itoa(100000+i), 1, 1_700_000_000))
	}
	tr := &sdkTransport{version: "2026-06-06"}
	cc := sdkClient(sdkServer(t, true, objs), tr, -1)
	pager := cc.NewListBlobsFlatPager(nil)
	var sizes []int
	for pager.More() {
		resp, err := pager.NextPage(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		sizes = append(sizes, len(resp.Segment.BlobItems))
	}
	if !slices.Equal(sizes, []int{5000, 1}) || len(tr.steps) != 2 {
		t.Fatalf("pages %v, %d calls", sizes, len(tr.steps))
	}
}

// TestSDKFixtureFaultAndRetries serves a name the profile cannot render
// (a control character: 500 InternalError): with retries off the SDK
// makes one call and reports the error code; with two retries, three.
func TestSDKFixtureFaultAndRetries(t *testing.T) {
	endpoint := sdkServer(t, false, []store.Object{object("bad\x01name", 1, 1_700_000_000)})
	for retries, calls := range map[int32]int{-1: 1, 2: 3} {
		tr := &sdkTransport{version: "2026-06-06"}
		_, err := sdkClient(endpoint, tr, retries).NewListBlobsFlatPager(nil).NextPage(context.Background())
		var re *azcore.ResponseError
		if !errors.As(err, &re) || re.StatusCode != 500 || re.ErrorCode != "InternalError" || len(tr.steps) != calls {
			t.Errorf("retries %d: %v, %d calls", retries, err, len(tr.steps))
		}
	}
}
