// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode/utf16"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"

	"github.com/varveio/swath/go/internal/replay/server"
	"github.com/varveio/swath/go/internal/replay/store"
	"github.com/varveio/swath/go/internal/replay/store/memstore"
)

const replayAccount, replayContainer = "replay", "bkt"

// replayKeys are the fixture's names: enough under a/ and data/ for many
// pages, and names whose UTF-16 order differs from their byte order
// (U+E000 and U+FFFD against a supplementary scalar), names Azure sends
// Encoded (U+FFFE, U+FFFF) and names that need escaping in a query or in
// XML.
func replayKeys() []string {
	keys := []string{"sp ace", "pl+us", "pct%41", "amp&<>", "q\"'", "a?b", "a#b",
		"u/\ue000", "u/\ufffd", "u/\U0001F600", "u/\U00010000", "u/\uffff", "u/\ufffe/x", "u/\ue000/y", "u/\U0001F600/z", "u/z"}
	for i := range 23 {
		keys = append(keys, fmt.Sprintf("a/%03d", i), fmt.Sprintf("data/year=%d/part-%05d.parquet", 2020+i%3, i))
	}
	return keys
}

// replayServer serves keys on the replay's Azure route alone (account
// replay, container bkt) and returns the server's base URL.
func replayServer(t *testing.T, keys []string) string {
	t.Helper()
	objs := make([]store.Object, len(keys))
	for i, k := range keys {
		size, lm := int64(10*i+1), time.Date(2026, 1, 2, 3, 4, i, 0, time.UTC)
		objs[i] = store.Object{Key: []byte(k), Size: &size, LastModified: &lm}
	}
	st, err := memstore.New(objs, memstore.Options{Rollup: true})
	if err != nil {
		t.Fatal(err)
	}
	srv, err := server.New(server.Config{Bucket: replayContainer, Host: "127.0.0.1", MetricsPort: -1, MaxConcurrentRequests: 16,
		ResponseBufferBudget: 256 << 20, MaxResponseBytes: 64 << 20, StopTimeout: time.Second, IdleTimeout: time.Minute,
		WriteTimeout: time.Minute, SlowRequestLog: -1, Store: st, ServingMode: "MEMORY", FixtureIdentity: "azblob-client",
		Protocols: []string{"azure"}, AzureAccount: replayAccount})
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	t.Cleanup(func() { srv.Shutdown(time.Second) })
	return fmt.Sprintf("http://127.0.0.1:%d", srv.Port())
}

// sdkTransport pins what the SDK cannot be told: the service version the
// replay answers, and startFrom, which the flat pager of SDK v1.7.0 drops
// (its hierarchy pager sends it).
type sdkTransport struct{ startFrom string }

func (tr sdkTransport) Do(req *http.Request) (*http.Response, error) {
	for k := range req.Header {
		if strings.EqualFold(k, headerVersion) {
			delete(req.Header, k)
		}
	}
	req.Header.Set(headerVersion, Version)
	if q := req.URL.Query(); tr.startFrom != "" && !q.Has("startFrom") {
		q.Set("startFrom", tr.startFrom)
		req.URL.RawQuery = strings.ReplaceAll(q.Encode(), "+", "%20")
	}
	return http.DefaultTransport.RoundTrip(req)
}

// utf16Less is the UTF-16 code-unit order, from the code units themselves
// (an oracle independent of compareNames).
func utf16Less(a, b string) int {
	return slices.Compare(utf16.Encode([]rune(a)), utf16.Encode([]rune(b)))
}

// TestClientPagesMatchSDK walks the replay's Azure route with the client
// and with the official SDK's flat and hierarchy pagers from the same
// inputs, page by page: the same items in the same order (the hierarchy
// pager's blobs and prefixes merged into UTF-16 order first, as the wire
// interleaves them), the same properties and the same NextMarker.
func TestClientPagesMatchSDK(t *testing.T) {
	ep := replayServer(t, replayKeys())
	ctx := context.Background()
	c, err := New(ctx, Config{Endpoint: ep, Account: replayAccount, Anonymous: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, in := range []ListInput{
		{MaxResults: 7},
		{},
		{Prefix: "data/year=2021/", MaxResults: 3},
		{Prefix: "u/", MaxResults: 2},
		{StartFrom: "a/010", MaxResults: 5},
		{StartFrom: "u/\ue000", MaxResults: 2},
		{Delimiter: "/", MaxResults: 3},
		{Prefix: "u/", Delimiter: "/", MaxResults: 2},
		{Prefix: "u/", Delimiter: "/", StartFrom: "u/\U0001F600"},
		{Prefix: "data/", Delimiter: "/"},
		{StartFrom: "pct%41", MaxResults: 1},
	} {
		in.Container = replayContainer
		ours := walkOurs(t, c, in)
		sdk := walkSDK(t, ep, in)
		if len(ours) != len(sdk) {
			t.Fatalf("%+v: %d pages, the SDK %d", in, len(ours), len(sdk))
		}
		total := 0
		for i := range ours {
			a, b := ours[i], sdk[i]
			if a.NextMarker != b.NextMarker || len(a.Items) != len(b.Items) {
				t.Fatalf("%+v page %d: ours %d items next %q, the SDK %d next %q", in, i, len(a.Items), a.NextMarker, len(b.Items), b.NextMarker)
			}
			for j := range a.Items {
				if a.Items[j] != b.Items[j] {
					t.Fatalf("%+v page %d item %d:\n ours %+v\n SDK  %+v", in, i, j, a.Items[j], b.Items[j])
				}
			}
			total += len(a.Items)
		}
		t.Logf("%+v: %d pages, %d items", in, len(ours), total)
		if total == 0 {
			t.Fatalf("%+v: an empty walk proves nothing", in)
		}
	}
}

func walkOurs(t *testing.T, c *Client, in ListInput) []ListOutput {
	t.Helper()
	var pages []ListOutput
	for {
		p, err := c.List(context.Background(), &in)
		if err != nil {
			t.Fatalf("%+v: %v", in, err)
		}
		pages = append(pages, *p)
		if p.NextMarker == "" {
			return pages
		}
		in.Marker = p.NextMarker
	}
}

func walkSDK(t *testing.T, ep string, in ListInput) []ListOutput {
	t.Helper()
	cl, err := azblob.NewClientWithNoCredential(ep+"/"+replayAccount+"/", &azblob.ClientOptions{ClientOptions: azcore.ClientOptions{
		Transport: sdkTransport{startFrom: in.StartFrom}, Retry: policy.RetryOptions{MaxRetries: -1}}})
	if err != nil {
		t.Fatal(err)
	}
	cc := cl.ServiceClient().NewContainerClient(in.Container)
	ptr := func(s string) *string {
		if s == "" {
			return nil
		}
		return &s
	}
	var max *int32
	if in.MaxResults > 0 {
		m := int32(in.MaxResults)
		max = &m
	}
	var pages []ListOutput
	if in.Delimiter == "" {
		pager := cc.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{Prefix: ptr(in.Prefix), MaxResults: max, StartFrom: ptr(in.StartFrom)})
		for pager.More() {
			r, err := pager.NextPage(context.Background())
			if err != nil {
				t.Fatalf("%+v: SDK: %v", in, err)
			}
			p := ListOutput{NextMarker: deref(r.NextMarker)}
			for _, b := range r.Segment.BlobItems {
				p.Items = append(p.Items, sdkItem(b))
			}
			pages = append(pages, p)
		}
		return pages
	}
	pager := cc.NewListBlobsHierarchyPager(in.Delimiter, &container.ListBlobsHierarchyOptions{Prefix: ptr(in.Prefix), MaxResults: max, StartFrom: ptr(in.StartFrom)})
	for pager.More() {
		r, err := pager.NextPage(context.Background())
		if err != nil {
			t.Fatalf("%+v: SDK: %v", in, err)
		}
		p := ListOutput{NextMarker: deref(r.NextMarker)}
		for _, b := range r.Segment.BlobItems {
			p.Items = append(p.Items, sdkItem(b))
		}
		for _, x := range r.Segment.BlobPrefixes {
			p.Items = append(p.Items, Item{Name: deref(x.Name), Prefix: true})
		}
		slices.SortFunc(p.Items, func(a, b Item) int { return utf16Less(a.Name, b.Name) })
		pages = append(pages, p)
	}
	return pages
}

func sdkItem(b *container.BlobItem) Item {
	it := Item{Name: deref(b.Name)}
	p := b.Properties
	if p.ContentLength != nil {
		it.Size = *p.ContentLength
	}
	if p.LastModified != nil {
		it.LastModified = p.LastModified.UnixMicro()
	}
	if p.CreationTime != nil {
		it.CreationTime = p.CreationTime.UnixMicro()
	}
	if p.ETag != nil {
		it.ETag = string(*p.ETag)
	}
	if len(p.ContentMD5) > 0 {
		it.ContentMD5 = base64Std(p.ContentMD5)
	}
	it.ContentType = deref(p.ContentType)
	if p.BlobType != nil {
		it.BlobType = string(*p.BlobType)
	}
	if p.AccessTier != nil {
		it.AccessTier = string(*p.AccessTier)
	}
	return it
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
