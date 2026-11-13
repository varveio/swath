// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblob

import (
	"context"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/varveio/swath/go/internal/source/keyrange"
)

// TestLiveAnonymous lists a public container (AZBLOB_LIVE_ROOT, e.g.
// az://azureopendatastorage/holidaydatacontainer; skipped without it, never
// run in CI) anonymously with 64 workers and the seed, and compares its
// keys and sizes with a flat walk by the official SDK (test only). Azure
// answers an anonymous Get Account Information with 401, so the listing is
// admitted through the Data Lake host's Filesystem Get Properties.
func TestLiveAnonymous(t *testing.T) {
	root := os.Getenv("AZBLOB_LIVE_ROOT")
	if root == "" {
		t.Skip("AZBLOB_LIVE_ROOT not set")
	}
	account, container, prefix, _, err := ParseURL(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	p, err := New(ctx, Options{Account: account, Container: container, Anonymous: true, Workers: 1})
	if err != nil {
		t.Fatalf("admission: %v", err)
	}
	if n := p.Stats().Namespace; n != "filesystem properties" {
		t.Fatalf("admitted by %q, want the filesystem properties", n)
	}
	t0 := time.Now() // the listing, its own admission included
	rows, st := list(t, Options{Account: account, Container: container, Prefix: prefix, Policy: keyrange.Policy{Seed: true, Structure: keyrange.StructureAuto}}, 64)
	took := time.Since(t0)

	cl, err := azblob.NewClientWithNoCredential("https://"+account+".blob.core.windows.net/", nil)
	if err != nil {
		t.Fatal(err)
	}
	type kv struct {
		key  string
		size int64
	}
	var want []kv
	pager := cl.ServiceClient().NewContainerClient(container).NewListBlobsFlatPager(nil)
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, b := range page.Segment.BlobItems {
			if prefix == "" || len(*b.Name) >= len(prefix) && (*b.Name)[:len(prefix)] == prefix {
				want = append(want, kv{*b.Name, *b.Properties.ContentLength})
			}
		}
	}
	slices.SortFunc(want, func(a, b kv) int {
		if a.key < b.key {
			return -1
		}
		if a.key > b.key {
			return 1
		}
		return 0
	})
	if len(rows) != len(want) {
		t.Fatalf("%d rows, the SDK walk %d", len(rows), len(want))
	}
	for i := range rows {
		if rows[i].Key != want[i].key || rows[i].Data.Size != want[i].size {
			t.Fatalf("row %d: %q %d, SDK %q %d", i, rows[i].Key, rows[i].Data.Size, want[i].key, want[i].size)
		}
	}
	v := st.Counters.Value
	t.Logf("%s: %d blobs in %s, %d requests, %d pages; equal to the SDK walk", root, len(rows), took.Round(time.Millisecond), v("requests"), v("pages"))
}
