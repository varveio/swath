// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package sdkclient

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/source/azblob/azblobtest"
	"github.com/varveio/swath/go/internal/source/azblob/client"
)

// hostileKeys are names whose UTF-16 order differs from their byte order
// (U+E000 and U+FFFD against supplementary scalars), names Azure sends
// Encoded (U+FFFE, U+FFFF, with '+', '%' and spaces beside them), names
// that need escaping in a query or in XML, and enough under a/ and data/
// for many pages (the client package's SDK oracle's corpus, and more).
func hostileKeys() []string {
	keys := []string{"sp ace", "pl+us", "pct%41", "amp&<>", "q\"'", "a?b", "a#b",
		"u/\ue000", "u/\ufffd", "u/\U0001F600", "u/\U00010000", "u/\uffff", "u/\ufffe/x", "u/\ue000/y", "u/\U0001F600/z", "u/z",
		"e/\ufffe+plus", "e/\ufffe%2B", "e/\ufffe sp", "e/\uffff/\ufffe", "e/x\uffff+/y"}
	for i := range 23 {
		keys = append(keys, fmt.Sprintf("a/%03d", i), fmt.Sprintf("data/year=%d/part-%05d.parquet", 2020+i%3, i))
	}
	return keys
}

// corpus is azblobtest.Keys and hostileKeys, without duplicates.
func corpus() []string {
	seen := map[string]bool{}
	var out []string
	for _, k := range append(azblobtest.Keys(), hostileKeys()...) {
		if !seen[k] {
			seen[k] = true
			out = append(out, k)
		}
	}
	return out
}

// pair builds both clients for the same configuration.
func pair(t *testing.T, cfg client.Config) (*client.Client, *Client) {
	t.Helper()
	hand, err := client.New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	sdk, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	return hand, sdk
}

type lister interface {
	List(ctx context.Context, in *client.ListInput) (*client.ListOutput, error)
}

// walk lists in to its end, page by page.
func walk(t *testing.T, c lister, in client.ListInput) []client.ListOutput {
	t.Helper()
	var pages []client.ListOutput
	for {
		p, err := c.List(context.Background(), &in)
		if err != nil {
			t.Fatalf("%T %+v: %v", c, in, err)
		}
		pages = append(pages, *p)
		if p.NextMarker == "" || len(pages) > 20000 {
			return pages
		}
		in.Marker = p.NextMarker
	}
}

// TestDifferentialReplay walks the replay's Azure route with both clients
// from the same inputs, flat and with a delimiter, from startFrom seeks,
// at page sizes 1 to 5,000, over the hostile corpus: the pages are deep
// equal (items in wire order, kinds interleaved, NextMarker), and the two
// clients sent the same number of attempts.
func TestDifferentialReplay(t *testing.T) {
	r := azblobtest.Serve(t, corpus())
	ep := strings.TrimSuffix(r.Endpoint, "/"+r.Account)
	for _, auth := range []client.Config{{Anonymous: true}, {SAS: "sv=2026-10-06&sp=rl&sig=Fake%2BSig%3D"}, {SharedKey: "c2VjcmV0LWtleS1mb3ItdGVzdHM="}} {
		auth.Endpoint, auth.Account = ep, r.Account
		hand, sdk := pair(t, auth)
		for _, in := range []client.ListInput{
			{MaxResults: 1, Prefix: "u/"},
			{MaxResults: 2, Prefix: "e/"},
			{MaxResults: 7},
			{MaxResults: 5000},
			{},
			{Prefix: "data/year=2021/", MaxResults: 3},
			{StartFrom: "a/010", MaxResults: 5},
			{StartFrom: "u/\ue000", MaxResults: 2},
			{StartFrom: "pct%41", MaxResults: 1, Prefix: "p"},
			{Delimiter: "/", MaxResults: 3},
			{Delimiter: "/"},
			{Prefix: "u/", Delimiter: "/", MaxResults: 2},
			{Prefix: "u/", Delimiter: "/", StartFrom: "u/\U0001F600"},
			{Prefix: "e/", Delimiter: "/", MaxResults: 1},
			{Prefix: "n1/", Delimiter: "/", MaxResults: 4},
			{Prefix: "d/", Delimiter: "/", StartFrom: "d/p\ue000"},
			{Prefix: "r", Delimiter: "\U00010000", MaxResults: 3},
		} {
			in.Container = r.Container
			before := [2]int64{hand.Counters().Attempts, sdk.Counters().Attempts}
			a, b := walk(t, hand, in), walk(t, sdk, in)
			if !reflect.DeepEqual(a, b) {
				for i := range min(len(a), len(b)) {
					if !reflect.DeepEqual(a[i], b[i]) {
						t.Fatalf("%+v page %d:\n hand %+v\n sdk  %+v", in, i, a[i], b[i])
					}
				}
				t.Fatalf("%+v: %d pages, the SDK client %d", in, len(a), len(b))
			}
			if n := hand.Counters().Attempts - before[0]; n != sdk.Counters().Attempts-before[1] || n != int64(len(a)) {
				t.Errorf("%+v: attempts hand %d, sdk %d, pages %d", in, n, sdk.Counters().Attempts-before[1], len(a))
			}
			items, prefixes := 0, 0
			for _, p := range a {
				for _, it := range p.Items {
					items++
					if it.Prefix {
						prefixes++
					}
				}
			}
			if items == 0 || in.Delimiter != "" && prefixes == 0 {
				t.Fatalf("%+v: %d items, %d prefixes: a walk that proves nothing", in, items, prefixes)
			}
		}
	}
}
