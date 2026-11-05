// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"reflect"
	"testing"

	"github.com/varveio/swath/go/internal/source/s3/client"
	"github.com/varveio/swath/go/internal/source/s3/sdkoracle"
)

// TestClientPagesMatchSDK sends the same requests through the custom
// client and the AWS SDK and compares every page field.
func TestClientPagesMatchSDK(t *testing.T) {
	keys := append(testKeys(), "sp ace", "pl+us", "pct%41", "amp&<>", "q\"'")
	url := fakeBucket(t, keys, 0)
	ours, err := client.New(client.Config{Endpoint: url, PathStyle: true, Anonymous: true})
	if err != nil {
		t.Fatal(err)
	}
	sdk := sdkoracle.New(url, "us-east-1", true)
	for _, start := range []client.ListInput{
		{MaxKeys: 7},
		{MaxKeys: 1000, FetchOwner: true},
		{Prefix: "data/year=2021/", MaxKeys: 100},
		{StartAfter: "a/x", MaxKeys: 50},
		{Delimiter: "/", MaxKeys: 3},
		{Prefix: "a/", Delimiter: "/"},
	} {
		in := start
		in.Bucket = "bkt"
		for pages := 0; ; pages++ {
			a, err := ours.ListObjectsV2(context.Background(), &in)
			if err != nil {
				t.Fatal(err)
			}
			b, err := sdk.ListObjectsV2(context.Background(), &in)
			if err != nil {
				t.Fatal(err)
			}
			a.EncodingType = "" // requested by the client only
			if !reflect.DeepEqual(a, b) {
				t.Fatalf("%+v page %d:\nclient %+v\n   sdk %+v", in, pages, a, b)
			}
			if !a.IsTruncated {
				break
			}
			in.StartAfter, in.ContinuationToken = "", a.NextContinuationToken
		}
	}
}

// TestRowsMatchSDK lists through both clients, sequential and parallel.
func TestRowsMatchSDK(t *testing.T) {
	keys := append(testKeys(), "sp ace", "pl+us", "pct%41", "amp&<>")
	url := fakeBucket(t, keys, 0)
	sdk := sdkoracle.New(url, "us-east-1", true)
	for _, seq := range []bool{true, false} {
		want, _ := list(t, Options{Bucket: "bkt", Client: sdk, Sequential: seq, FetchOwner: true, Workers: 4}, 4)
		got, _ := list(t, Options{Bucket: "bkt", Endpoint: url, PathStyle: true, Sequential: seq, FetchOwner: true, Workers: 4}, 4)
		if len(got) != len(keys) || len(got) != len(want) {
			t.Fatalf("rows %d, sdk %d, keys %d", len(got), len(want), len(keys))
		}
		for i := range got {
			if got[i].Key != want[i].Key || got[i].Size != want[i].Size || got[i].Mtime != want[i].Mtime || *got[i].Object != *want[i].Object {
				t.Fatalf("sequential=%v row %d: %+v %+v vs %+v %+v", seq, i, got[i], got[i].Object, want[i], want[i].Object)
			}
		}
	}
}
