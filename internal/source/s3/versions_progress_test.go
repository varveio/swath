// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/source/keyrange"
	"github.com/varveio/swath/internal/source/objhttp"
	"github.com/varveio/swath/internal/source/s3/client"
)

type unchangedMarkersAPI struct{ calls int }

func (*unchangedMarkersAPI) ListObjectsV2(context.Context, *client.ListInput) (*client.Page, error) {
	panic("wrong mode")
}
func (p *unchangedMarkersAPI) ListObjectVersions(_ context.Context, _ *client.VersionsInput) (*client.VersionPage, error) {
	p.calls++
	k := string(rune('a' + p.calls - 1))
	out := &client.VersionPage{Versions: []client.Version{{Object: client.Object{Key: k}, VersionID: "v"}}}
	if p.calls < 3 {
		out.IsTruncated, out.NextKeyMarker, out.NextVersionIDMarker = true, "a", "v"
	}
	return out, nil
}

func TestRejectUnchangedMarkersOnKeyAdvance(t *testing.T) {
	for _, sequential := range []bool{false, true} {
		t.Run(fmt.Sprint("sequential=", sequential), func(t *testing.T) {
			api := &unchangedMarkersAPI{}
			p, err := NewVersions(context.Background(), Options{Bucket: "b", Workers: 1, Client: api, Policy: keyrange.Policy{Sequential: sequential}})
			if err != nil {
				t.Fatal(err)
			}
			st, err := order.Count(context.Background(), p, nil, order.Options{Workers: 1, Groups: true})
			if !errors.Is(err, objhttp.ErrInvalidPage) || api.calls != 2 {
				t.Fatalf("calls=%d rows=%d error=%v; want invalid page at second request", api.calls, st.Rows, err)
			}
		})
	}
}
