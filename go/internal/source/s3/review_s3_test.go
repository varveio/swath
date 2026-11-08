// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"errors"
	"github.com/varveio/swath/go/internal/order"
	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source/keyrange"
	"github.com/varveio/swath/go/internal/source/s3/client"
	"reflect"
	"strings"
	"testing"
)

type reviewMissingMarker struct{}

func (reviewMissingMarker) ListObjectsV2(context.Context, *client.ListInput) (*client.Page, error) {
	return nil, errors.New("unexpected current-object request")
}
func (reviewMissingMarker) ListObjectVersions(_ context.Context, in *client.VersionsInput) (*client.VersionPage, error) {
	if in.KeyMarker == "" {
		return &client.VersionPage{Versions: []client.Version{{Object: client.Object{Key: "k"}, VersionID: "new"}}, IsTruncated: true, NextKeyMarker: "k"}, nil
	}
	// Native semantics: key-marker alone skips every remaining version of k.
	return &client.VersionPage{}, nil
}
func TestReviewMissingVersionMarkerLosesTail(t *testing.T) {
	p, err := NewVersions(context.Background(), Options{Bucket: "b", Workers: 1, Client: reviewMissingMarker{}})
	if err != nil {
		t.Fatal(err)
	}
	st, err := order.Count(context.Background(), p, nil, order.Options{Workers: 1, Groups: true})
	if !errors.Is(err, client.ErrInvalidPage) {
		t.Fatalf("missing version marker accepted: rows=%d err=%v; the older versions of k are skipped", st.Rows, err)
	}
}

type fakeVersionsChain struct {
	t              *testing.T
	calls          int
	badKey, denied bool
}

func (f *fakeVersionsChain) ListObjectsV2(context.Context, *client.ListInput) (*client.Page, error) {
	f.t.Fatal("current listing called")
	return nil, nil
}
func (f *fakeVersionsChain) ListObjectVersions(_ context.Context, in *client.VersionsInput) (*client.VersionPage, error) {
	f.calls++
	if f.denied {
		return nil, &client.Error{Status: 403, Code: "AccessDenied"}
	}
	if f.badKey {
		return &client.VersionPage{IsTruncated: true}, nil
	}
	if in.Bucket != "b" || in.MaxKeys != 2 {
		f.t.Fatalf("input=%+v", in)
	}
	switch f.calls {
	case 1:
		if in.KeyMarker != "" || in.VersionIDMarker != "" {
			f.t.Fatal(in)
		}
		return &client.VersionPage{Versions: []client.Version{
			{Object: client.Object{Key: "a", Size: 7}, VersionID: "new", IsLatest: true},
			{Object: client.Object{Key: "a"}, VersionID: "null", DeleteMarker: true},
		}, IsTruncated: true, NextKeyMarker: "a", NextVersionIDMarker: "null"}, nil
	case 2:
		if in.KeyMarker != "a" || in.VersionIDMarker != "null" {
			f.t.Fatal(in)
		}
		return &client.VersionPage{Versions: []client.Version{{Object: client.Object{Key: "a", Size: 3}, VersionID: "old"}},
			IsTruncated: true, NextKeyMarker: "a", NextVersionIDMarker: "old"}, nil
	case 3:
		if in.KeyMarker != "a" || in.VersionIDMarker != "old" {
			f.t.Fatal(in)
		}
		return &client.VersionPage{CommonPrefixes: []string{"b/"}, IsTruncated: true, NextKeyMarker: "b/"}, nil
	case 4:
		if in.KeyMarker != "b/" || in.VersionIDMarker != "" {
			f.t.Fatal(in)
		}
		return &client.VersionPage{Versions: []client.Version{{Object: client.Object{Key: "c", Size: 1}, VersionID: "null", IsLatest: true}}}, nil
	default:
		f.t.Fatal("extra call")
		return nil, nil
	}
}

type versionCollector struct{ rows []VersionRow }

func (c *versionCollector) Write(_ context.Context, segs [][]schema.Row[Version]) error {
	for _, s := range segs {
		c.rows = append(c.rows, s...)
	}
	return nil
}
func TestVersionsFakeClientEndToEnd(t *testing.T) {
	f := &fakeVersionsChain{t: t}
	p, err := NewVersions(context.Background(), Options{Bucket: "b", Workers: 1, Client: f, Policy: keyrange.Policy{PageSize: 2}})
	if err != nil {
		t.Fatal(err)
	}
	sink := &versionCollector{}
	st, err := order.Run(context.Background(), p, sink, &VersionsSchema, order.Options{Workers: 1, Budget: 1 << 20, Groups: true})
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	var markers []bool
	for _, r := range sink.rows {
		ids = append(ids, r.Data.VersionID)
		markers = append(markers, r.Data.DeleteMarker)
	}
	if st.Rows != 4 || st.DistinctKeys != 2 || f.calls != 4 || !reflect.DeepEqual(ids, []string{"new", "null", "old", "null"}) || !reflect.DeepEqual(markers, []bool{false, true, false, false}) {
		t.Fatalf("stats=%+v calls=%d rows=%+v", st, f.calls, sink.rows)
	}
	if p.Stats().Counters.Value("groups.open_pages") == 0 {
		t.Fatal("hot group path did not engage")
	}
}
func TestVersionsFakeClientRefusals(t *testing.T) {
	for _, denied := range []bool{false, true} {
		f := &fakeVersionsChain{t: t, badKey: !denied, denied: denied}
		p, err := NewVersions(context.Background(), Options{Bucket: "b", Workers: 1, Client: f})
		if err != nil {
			t.Fatal(err)
		}
		_, err = order.Count(context.Background(), p, nil, order.Options{Workers: 1, Groups: true})
		if denied {
			if err == nil || !strings.Contains(err.Error(), "s3:ListBucketVersions") {
				t.Fatalf("403: %v", err)
			}
		} else if !errors.Is(err, client.ErrInvalidPage) {
			t.Fatal(err)
		}
		if f.calls != 1 {
			t.Fatal(f.calls)
		}
	}
}
