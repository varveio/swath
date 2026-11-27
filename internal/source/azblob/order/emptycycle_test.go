// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package order

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/varveio/swath/internal/source/azblob/client"
	"github.com/varveio/swath/internal/source/keyrange"
)

type markerReply struct {
	marker, next, item string
}

type markerScript struct {
	t       *testing.T
	replies []markerReply
	calls   int
}

func (s *markerScript) List(_ context.Context, in *client.ListInput) (*client.ListOutput, error) {
	s.t.Helper()
	if s.calls >= len(s.replies) {
		s.t.Fatalf("unexpected wire request after %d calls, marker %q", s.calls, in.Marker)
	}
	r := s.replies[s.calls]
	s.calls++
	if in.Delimiter != "" || in.Marker != r.marker {
		s.t.Fatalf("wire request %d: delimiter %q, marker %q, want flat listing with marker %q", s.calls, in.Delimiter, in.Marker, r.marker)
	}
	out := &client.ListOutput{NextMarker: r.next}
	if r.item != "" {
		out.Items = []client.Item{{Name: r.item}}
	}
	return out, nil
}

func TestFlatEmptyWireMarkerCycle(t *testing.T) {
	l := &markerScript{t: t, replies: []markerReply{
		{next: "M"},
		{marker: "M", next: "N"},
		{marker: "N", next: "M"},
	}}
	a := New(l, "c", nil, 0)
	_, err := a.Fetch(context.Background(), &keyrange.Query{Max: 1})
	if !errors.Is(err, ErrInvariant) || l.calls != len(l.replies) {
		t.Fatalf("after %d requests: error %v, want %v after %d", l.calls, err, ErrInvariant, len(l.replies))
	}
}

func TestFlatEmptyWireMarkersResetAfterRows(t *testing.T) {
	l := &markerScript{t: t, replies: []markerReply{
		{next: "M"},
		{marker: "M", next: "N", item: "a"},
		{marker: "N", next: "M"},
		{marker: "M", item: "b"},
	}}
	a := New(l, "c", nil, 0)
	items, err := flat(walk(a, keyrange.Query{Max: 1}, 0, nil))
	if got, want := names(items), []string{"a", "b"}; err != nil || !slices.Equal(got, want) || l.calls != len(l.replies) {
		t.Fatalf("after %d requests: rows %q, error %v, want %q after %d", l.calls, got, err, want, len(l.replies))
	}
}
