// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package memstore

import (
	"bytes"
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/replay/store"
)

func keys(objs []store.Object) string {
	var s []string
	for _, o := range objs {
		s = append(s, string(o.Key))
	}
	return strings.Join(s, ",")
}

func TestRowsHonoursBoundsAndLimit(t *testing.T) {
	s := Keys(Options{}, "c", "a", "b", "d", "e")
	ctx := context.Background()
	for _, c := range []struct {
		from      string
		inclusive bool
		to        string
		limit     int
		want      string
	}{
		{"", true, "", 10, "a,b,c,d,e"},
		{"b", true, "", 10, "b,c,d,e"},
		{"b", false, "", 10, "c,d,e"},
		{"bb", false, "d", 10, "c"},
		{"", true, "", 2, "a,b"},
		{"e", false, "", 10, ""},
	} {
		var from, to []byte
		if c.from != "" {
			from = []byte(c.from)
		}
		if c.to != "" {
			to = []byte(c.to)
		}
		rows, err := s.Rows(ctx, from, c.inclusive, to, c.limit, store.Projection{})
		if err != nil || keys(rows) != c.want {
			t.Errorf("%+v: %q %v", c, keys(rows), err)
		}
	}
	if _, err := New([]store.Object{{Key: []byte("a")}, {Key: []byte("a")}}, Options{}); err == nil {
		t.Error("duplicate keys accepted")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Rows(ctx, nil, true, nil, 1, store.Projection{}); err == nil {
		t.Error("Rows after Close succeeded")
	}
}

func TestRollupDeclinesOtherDelimiters(t *testing.T) {
	s := Keys(Options{Rollup: true}, "a/1", "a/2", "b")
	if _, ok, _ := s.DelimitedRollup(context.Background(), nil, true, nil, nil, []byte("-"), 10, store.Projection{}, true); ok {
		t.Error("rollup answered a '-' delimiter")
	}
	got, ok, err := s.DelimitedRollup(context.Background(), nil, true, nil, nil, []byte("/"), 10, store.Projection{}, true)
	if err != nil || !ok || len(got) != 2 || string(got[0].CommonPrefix) != "a/" || string(got[1].Object.Key) != "b" {
		t.Errorf("%+v %v %v", got, ok, err)
	}
	// The floor rule: with suppression a prefix at or before from is not
	// emitted; without it, it is.
	got, _, _ = s.DelimitedRollup(context.Background(), []byte("a/1"), false, nil, nil, []byte("/"), 10, store.Projection{}, true)
	if len(got) != 1 || string(got[0].Object.Key) != "b" {
		t.Errorf("suppressed: %+v", got)
	}
	got, _, _ = s.DelimitedRollup(context.Background(), []byte("a/1"), false, nil, nil, []byte("/"), 10, store.Projection{}, false)
	if len(got) != 2 || string(got[0].CommonPrefix) != "a/" {
		t.Errorf("not suppressed: %+v", got)
	}
}

func TestTSVRoundTrip(t *testing.T) {
	size := int64(-5)
	mod := time.UnixMicro(1_767_225_600_123_456).UTC()
	empty := ""
	odd := "tab\there\nnewline %41 \\N é"
	objs := []store.Object{
		{Key: []byte("k\t\n%\\N\xff"), Size: &size, LastModified: &mod, ETag: &empty, StorageClass: &odd,
			OwnerID: &odd, OwnerDisplayName: &empty, ChecksumAlgorithm: &odd, ChecksumType: &odd},
		{Key: []byte(`\N`)},
		{Key: []byte("")},
	}
	var buf []byte
	buf = append(buf, "# "+Columns+"\n"...)
	for i := range objs {
		buf = AppendTSV(buf, &objs[i])
	}
	got, err := ReadTSV(bytes.NewReader(buf))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, objs) {
		t.Errorf("round trip:\n%+v\nwant\n%+v", got, objs)
	}
	// Missing trailing columns are null.
	got, err = ReadTSV(strings.NewReader("a/b\t12\n"))
	if err != nil || len(got) != 1 || *got[0].Size != 12 || got[0].LastModified != nil || got[0].ETag != nil {
		t.Errorf("short line: %+v %v", got, err)
	}
	if _, err := ReadTSV(strings.NewReader("k\tnotanumber\n")); err == nil {
		t.Error("a bad size was accepted")
	}
}
