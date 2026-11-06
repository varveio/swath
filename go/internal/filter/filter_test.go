// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package filter

import (
	"slices"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/schema"
)

type obj struct {
	size, mtime int64
	class       string
}

var sch = &schema.Schema[obj]{Info: schema.Info{ID: "t", Key: schema.String}, Columns: []schema.Column[obj]{
	{Spec: schema.Spec{Name: "size"}, Int: func(o *obj) (int64, bool) { return o.size, true }},
	{Spec: schema.Spec{Name: "last_modified"}, Int: func(o *obj) (int64, bool) { return o.mtime, o.mtime != 0 }},
	{Spec: schema.Spec{Name: "storage_class"}, Str: func(o *obj) (string, bool) { return o.class, o.class != "" }},
}}

func us(t *testing.T, s string) int64 {
	v, err := ParseTime(s)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// TestEachFilter: Java's semantics for each of the seven filters, alone
// and combined.
func TestEachFilter(t *testing.T) {
	jan, feb := us(t, "2026-01-01"), us(t, "2026-02-01T00:00:00Z")
	rows := []schema.Row[obj]{
		{Key: "logs/a.parquet", Data: obj{100, jan, "STANDARD"}},
		{Key: "logs/b.tmp", Data: obj{2048, feb, "GLACIER"}},
		{Key: "data/c.parquet", Data: obj{1 << 20, feb, "standard"}},
		{Key: "data/d", Data: obj{0, 0, ""}},
	}
	for _, tc := range []struct {
		spec Spec
		want []string
	}{
		{Spec{Include: `\.parquet$`}, []string{"logs/a.parquet", "data/c.parquet"}},
		{Spec{Include: `parquet`}, []string{"logs/a.parquet", "data/c.parquet"}}, // found anywhere
		{Spec{Exclude: `\.tmp$`}, []string{"logs/a.parquet", "data/c.parquet", "data/d"}},
		{Spec{MinSize: "2k"}, []string{"logs/b.tmp", "data/c.parquet"}},
		{Spec{MaxSize: "2048"}, []string{"logs/a.parquet", "logs/b.tmp", "data/d"}},     // inclusive
		{Spec{Since: "2026-02-01"}, []string{"logs/b.tmp", "data/c.parquet", "data/d"}}, // no time: kept
		{Spec{Until: "2026-01-01T00:00:00"}, []string{"logs/a.parquet", "data/d"}},
		{Spec{StorageClasses: []string{"standard"}}, []string{"logs/a.parquet", "data/c.parquet"}},
		{Spec{Include: `^data/`, MinSize: "1", StorageClasses: []string{"STANDARD", "GLACIER"}}, []string{"data/c.parquet"}},
	} {
		if err := tc.spec.Validate(); err != nil {
			t.Fatal(err)
		}
		keep, err := Compile(&tc.spec, sch)
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for i := range rows {
			if keep(&rows[i]) {
				got = append(got, rows[i].Key)
			}
		}
		if !slices.Equal(got, tc.want) {
			t.Errorf("%v: %q, want %q", tc.spec.Descriptions(), got, tc.want)
		}
	}
}

func TestParseAndDescribe(t *testing.T) {
	for in, want := range map[string]int64{"1k": 1024, "256mb": 256 << 20, "1.5GiB": 3 << 29, "0": 0, "7 B": 7, "2T": 2 << 40} {
		if got, err := ParseSize(in); err != nil || got != want {
			t.Errorf("%s: %d %v", in, got, err)
		}
	}
	for _, bad := range []string{"", "k", "1.1b", "-1", "3x", "99999999999T"} {
		if _, err := ParseSize(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
	if a, b := us(t, "2026-01-01T12:00:00+02:00"), us(t, "2026-01-01T10:00"); a != b {
		t.Errorf("offset %d, local %d", a, b)
	}
	for _, s := range []Spec{{Include: `(a)\1`}, {Include: `(?=x)`}, {MinSize: "2k", MaxSize: "1k"}, {Since: "2026-02-01", Until: "2026-01-01"},
		{Since: "yesterday"}, {StorageClasses: []string{" "}}} {
		if err := s.Validate(); err == nil {
			t.Errorf("%+v accepted", s)
		}
	}
	s := Spec{Include: "a", Exclude: "b", MinSize: "1k", MaxSize: "2k", Since: "2026-01-01", Until: "2026-02-01", StorageClasses: []string{"STANDARD", "GLACIER"}}
	want := "include=a exclude=b min_size=1k max_size=2k modified_after=2026-01-01 modified_before=2026-02-01 storage_class=STANDARD,GLACIER"
	if got := strings.Join(s.Descriptions(), " "); got != want {
		t.Errorf("descriptions %q", got)
	}
	if !(&Spec{}).Empty() || (&Spec{Since: "x"}).Empty() {
		t.Error("Empty")
	}
}

// TestNarrowPrefix: only a start-anchored literal narrows, inside scope.
func TestNarrowPrefix(t *testing.T) {
	for _, tc := range []struct{ include, scope, want string }{
		{`^logs/2026/`, "", "logs/2026/"},
		{`^logs/2026/.*\.gz$`, "logs/", "logs/2026/"},
		{`^logs/`, "logs/2026/", "logs/2026/"},
		{`^data/`, "logs/", "logs/"},
		{`logs/`, "", ""},
		{`^(?i)logs/`, "", ""},
		{`^a|^b`, "", ""},
		{`^l+`, "", ""},
		{`^ab+`, "", "a"},
	} {
		if got := (&Spec{Include: tc.include}).NarrowPrefix(tc.scope); got != tc.want {
			t.Errorf("%s in %q: %q, want %q", tc.include, tc.scope, got, tc.want)
		}
	}
}

func TestMissingColumn(t *testing.T) {
	s := &Spec{MinSize: "1"}
	if _, err := Compile(s, &schema.Schema[obj]{Info: schema.Info{ID: "bare"}}); err == nil {
		t.Fatal("size filter without a size column")
	}
}
