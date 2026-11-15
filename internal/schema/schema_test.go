// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"testing"

	"github.com/varveio/swath/reader"
)

func TestNonEmpty(t *testing.T) {
	type payload struct{ value string }
	calls := 0
	get := NonEmpty(func(p *payload) string {
		calls++
		return p.value
	})
	for _, value := range []string{"", "etag", " ", "\x00\xff"} {
		before := calls
		got, ok := get(&payload{value})
		if got != value || ok != (value != "") || calls != before+1 {
			t.Fatalf("%q: (%q, %v), %d calls", value, got, ok, calls-before)
		}
	}
}

// TestBindDerivesContainers: Bind derives Container from Info.Containers
// (a row is a container when its string column holds the value), refuses
// a declaration naming no string column, and refuses Container set
// directly, so every consumer reads containers by the one declaration.
func TestBindDerivesContainers(t *testing.T) {
	type entry struct{ kind string }
	spec, _ := reader.Schema(reader.LocalV1)
	cols := func() []Column[entry] {
		cols := make([]Column[entry], len(spec.Columns))
		for i, c := range spec.Columns {
			switch {
			case c.Name == "kind":
				cols[i].Str = func(e *entry) (string, bool) { return e.kind, true }
			case c.Type == String || c.Type == Binary:
				cols[i].Str = func(*entry) (string, bool) { return "", false }
			default:
				cols[i].Int = func(*entry) (int64, bool) { return 0, false }
			}
		}
		return cols
	}
	s := Bind(reader.LocalV1, Schema[entry]{Info: Info{Containers: Match{Column: "kind", Value: "dir"}}, Columns: cols()})
	if s.Container == nil || !s.Container(&entry{"dir"}) || s.Container(&entry{"file"}) || s.Container(&entry{"DIR"}) {
		t.Fatal("Container does not follow the declaration")
	}
	if s := Bind(reader.LocalV1, Schema[entry]{Columns: cols()}); s.Container != nil {
		t.Fatal("a Container without a declaration")
	}
	for name, bad := range map[string]Schema[entry]{
		"no such column": {Info: Info{Containers: Match{Column: "type", Value: "DIR"}}, Columns: cols()},
		"not a string":   {Info: Info{Containers: Match{Column: "size", Value: "DIR"}}, Columns: cols()},
		"set directly":   {Columns: cols(), Container: func(*entry) bool { return true }},
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s: bound", name)
				}
			}()
			Bind(reader.LocalV1, bad)
		}()
	}
}

// TestObjectSize: a row's object bytes are its size, 0 for a row that is
// no object (a container, a delete marker), and unknown (false) for an
// object whose size is null; a schema without a size column has none.
func TestObjectSize(t *testing.T) {
	type row struct {
		size          int64
		sized, marker bool
		kind          string
	}
	s := &Schema[row]{
		Info: Info{ID: "test/sizes", Groups: true, Versions: reader.VersionSpec{ID: "kind", Latest: "latest", DeleteMarker: "marker"},
			Containers: Match{Column: "kind", Value: "dir"}},
		Columns: []Column[row]{
			{Spec: Spec{Name: "size", Type: Int64}, Int: func(r *row) (int64, bool) { return r.size, r.sized }},
			{Spec: Spec{Name: "kind", Type: String}, Str: func(r *row) (string, bool) { return r.kind, true }},
			{Spec: Spec{Name: "latest", Type: Bool, Required: true}, Bool: func(*row) (bool, bool) { return true, true }},
			{Spec: Spec{Name: "marker", Type: Bool, Required: true}, Bool: func(r *row) (bool, bool) { return r.marker, true }},
		},
	}
	s.Container = func(r *row) bool { return r.kind == "dir" }
	size := ObjectSize(s)
	for _, c := range []struct {
		r    row
		n    int64
		sure bool
	}{
		{row{size: 7, sized: true, kind: "file"}, 7, true},
		{row{size: 4096, sized: true, kind: "dir"}, 0, true},
		{row{kind: "dir"}, 0, true},
		{row{marker: true, kind: "v"}, 0, true},
		{row{kind: "file"}, 0, false},
	} {
		if n, ok := size(&c.r); n != c.n || ok != c.sure {
			t.Errorf("%+v: (%d, %v), want (%d, %v)", c.r, n, ok, c.n, c.sure)
		}
	}
	s.Columns = s.Columns[1:]
	s.Info.Versions = reader.VersionSpec{}
	if ObjectSize(s) != nil {
		t.Fatal("a schema without a size column has an object size")
	}
}
