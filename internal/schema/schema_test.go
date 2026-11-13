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
	s := Bind(reader.LocalV1, Schema[entry]{Info: Info{Containers: Match{Column: "kind", Value: "DIR"}}, Columns: cols()})
	if s.Container == nil || !s.Container(&entry{"DIR"}) || s.Container(&entry{"FILE"}) || s.Container(&entry{"dir"}) {
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
