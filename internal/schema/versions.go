// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"fmt"
	"slices"
	"strconv"

	"github.com/varveio/swath/reader"
)

// Versions reads a versions row's roles, derived from the schema's
// declaration (Info.Versions) by BindVersions: the one interpretation of
// version rows that the text output, the versions counts and the
// per-prefix totals share.
type Versions[P any] struct {
	// ID is the version's identifier (an integer column's value in
	// decimal); false when the row has none.
	ID func(p *P) (string, bool)
	// Latest: the row is its key's latest (from the latest column, or a
	// null deleted-at column).
	Latest func(p *P) bool
	// DeleteMarker, nil for a schema without delete markers: the row is
	// a delete marker, not a version of the object.
	DeleteMarker func(p *P) bool
}

// HasDeleteMarkers reports a schema whose rows may be delete markers.
func (v *Versions[P]) HasDeleteMarkers() bool { return v.DeleteMarker != nil }

// Marker reports a delete marker row; always false without markers.
func (v *Versions[P]) Marker(p *P) bool { return v.DeleteMarker != nil && v.DeleteMarker(p) }

// BindVersions derives the accessors of s's version roles, or nil when s
// declares none. It refuses a declaration that does not define every
// row's roles: a role column that is missing, repeated or of the wrong
// type, a nullable latest or delete-marker column (null would mean
// neither), a required deleted-at column (no row could be latest), not
// exactly one of Latest and DeletedAt, or a schema whose keys do not
// group.
func BindVersions[P any](s *Schema[P]) (*Versions[P], error) {
	d := s.Info.Versions
	if d == (reader.VersionSpec{}) {
		return nil, nil
	}
	fail := func(format string, a ...any) error {
		return fmt.Errorf("schema %s: version roles: %s", s.ID, fmt.Sprintf(format, a...))
	}
	if !s.Groups {
		return nil, fail("its keys are unique, and versions repeat a key")
	}
	if (d.Latest == "") == (d.DeletedAt == "") {
		return nil, fail("exactly one of a latest and a deleted-at column is needed, not %q and %q", d.Latest, d.DeletedAt)
	}
	var seen []string
	column := func(role, name string) (*Column[P], error) {
		if name == "" {
			return nil, fail("no %s column", role)
		}
		if slices.Contains(seen, name) {
			return nil, fail("column %s has two roles", name)
		}
		seen = append(seen, name)
		i := slices.IndexFunc(s.Columns, func(c Column[P]) bool { return c.Name == name })
		if i < 0 {
			return nil, fail("the %s column %s is not in the schema", role, name)
		}
		return &s.Columns[i], nil
	}
	flag := func(role, name string) (func(*P) bool, error) {
		c, err := column(role, name)
		if err != nil {
			return nil, err
		}
		if c.Type != Bool || c.Bool == nil || !c.Required {
			return nil, fail("the %s column %s is not a required boolean", role, name)
		}
		get := c.Bool
		return func(p *P) bool { v, _ := get(p); return v }, nil
	}
	v := &Versions[P]{}
	id, err := column("version ID", d.ID)
	if err != nil {
		return nil, err
	}
	switch {
	case id.Type == String && id.Str != nil:
		v.ID = id.Str
	case id.Type == Int64 && id.Int != nil:
		get := id.Int
		v.ID = func(p *P) (string, bool) {
			n, ok := get(p)
			if !ok {
				return "", false
			}
			return strconv.FormatInt(n, 10), true
		}
	default:
		return nil, fail("the version ID column %s is not a string or a signed 64-bit integer", d.ID)
	}
	if d.Latest != "" {
		if v.Latest, err = flag("latest", d.Latest); err != nil {
			return nil, err
		}
	} else {
		c, err := column("deleted-at", d.DeletedAt)
		if err != nil {
			return nil, err
		}
		if c.Type != TimestampMicros || c.Int == nil || c.Required {
			return nil, fail("the deleted-at column %s is not a nullable timestamp", d.DeletedAt)
		}
		// presence, not magnitude: a zero time is a deletion time
		get := c.Int
		v.Latest = func(p *P) bool { _, deleted := get(p); return !deleted }
	}
	if d.DeleteMarker != "" {
		if v.DeleteMarker, err = flag("delete marker", d.DeleteMarker); err != nil {
			return nil, err
		}
	}
	return v, nil
}
