// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package schema_test

import (
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/reader"
)

// rec is a synthetic payload whose role columns have unconventional names.
type rec struct {
	tag            string
	hasTag         bool
	serial         int64
	hasSerial      bool
	head, tomb     bool
	gone           int64
	hasGone, decoy bool
}

// recSchema has a column of every role type, and is_latest, a decoy
// meaning the opposite of head.
func recSchema(roles reader.VersionSpec) *schema.Schema[rec] {
	return &schema.Schema[rec]{
		Info: schema.Info{ID: "test/roles", Key: schema.String, Groups: true, Versions: roles},
		Columns: []schema.Column[rec]{
			{Spec: schema.Spec{Name: "tag", Type: schema.String}, Str: func(p *rec) (string, bool) { return p.tag, p.hasTag }},
			{Spec: schema.Spec{Name: "serial", Type: schema.Int64}, Int: func(p *rec) (int64, bool) { return p.serial, p.hasSerial }},
			{Spec: schema.Spec{Name: "head", Type: schema.Bool, Required: true}, Bool: func(p *rec) (bool, bool) { return p.head, true }},
			{Spec: schema.Spec{Name: "tomb", Type: schema.Bool, Required: true}, Bool: func(p *rec) (bool, bool) { return p.tomb, true }},
			{Spec: schema.Spec{Name: "gone", Type: schema.TimestampMicros}, Int: func(p *rec) (int64, bool) { return p.gone, p.hasGone }},
			{Spec: schema.Spec{Name: "is_latest", Type: schema.Bool, Required: true}, Bool: func(p *rec) (bool, bool) { return !p.head, true }},
			{Spec: schema.Spec{Name: "maybe", Type: schema.Bool}, Bool: func(p *rec) (bool, bool) { return p.decoy, p.decoy }},
			{Spec: schema.Spec{Name: "required_at", Type: schema.TimestampMicros, Required: true}, Int: func(p *rec) (int64, bool) { return p.gone, true }},
			{Spec: schema.Spec{Name: "small", Type: schema.Int32}, Int: func(p *rec) (int64, bool) { return p.serial, true }},
		},
	}
}

func TestBindVersionsNone(t *testing.T) {
	s := recSchema(reader.VersionSpec{})
	if v, err := schema.BindVersions(s); v != nil || err != nil {
		t.Fatalf("no roles: %v, %v", v, err)
	}
	s.Groups = false
	if v, err := schema.BindVersions(s); v != nil || err != nil {
		t.Fatalf("no roles, unique keys: %v, %v", v, err)
	}
}

// TestBindVersionsLatest: a string ID, a latest boolean and delete markers,
// read from the declared columns and not the conventionally named decoy.
func TestBindVersionsLatest(t *testing.T) {
	v, err := schema.BindVersions(recSchema(reader.VersionSpec{ID: "tag", Latest: "head", DeleteMarker: "tomb"}))
	if err != nil {
		t.Fatal(err)
	}
	if !v.HasDeleteMarkers() {
		t.Fatal("no delete markers")
	}
	p := rec{tag: "v1", hasTag: true, head: true}
	if id, ok := v.ID(&p); id != "v1" || !ok || !v.Latest(&p) || v.Marker(&p) {
		t.Fatalf("latest version: %q %v %v %v", id, ok, v.Latest(&p), v.Marker(&p))
	}
	p = rec{tomb: true}
	if id, ok := v.ID(&p); id != "" || ok || v.Latest(&p) || !v.Marker(&p) {
		t.Fatalf("delete marker without an ID: %q %v %v %v", id, ok, v.Latest(&p), v.Marker(&p))
	}
}

// TestBindVersionsDeletedAt: an integer ID in decimal and latest by a null
// deleted-at column; a zero but present time is a deletion.
func TestBindVersionsDeletedAt(t *testing.T) {
	v, err := schema.BindVersions(recSchema(reader.VersionSpec{ID: "serial", DeletedAt: "gone"}))
	if err != nil {
		t.Fatal(err)
	}
	if v.HasDeleteMarkers() || v.DeleteMarker != nil {
		t.Fatal("delete markers without a role")
	}
	for _, c := range []struct {
		p      rec
		id     string
		hasID  bool
		latest bool
	}{
		{rec{serial: -1700000000000001, hasSerial: true}, "-1700000000000001", true, true},
		{rec{serial: 0, hasSerial: true, hasGone: true}, "0", true, false},
		{rec{gone: 5, hasGone: true, head: true}, "", false, false},
		{rec{head: false}, "", false, true},
	} {
		id, ok := v.ID(&c.p)
		if id != c.id || ok != c.hasID || v.Latest(&c.p) != c.latest || v.Marker(&c.p) {
			t.Errorf("%+v: id %q %v latest %v marker %v", c.p, id, ok, v.Latest(&c.p), v.Marker(&c.p))
		}
	}
}

// TestBindVersionsMalformed: a declaration that does not define every
// row's roles is refused.
func TestBindVersionsMalformed(t *testing.T) {
	for _, c := range []struct {
		roles   reader.VersionSpec
		unique  bool
		edit    func(*schema.Schema[rec])
		message string
	}{
		{roles: reader.VersionSpec{ID: "tag", Latest: "head"}, unique: true, message: "keys are unique"},
		{roles: reader.VersionSpec{ID: "tag"}, message: "exactly one"},
		{roles: reader.VersionSpec{ID: "tag", Latest: "head", DeletedAt: "gone"}, message: "exactly one"},
		{roles: reader.VersionSpec{Latest: "head"}, message: "no version ID column"},
		{roles: reader.VersionSpec{DeleteMarker: "tomb"}, message: "exactly one"},
		{roles: reader.VersionSpec{ID: "nope", Latest: "head"}, message: "version ID column nope is not in the schema"},
		{roles: reader.VersionSpec{ID: "tag", Latest: "nope"}, message: "latest column nope is not in the schema"},
		{roles: reader.VersionSpec{ID: "tag", Latest: "head", DeleteMarker: "nope"}, message: "delete marker column nope is not in the schema"},
		{roles: reader.VersionSpec{ID: "tag", DeletedAt: "nope"}, message: "deleted-at column nope is not in the schema"},
		{roles: reader.VersionSpec{ID: "head", Latest: "is_latest"}, message: "not a string or a signed 64-bit integer"},
		{roles: reader.VersionSpec{ID: "gone", Latest: "head"}, message: "not a string or a signed 64-bit integer"},
		{roles: reader.VersionSpec{ID: "small", Latest: "head"}, message: "not a string or a signed 64-bit integer"},
		{roles: reader.VersionSpec{ID: "tag", Latest: "maybe"}, message: "latest column maybe is not a required boolean"},
		{roles: reader.VersionSpec{ID: "tag", Latest: "serial"}, message: "latest column serial is not a required boolean"},
		{roles: reader.VersionSpec{ID: "tag", Latest: "head", DeleteMarker: "maybe"}, message: "delete marker column maybe is not a required boolean"},
		{roles: reader.VersionSpec{ID: "tag", DeletedAt: "required_at"}, message: "deleted-at column required_at is not a nullable timestamp"},
		{roles: reader.VersionSpec{ID: "tag", DeletedAt: "serial"}, message: "deleted-at column serial is not a nullable timestamp"},
		{roles: reader.VersionSpec{ID: "tag", Latest: "head", DeleteMarker: "head"}, message: "column head has two roles"},
		{roles: reader.VersionSpec{ID: "tag", Latest: "head"}, edit: func(s *schema.Schema[rec]) { s.Columns[0].Str = nil },
			message: "version ID column tag is not a string"},
		{roles: reader.VersionSpec{ID: "tag", Latest: "head"}, edit: func(s *schema.Schema[rec]) { s.Columns[2].Bool = nil },
			message: "latest column head is not a required boolean"},
		{roles: reader.VersionSpec{ID: "serial", Latest: "head"}, edit: func(s *schema.Schema[rec]) { s.Columns[1].Int = nil },
			message: "version ID column serial is not a string or a signed 64-bit integer"},
		{roles: reader.VersionSpec{ID: "tag", DeletedAt: "gone"}, edit: func(s *schema.Schema[rec]) { s.Columns[4].Int = nil },
			message: "deleted-at column gone is not a nullable timestamp"},
		{roles: reader.VersionSpec{ID: "tag", Latest: "head", DeleteMarker: "tomb"}, edit: func(s *schema.Schema[rec]) { s.Columns[3].Bool = nil },
			message: "delete marker column tomb is not a required boolean"},
	} {
		s := recSchema(c.roles)
		s.Groups = !c.unique
		if c.edit != nil {
			c.edit(s)
		}
		v, err := schema.BindVersions(s)
		if v != nil || err == nil || !strings.Contains(err.Error(), c.message) || !strings.Contains(err.Error(), "test/roles") {
			t.Errorf("%+v: %v, %v; want %q", c.roles, v, err, c.message)
		}
	}
}

// TestBindCopiesVersions: Bind takes the version roles from the reader
// spec, and the versions schemas' declarations bind.
func TestBindCopiesVersions(t *testing.T) {
	for _, id := range []reader.SchemaID{reader.S3VersionsV1, reader.GCSGenerationsV1, reader.S3ObjectsV1} {
		spec, _ := reader.Schema(id)
		cols := make([]schema.Column[rec], len(spec.Columns))
		for i, c := range spec.Columns {
			switch c.Type {
			case schema.String, schema.Binary:
				cols[i].Str = func(*rec) (string, bool) { return "", false }
			case schema.Bool:
				cols[i].Bool = func(*rec) (bool, bool) { return false, true }
			default:
				cols[i].Int = func(*rec) (int64, bool) { return 0, false }
			}
		}
		s := schema.Bind(id, schema.Schema[rec]{Columns: cols})
		if s.Versions != spec.Versions {
			t.Errorf("%s: versions %+v, want %+v", id, s.Versions, spec.Versions)
		}
		v, err := schema.BindVersions(&s)
		if err != nil || (v != nil) != (spec.Versions != reader.VersionSpec{}) {
			t.Errorf("%s: %v, %v", id, v, err)
		}
	}
}
