// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package registry_test

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/order"
	"github.com/varveio/swath/go/internal/registry"
	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source/keyrange"
	"github.com/varveio/swath/go/internal/textout"
	"github.com/varveio/swath/go/internal/totals"
	"github.com/varveio/swath/go/reader"
)

// roleRow is a versions payload whose role columns have unconventional
// names (label, serial, head, tomb, retired) beside decoy columns with the
// conventional names that mean the opposite.
type roleRow struct {
	size             int64
	hasSize          bool
	label            string
	serial           int64
	head, tomb       bool
	retired          int64
	hasRetired       bool
	hasLabel, hasSer bool
}

// roleSchema declares roles over the unconventional columns; the decoys
// (version_id, generation, is_latest, is_delete_marker, time_deleted)
// contradict every role, so a consumer reading names gets each row wrong.
func roleSchema(roles reader.VersionSpec) *schema.Schema[roleRow] {
	return &schema.Schema[roleRow]{
		Info: schema.Info{Name: "roles", ID: "test/version-roles", Key: schema.String, Groups: true, Versions: roles},
		Columns: []schema.Column[roleRow]{
			{Spec: schema.Spec{Name: "size", Type: schema.Int64}, Int: func(p *roleRow) (int64, bool) { return p.size, p.hasSize }},
			{Spec: schema.Spec{Name: "last_modified", Type: schema.TimestampMicros}, Int: func(*roleRow) (int64, bool) { return 0, false }},
			{Spec: schema.Spec{Name: "label", Type: schema.String}, Str: func(p *roleRow) (string, bool) { return p.label, p.hasLabel }},
			{Spec: schema.Spec{Name: "serial", Type: schema.Int64}, Int: func(p *roleRow) (int64, bool) { return p.serial, p.hasSer }},
			{Spec: schema.Spec{Name: "head", Type: schema.Bool, Required: true}, Bool: func(p *roleRow) (bool, bool) { return p.head, true }},
			{Spec: schema.Spec{Name: "tomb", Type: schema.Bool, Required: true}, Bool: func(p *roleRow) (bool, bool) { return p.tomb, true }},
			{Spec: schema.Spec{Name: "retired", Type: schema.TimestampMicros}, Int: func(p *roleRow) (int64, bool) { return p.retired, p.hasRetired }},
			{Spec: schema.Spec{Name: "version_id", Type: schema.String}, Str: func(*roleRow) (string, bool) { return "decoy", true }},
			{Spec: schema.Spec{Name: "generation", Type: schema.Int64}, Int: func(*roleRow) (int64, bool) { return -9, true }},
			{Spec: schema.Spec{Name: "is_latest", Type: schema.Bool, Required: true}, Bool: func(p *roleRow) (bool, bool) { return !p.head && p.hasRetired, true }},
			{Spec: schema.Spec{Name: "is_delete_marker", Type: schema.Bool, Required: true}, Bool: func(p *roleRow) (bool, bool) { return !p.tomb, true }},
			{Spec: schema.Spec{Name: "time_deleted", Type: schema.TimestampMicros}, Int: func(p *roleRow) (int64, bool) { return 1, !p.hasRetired }},
		},
		Bytes: func(*roleRow) int64 { return 8 },
		Heap:  func(p *roleRow) int64 { return int64(len(p.label)) },
	}
}

// roleSource serves rows through the range engine, bound to s.
func roleSource(t *testing.T, s *schema.Schema[roleRow], rows []schema.Row[roleRow]) registry.Bound[roleRow] {
	t.Helper()
	p, err := keyrange.New(keyrange.Options{Workers: 2, PageSize: 2}, keyrange.Description[roleRow]{Target: "test", Listing: keyrange.Listing, Groups: true, Detach: s.Detach},
		func(_ context.Context, q *keyrange.Query) (*keyrange.Page[roleRow], error) {
			at := 0
			if q.Token != "" {
				at, _ = strconv.Atoi(q.Token)
			} else {
				for at < len(rows) && rows[at].Key <= q.After {
					at++
				}
			}
			end := min(len(rows), at+q.Max)
			out := &keyrange.Page[roleRow]{Rows: append([]schema.Row[roleRow](nil), rows[at:end]...)}
			if end < len(rows) {
				out.Truncated, out.Token = true, strconv.Itoa(end)
			}
			return out, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	return registry.Bind(p, s)
}

// TestVersionRolesFollowDeclaration: the text output and the versions
// counts of one run, and the per-prefix totals of a count, read the
// declared role columns together, never the conventionally named decoys:
// a string ID with a latest boolean and delete markers, and an integer ID
// with a nullable deleted-at column (a zero time present is a deletion).
func TestVersionRolesFollowDeclaration(t *testing.T) {
	for _, c := range []struct {
		name    string
		roles   reader.VersionSpec
		rows    []schema.Row[roleRow]
		text    string
		markers bool
		// versions, delete markers, noncurrent, live
		counts [4]int64
		// objects and bytes in the total
		objects, bytes int64
	}{
		{
			name:  "string ID, latest, delete markers",
			roles: reader.VersionSpec{ID: "label", Latest: "head", DeleteMarker: "tomb"},
			rows: []schema.Row[roleRow]{
				{Key: "d/a", Data: roleRow{label: "a2", hasLabel: true, head: true, size: 1, hasSize: true}},
				{Key: "d/a", Data: roleRow{label: "a1", hasLabel: true, size: 2, hasSize: true}},
				{Key: "d/b", Data: roleRow{label: "bm", hasLabel: true, head: true, tomb: true}},
				{Key: "d/b", Data: roleRow{label: "b1", hasLabel: true, size: 4, hasSize: true}},
				{Key: "d/c", Data: roleRow{head: true, size: 8, hasSize: true}},
			},
			text: `{"Key":"d/a","VersionId":"a2","IsLatest":true,"IsDeleteMarker":false,"Size":1}
{"Key":"d/a","VersionId":"a1","IsLatest":false,"IsDeleteMarker":false,"Size":2}
{"Key":"d/b","VersionId":"bm","IsLatest":true,"IsDeleteMarker":true}
{"Key":"d/b","VersionId":"b1","IsLatest":false,"IsDeleteMarker":false,"Size":4}
{"Key":"d/c","VersionId":"","IsLatest":true,"IsDeleteMarker":false,"Size":8}
`,
			markers: true,
			counts:  [4]int64{4, 1, 2, 2},
			objects: 4, bytes: 15,
		},
		{
			name:  "integer ID, deleted-at",
			roles: reader.VersionSpec{ID: "serial", DeletedAt: "retired"},
			rows: []schema.Row[roleRow]{
				{Key: "d/a", Data: roleRow{serial: 1700000000000001, hasSer: true, retired: 5, hasRetired: true, size: 1, hasSize: true}},
				{Key: "d/a", Data: roleRow{serial: 1700000000000002, hasSer: true, size: 2, hasSize: true, tomb: true}},
				{Key: "d/b", Data: roleRow{serial: 0, hasSer: true, retired: 0, hasRetired: true, head: true, size: 4, hasSize: true}},
				{Key: "d/c", Data: roleRow{head: true, size: 8, hasSize: true}},
			},
			text: `{"Key":"d/a","VersionId":"1700000000000001","IsLatest":false,"Size":1}
{"Key":"d/a","VersionId":"1700000000000002","IsLatest":true,"Size":2}
{"Key":"d/b","VersionId":"0","IsLatest":false,"Size":4}
{"Key":"d/c","VersionId":"","IsLatest":true,"Size":8}
`,
			counts:  [4]int64{4, 0, 2, 2},
			objects: 4, bytes: 15,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			s := roleSchema(c.roles)
			var buf bytes.Buffer
			o, err := textout.Open(textout.Config{Format: textout.JSONL, Stdout: &buf})
			if err != nil {
				t.Fatal(err)
			}
			vc := &registry.VersionCounts{}
			if _, _, err := roleSource(t, s, c.rows).Run(context.Background(), registry.Env{Order: order.Options{Workers: 2, Budget: 1 << 20}, Text: o, Versions: vc}); err != nil {
				t.Fatal(err)
			}
			if err := o.Commit(); err != nil {
				t.Fatal(err)
			}
			if buf.String() != c.text {
				t.Errorf("text:\n%s\nwant:\n%s", buf.String(), c.text)
			}
			got := [4]int64{vc.Versions.Load(), vc.DeleteMarkers.Load(), vc.Noncurrent.Load(), vc.Live.Load()}
			if !vc.Enabled || vc.HasDeleteMarkers != c.markers || got != c.counts {
				t.Errorf("counts: enabled %v, markers %v, %v; want %v, %v", vc.Enabled, vc.HasDeleteMarkers, got, c.markers, c.counts)
			}

			tl := totals.New(totals.Options{Depth: 1})
			if _, _, err := roleSource(t, s, c.rows).Run(context.Background(), registry.Env{Count: true, Order: order.Options{Workers: 2}, Totals: tl}); err != nil {
				t.Fatal(err)
			}
			st := tl.Stats()
			if st.Objects != c.objects || st.Bytes != c.bytes || st.NotObjects != int64(len(c.rows))-c.objects {
				t.Errorf("totals: %d objects, %d bytes, %d not objects; want %d, %d", st.Objects, st.Bytes, st.NotObjects, c.objects, c.bytes)
			}
		})
	}
}

// TestVersionRolesMalformed: a declaration that does not define the rows'
// roles is refused when the source is bound (registry.Bind panics, as
// schema.Bind does), before any run can open an output.
func TestVersionRolesMalformed(t *testing.T) {
	for _, roles := range []reader.VersionSpec{
		{ID: "label", Latest: "head", DeletedAt: "retired"},
		{ID: "label", Latest: "missing"},
		{ID: "head", Latest: "tomb"},
		{ID: "label", DeletedAt: "size"},
	} {
		t.Run(fmt.Sprintf("%+v", roles), func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil || !strings.Contains(fmt.Sprint(r), "test/version-roles: version roles") {
					t.Fatalf("recovered %v", r)
				}
			}()
			roleSource(t, roleSchema(roles), nil)
			t.Fatal("bound")
		})
	}
}
