// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/varveio/swath/go/internal/registry"
	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source"
	"github.com/varveio/swath/go/internal/textout"
	"github.com/varveio/swath/go/reader"
)

// rolePayload is a versions row whose role columns have unconventional
// names (label, serial, head, tomb, retired) beside decoys with the
// conventional names that contradict them.
type rolePayload struct {
	size              int64
	label             string
	serial            int64
	head, tomb, alive bool
	retired           int64
}

// roleCases are the declarations a versionroles:// root names: a string ID
// with a latest boolean and delete markers, an integer ID with a deleted-at
// column, and a malformed one (both latest and deleted-at).
var roleCases = map[string]reader.VersionSpec{
	"latest":    {ID: "label", Latest: "head", DeleteMarker: "tomb"},
	"deletedat": {ID: "serial", DeletedAt: "retired"},
	"malformed": {ID: "label", Latest: "head", DeletedAt: "retired"},
}

func roleSchema(roles reader.VersionSpec) *schema.Schema[rolePayload] {
	return &schema.Schema[rolePayload]{
		Info: schema.Info{Name: "roles", ID: "test/version-roles", Key: schema.String, Groups: true, Versions: roles},
		Columns: []schema.Column[rolePayload]{
			{Spec: schema.Spec{Name: "size", Type: schema.Int64}, Int: func(p *rolePayload) (int64, bool) { return p.size, !p.tomb }},
			{Spec: schema.Spec{Name: "last_modified", Type: schema.TimestampMicros}, Int: func(*rolePayload) (int64, bool) { return 0, false }},
			{Spec: schema.Spec{Name: "label", Type: schema.String}, Str: func(p *rolePayload) (string, bool) { return p.label, true }},
			{Spec: schema.Spec{Name: "serial", Type: schema.Int64}, Int: func(p *rolePayload) (int64, bool) { return p.serial, true }},
			{Spec: schema.Spec{Name: "head", Type: schema.Bool, Required: true}, Bool: func(p *rolePayload) (bool, bool) { return p.head, true }},
			{Spec: schema.Spec{Name: "tomb", Type: schema.Bool, Required: true}, Bool: func(p *rolePayload) (bool, bool) { return p.tomb, true }},
			{Spec: schema.Spec{Name: "retired", Type: schema.TimestampMicros}, Int: func(p *rolePayload) (int64, bool) { return p.retired, !p.alive }},
			{Spec: schema.Spec{Name: "version_id", Type: schema.String}, Str: func(*rolePayload) (string, bool) { return "decoy", true }},
			{Spec: schema.Spec{Name: "is_latest", Type: schema.Bool, Required: true}, Bool: func(p *rolePayload) (bool, bool) { return !p.head && p.alive, true }},
			{Spec: schema.Spec{Name: "is_delete_marker", Type: schema.Bool, Required: true}, Bool: func(p *rolePayload) (bool, bool) { return !p.tomb, true }},
			{Spec: schema.Spec{Name: "time_deleted", Type: schema.TimestampMicros}, Int: func(p *rolePayload) (int64, bool) { return 1, p.alive }},
		},
		Bytes: func(*rolePayload) int64 { return 8 },
		Heap:  func(p *rolePayload) int64 { return int64(len(p.label)) },
	}
}

// roleRows: a: latest and noncurrent; b: a latest delete marker over a
// noncurrent version; c: one row. Under "latest", head and tomb decide;
// under "deletedat", alive (a null retired) does, and tomb means nothing.
var roleRows = []schema.Row[rolePayload]{
	{Key: "a", Data: rolePayload{label: "a2", serial: 2, head: true, alive: true, size: 1}},
	{Key: "a", Data: rolePayload{label: "a1", serial: 1, retired: 0, size: 2}},
	{Key: "b", Data: rolePayload{label: "bm", serial: 4, head: true, tomb: true, retired: 7}},
	{Key: "b", Data: rolePayload{label: "b1", serial: 3, retired: 5, size: 4}},
	{Key: "c", Data: rolePayload{label: "c1", serial: 5, head: true, alive: true, size: 8}},
}

type roleTree struct{ listed atomic.Int64 }

func (*roleTree) Root() int { return 0 }

func (p *roleTree) Expand(_ context.Context, _ int, e source.Emitter[int, rolePayload]) error {
	p.listed.Add(int64(len(roleRows)))
	return e.Rows(append([]schema.Row[rolePayload](nil), roleRows...))
}

type roleSource struct {
	registry.Bound[rolePayload]
	p    *roleTree
	root string
}

func (o *roleSource) Locator() source.Locator {
	return source.Locator{Kind: "versionroles", Root: o.root}
}
func (o *roleSource) Listed() int64 { return o.p.listed.Load() }

func init() {
	registry.Register(registry.Factory{Scheme: "versionroles", Defaults: func() any { return nil },
		Open: func(_ context.Context, req registry.Request) (registry.Source, error) {
			roles, ok := roleCases[strings.TrimPrefix(req.Root, "versionroles://")]
			if !ok {
				return nil, fmt.Errorf("no case %s", req.Root)
			}
			p := &roleTree{}
			return &roleSource{Bound: registry.Bind[int, rolePayload](p, roleSchema(roles)), p: p, root: req.Root}, nil
		}})
}

// TestVersionReportFollowsDeclaration: the report's version fields of a
// listing over a schema whose roles are unconventionally named, beside
// contradictory conventional-name decoys, follow the declaration: a schema
// with delete markers reports delete_markers and no noncurrent, one
// without them the reverse, with versions and live objects from the
// declared columns.
func TestVersionReportFollowsDeclaration(t *testing.T) {
	for _, c := range []struct {
		name   string
		want   map[string]int64
		absent string
	}{
		{"latest", map[string]int64{"versions": 4, "delete_markers": 1, "live_objects": 2, "distinct_keys": 3, "rows_emitted": 5}, "noncurrent"},
		{"deletedat", map[string]int64{"versions": 5, "noncurrent": 3, "live_objects": 2, "distinct_keys": 3, "rows_emitted": 5}, "delete_markers"},
	} {
		t.Run(c.name, func(t *testing.T) {
			out := filepath.Join(t.TempDir(), "rows.txt")
			rep, err := Run(context.Background(), Config{Source: SourceConfig{URI: "versionroles://" + c.name},
				Output: OutputConfig{Text: &textout.Config{Path: out, Format: textout.Table}}, Limits: Limits{Workers: 1}})
			if err != nil {
				t.Fatal(err)
			}
			checkVersionReport(t, rep, c.want, c.absent)
			b, err := os.ReadFile(out)
			if err != nil || strings.Contains(string(b), "decoy") || strings.Count(string(b), "\n") != len(roleRows)+1 { // the table's header, then the rows
				t.Fatalf("text %q, %v", b, err)
			}
		})
	}
}

// TestVersionRolesRefusedBeforeOutput: a malformed declaration is refused
// when the provider binds its source (registry.Bind panics), before the
// listing creates a dataset, a journal or a text file.
func TestVersionRolesRefusedBeforeOutput(t *testing.T) {
	dir := t.TempDir()
	ds, text := filepath.Join(dir, "ds"), filepath.Join(dir, "rows.tsv")
	for _, o := range []OutputConfig{{Dir: ds}, {Text: &textout.Config{Path: text, Format: textout.TSV}}} {
		func() {
			defer func() {
				if r := recover(); r == nil || !strings.Contains(fmt.Sprint(r), "test/version-roles: version roles: exactly one") {
					t.Fatalf("recovered %v", r)
				}
			}()
			_, _ = Run(context.Background(), Config{Source: SourceConfig{URI: "versionroles://malformed"}, Output: o, Limits: Limits{Workers: 1}})
		}()
	}
	if ents, err := os.ReadDir(dir); err != nil || len(ents) != 0 {
		t.Fatalf("output created: %v, %v", ents, err)
	}
}
