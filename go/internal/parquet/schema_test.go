// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package parquet

import "github.com/varveio/swath/go/internal/schema"

// tr is the tests' row payload: one column of every type the binding
// supports, optional ones present per bit.
type tr struct {
	Size, Mtime, Inode int64
	Mode               uint32
	Small              int32
	Flag               bool
	Kind, Link, ETag   string
	Has                uint8 // 1: the stat values, 2: the link target
}

func (p *tr) has(b uint8) bool { return p.Has&b != 0 }

type row = schema.Row[tr]

// testSchema has every column type; cappedSchema adds a 1,024-byte key cap.
var testSchema = schema.Schema[tr]{
	Info: schema.Info{Name: "swath_listing", ID: "test/all-types", Key: schema.String},
	Columns: []schema.Column[tr]{
		{Spec: schema.Spec{Name: "size", Type: schema.Int64}, Int: func(p *tr) (int64, bool) { return p.Size, true }},
		{Spec: schema.Spec{Name: "last_modified", Type: schema.TimestampMicros}, Int: func(p *tr) (int64, bool) { return p.Mtime, true }},
		{Spec: schema.Spec{Name: "kind", Type: schema.String, Required: true}, Str: func(p *tr) (string, bool) { return p.Kind, true }},
		{Spec: schema.Spec{Name: "mode", Type: schema.Uint32}, Int: func(p *tr) (int64, bool) { return int64(int32(p.Mode)), p.has(1) }},
		{Spec: schema.Spec{Name: "inode", Type: schema.Uint64}, Int: func(p *tr) (int64, bool) { return p.Inode, p.has(1) }},
		{Spec: schema.Spec{Name: "small", Type: schema.Int32}, Int: func(p *tr) (int64, bool) { return int64(p.Small), p.has(1) }},
		{Spec: schema.Spec{Name: "flag", Type: schema.Bool}, Bool: func(p *tr) (bool, bool) { return p.Flag, p.has(1) }},
		{Spec: schema.Spec{Name: "link_target", Type: schema.Binary}, Str: func(p *tr) (string, bool) { return p.Link, p.has(2) }},
		{Spec: schema.Spec{Name: "etag", Type: schema.String}, Str: func(p *tr) (string, bool) { return p.ETag, p.ETag != "" }, Delta: true},
	},
	Bytes: func(p *tr) int64 { return 40 + int64(len(p.Kind)+len(p.Link)+len(p.ETag)) },
	Heap:  func(p *tr) int64 { return int64(len(p.Link) + len(p.ETag)) },
}

var cappedSchema = func() schema.Schema[tr] {
	s := testSchema
	s.MaxKeyBytes = 1024
	return s
}()
