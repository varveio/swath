// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblob

import (
	"encoding/binary"

	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source/azblob/client"
	"github.com/varveio/swath/go/reader"
)

// Version is one entry of a name in a listing of every entry
// (include=versions,snapshots,deleted): a version, a snapshot, a
// soft-deleted blob, or the current blob. A name's entries are
// consecutive rows in Azure's order, which keys them by their time within
// the name: snapshots and versions oldest first, the base blob or current
// version last (List Blobs documents snapshots oldest to newest; its
// continuation marker names an entry as the name and that time, the base
// blob's 9999-12-31T23:59:59.9999999Z). A soft-deleted entry has its
// bytes: its size is its size, never null.
type Version struct {
	Object
	VersionID     string // as listed; "" for a snapshot and on an account without versioning
	Snapshot      string // the snapshot's time, as listed; "" unless a snapshot
	IsLatest      bool   // the name's current blob, live (latest)
	Deleted       bool   // soft-deleted
	DeletedTime   int64  // UTC epoch microseconds; 0 when absent
	RetentionDays int64  // RemainingRetentionDays, when HasRetention
	HasRetention  bool
}

// VersionRow is one listed entry.
type VersionRow = schema.Row[Version]

// latest reports the entry a listing of current blobs lists for its name:
// the base blob (no snapshot), not soft-deleted, and the current version
// where the account versions blobs (a blob written before versioning was
// enabled lists no version id).
func latest(e *client.Entry) bool {
	return e.Snapshot == "" && !e.Deleted && (e.IsCurrentVersion || e.VersionID == "")
}

// version is a listed entry's row payload.
func (p *provider[P]) version(it *client.Item) Version {
	return Version{Object: p.object(it), VersionID: it.VersionID, Snapshot: it.Snapshot, IsLatest: latest(&it.Entry),
		Deleted: it.Deleted, DeletedTime: it.DeletedTime, RetentionDays: it.RetentionDays, HasRetention: it.HasRetention}
}

// VersionSchema is swath/azblob-versions/v1: the swath/azblob-objects/v1
// columns in the same order, then version_id, is_latest, snapshot,
// is_deleted, deleted_time and remaining_retention_days. A name repeats
// once per entry, so the schema is grouped.
var VersionSchema = schema.Bind(reader.AzblobVersionsV1, schema.Schema[Version]{
	Info: schema.Info{Name: "swath_listing", StorageClass: "access_tier"},
	Columns: append(lift(objectColumns),
		// Version ids are unique per name (timestamps): a dictionary rarely pays.
		schema.Column[Version]{Str: schema.NonEmpty(func(v *Version) string { return v.VersionID }), Delta: true},
		schema.Column[Version]{Bool: func(v *Version) (bool, bool) { return v.IsLatest, true }},
		schema.Column[Version]{Str: schema.NonEmpty(func(v *Version) string { return v.Snapshot }), Delta: true},
		schema.Column[Version]{Bool: func(v *Version) (bool, bool) { return v.Deleted, true }},
		schema.Column[Version]{Int: func(v *Version) (int64, bool) { return v.DeletedTime, v.DeletedTime != 0 }},
		schema.Column[Version]{Int: func(v *Version) (int64, bool) { return v.RetentionDays, v.HasRetention }}),
	Bytes: versionBytes,
	Heap:  versionHeap,
	Codec: versionCodec{},
})

// lift binds the current-blob columns to a Version's Object, so the two
// schemas share their columns' names, types and accessors.
func lift(cols []schema.Column[Object]) []schema.Column[Version] {
	out := make([]schema.Column[Version], len(cols))
	for i, c := range cols {
		out[i] = schema.Column[Version]{Spec: c.Spec, Delta: c.Delta}
		switch {
		case c.Str != nil:
			out[i].Str = func(v *Version) (string, bool) { return c.Str(&v.Object) }
		case c.Int != nil:
			out[i].Int = func(v *Version) (int64, bool) { return c.Int(&v.Object) }
		case c.Bool != nil:
			out[i].Bool = func(v *Version) (bool, bool) { return c.Bool(&v.Object) }
		}
	}
	return out
}

// versionBytes is the Object's logical size, the two flags, the integers
// with a value and the strings.
func versionBytes(v *Version) int64 {
	n := objectBytes(&v.Object) + 2 + int64(len(v.VersionID)+len(v.Snapshot))
	if v.DeletedTime != 0 {
		n += 8
	}
	if v.HasRetention {
		n += 8
	}
	return n
}

func versionHeap(v *Version) int64 {
	return objectHeap(&v.Object) + int64(len(v.VersionID)+len(v.Snapshot))
}

// versionCodec is the spill encoding of a Version: the Object's
// (objectCodec), a flags byte, the deleted time and retention days, then
// the version id and the snapshot; its delta form encodes the Object and
// the rest each against the previous row's.
type versionCodec struct{}

const (
	versionLatest = 1 << iota
	versionDeleted
	versionRetention
	versionFlags = versionLatest | versionDeleted | versionRetention
)

func versionFlagsOf(v *Version) byte {
	var f byte
	if v.IsLatest {
		f |= versionLatest
	}
	if v.Deleted {
		f |= versionDeleted
	}
	if v.HasRetention {
		f |= versionRetention
	}
	return f
}

func setVersionFlags(d *schema.Decoder, v *Version, f byte) {
	if f&^versionFlags != 0 {
		d.Fixed(-1)
		return
	}
	v.IsLatest, v.Deleted, v.HasRetention = f&versionLatest != 0, f&versionDeleted != 0, f&versionRetention != 0
}

func versionFields(v *Version) schema.DeltaFields {
	return schema.DeltaFields{Ints: []*int64{&v.DeletedTime, &v.RetentionDays}, Strings: []*string{&v.VersionID, &v.Snapshot}}
}

func (versionCodec) Append(dst []byte, v *Version) []byte {
	dst = append(objectCodec{}.Append(dst, &v.Object), versionFlagsOf(v))
	dst = binary.AppendVarint(binary.AppendVarint(dst, v.DeletedTime), v.RetentionDays)
	return schema.AppendString(schema.AppendString(dst, v.VersionID), v.Snapshot)
}

func (versionCodec) Decode(d *schema.Decoder, v *Version) {
	objectCodec{}.Decode(d, &v.Object)
	setVersionFlags(d, v, d.Byte())
	v.DeletedTime, v.RetentionDays = d.Varint(), d.Varint()
	v.VersionID, v.Snapshot = d.Str(), d.Str()
}

func (versionCodec) AppendDelta(dst []byte, prev, v *Version) []byte {
	dst = append(objectCodec{}.AppendDelta(dst, &prev.Object, &v.Object), versionFlagsOf(v))
	return schema.AppendDelta(dst, versionFields(prev), versionFields(v))
}

func (versionCodec) DecodeDelta(d *schema.Decoder, prev, v *Version) {
	objectCodec{}.DecodeDelta(d, &prev.Object, &v.Object)
	setVersionFlags(d, v, d.Byte())
	schema.DecodeDelta(d, versionFields(prev), versionFields(v))
}

var _ schema.DeltaCodec[Version] = versionCodec{}
