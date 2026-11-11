// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/internal/source/keyrange"
	"github.com/varveio/swath/internal/source/objhttp"
	"github.com/varveio/swath/internal/source/s3/client"
	"github.com/varveio/swath/reader"
)

// Version is a versions row's payload: one ListObjectVersions entry, a
// version or a delete marker. A delete marker has no size, ETag, storage
// class or checksums: its kind says they are absent, so its size is null,
// never a zero-byte object, and its strings are empty (null).
type Version struct {
	Object
	VersionID    string // verbatim: "null" is a real ID, never SQL NULL
	IsLatest     bool
	DeleteMarker bool
}

// VersionRow is one listed version or delete marker.
type VersionRow = schema.Row[Version]

// VersionsSchema is the versions schema (swath/s3-versions/v1): the
// s3-objects/v1 columns in the same order, size null on delete markers,
// then version_id, is_latest and is_delete_marker. A key repeats once per
// entry, in S3's order within the key (newest first), so the schema is
// grouped.
var VersionsSchema = schema.Bind(reader.S3VersionsV1, schema.Schema[Version]{
	Info: schema.Info{Name: "swath_listing", StorageClass: "storage_class"},
	Columns: []schema.Column[Version]{
		{Int: func(v *Version) (int64, bool) { return v.Size, !v.DeleteMarker }},
		{Int: func(v *Version) (int64, bool) { return v.Mtime, true }},
		{Str: schema.NonEmpty(func(v *Version) string { return v.ETag }), Delta: true},
		{Str: schema.NonEmpty(func(v *Version) string { return v.StorageClass })},
		{Str: schema.NonEmpty(func(v *Version) string { return v.OwnerID })},
		{Str: schema.NonEmpty(func(v *Version) string { return v.OwnerDisplayName })},
		{Str: schema.NonEmpty(func(v *Version) string { return v.ChecksumAlgorithm })},
		{Str: schema.NonEmpty(func(v *Version) string { return v.ChecksumType })},
		// Version IDs are unique except "null": a dictionary rarely pays.
		{Str: schema.NonEmpty(func(v *Version) string { return v.VersionID }), Delta: true},
		{Bool: func(v *Version) (bool, bool) { return v.IsLatest, true }},
		{Bool: func(v *Version) (bool, bool) { return v.DeleteMarker, true }},
	},
	Bytes: func(v *Version) int64 { return 18 + versionHeap(v) },
	Heap:  versionHeap,
	Codec: versionCodec{},
})

func versionHeap(v *Version) int64 { return objectHeap(&v.Object) + int64(len(v.VersionID)) }

// versionCodec is the spill encoding of a Version: a flags byte, the
// Object (objectCodec), then the version ID; its delta form encodes the
// Object against the previous row's (objectCodec's delta) and sends the
// version ID only when it differs from the previous row's ("null" repeats
// across an unversioned bucket).
type versionCodec struct{}

const (
	versionLatest       = 1 << 0
	versionDeleteMarker = 1 << 1
	versionSameID       = 1 << 2 // delta frames only
	versionFlags        = versionLatest | versionDeleteMarker
)

func versionFlagsOf(v *Version) byte {
	var f byte
	if v.IsLatest {
		f |= versionLatest
	}
	if v.DeleteMarker {
		f |= versionDeleteMarker
	}
	return f
}

// setFlags sets v's booleans from f, or marks d malformed when f holds a
// bit outside allowed.
func setFlags(d *schema.Decoder, v *Version, f, allowed byte) bool {
	if f&^allowed != 0 {
		d.Fixed(-1)
		return false
	}
	v.IsLatest, v.DeleteMarker = f&versionLatest != 0, f&versionDeleteMarker != 0
	return true
}

func (versionCodec) Append(dst []byte, v *Version) []byte {
	dst = append(dst, versionFlagsOf(v))
	dst = objectCodec{}.Append(dst, &v.Object)
	return schema.AppendString(dst, v.VersionID)
}

func (versionCodec) Decode(d *schema.Decoder, v *Version) {
	if !setFlags(d, v, d.Byte(), versionFlags) {
		return
	}
	objectCodec{}.Decode(d, &v.Object)
	v.VersionID = d.Str()
}

func (versionCodec) AppendDelta(dst []byte, prev, v *Version) []byte {
	f := versionFlagsOf(v)
	if v.VersionID == prev.VersionID {
		f |= versionSameID
	}
	dst = append(dst, f)
	dst = objectCodec{}.AppendDelta(dst, &prev.Object, &v.Object)
	if f&versionSameID == 0 {
		dst = schema.AppendString(dst, v.VersionID)
	}
	return dst
}

func (versionCodec) DecodeDelta(d *schema.Decoder, prev, v *Version) {
	f := d.Byte()
	if !setFlags(d, v, f, versionFlags|versionSameID) {
		return
	}
	objectCodec{}.DecodeDelta(d, &prev.Object, &v.Object)
	if f&versionSameID != 0 {
		v.VersionID = prev.VersionID
	} else {
		v.VersionID = d.Str()
	}
}

var _ schema.DeltaCodec[Version] = versionCodec{}

// A versions chain's continuation token carries the response's
// NextKeyMarker and NextVersionIdMarker exactly as returned (the key
// marker decoded, as keys are), behind a one-byte tag: the engine holds it
// opaquely and versionsInput turns it back into the next request's
// markers. The key is length-prefixed, so any key bytes survive.
const (
	tokenKey     = 'k' // "k" key: key-marker only (a page that ends at a common prefix)
	tokenVersion = 'v' // "v" len(key) ":" key versionID: key-marker and version-id-marker
)

// errToken marks a token versionsToken did not write.
var errToken = errors.New("malformed ListObjectVersions continuation token")

// versionsToken encodes a truncated page's markers.
func versionsToken(key, versionID string) string {
	if versionID == "" {
		return string(rune(tokenKey)) + key
	}
	return string(rune(tokenVersion)) + strconv.Itoa(len(key)) + ":" + key + versionID
}

// parseVersionsToken is versionsToken's inverse.
func parseVersionsToken(tok string) (key, versionID string, err error) {
	if tok == "" {
		return "", "", errToken
	}
	rest := tok[1:]
	switch tok[0] {
	case tokenKey:
		key = rest
	case tokenVersion:
		n, after, ok := strings.Cut(rest, ":")
		size, err := strconv.Atoi(n)
		if !ok || err != nil || size < 0 || size > len(after) || strconv.Itoa(size) != n {
			return "", "", errToken
		}
		key, versionID = after[:size], after[size:]
		if versionID == "" {
			return "", "", errToken
		}
	default:
		return "", "", errToken
	}
	if key == "" {
		return "", "", errToken
	}
	return key, versionID, nil
}

// versionsInput is the ListObjectVersions request for an engine query: on
// a chain's first request the key marker is After (key-marker alone is
// exclusive: the versions of the keys after it), then the markers the
// previous page's token carries (version-id-marker only when that page
// returned one).
func versionsInput(bucket string, q *keyrange.Query) (*client.VersionsInput, error) {
	in := &client.VersionsInput{Bucket: bucket, Prefix: q.Prefix, Delimiter: q.Delimiter, MaxKeys: min(q.Max, maxKeys)}
	if q.Token == "" {
		in.KeyMarker = q.After
		return in, nil
	}
	var err error
	if in.KeyMarker, in.VersionIDMarker, err = parseVersionsToken(q.Token); err != nil {
		return nil, err
	}
	return in, nil
}

// versionsPage is a ListObjectVersions response as the engine's native
// page: every entry a row in wire order (versions and delete markers
// interleaved; delete markers kept on probe pages too, so a folder of
// deleted keys is seen), the common prefixes, and, when truncated, a token
// of the response's own markers. A truncated page without a NextKeyMarker
// is invalid: a marker is never made up from the last row. Owners are
// kept when owner is set (--s3-fetch-owner); ETags lose their quotes as
// Object's do.
func versionsPage(out *client.VersionPage, owner bool) (*keyrange.Page[Version], error) {
	if err := out.Validate(); err != nil {
		return nil, err
	}
	pg := &keyrange.Page[Version]{Rows: versionRows(out, owner), Prefixes: out.CommonPrefixes, Truncated: out.IsTruncated}
	if out.IsTruncated {
		pg.Token = versionsToken(out.NextKeyMarker, out.NextVersionIDMarker)
	}
	return pg, nil
}

// versionRows converts a page's entries to rows, in wire order. Rows share
// the page's strings.
func versionRows(out *client.VersionPage, owner bool) []VersionRow {
	rows := make([]VersionRow, len(out.Versions))
	for i := range out.Versions {
		e, r := &out.Versions[i], &rows[i]
		v := &r.Data
		r.Key = e.Key
		v.VersionID, v.IsLatest, v.DeleteMarker = e.VersionID, e.IsLatest, e.DeleteMarker
		if e.DeleteMarker {
			v.Mtime = e.Mtime
			if owner {
				v.OwnerID, v.OwnerDisplayName = e.OwnerID, e.OwnerDisplayName
			}
			continue // no object fields: absent, not zero
		}
		v.Object = object(&e.Object, owner)
	}
	return rows
}

// fetchVersions is the versions engine's page fetch: one
// ListObjectVersions (versionsInput, versionsPage).
func (p *provider[P]) fetchVersions(ctx context.Context, api VersionsAPI, q *keyrange.Query) (*keyrange.Page[Version], error) {
	in, err := versionsInput(p.opt.Bucket, q)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", objhttp.ErrInvalidPage, err)
	}
	// ListObjectVersions always returns owners (no fetch-owner
	// parameter); --s3-fetch-owner decides whether they are kept.
	out, err := api.ListObjectVersions(ctx, in)
	if err != nil {
		if versionsPermission(err) {
			// Likely, not certain: an explicit deny or a prefix condition
			// answers 403 too. Never retried as a current-only listing.
			return nil, fmt.Errorf("%w (--all-versions calls ListObjectVersions, which needs s3:ListBucketVersions; s3:ListBucket alone does not grant it)", err)
		}
		return nil, err
	}
	if out.IsTruncated && out.NextKeyMarker == in.KeyMarker && out.NextVersionIDMarker == in.VersionIDMarker {
		return nil, fmt.Errorf("%w: truncated versions page did not advance continuation markers", objhttp.ErrInvalidPage)
	}
	return versionsPage(out, p.opt.FetchOwner && !q.Probe)
}

// versionsPermission recognizes only listing permission refusals. Codes
// for credentials, routing and missing buckets take precedence over 403.
func versionsPermission(err error) bool {
	f := objhttp.Normalize(err)
	if f == nil || f.Transient || f.Throttle || f.Timeout || f.Status == 301 {
		return false
	}
	switch f.Code {
	case "NoSuchBucket", "PermanentRedirect", "AuthorizationHeaderMalformed",
		"InvalidAccessKeyId", "SignatureDoesNotMatch", "ExpiredToken", "InvalidToken", "TokenRefreshRequired":
		return false
	}
	return f.Status == 403 || f.Code == "AccessDenied" || f.Code == "AllAccessDisabled"
}
