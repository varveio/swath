// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"cmp"
	"fmt"
	"net/url"
	"slices"
	"strings"

	"github.com/varveio/swath/internal/counters"
	"github.com/varveio/swath/internal/source/objhttp"
	"github.com/varveio/swath/internal/source/s3/client"
)

// Backblaze B2's ListObjectVersions departs from S3's in two ways
// (measured 2026-11-27; providers/s3.md#s3-compatible-stores):
//
//   - a page lists all its delete markers, then all its versions, each
//     run in key order and newest first, where S3 sends one sequence;
//   - on a continuation (version-id-marker), the first entry of the
//     marker's key is marked IsLatest.
//
// The page cuts are right (NextKeyMarker is the last entry in S3's order),
// so each page is repaired alone. B2 orders a key's entries by
// LastModified (a multipart upload's is its start, and B2 lists and
// serves by it), at millisecond resolution, so a stable sort by key, then
// LastModified newest first, restores the order. A delete marker and a
// version of one key in the same millisecond cannot be placed: the page
// is invalid. Entries of the marker's key on a version-id-marker
// continuation are older than an entry already listed, so none is the
// latest.

// The versions provider's counters, after the client's and the provider's.
const (
	cB2Reordered = cCredentialExpired + 1 + iota
	cB2LatestCleared
)

var versionsCounters = append(slices.Clone(clientCounters),
	counters.Def{Key: "b2.versions_reordered", Kind: counters.Count, Unit: "pages", Help: "Backblaze B2 ListObjectVersions pages whose delete markers came before their versions, put in S3's order (key, then newest first)"},
	counters.Def{Key: "b2.latest_cleared", Kind: counters.Count, Unit: "entries", Help: "Backblaze B2 entries marked IsLatest on a version-id-marker continuation, of the marker's key, where none can be the latest; cleared"})

// b2Endpoint reports whether endpoint is Backblaze B2's S3 API
// (s3.REGION.backblazeb2.com, or a bucket's virtual host under it).
func b2Endpoint(endpoint string) bool {
	if endpoint == "" {
		return false
	}
	host := endpoint
	if u, err := url.Parse(endpoint); err == nil && u.Host != "" {
		host = u.Hostname()
	}
	host = strings.TrimSuffix(strings.ToLower(host), ".")
	return strings.HasSuffix(host, ".backblazeb2.com")
}

// b2Versions repairs one B2 page in place: it returns whether the entries
// were reordered and how many IsLatest flags were cleared.
func b2Versions(in *client.VersionsInput, out *client.VersionPage) (reordered bool, cleared int, err error) {
	v := out.Versions
	byKeyNewest := func(a, b client.Version) int {
		return cmp.Or(strings.Compare(a.Key, b.Key), cmp.Compare(b.Mtime, a.Mtime))
	}
	if !slices.IsSortedFunc(v, byKeyNewest) {
		slices.SortStableFunc(v, byKeyNewest)
		reordered = true
	}
	for i := 1; i < len(v); i++ {
		if byKeyNewest(v[i-1], v[i]) == 0 && v[i-1].DeleteMarker != v[i].DeleteMarker {
			return false, 0, fmt.Errorf("%w: a delete marker and a version of %q share a LastModified, so Backblaze B2's order cannot be restored",
				objhttp.ErrInvalidPage, v[i].Key)
		}
	}
	if in.VersionIDMarker != "" {
		for i := range v {
			if v[i].Key == in.KeyMarker && v[i].IsLatest {
				v[i].IsLatest = false
				cleared++
			}
		}
	}
	return reordered, cleared, nil
}
