// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/varveio/swath/internal/counters"
	"github.com/varveio/swath/internal/source"
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

// b2MaxBound is the longest start-after or key-marker B2 accepts: 1,023
// bytes, though its keys reach 1,024 (a stored key of 1,024 bytes is
// refused as a bound: 400 InvalidRequest, "File name in UTF8 must be no
// more than 1024 bytes"). Prefixes of 1,024 bytes are accepted.
const b2MaxBound = 1023

// acceptBound is the start bounds the endpoint takes (Description.AcceptBound):
// on B2 none over b2MaxBound, so the engine sends the longest prefix of a
// key that fits (keyrange's safeStart) and drops the keys at or below the
// key itself, and never splits at such a key.
func acceptBound(endpoint string) func(string) bool {
	if !source.Backblaze(endpoint) {
		return AcceptBound
	}
	return func(s string) bool { return len(s) <= b2MaxBound && AcceptBound(s) }
}

// The versions provider's counters, after the client's and the provider's.
const (
	cB2Reordered = cB2LongKeyRestarts + 1 + iota
	cB2LatestCleared
)

var versionsCounters = append(slices.Clone(clientCounters),
	counters.Def{Key: "b2.versions_reordered", Kind: counters.Count, Unit: "pages", Help: "Backblaze B2 ListObjectVersions pages whose delete markers came before their versions, put in S3's order (key, then newest first)"},
	counters.Def{Key: "b2.latest_cleared", Kind: counters.Count, Unit: "entries", Help: "Backblaze B2 entries marked IsLatest on a version-id-marker continuation, of the marker's key, where none can be the latest; cleared"})

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

// B2 refuses its own continuation token after a page whose last entry is a
// key over b2MaxBound bytes (the token is that key, and B2 applies the
// bound's limit to it), so every token-following client stops there (the
// AWS CLI, rclone's S3 and B2 backends, 2026-11-27). b2Restart is the
// token swath gives such a page instead: the last entry behind a NUL, which
// no B2 token (base64) starts with. Its next request is start-after the
// longest prefix of the entry B2 takes, the entries at or below the entry
// itself dropped: listed by this chain already.
const b2Restart = "\x00"

// b2Continue rewrites a B2 page's continuation: the request it answered
// (in, and the restart point when in was a restart) and the page as B2 sent
// it. It drops what a restart listed again and returns the token to hand
// on.
func b2Continue(restart string, out *client.Page) (token string, restarted bool, err error) {
	raw := lastEntry(out)
	if restart != "" {
		out.Contents = slices.DeleteFunc(out.Contents, func(o client.Object) bool { return o.Key <= restart })
		out.CommonPrefixes = slices.DeleteFunc(out.CommonPrefixes, func(cp string) bool { return cp <= restart })
	}
	if !out.IsTruncated || len(raw) <= b2MaxBound {
		return out.NextContinuationToken, false, nil
	}
	if raw <= restart {
		return "", false, fmt.Errorf("%w: Backblaze B2 cannot continue past %q: more than a page of keys share its longest prefix that B2 takes as a start-after",
			objhttp.ErrInvalidPage, restart)
	}
	return b2Restart + raw, true, nil
}

// lastEntry is the greatest key or common prefix of a page ("" for none).
func lastEntry(out *client.Page) string {
	var last string
	if n := len(out.Contents); n > 0 {
		last = out.Contents[n-1].Key
	}
	if n := len(out.CommonPrefixes); n > 0 && out.CommonPrefixes[n-1] > last {
		last = out.CommonPrefixes[n-1]
	}
	return last
}

// b2Start is the longest prefix of key, on a scalar boundary, that B2
// takes as a start-after.
func b2Start(key string) string {
	s := key
	for len(s) > b2MaxBound {
		_, n := utf8.DecodeLastRuneInString(s)
		s = s[:len(s)-n]
	}
	return s
}
