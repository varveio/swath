// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azure

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"strings"
)

// A marker is the resume boundary of one listing scope:
//
//	"az1." + base64url(flag || progress || binding || boundary), unpadded
//
// flag is 0x00 (resume after the boundary) or 0x01 (resume at it);
// progress (0 or 1) marks a walk that has passed a pagination test
// profile's injected empty first page, so that page is not injected
// again; binding is the first 16 bytes of a SHA-256 over the scope (fixture
// identity, account, container, service version, prefix, delimiter,
// startFrom), so a marker used under another scope or fixture is refused.
// The boundary is at most 4,096 bytes and the marker at most 8,192
// characters.
//
// This server serves only the production pagination profile, which never
// injects a page or sets progress, but a marker that carries progress 1
// passes it to every marker issued from it: the byte sits outside the
// binding, and a walk's later markers keep it whichever page the walk
// resumed from (TestMarkerProgressIsRetained).
//
// The bytes are the replay wire contract: a client walking a listing sees
// the same markers from this server and the Java one for the same fixture
// identity (TestMarkerGolden, TestMarkerProgressIsRetained). A marker
// carries no secret; the binding only keeps a marker from silently resuming
// a different listing.
const (
	markerPrefix      = "az1."
	bindingBytes      = 16
	maxMarkerChars    = 8192
	maxBoundaryBytes  = 4096
	bindingDomain     = "azure-flat-v2"
	bindingProfile    = "synthetic-v1"
	paginationProfile = "short=0;empty-first=false"
)

var errInvalidMarker = errors.New("invalid Azure marker")

// binding is the scope digest a marker carries. The parts are
// length-prefixed (4-byte big-endian) UTF-8, an absent value empty.
func binding(r *Request, fixtureIdentity string) []byte {
	d := sha256.New()
	for _, p := range []string{bindingDomain, bindingProfile, fixtureIdentity, r.Account, r.Container,
		r.Version, r.Prefix, r.Delimiter, r.StartFrom, paginationProfile} {
		d.Write(binary.BigEndian.AppendUint32(nil, uint32(len(p))))
		d.Write([]byte(p))
	}
	return d.Sum(nil)[:bindingBytes]
}

// markerState is what every marker a request issues carries besides its
// boundary: the scope binding and the progress byte of the marker the
// request resumed from (0 for a first page).
type markerState struct {
	binding  []byte
	progress byte
}

func (s markerState) encode(inclusive bool, boundary []byte) string {
	return encodeMarker(inclusive, boundary, s.binding, s.progress)
}

func encodeMarker(inclusive bool, boundary, bind []byte, progress byte) string {
	raw := make([]byte, 0, 2+bindingBytes+len(boundary))
	flag := byte(0)
	if inclusive {
		flag = 1
	}
	raw = append(raw, flag, progress)
	raw = append(raw, bind...)
	raw = append(raw, boundary...)
	return markerPrefix + base64.RawURLEncoding.EncodeToString(raw)
}

// marker is a decoded marker: where the next page resumes, and the
// progress byte it carried.
type marker struct {
	inclusive bool
	boundary  []byte
	progress  byte
}

func decodeMarker(m string, bind []byte) (marker, error) {
	if len(m) > maxMarkerChars || !strings.HasPrefix(m, markerPrefix) {
		return marker{}, errInvalidMarker
	}
	raw, err := decodeBase64URL(m[len(markerPrefix):])
	if err != nil || len(raw) < 2+bindingBytes || len(raw) > 2+bindingBytes+maxBoundaryBytes ||
		raw[0] > 1 || raw[1] > 1 || subtle.ConstantTimeCompare(raw[2:2+bindingBytes], bind) != 1 {
		return marker{}, errInvalidMarker
	}
	return marker{inclusive: raw[0] == 1, boundary: raw[2+bindingBytes:], progress: raw[1]}, nil
}

// decodeBase64URL accepts what the replay wire contract accepts: the
// URL-safe alphabet, padded or not (padding, when present, must be
// complete), and unused trailing bits that need not be zero
// (TestMarkerDecodeLeniency). Go's decoder silently skips CR and LF; the
// contract rejects them, so they are refused first.
func decodeBase64URL(body string) ([]byte, error) {
	if strings.ContainsAny(body, "\r\n") {
		return nil, errInvalidMarker
	}
	if strings.Contains(body, "=") {
		return base64.URLEncoding.DecodeString(body)
	}
	return base64.RawURLEncoding.DecodeString(body)
}
