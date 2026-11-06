// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"strings"
)

// A page token is "gcs1." + unpadded base64url of
//
//	flag (0 exclusive, 1 inclusive) | progress (0) | binding (16 bytes) | boundary key
//
// The binding is the first 16 bytes of a SHA-256 over the request's scope
// (route profile, metadata profile, fixture identity, bucket, prefix,
// delimiter, startOffset, endOffset, pagination profile), so a token
// replayed with another scope or against another fixture is refused
// rather than resuming somewhere meaningless. It is not a signature: a
// client that forges one can only choose a resume point it could already
// choose with startOffset. The layout and the binding are the wire
// contract's, so the same state is the same token bytes on either server
// (TestTokenGolden). maxResults is not bound: a walk may change its page
// size between pages.
const tokenPrefix = "gcs1."

const bindingBytes = 16

// paginationProfile names the production pagination profile in the
// binding (no injected short or empty pages).
const paginationProfile = "short=0;empty-first=false"

var errToken = errors.New("invalid GCS pageToken")

// cursor is a decoded token: where the next page resumes.
type cursor struct {
	inclusive bool
	boundary  []byte
}

func encodeToken(inclusive bool, boundary, binding []byte) string {
	raw := make([]byte, 0, 2+bindingBytes+len(boundary))
	flag := byte(0)
	if inclusive {
		flag = 1
	}
	raw = append(raw, flag, 0)
	raw = append(raw, binding...)
	raw = append(raw, boundary...)
	return tokenPrefix + base64.RawURLEncoding.EncodeToString(raw)
}

func decodeToken(token string, binding []byte) (cursor, error) {
	if len(token) > maxTokenBytes || !strings.HasPrefix(token, tokenPrefix) {
		return cursor{}, errToken
	}
	raw, ok := decodeBase64URL(token[len(tokenPrefix):])
	if !ok || len(raw) < 2+bindingBytes || len(raw) > 2+bindingBytes+maxFieldBytes || raw[0] > 1 || raw[1] > 1 ||
		subtle.ConstantTimeCompare(raw[2:2+bindingBytes], binding) != 1 {
		return cursor{}, errToken
	}
	return cursor{inclusive: raw[0] == 1, boundary: raw[2+bindingBytes:]}, nil
}

// decodeBase64URL decodes the URL-safe alphabet with padding optional, as
// the wire contract's decoder does: a correct "=" or "==" ending is
// accepted, any other '=' or a line break is refused, and the unused bits
// of the last unit are not checked. encoding/base64 alone would accept
// line breaks anywhere and refuse a padded token.
func decodeBase64URL(s string) ([]byte, bool) {
	if strings.ContainsAny(s, "\r\n") {
		return nil, false
	}
	switch {
	case strings.HasSuffix(s, "==") && (len(s)-2)%4 == 2:
		s = s[:len(s)-2]
	case strings.HasSuffix(s, "=") && !strings.HasSuffix(s, "==") && (len(s)-1)%4 == 3:
		s = s[:len(s)-1]
	}
	raw, err := base64.RawURLEncoding.DecodeString(s)
	return raw, err == nil
}

// tokenBinding is the scope digest a request's tokens carry.
func tokenBinding(r *Request, fixtureIdentity string) []byte {
	h := sha256.New()
	var n [4]byte
	for _, part := range [][]byte{
		[]byte("gcs-json-v1"), []byte("synthetic-v1"), []byte(fixtureIdentity), []byte(r.Bucket),
		r.Prefix, r.Delimiter, r.StartOffset, r.EndOffset, []byte(paginationProfile),
	} {
		binary.BigEndian.PutUint32(n[:], uint32(len(part)))
		h.Write(n[:])
		h.Write(part)
	}
	return h.Sum(nil)[:bindingBytes]
}
