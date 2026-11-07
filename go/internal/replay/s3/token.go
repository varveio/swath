// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"bytes"
	"encoding/base64"
	"strings"
)

// A continuation token is the resume boundary of a listing: a key and
// whether the key itself is included. The wire form is
//
//	"v2:" + base64url(flag || boundary), unpadded
//
// with flag 0x00 (exclusive: resume after the key) or 0x01 (inclusive:
// resume at the key). A page that ends on an object resumes exclusive at
// it; a delimiter page that ends on a rolled-up common prefix P resumes
// inclusive at Successor(P), past P's whole subtree in one store read.
// "v1:" + base64url(boundary) is the older form and decodes as exclusive.
//
// The bytes are the replay wire contract: a client walking a listing
// sees the same tokens from this server and the Java one
// (TestTokenGolden). A token is checked for shape only. It carries no
// signature: forging one picks an arbitrary resume boundary, which
// start-after already allows, so there is nothing to protect.
type Token struct {
	Boundary  []byte
	Inclusive bool
}

const (
	tokenV1 = "v1:"
	tokenV2 = "v2:"
)

// EncodeToken returns the v2 wire form of a resume boundary.
func EncodeToken(boundary []byte, inclusive bool) string {
	payload := make([]byte, 1+len(boundary))
	if inclusive {
		payload[0] = 1
	}
	copy(payload[1:], boundary)
	return tokenV2 + base64.RawURLEncoding.EncodeToString(payload)
}

// errInvalidToken is the answer to every token that is not one: live
// S3's code and message (TestLiveEmptyTokenIsRefused).
var errInvalidToken = &Error{Status: 400, Code: "InvalidArgument", Message: "The continuation token provided is incorrect"}

// DecodeToken parses a token. Any string that is not a v1 or v2 token,
// the empty and blank strings included, is a 400 InvalidArgument: a
// continuation-token parameter that is present must resume something, as
// on live S3.
func DecodeToken(token string) (t Token, ok bool, err error) {
	switch {
	case strings.HasPrefix(token, tokenV2):
		payload, err := decodeBase64URL(token[len(tokenV2):])
		if err != nil || len(payload) == 0 || payload[0] > 1 {
			return Token{}, false, errInvalidToken
		}
		return Token{Boundary: bytes.Clone(payload[1:]), Inclusive: payload[0] == 1}, true, nil
	case strings.HasPrefix(token, tokenV1):
		payload, err := decodeBase64URL(token[len(tokenV1):])
		if err != nil {
			return Token{}, false, errInvalidToken
		}
		return Token{Boundary: payload}, true, nil
	}
	return Token{}, false, errInvalidToken
}

// decodeBase64URL accepts what the replay wire contract accepts: the
// URL-safe alphabet, padded or not (padding, when present, must be
// complete), and unused trailing bits that need not be zero. Go's decoder
// silently skips CR and LF; the contract rejects them, so they are refused
// first.
func decodeBase64URL(body string) ([]byte, error) {
	if strings.ContainsAny(body, "\r\n") {
		return nil, errInvalidToken
	}
	if strings.Contains(body, "=") {
		return base64.URLEncoding.DecodeString(body)
	}
	return base64.RawURLEncoding.DecodeString(body)
}
