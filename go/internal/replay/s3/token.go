// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"hash/crc32"
	"strings"
)

// A continuation token is the resume boundary of a listing: a key and
// whether the key itself is included. The wire form is
//
//	base64url(flag || boundary || crc), unpadded
//
// with flag 0x00 (exclusive: resume after the key) or 0x01 (inclusive:
// resume at the key) and crc the big-endian CRC-32 (IEEE) of flag ||
// boundary. A page that ends on an object resumes exclusive at it; a
// delimiter page that ends on a rolled-up common prefix P resumes
// inclusive at Successor(P), past P's whole subtree in one store read.
//
// The checksum is what makes an altered token a refusal. Live S3 answers
// its own token with the last character replaced or four characters
// dropped with 400 InvalidArgument (TestLiveAlteredTokenIsRefused); with
// one decoded byte flipped it refused one position and served the others
// (from the first key, or, for the last byte, as if unaltered), so its
// tokens are not uniformly checked. This server refuses every alteration
// instead, a page from a wrong position being worse for a client than a
// refusal: the decoder is strict
// (no padding, no stray bits after the last byte, no line breaks), so
// any change to the text changes the decoded bytes or fails to decode.
// A payload changed in place fails the checksum: CRC-32 detects every
// error burst of 32 bits or fewer, which covers a replaced character or
// byte. A payload shortened or lengthened is accepted only when its last
// four bytes happen to be the checksum of the rest (1 in 2^32), and it is
// then exactly the token of that other boundary (FuzzToken's seed
// four-dropped-is-a-token). It is a checksum, not a
// signature or a binding: it covers only flag || boundary, so a token
// survives restarts and is accepted by a server on another fixture, and
// a client that builds a token picks an arbitrary resume boundary, which
// start-after already allows (TestTokenIsNotBound). The Java server's tokens
// ("v2:" + base64url(flag || boundary), "v1:" + base64url(boundary))
// carry no check, so this server issues different token bytes for the
// same boundary and refuses those forms; the differential driver
// compares walks by where each server's own tokens lead.
type Token struct {
	Boundary  []byte
	Inclusive bool
}

// tokenEncoding is unpadded base64url that refuses non-zero trailing
// bits, so each payload has exactly one spelling.
var tokenEncoding = base64.RawURLEncoding.Strict()

const tokenCRCLen = 4

// EncodeToken returns the wire form of a resume boundary.
func EncodeToken(boundary []byte, inclusive bool) string {
	payload := make([]byte, 1+len(boundary), 1+len(boundary)+tokenCRCLen)
	if inclusive {
		payload[0] = 1
	}
	copy(payload[1:], boundary)
	payload = binary.BigEndian.AppendUint32(payload, crc32.ChecksumIEEE(payload))
	return tokenEncoding.EncodeToString(payload)
}

// errInvalidToken is the answer to every token that is not one: live
// S3's code, message and ArgumentName, without the value
// (TestLiveEmptyTokenIsRefused, TestLiveErrorDocuments).
var errInvalidToken = (&Error{Status: 400, Code: "InvalidArgument", Message: "The continuation token provided is incorrect"}).argument("continuation-token")

// DecodeToken parses a token. Any string not in the token form or whose
// checksum fails, the empty and blank strings included, is a 400
// InvalidArgument: a continuation-token parameter that is present must
// resume something, as on live S3. A well-formed token is accepted
// whoever built it.
func DecodeToken(token string) (t Token, ok bool, err error) {
	// The decoder skips CR and LF even when strict; a token holds neither.
	if strings.ContainsAny(token, "\r\n") {
		return Token{}, false, errInvalidToken
	}
	payload, err := tokenEncoding.DecodeString(token)
	if err != nil || len(payload) < 1+tokenCRCLen {
		return Token{}, false, errInvalidToken
	}
	body, sum := payload[:len(payload)-tokenCRCLen], payload[len(payload)-tokenCRCLen:]
	if body[0] > 1 || crc32.ChecksumIEEE(body) != binary.BigEndian.Uint32(sum) {
		return Token{}, false, errInvalidToken
	}
	return Token{Boundary: bytes.Clone(body[1:]), Inclusive: body[0] == 1}, true, nil
}
