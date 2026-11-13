// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package azblobtest is test support for the Azure Blob provider (nothing
// in the tool imports it): Keys, a hostile corpus for Azure's UTF-16
// code-unit order; Serve, the replay server in-process over a sorted
// fixture of any keys with its S3 and Azure routes on; CompareUTF16, that
// order written from its definition for fakes to sort with; and Store, an
// in-memory List Blobs in that order with injected short, empty and failed
// responses.
package azblobtest

import (
	"fmt"
	"net/http"
	"slices"
	"strings"
	"unicode/utf16"

	"github.com/varveio/swath/internal/replay/azure"
)

// The long names' lengths in UTF-8 bytes; MaxNameBytes is the longest an
// Azure name can be (1,024 UTF-16 code units, each a three-byte scalar).
const (
	MaxNameBytes = 3072
	fillerNames  = 5000
)

// Keys is the hostile corpus, byte-sorted and without duplicates. It holds:
//
//   - ASCII names with and without '/', and "a/" beside "a/b";
//   - U+00E9, and the root names U+E000, U+FF5A and U+FFFD (lead bytes
//     EE/EF) and U+10000 and U+1F600 (lead bytes F0..F4);
//   - sibling pairs Q+<EE/EF scalar> and Q+<F0..F4 scalar>, with other
//     names under Q, at the root, under "d/" and under "n1/n2/n3/";
//   - pairs nested inside the F0..F4 branch three deep under "r", and one
//     inside the EE/EF branch;
//   - "only-s/" with only an F0..F4 child and "only-e/" with only an EE/EF
//     child;
//   - names of 1,024, 2,010 and 3,072 bytes, the 3,072-byte ones a U+E000 /
//     U+10000 pair (the latter 3,070 bytes) and a plain name under one
//     3,066-byte parent;
//   - 5,000 filler names "f/0000".."f/4999", so every page size up to 5,000
//     (1, 2, 7 and 5,000 included) ends a page inside the corpus, with
//     hostile names on both sides of the filler.
//
// Every name is a valid Azure name: valid UTF-8, no control character, no
// trailing dot, at most 1,024 UTF-16 code units.
func Keys() []string {
	keys := []string{
		"a", "a/", "a/b", "b", "dir/file.txt", "dir/sub/leaf", "plain-name",
		"café", "é", "", "ｚ", "�", "\U00010000", "\U0001f600",
		"only-s/\U0001f600", "only-e/ｚ",
		"r", "r\U0001f600",
	}
	for _, parent := range []string{"", "d/", "n1/n2/n3/"} {
		q := parent + "p"
		keys = append(keys, q, q+"/plain", q+"é", q+"", q+"/x", q+"ｚ",
			q+"\U00010000", q+"\U00010000/y", q+"\U0001f600")
	}
	for q := "r"; len(q) < len("r")+3*4; q += "\U00010000" {
		keys = append(keys, q+"", q+"\U00010000")
	}
	keys = append(keys,
		"long/"+strings.Repeat("a", 1024-len("long/")),
		"long/"+strings.Repeat("é", 1002)+"z",
	)
	parent := strings.Repeat("中", MaxNameBytes/3-2)
	keys = append(keys, parent+"中中", parent+"中", parent+"\U00010000")
	for i := range fillerNames {
		keys = append(keys, fmt.Sprintf("f/%04d", i))
	}
	slices.Sort(keys)
	return slices.Compact(keys)
}

// CompareUTF16 orders two names as Azure lists them, from the definition:
// both are encoded to UTF-16 code units and the sequences compared
// lexicographically, a proper prefix first. It is written independently of
// the replay's byte-rank comparison (replay/azure.Compare) so that a fake
// sorted with it can check code that relies on that one. Defined for valid
// UTF-8; an invalid byte reads as U+FFFD.
func CompareUTF16(a, b string) int {
	return slices.Compare(utf16.Encode([]rune(a)), utf16.Encode([]rune(b)))
}

// SDKVersion is the x-ms-version PinVersion sends: one of the captured
// service versions the replay's Azure route answers.
const SDKVersion = azure.Version20261006

// PinVersion is a transport for the Azure SDK (an azcore
// policy.Transporter: set it as azcore.ClientOptions.Transport) that sends
// every request with x-ms-version SDKVersion. It exists because the replay
// answers only the service versions it has captures for, on purpose, and
// the pinned azblob release sends a newer one (2026-04-06) with no option
// to change it; SDK-oracle tests against Serve need the override. The SDK
// keeps its x-ms-* header names in lower case, outside the canonical key,
// so every spelling is replaced. Next carries the request
// (http.DefaultTransport when nil).
type PinVersion struct {
	Next http.RoundTripper
}

// Do sends req with the pinned version.
func (p PinVersion) Do(req *http.Request) (*http.Response, error) {
	for k := range req.Header {
		if strings.EqualFold(k, "x-ms-version") {
			delete(req.Header, k)
		}
	}
	req.Header["x-ms-version"] = []string{SDKVersion}
	next := p.Next
	if next == nil {
		next = http.DefaultTransport
	}
	return next.RoundTrip(req)
}
