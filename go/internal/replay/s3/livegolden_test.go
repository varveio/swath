// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/replay/store/memstore"
)

// The tests in this file pin answers live S3 gave on 2026-11-07 to
// anonymous ListObjectsV2 requests on a public us-east-1 bucket, where the
// replay wire contract answered differently. Each request is transcribed
// onto synthetic keys of the same shape; the want is S3's answer under the
// provider conformance normalizations: a continuation token's value is
// masked, an error is its status, code and message (S3's own document also
// carries ArgumentName, ArgumentValue and HostId, which the replay's does
// not).

// liveKeys is a small tree of the shape the probes listed: six keys in
// one directory, then a directory of three subdirectories.
var liveKeys = []string{
	"index.html",
	"r/k01", "r/k02", "r/k03", "r/k04", "r/k05", "r/k06",
	"t/a/k1", "t/a/k2", "t/a/k3", "t/b/k1", "t/c/k1",
}

var tokenValueRe = regexp.MustCompile(`<(Next)?ContinuationToken>[^<]*<`)

// livePage answers query over liveKeys and splits the document into its
// head (everything before the first entry, token values masked) and the
// listed entries ("O:key" or "P:prefix").
func livePage(t *testing.T, query string) (head string, entries string) {
	t.Helper()
	req, err := ParseRequest("bkt", query)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	res, err := (&Pager{Store: memstore.Keys(memstore.Options{}, liveKeys...)}).List(context.Background(), req)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	doc := string(AppendList(nil, res))
	doc = tokenValueRe.ReplaceAllString(doc, "<${1}ContinuationToken>T<")
	if i := strings.Index(doc, "<Contents>"); i >= 0 {
		head = doc[:i]
	} else if i := strings.Index(doc, "<CommonPrefixes>"); i >= 0 {
		head = doc[:i]
	} else {
		head = strings.TrimSuffix(doc, "</ListBucketResult>")
	}
	return strings.TrimPrefix(head, xmlDecl+listOpen), render(res)
}

// liveError answers query and returns its S3 error.
func liveError(t *testing.T, query string) *Error {
	t.Helper()
	req, err := ParseRequest("bkt", query)
	if err == nil {
		_, err = (&Pager{Store: memstore.Keys(memstore.Options{}, liveKeys...)}).List(context.Background(), req)
	}
	var e *Error
	if !errors.As(err, &e) {
		t.Fatalf("%s: %v, want an S3 error", query, err)
	}
	return e
}

func wantLiveError(t *testing.T, query string, status int, code, message string) {
	t.Helper()
	if e := liveError(t, query); e.Status != status || e.Code != code || e.Message != message {
		t.Errorf("%s: %d %s %q, want %d %s %q", query, e.Status, e.Code, e.Message, status, code, message)
	}
}

func wantLivePage(t *testing.T, query, head, entries string) {
	t.Helper()
	gotHead, gotEntries := livePage(t, query)
	if gotHead != head || gotEntries != entries {
		t.Errorf("%s:\n got %s | %s\nwant %s | %s", query, gotHead, gotEntries, head, entries)
	}
}

// An empty start-after bounds nothing but is echoed: S3 answered
// prefix=P&start-after=&max-keys=2 with <StartAfter></StartAfter> after
// the Prefix and the first two keys under P.
func TestLiveEmptyStartAfterIsEchoed(t *testing.T) {
	wantLivePage(t, "list-type=2&prefix=r/&start-after=&max-keys=2",
		"<Name>bkt</Name><Prefix>r/</Prefix><StartAfter></StartAfter><NextContinuationToken>T</NextContinuationToken>"+
			"<KeyCount>2</KeyCount><MaxKeys>2</MaxKeys><IsTruncated>true</IsTruncated>",
		"O:r/k01 O:r/k02")
	// Absent, it is not echoed.
	wantLivePage(t, "list-type=2&prefix=r/&max-keys=2",
		"<Name>bkt</Name><Prefix>r/</Prefix><NextContinuationToken>T</NextContinuationToken>"+
			"<KeyCount>2</KeyCount><MaxKeys>2</MaxKeys><IsTruncated>true</IsTruncated>",
		"O:r/k01 O:r/k02")
}

// A malformed escape ('%' not followed by two hex digits) drops the whole
// parameter: S3 answered prefix=P/a%ZZ, prefix=P/pct% and prefix=P/a%4
// with an empty Prefix and the bucket's first keys; a malformed
// start-after, delimiter, max-keys, encoding-type or continuation-token
// was likewise absent, and a valid duplicate after a malformed one was
// used.
func TestLiveMalformedEscapeDropsTheParameter(t *testing.T) {
	root := "<Name>bkt</Name><Prefix></Prefix><NextContinuationToken>T</NextContinuationToken>" +
		"<KeyCount>2</KeyCount><MaxKeys>2</MaxKeys><IsTruncated>true</IsTruncated>"
	for _, p := range []string{"r/a%ZZ", "r/pct%", "r/a%4", "r/%g1"} {
		wantLivePage(t, "list-type=2&prefix="+p+"&max-keys=2", root, "O:index.html O:r/k01")
	}
	wantLivePage(t, "list-type=2&pre%ZZfix=r/&max-keys=2", root, "O:index.html O:r/k01")
	inR := "<Name>bkt</Name><Prefix>r/</Prefix><NextContinuationToken>T</NextContinuationToken>" +
		"<KeyCount>2</KeyCount><MaxKeys>2</MaxKeys><IsTruncated>true</IsTruncated>"
	for _, q := range []string{"start-after=r/k%ZZ", "delimiter=%ZZ", "encoding-type=url%ZZ", "continuation-token=%ZZ", "fetch-owner=true%ZZ"} {
		wantLivePage(t, "list-type=2&prefix=r/&max-keys=2&"+q, inR, "O:r/k01 O:r/k02")
	}
	wantLivePage(t, "list-type=2&prefix=r/&max-keys=2%ZZ",
		"<Name>bkt</Name><Prefix>r/</Prefix><KeyCount>6</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>",
		"O:r/k01 O:r/k02 O:r/k03 O:r/k04 O:r/k05 O:r/k06")
	wantLivePage(t, "list-type=2&prefix=r/a%ZZ&prefix=r/&max-keys=2", inR, "O:r/k01 O:r/k02")
}

// The first of duplicated parameters wins: S3 answered
// prefix=P&prefix=P/key with P's listing, max-keys=1&max-keys=2 with one
// key and MaxKeys 1, an empty max-keys before a 2 with the default, and
// an empty start-after before a key with the empty echo and P's first
// keys.
func TestLiveFirstDuplicateWins(t *testing.T) {
	wantLivePage(t, "list-type=2&prefix=r/&prefix=r/k01",
		"<Name>bkt</Name><Prefix>r/</Prefix><KeyCount>6</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>",
		"O:r/k01 O:r/k02 O:r/k03 O:r/k04 O:r/k05 O:r/k06")
	wantLivePage(t, "list-type=2&prefix=r/&max-keys=1&max-keys=2",
		"<Name>bkt</Name><Prefix>r/</Prefix><NextContinuationToken>T</NextContinuationToken>"+
			"<KeyCount>1</KeyCount><MaxKeys>1</MaxKeys><IsTruncated>true</IsTruncated>",
		"O:r/k01")
	wantLivePage(t, "list-type=2&prefix=r/&max-keys=&max-keys=2",
		"<Name>bkt</Name><Prefix>r/</Prefix><KeyCount>6</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>",
		"O:r/k01 O:r/k02 O:r/k03 O:r/k04 O:r/k05 O:r/k06")
	wantLivePage(t, "list-type=2&prefix=r/&start-after=&start-after=r/k01&max-keys=2",
		"<Name>bkt</Name><Prefix>r/</Prefix><StartAfter></StartAfter><NextContinuationToken>T</NextContinuationToken>"+
			"<KeyCount>2</KeyCount><MaxKeys>2</MaxKeys><IsTruncated>true</IsTruncated>",
		"O:r/k01 O:r/k02")
}
