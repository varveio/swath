// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/replay/metrics"
	"github.com/varveio/swath/go/internal/replay/s3"
	"github.com/varveio/swath/go/internal/replay/store"
	"github.com/varveio/swath/go/internal/replay/store/memstore"
	"github.com/varveio/swath/go/sortedparquet/sortedparquettest"
)

// The tests below port the contract's sorted-serving differential
// (SortedServingFullDifferentialTest) under its own case names: each
// builds a keyspace as a stamped sorted Parquet fixture with many small row
// groups (or several parts), serves it through the real store, and serves
// the same objects from memstore's range walk, an independent
// implementation, then requires every page of every walk to be
// byte-identical over HTTP across the encoding-type=url and fetch-owner
// matrix. The driver compares against the Java server on large fixtures;
// these hold the adversarial shapes no recording is guaranteed to have.

type scenario struct {
	prefix, delimiter, startAfter []byte
	maxKeys                       int
	encodingURL, fetchOwner       bool
}

func projectionMatrix(prefix, delimiter, startAfter []byte, maxKeys ...int) []scenario {
	var out []scenario
	for _, mk := range maxKeys {
		for _, enc := range []bool{true, false} {
			for _, owner := range []bool{true, false} {
				out = append(out, scenario{prefix, delimiter, startAfter, mk, enc, owner})
			}
		}
	}
	return out
}

func (sc scenario) String() string {
	return fmt.Sprintf("prefix=%q delimiter=%q start-after=%q max-keys=%d url=%v owner=%v",
		sc.prefix, sc.delimiter, sc.startAfter, sc.maxKeys, sc.encodingURL, sc.fetchOwner)
}

// sortedPair serves keys from a stamped sorted fixture (parts of at most
// perPart rows, row groups of groupRows) through the real store, and from
// memstore without the rollup, and returns both servers.
func sortedPair(t *testing.T, keys [][]byte, perPart, groupRows int) (sorted, oracle *Server) {
	t.Helper()
	keys = slices.Clone(keys)
	slices.SortFunc(keys, bytes.Compare)
	var rows []sortedparquettest.Row
	var objs []store.Object
	for i, k := range keys {
		r := sortedparquettest.Object(string(k), i)
		rows = append(rows, r)
		lm := time.UnixMicro(*r.LastModified).UTC()
		objs = append(objs, store.Object{Key: k, Size: r.Size, LastModified: &lm, ETag: r.ETag, StorageClass: r.StorageClass,
			OwnerID: r.OwnerID, OwnerDisplayName: r.OwnerDisplayName, ChecksumAlgorithm: r.ChecksumAlgorithm, ChecksumType: r.ChecksumType})
	}
	dir := t.TempDir()
	parts := (len(rows) + perPart - 1) / perPart
	for p := range parts {
		part := sortedparquettest.Part{Rows: rows[p*perPart : min(len(rows), (p+1)*perPart)], GroupRows: groupRows,
			Stamps: sortedparquettest.Stamps(p+1, p == parts-1)}
		if err := sortedparquettest.Write(filepath.Join(dir, fmt.Sprintf("part-%05d.parquet", p+1)), part); err != nil {
			t.Fatal(err)
		}
	}
	st, _, err := store.Open(context.Background(), dir, store.Options{Readers: 2})
	if err != nil {
		t.Fatal(err)
	}
	mem, err := memstore.New(objs, memstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	return start(t, config(st)), start(t, config(mem))
}

var nextTokenRe = regexp.MustCompile(`<NextContinuationToken>([^<]*)</NextContinuationToken>`)

func body(t *testing.T, s *Server, query string) string {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/bucket?%s", s.Port(), query))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("%s: %d %s", query, resp.StatusCode, b)
	}
	return string(b)
}

// walkPages follows a scenario to the end, sending only the token after
// the first page, as a well-behaved client does.
func walkPages(t *testing.T, s *Server, sc scenario) []string {
	t.Helper()
	var pages []string
	token := ""
	for range 100_000 {
		q := fmt.Sprintf("list-type=2&max-keys=%d", sc.maxKeys)
		if sc.encodingURL {
			q += "&encoding-type=url"
		}
		if sc.fetchOwner {
			q += "&fetch-owner=true"
		}
		if sc.prefix != nil {
			q += "&prefix=" + s3.PercentEncode(sc.prefix)
		}
		if sc.delimiter != nil {
			q += "&delimiter=" + s3.PercentEncode(sc.delimiter)
		}
		if token != "" {
			q += "&continuation-token=" + s3.PercentEncode([]byte(token))
		} else if sc.startAfter != nil {
			q += "&start-after=" + s3.PercentEncode(sc.startAfter)
		}
		page := body(t, s, q)
		pages = append(pages, page)
		if !strings.Contains(page, "<IsTruncated>true</IsTruncated>") {
			return pages
		}
		m := nextTokenRe.FindStringSubmatch(page)
		if m == nil {
			t.Fatalf("%v: a truncated page without a token", sc)
		}
		token = strings.NewReplacer("&amp;", "&", "&lt;", "<", "&gt;", ">", "&quot;", `"`, "&apos;", "'").Replace(m[1])
	}
	t.Fatalf("%v: the listing did not end", sc)
	return nil
}

func walkAll(t *testing.T, sorted, oracle *Server, scenarios []scenario) {
	t.Helper()
	for _, sc := range scenarios {
		a, b := walkPages(t, sorted, sc), walkPages(t, oracle, sc)
		if !slices.Equal(a, b) {
			for i := range min(len(a), len(b)) {
				if a[i] != b[i] {
					t.Fatalf("%v: page %d differs\nsorted %s\noracle %s", sc, i+1, a[i], b[i])
				}
			}
			t.Fatalf("%v: %d pages against %d", sc, len(a), len(b))
		}
	}
}

func utf8Keys(format string, n int) [][]byte {
	var out [][]byte
	for i := range n {
		out = append(out, []byte(fmt.Sprintf(format, i)))
	}
	return out
}

func strs(keys ...string) [][]byte {
	var out [][]byte
	for _, k := range keys {
		out = append(out, []byte(k))
	}
	return out
}

func TestFlatListingIsByteIdentical(t *testing.T) {
	sorted, oracle := sortedPair(t, utf8Keys("key-%05d", 180), 1000, 7)
	walkAll(t, sorted, oracle, projectionMatrix(nil, nil, nil, 0, 1, 3, 7, 50, 1000))
}

func deepHierarchy(outside bool) [][]byte {
	var keys [][]byte
	for a := range 3 {
		for b := range 3 {
			for c := range 4 {
				keys = append(keys, []byte(fmt.Sprintf("d/a%d/b%d/c%d.txt", a, b, c)))
			}
		}
	}
	for n := range 6 {
		keys = append(keys, []byte(fmt.Sprintf("d/top-%d.txt", n)))
		if outside {
			keys = append(keys, []byte(fmt.Sprintf("e-%d", n)))
		}
	}
	return keys
}

func TestDeepDelimitedHierarchyIsByteIdentical(t *testing.T) {
	sorted, oracle := sortedPair(t, deepHierarchy(true), 1000, 7)
	var scenarios []scenario
	scenarios = append(scenarios, projectionMatrix(nil, []byte("/"), nil, 1, 2, 3, 7)...)
	scenarios = append(scenarios, projectionMatrix([]byte("d/"), []byte("/"), nil, 1, 2, 1000)...)
	scenarios = append(scenarios, projectionMatrix([]byte("d/a1/"), []byte("/"), nil, 1, 3)...)
	walkAll(t, sorted, oracle, scenarios)
}

func TestStartAfterInsideAndAtACommonPrefixBoundaryWalksIdenticallyWithTokens(t *testing.T) {
	sorted, oracle := sortedPair(t, deepHierarchy(false), 1000, 7)
	var scenarios []scenario
	scenarios = append(scenarios, projectionMatrix([]byte("d/"), []byte("/"), []byte("d/a1/b1/c2.txt"), 1, 2, 3, 1000)...)
	scenarios = append(scenarios, projectionMatrix([]byte("d/"), []byte("/"), []byte("d/a1/"), 1, 2, 3, 1000)...)
	walkAll(t, sorted, oracle, scenarios)
}

// resume takes a real token from a truncated two-key page and resends it
// with extra appended.
func resume(t *testing.T, s *Server, extra string) string {
	t.Helper()
	m := nextTokenRe.FindStringSubmatch(body(t, s, "list-type=2&max-keys=2"))
	if m == nil {
		t.Fatal("the first page carried no token")
	}
	return body(t, s, "list-type=2&max-keys=1000&continuation-token="+url.QueryEscape(m[1])+extra)
}

func TestContinuationTokenWinsOverConflictingStartAfterOnHTTP(t *testing.T) {
	sorted, oracle := sortedPair(t, strs("a/1", "a/2", "a/3", "a/4"), 1000, 7)
	for _, conflicting := range []string{"a/1", "a/3"} {
		extra := "&start-after=" + s3.PercentEncode([]byte(conflicting))
		got := resume(t, sorted, extra)
		if got != resume(t, oracle, extra) {
			t.Errorf("start-after=%s: the stores differ", conflicting)
		}
		if !strings.Contains(got, "<Key>a/3</Key>") || !strings.Contains(got, "<Key>a/4</Key>") || strings.Contains(got, "<Key>a/1</Key>") ||
			strings.Contains(got, "<Key>a/2</Key>") || strings.Contains(got, "<StartAfter>") {
			t.Errorf("start-after=%s moved the token's boundary: %s", conflicting, got)
		}
	}
}

func TestBlankContinuationTokenLeavesStartAfterAsTheBoundaryOnHTTP(t *testing.T) {
	sorted, oracle := sortedPair(t, strs("a/1", "a/2", "a/3", "a/4"), 1000, 7)
	q := "list-type=2&max-keys=1000&continuation-token=&start-after=a%2F1"
	got := body(t, sorted, q)
	if got != body(t, oracle, q) {
		t.Error("the stores differ")
	}
	for _, want := range []string{"<Key>a/2</Key>", "<Key>a/3</Key>", "<Key>a/4</Key>", "<StartAfter>a/1</StartAfter>"} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %s: %s", want, got)
		}
	}
	if strings.Contains(got, "<Key>a/1</Key>") || strings.Contains(got, "<ContinuationToken>") {
		t.Errorf("%s", got)
	}
}

func TestGiantSinglePrefixSpanningManyRowGroupsIsByteIdentical(t *testing.T) {
	keys := append(utf8Keys("big/%04d", 220), strs("aaa", "aab", "zzy", "zzz")...)
	sorted, oracle := sortedPair(t, keys, 1000, 7)
	var scenarios []scenario
	scenarios = append(scenarios, projectionMatrix(nil, []byte("/"), nil, 1, 2, 3)...)
	scenarios = append(scenarios, projectionMatrix([]byte("big/"), nil, nil, 1, 2, 3, 7, 37)...)
	scenarios = append(scenarios, projectionMatrix([]byte("big/"), nil, []byte("big/0100"), 5)...)
	walkAll(t, sorted, oracle, scenarios)
}

func TestSuccessorOfPrefixIsARealEmittedKey(t *testing.T) {
	keys := strs("album/1", "album/2", "photos0", "photos00", "photos1", "zeta")
	for i := 1; i <= 9; i++ {
		keys = append(keys, []byte(fmt.Sprintf("photos/%d", i)))
	}
	sorted, oracle := sortedPair(t, keys, 1000, 7)
	var scenarios []scenario
	scenarios = append(scenarios, projectionMatrix(nil, []byte("/"), nil, 1, 2, 3, 1000)...)
	scenarios = append(scenarios, projectionMatrix(nil, nil, []byte("photos/9"), 1, 2)...)
	walkAll(t, sorted, oracle, scenarios)
}

func TestHighScalarKeysAreByteIdentical(t *testing.T) {
	var keys [][]byte
	for i := range 12 {
		keys = append(keys, []byte{'k', byte(i)}, []byte{'k', byte(i), 0xF4, 0x8F, 0xBF, 0xBF},
			[]byte{'k', byte(i), 0xF4, 0x8F, 0xBF, 0xBF, 0x01})
	}
	keys = append(keys, []byte("\U0010FFFF"), []byte("\U0010FFFF\U0010FFFF"))
	sorted, oracle := sortedPair(t, keys, 1000, 7)
	walkAll(t, sorted, oracle, []scenario{
		{nil, nil, nil, 1, true, false}, {nil, nil, nil, 3, true, true}, {nil, nil, nil, 1000, true, true},
		{[]byte("k"), nil, nil, 2, true, false},
	})
}

func TestWideNoPrefixRollupWithRowGroupSpanningPrefixesAndTrailingPlainKeysIsByteIdentical(t *testing.T) {
	var keys [][]byte
	for p := range 12 {
		for i := range 25 {
			keys = append(keys, []byte(fmt.Sprintf("wide%02d/obj-%03d", p, i)))
		}
	}
	keys = append(keys, utf8Keys("zzz-plain-%d", 4)...)
	sorted, oracle := sortedPair(t, keys, 1000, 7)
	walkAll(t, sorted, oracle, projectionMatrix(nil, []byte("/"), nil, 1, 2, 3, 5))
	// The sorted store's rollup served the no-prefix requests: a silent
	// fallback to the range walk would match byte for byte too.
	if n := count(sorted, metrics.DelimiterPath, "rollup"); n <= 0 {
		t.Errorf("rollup served %d requests", n)
	}
}

func TestMultiByteDelimiterIsByteIdentical(t *testing.T) {
	sorted, oracle := sortedPair(t, strs("a--1", "a--2", "a--3", "b--1", "b--2", "plain-1", "plain-2", "z"), 1000, 7)
	walkAll(t, sorted, oracle, projectionMatrix(nil, []byte("--"), nil, 1, 2, 1000))
}

func TestRolledMultiFileFixtureServesIdentically(t *testing.T) {
	sorted, oracle := sortedPair(t, utf8Keys("shard/%05d/obj", 200), 37, 5)
	var scenarios []scenario
	scenarios = append(scenarios, projectionMatrix(nil, nil, nil, 1, 3, 7, 1000)...)
	scenarios = append(scenarios, projectionMatrix(nil, []byte("/"), nil, 2, 3)...)
	scenarios = append(scenarios, projectionMatrix([]byte("shard/00050/"), nil, nil, 1, 2)...)
	walkAll(t, sorted, oracle, scenarios)
}
