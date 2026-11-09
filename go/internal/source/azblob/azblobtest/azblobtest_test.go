// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblobtest

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"testing"
	"unicode"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/varveio/swath/go/internal/replay/azure"
)

// validName is the replay's rule for a name it lists unencoded, which is
// Azure's: valid UTF-8, no control character, at most 1,024 UTF-16 code
// units, and no trailing dot (Azure trims it).
func validName(name string) error {
	switch {
	case name == "" || !utf8.ValidString(name):
		return fmt.Errorf("empty or not UTF-8")
	case strings.HasSuffix(name, "."):
		return fmt.Errorf("trailing dot")
	case len(utf16.Encode([]rune(name))) > 1024:
		return fmt.Errorf("over 1,024 UTF-16 code units")
	case strings.ContainsFunc(name, func(r rune) bool { return unicode.IsControl(r) || r == 0xfffe || r == 0xffff }):
		return fmt.Errorf("control character or noncharacter")
	}
	return nil
}

func TestKeysAreAValidHostileCorpus(t *testing.T) {
	keys := Keys()
	if !slices.IsSorted(keys) || len(slices.Compact(slices.Clone(keys))) != len(keys) {
		t.Fatal("not byte-sorted and unique")
	}
	if len(keys) <= 5000 || len(keys) > 6000 {
		t.Fatalf("%d names", len(keys))
	}
	if !slices.Equal(Keys(), keys) {
		t.Fatal("not deterministic")
	}
	lengths := map[int]int{}
	for _, k := range keys {
		if err := validName(k); err != nil {
			t.Errorf("%.40q: %v", k, err)
		}
		lengths[len(k)]++
	}
	if lengths[1024] != 1 || lengths[2010] != 1 || lengths[MaxNameBytes] != 2 {
		t.Errorf("long names: %d of 1,024 bytes, %d of 2,010, %d of 3,072", lengths[1024], lengths[2010], lengths[MaxNameBytes])
	}
	for _, want := range []string{"a/", "a/b", "dir/file.txt", "é", "", "ｚ", "�", "\U00010000", "\U0001f600",
		"only-s/\U0001f600", "only-e/ｚ", "r\U00010000\U00010000", "r\U00010000\U00010000\U00010000"} {
		if _, ok := slices.BinarySearch(keys, want); !ok {
			t.Errorf("lacks %q", want)
		}
	}
	// A sibling pair under the same parent at the root, one level down and
	// three deep: the two orders disagree on each.
	for _, parent := range []string{"", "d/", "n1/n2/n3/"} {
		e, s := parent+"p", parent+"p\U00010000"
		_, okE := slices.BinarySearch(keys, e)
		_, okS := slices.BinarySearch(keys, s)
		if !okE || !okS || e > s || azure.Compare([]byte(s), []byte(e)) >= 0 {
			t.Errorf("pair under %q", parent)
		}
	}
	// The only-child parents hold one name each.
	for _, p := range []string{"only-s/", "only-e/"} {
		n := 0
		for _, k := range keys {
			if strings.HasPrefix(k, p) {
				n++
			}
		}
		if n != 1 {
			t.Errorf("%s holds %d names", p, n)
		}
	}
}

// enumeration is the part of a List Blobs page the tests read.
type enumeration struct {
	Names      []string `xml:"Blobs>Blob>Name"`
	NextMarker string   `xml:"NextMarker"`
}

func get(t *testing.T, target string, header ...string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest("GET", target, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i+1 < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, body
}

// walkAzure lists the container flat with maxresults pageSize, following
// NextMarker, and returns the names in the order served.
func walkAzure(t *testing.T, r *Replay, pageSize int) []string {
	t.Helper()
	var names []string
	marker := ""
	for {
		target := fmt.Sprintf("%s/%s?restype=container&comp=list&maxresults=%d", r.Endpoint, r.Container, pageSize)
		if marker != "" {
			target += "&marker=" + url.QueryEscape(marker)
		}
		resp, body := get(t, target, "x-ms-version", azure.Version20261006)
		if resp.StatusCode != 200 {
			t.Fatalf("%d %s", resp.StatusCode, body)
		}
		var page enumeration
		if err := xml.Unmarshal(bytes.TrimPrefix(body, []byte("\xef\xbb\xbf")), &page); err != nil {
			t.Fatal(err)
		}
		if len(page.Names) > pageSize {
			t.Fatalf("a page of %d at maxresults=%d", len(page.Names), pageSize)
		}
		names = append(names, page.Names...)
		if marker = page.NextMarker; marker == "" {
			return names
		}
	}
}

// TestServeListsTheCorpusInProviderOrder walks the corpus through the
// Azure route at page sizes 1, 2, 7 and 5,000, each walk the corpus in the
// replay's provider order, and through the S3 route in byte order.
func TestServeListsTheCorpusInProviderOrder(t *testing.T) {
	keys := Keys()
	r := Serve(t, keys)
	if r.Account != "replay" || r.Container != Container || !strings.HasSuffix(r.Endpoint, "/replay") {
		t.Fatalf("%+v", r)
	}
	want := slices.Clone(keys)
	slices.SortFunc(want, func(a, b string) int { return azure.Compare([]byte(a), []byte(b)) })
	if slices.Equal(want, keys) {
		t.Fatal("the corpus does not exercise the provider order")
	}
	for _, size := range []int{1, 2, 7, 5000} {
		if size == 1 && testing.Short() {
			continue
		}
		if got := walkAzure(t, r, size); !slices.Equal(got, want) {
			t.Errorf("maxresults=%d: %d names, not the provider order", size, len(got))
		}
	}

	var s3keys []string
	token := ""
	for {
		target := r.S3Endpoint + "/" + r.Container + "?list-type=2&max-keys=1000"
		if token != "" {
			target += "&continuation-token=" + url.QueryEscape(token)
		}
		resp, body := get(t, target)
		if resp.StatusCode != 200 {
			t.Fatalf("S3 %d %s", resp.StatusCode, body)
		}
		var page struct {
			Keys  []string `xml:"Contents>Key"`
			Token string   `xml:"NextContinuationToken"`
		}
		if err := xml.Unmarshal(body, &page); err != nil {
			t.Fatal(err)
		}
		s3keys = append(s3keys, page.Keys...)
		if token = page.Token; token == "" {
			break
		}
	}
	if !slices.Equal(s3keys, keys) {
		t.Errorf("S3 route: %d keys, not the byte order", len(s3keys))
	}

	r.Stop()
	r.Stop()
}

// TestServeAccountInformation reads Get Account Information flat and, with
// HNS, hierarchical.
func TestServeAccountInformation(t *testing.T) {
	for _, hns := range []bool{false, true} {
		var opts []Option
		if hns {
			opts = append(opts, HNS())
		}
		r := Serve(t, []string{"a", "b/c"}, opts...)
		resp, _ := get(t, r.Endpoint+"/"+r.Container+"?restype=account&comp=properties", "x-ms-version", azure.Version20260606)
		if resp.StatusCode != 200 || resp.Header.Get("x-ms-is-hns-enabled") != fmt.Sprint(hns) {
			t.Errorf("hns=%v: %d %v", hns, resp.StatusCode, resp.Header)
		}
	}
}
