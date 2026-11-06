// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package keyrangetest holds the range engine's test fixtures: a fake
// listing API over a sorted key slice and the keyspaces the engine and its
// providers are tested on.
package keyrangetest

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// ListInput is one request: Prefix, Delimiter, StartAfter (exclusive),
// ContinuationToken, MaxKeys.
type ListInput struct {
	Prefix, Delimiter, StartAfter, ContinuationToken string
	MaxKeys                                          int
}

// Object is one listed key.
type Object struct {
	Key  string
	Size int64
}

// Page is one answer.
type Page struct {
	Contents              []Object
	CommonPrefixes        []string
	IsTruncated           bool
	HasContinuation       bool
	NextContinuationToken string
}

// Sorted is a fake listing API (S3-shaped: start-after, continuation token) over a sorted key slice: Prefix,
// StartAfter (400 for scalars real S3 rejects), ContinuationToken,
// Delimiter, MaxKeys; optional short truncated pages and latency.
type Sorted struct {
	Keys    []string
	Short   bool
	PageCap int // if set, no page holds more than this many entries
	Latency time.Duration
	// DelimLatency, if set, is a delimiter request's latency instead of
	// Latency: a small delimiter page (a structure probe's 32 entries)
	// answers faster than a full page of keys.
	DelimLatency time.Duration
	mu           sync.Mutex
	rng          *rand.Rand
	Calls        int
}

func NewSorted(keys []string, short bool, seed int64) *Sorted {
	k := slices.Clone(keys)
	slices.Sort(k)
	return &Sorted{Keys: slices.Compact(k), Short: short, rng: rand.New(rand.NewSource(seed))}
}

var ErrBadStartAfter = errors.New("400 InvalidArgument: start-after")

func (f *Sorted) List(ctx context.Context, in *ListInput) (*Page, error) {
	lat := f.Latency
	if in.Delimiter != "" && f.DelimLatency > 0 {
		lat = f.DelimLatency
	}
	if lat > 0 {
		select {
		case <-time.After(lat):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	f.mu.Lock()
	f.Calls++
	size := in.MaxKeys
	if f.Short {
		size = 1 + f.rng.Intn(size)
	}
	if f.PageCap > 0 {
		size = min(size, f.PageCap)
	}
	f.mu.Unlock()
	prefix, delim := in.Prefix, in.Delimiter
	i := sort.SearchStrings(f.Keys, prefix)
	if tok := in.ContinuationToken; tok != "" {
		i, _ = strconv.Atoi(tok)
	} else if sa := in.StartAfter; sa != "" {
		for _, r := range sa {
			if refused(r) {
				return nil, ErrBadStartAfter
			}
		}
		i = max(i, sort.Search(len(f.Keys), func(j int) bool { return f.Keys[j] > sa }))
	}
	out := &Page{}
	n := 0
	for ; i < len(f.Keys) && strings.HasPrefix(f.Keys[i], prefix); i++ {
		if n == size {
			out.IsTruncated, out.NextContinuationToken, out.HasContinuation = true, strconv.Itoa(i), true
			break
		}
		k := f.Keys[i]
		if delim != "" {
			if j := strings.Index(k[len(prefix):], delim); j >= 0 {
				cp := k[:len(prefix)+j+len(delim)]
				if in.ContinuationToken == "" && in.StartAfter != "" && cp <= in.StartAfter {
					// S3 returns no common prefix at or below StartAfter
					// (measured: "a/" is not listed for start-after "a/"
					// or "a/b"), so the keys it covers are skipped.
					continue
				}
				if m := len(out.CommonPrefixes); m == 0 || out.CommonPrefixes[m-1] != cp {
					out.CommonPrefixes = append(out.CommonPrefixes, cp)
					n++
				}
				continue
			}
		}
		out.Contents = append(out.Contents, Object{Key: k, Size: int64(len(k))})
		n++
	}
	return out, nil
}

// Keyspaces after Swath's testkit Keyspaces, plus markers and keys with '%'
// and control characters.
func Keyspaces() map[string][]string {
	rng := rand.New(rand.NewSource(7))
	gen := func(n int, f func(i int) string) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = f(i)
		}
		return out
	}
	alnum := "abcdefghijklmnopqrstuvwxyz0123456789"
	randStr := func(n int, alphabet string) string {
		b := make([]byte, n)
		for i := range b {
			b[i] = alphabet[rng.Intn(len(alphabet))]
		}
		return string(b)
	}
	ks := map[string][]string{
		"empty":            nil,
		"oneKey":           {"a"},
		"exactly3000":      gen(3000, func(i int) string { return fmt.Sprintf("k%06d", i) }),
		"flatRandom":       gen(4000, func(int) string { return randStr(1+rng.Intn(16), alnum) }),
		"singlePrefixFlat": gen(4000, func(i int) string { return fmt.Sprintf("p/%07d", i) }),
		"flatUpperHex":     gen(4000, func(int) string { return randStr(8, "0123456789ABCDEF") }),
		"longKeys1024":     gen(1500, func(i int) string { return strings.Repeat("L", 1018) + fmt.Sprintf("%06d", i) }),
		"lastByteDiffer":   gen(2000, func(i int) string { return "same/long/prefix/" + string(rune(0x21+i%90)) + string(rune(0x21+i/90)) }),
		"maxScalarRun":     gen(300, func(i int) string { return strings.Repeat("\U0010FFFF", 1+i%40) + strconv.Itoa(i) }),
		"supplementaryPlane": gen(3000, func(int) string {
			return string(rune(0x10000+rng.Intn(0xFFFFF-0x10000))) + string(rune(0x1F600+rng.Intn(80))) + randStr(3, alnum)
		}),
		"markersAndOddChars": {"a/", "a/x", "a/y/", "a/y/z", "b", "c%20d", "c%", "d\x01e", "d\x7f", "e/\x01/f", "f/%", "zz/"},
	}
	var deep []string
	for a := range 5 {
		for b := range 5 {
			for c := range 5 {
				for d := range 5 {
					for f := range 8 {
						deep = append(deep, fmt.Sprintf("d%d/e%d/f%d/g%d/file%d", a, b, c, d, f))
					}
				}
			}
		}
	}
	ks["deepTree"] = deep
	skew := gen(4000, func(i int) string { return fmt.Sprintf("big/%06d", i) })
	skew = append(skew, gen(50, func(i int) string { return fmt.Sprintf("small%02d/x", i) })...)
	ks["badlySkewed"] = skew
	needle := gen(4000, func(i int) string { return fmt.Sprintf("h/needle/%05d", i) })
	needle = append(needle, "a", "m/x", "z/y")
	ks["hotNeedle"] = needle
	for k, v := range ks {
		for _, s := range v {
			if !utf8.ValidString(s) {
				panic(k)
			}
		}
	}
	return ks
}

// Hostile are adversarial keyspaces on the fake bucket.
func Hostile() map[string][]string {
	ks := map[string][]string{}
	// The cardinality bomb: one prefix with an enormous fan-out of keys that
	// differ only in their last bytes, after a long shared run.
	var bomb []string
	for i := range 6000 {
		bomb = append(bomb, "bomb/"+strings.Repeat("a", 200)+fmt.Sprintf("%04d", i))
	}
	bomb = append(bomb, "a", "bomb", "bomb0", "bomb/", "zz")
	ks["cardinalityBomb"] = bomb
	// The encoding minefield: every awkward byte sequence a key may hold.
	mine := []string{"%", "%%", "%2F", "%2f", "a%2Fb", "%25", "+", "a+b", " ", "a b", "  ", "a ", " a", "?", "#", "&", "=",
		"a?b=c&d", "\\", "\"", "'", "<", ">", "<a>&amp;", "/", "//", "a//b", "/a", "a/", "~", "\x7f", "\u00a0", "\u2028",
		"\ufeff", "\ufffd", "\U0001F600", "\U0010FFFF", "e\u0301", "\u00e9", "\u03a9", "\u65e5\u672c\u8a9e/\u30ad\u30fc"}
	for c := 1; c < 0x20; c++ {
		mine = append(mine, "ctl"+string(rune(c))+"x", string(rune(c)))
	}
	for _, b := range []string{"a", "z", "~", "/", "%", " ", "\x7f", "\u00ff", "\U0010FFFF"} {
		for _, n := range []int{1, 2, 63, 64, 255, 256, 511} {
			mine = append(mine, strings.Repeat(b, n))
		}
	}
	for i := range 2000 { // enough keys to split among the mines
		mine = append(mine, fmt.Sprintf("m/%%%02X/%04d", i%256, i))
	}
	ks["encodingMinefield"] = mine
	return ks
}

// TestHostileFixturesMatchSequential: both hostile keyspaces list
// row-identical to the sequential oracle, parallel and seeded.

// refused: scalars the fake refuses in start-after, as S3 does: C0, DEL,
// C1, non-characters and surrogates.
func refused(r rune) bool {
	cp := int(r)
	switch {
	case cp < 0x20, cp == 0x7F, cp >= 0x80 && cp <= 0x9F, cp >= 0xFDD0 && cp <= 0xFDEF, cp&0xFFFE == 0xFFFE:
		return true
	}
	return cp >= 0xD800 && cp < 0xE000
}
