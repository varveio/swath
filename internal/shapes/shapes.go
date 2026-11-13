// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package shapes generates the bucket shapes the range engine is tested
// and measured on: deterministic key lists (the same keys for the same
// shape and scale, on every machine) that are hard for the engine in one
// way each, and runs one listing of a shape through the in-process fake
// (package scalefake) with the fake's service behaviours (Run). The
// default-tier tests list every shape at a small scale; the nightly tier
// (bench/shapes) lists them at full scale and compares the numbers with a
// committed baseline. Test and bench support only: nothing in the tool
// imports it (TestImportBoundary).
package shapes

import (
	"crypto/md5"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"slices"
	"strings"
)

// Full is the scale the shapes were designed at: a shape of scale n has
// about n keys (the tiny shapes keep their size at every scale).
const Full = 2_000_000

// Shape is one generator.
type Shape struct {
	Name string
	What string // what makes it hard
	// gen returns the keys at scale n, in any order, possibly repeated.
	gen func(n int, r *rand.Rand) []string
}

// Keys is the shape's key list at scale n: sorted as bytes, without
// repeats, each key at most 1,024 bytes and without a newline. The same
// name and n give the same keys.
func (s Shape) Keys(n int) []string {
	h := fnv.New64a()
	h.Write([]byte(s.Name))
	keys := s.gen(n, rand.New(rand.NewPCG(h.Sum64(), uint64(n))))
	slices.Sort(keys)
	keys = slices.Compact(keys)
	for _, k := range keys {
		if len(k) > 1024 || len(k) == 0 || strings.ContainsRune(k, '\n') {
			panic(fmt.Sprintf("shapes: %s makes key %q", s.Name, k))
		}
	}
	return keys
}

// Lookup is the shape named name.
func Lookup(name string) (Shape, bool) {
	for _, s := range All {
		if s.Name == name {
			return s, true
		}
	}
	return Shape{}, false
}

// part is n scaled by f, at least 1.
func part(n int, f float64) int { return max(1, int(float64(n)*f)) }

func be8(i int) []byte { return binary.BigEndian.AppendUint64(nil, uint64(i)) }

func sha1hex(i int) string { s := sha1.Sum(be8(i)); return hex.EncodeToString(s[:]) }

func md5hex(i int) string { s := md5.Sum(be8(i)); return hex.EncodeToString(s[:]) }

func each(n int, f func(i int) string) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = f(i)
	}
	return out
}

// All is every shape, the reference first.
var All = []Shape{
	{"baseline", "the reference: 200 folders of equal size", func(n int, _ *rand.Rand) []string {
		per := part(n, 1.0/200)
		return each(200*per, func(i int) string { return fmt.Sprintf("d/%03d/file-%05d", i/per, i%per) })
	}},
	{"counter", "one long shared prefix, a zero-padded counter at the end", func(n int, _ *rand.Rand) []string {
		return each(n, func(i int) string {
			return fmt.Sprintf("tenant-0042/ingest/2026/stream/partition-0000/segment/obj-%09d.bin", i)
		})
	}},
	{"counter_unpadded", "an unpadded counter: lexicographic density is lumpy", func(n int, _ *rand.Rand) []string {
		return each(n, func(i int) string { return fmt.Sprintf("logs/evt-%d", i+1) })
	}},
	{"hex_flat", "uniform random hex, no folders", func(n int, _ *rand.Rand) []string {
		return each(n, sha1hex)
	}},
	{"hash_under_folders", "16 folders of uniform hashes: folders give little", func(n int, _ *rand.Rand) []string {
		return each(n, func(i int) string { h := md5hex(i); return "cas/" + h[:1] + "/" + h })
	}},
	{"zipf_folders", "1,000 folders: the first holds 90 %, the rest Zipf", func(n int, _ *rand.Rand) []string {
		big := part(n, 0.9)
		out := each(big, func(j int) string { return fmt.Sprintf("data/f0000/part-%08d", j) })
		rest, s := n-big, 0.0
		for k := 1; k < 1000; k++ {
			s += 1 / float64(k)
		}
		for k := 1; k < 1000; k++ {
			for j := range max(1, int(float64(rest)/float64(k)/s)) {
				out = append(out, fmt.Sprintf("data/f%04d/part-%08d", k, j))
			}
		}
		return out
	}},
	{"onekey_folders", "a folder per key", func(n int, _ *rand.Rand) []string {
		return each(part(n, 0.5), func(i int) string { return "users/" + md5hex(i)[:12] + "/profile.json" })
	}},
	{"highbytes", "multi-byte scalars everywhere (CJK, accents, emoji)", func(n int, r *rand.Rand) []string {
		alpha := []string{"日", "本", "語", "é", "ñ", "ü", "Ω", "ж", "한", "🎉", "😀", "a", "z", "-", "_"}
		pick := func(k int) string {
			var b strings.Builder
			for range k {
				b.WriteString(alpha[r.IntN(len(alpha))])
			}
			return b.String()
		}
		want := part(n, 0.75)
		seen := make(map[string]bool, want)
		for len(seen) < want {
			seen["文書/"+pick(2)+"/"+pick(8)+".txt"] = true
		}
		out := make([]string, 0, want)
		for k := range seen {
			out = append(out, k)
		}
		return out
	}},
	{"maxlen", "1,024-byte keys: a 996-byte shared prefix and a counter", func(n int, _ *rand.Rand) []string {
		p := "deep/" + strings.Repeat("x", 990) + "/"
		w := 1024 - len(p)
		return each(part(n, 0.2), func(i int) string { s := fmt.Sprintf("%0*d", w, i); return p + s[len(s)-w:] })
	}},
	{"punct_siblings", "siblings that sort around '/': tree order is not key order", func(n int, _ *rand.Rand) []string {
		per := part(n, 1.0/1600)
		var out []string
		for k := range 200 {
			base := fmt.Sprintf("proj/name%03d", k)
			for _, suf := range []string{"", "!", " copy", "-old", ".bak", "/", "_v2", "~"} {
				for j := range per {
					out = append(out, fmt.Sprintf("%s%s/f%05d", base, suf, j))
				}
			}
		}
		return out
	}},
	{"chain50", "a 50-level single-child chain, every key at the bottom", func(n int, _ *rand.Rand) []string {
		var p strings.Builder
		for i := range 50 {
			fmt.Fprintf(&p, "lvl%02d/", i)
		}
		return each(n, func(i int) string { return fmt.Sprintf("%sk%08d", p.String(), i) })
	}},
	{"date_hot", "year/month/day/hour; the newest day holds 99 %", func(n int, _ *rand.Rand) []string {
		var out []string
		for _, y := range []int{2023, 2024, 2025} {
			for m := 1; m <= 12; m++ {
				for d := 1; d <= 28; d++ {
					for h := range 2 {
						out = append(out, fmt.Sprintf("events/year=%d/month=%02d/day=%02d/hour=%02d/e-0.json", y, m, d, h))
					}
				}
			}
		}
		per := part(n, 0.99/24)
		for h := range 24 {
			for j := range per {
				out = append(out, fmt.Sprintf("events/year=2026/month=11/day=07/hour=%02d/e-%06d.json", h, j))
			}
		}
		return out
	}},
	{"sparse_then_dense", "10 keys over the top level, every other key in one folder", func(n int, _ *rand.Rand) []string {
		out := each(10, func(i int) string { return string(rune('a'+i)) + "/readme.txt" })
		return append(out, each(n, func(i int) string { return fmt.Sprintf("m/blob/%08d", i) })...)
	}},
	{"folder_objects", "folder-marker objects ('x/') at every level", func(n int, _ *rand.Rand) []string {
		var out []string
		for a := range part(n, 1.0/2000) {
			out = append(out, fmt.Sprintf("site/%04d/", a))
			for b := range 20 {
				out = append(out, fmt.Sprintf("site/%04d/%02d/", a, b))
				for c := range 50 {
					out = append(out, fmt.Sprintf("site/%04d/%02d/p%02d", a, b, c))
				}
			}
		}
		return out
	}},
	{"trailing_variants", "keys that differ only in trailing bytes", func(n int, _ *rand.Rand) []string {
		var out []string
		for i := range part(n, 0.1) {
			for _, s := range []string{"", " ", " (1)", "-", ".1", ".2", ".orig", "~", "_", "!"} {
				out = append(out, fmt.Sprintf("img/IMG_%07d.jpg%s", i, s))
			}
		}
		return out
	}},
	{"percent_keys", "'%' in every key (URL-encoded names)", func(n int, _ *rand.Rand) []string {
		return each(n, func(i int) string {
			return fmt.Sprintf("web/https%%3A%%2F%%2Fexample.com%%2Fp%%2F%08d%%3Fq%%3D%d", i, i%97)
		})
	}},
	{"tiny100", "100 keys", func(int, *rand.Rand) []string {
		return each(100, func(i int) string { return fmt.Sprintf("a/b/%03d", i) })
	}},
	{"tiny1k", "1,000 keys in 10 folders", func(int, *rand.Rand) []string {
		return each(1000, func(i int) string { return fmt.Sprintf("a/%d/%04d", i/100, i) })
	}},
	{"small10k", "10,000 keys in 10 folders", func(int, *rand.Rand) []string {
		return each(10_000, func(i int) string { return fmt.Sprintf("a/%d/%05d", i/1000, i) })
	}},
	{"wide_top", "a top level of n/10 folders: the seed truncates", func(n int, _ *rand.Rand) []string {
		return each(part(n, 0.1)*10, func(i int) string { return fmt.Sprintf("%06d/%d", i/10, i%10) })
	}},
	{"density_cliff", "one flat folder: a dense half, then a sparse random tail", func(n int, r *rand.Rand) []string {
		half := part(n, 0.5)
		out := each(half, func(i int) string { return fmt.Sprintf("k/%010d", i) })
		for range half {
			out = append(out, fmt.Sprintf("k/%010d", 1_000_000_000+r.Int64N(9_000_000_000)))
		}
		return out
	}},
}

// Tiny reports whether a shape keeps its size at every scale: too small
// for the per-key bounds, which assume many pages per worker.
func Tiny(name string) bool { return name == "tiny100" || name == "tiny1k" || name == "small10k" }
