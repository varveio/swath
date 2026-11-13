// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package order_test

import (
	"bufio"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/internal/source/gcs"
	"github.com/varveio/swath/internal/source/localfs"
	"github.com/varveio/swath/internal/source/s3"
)

// sortedKeys is n distinct keys in byte order, built to stress prefix
// coding: arbitrary bytes (0x00, 0xff, invalid UTF-8), the empty key,
// keys that are a prefix of the next and long shared prefixes. Each frame
// starts over, so a frame's first key shares nothing it can see.
func sortedKeys(r *rand.Rand, n int) []string {
	set := map[string]bool{}
	alpha := []byte{0x00, 0x01, '/', '0', 'a', 'z', 0x7f, 0x80, 0xc3, 0xff}
	var prev string
	for len(set) < n {
		var k string
		switch r.IntN(6) {
		case 0: // the previous key extended: prev is a prefix of k
			k = prev + string(alpha[r.IntN(len(alpha))])
		case 1: // a truncation: k is a prefix of prev
			k = prev[:r.IntN(len(prev)+1)]
		case 2: // a long shared prefix
			k = strings.Repeat("deep/", 1+r.IntN(150)) + fmt.Sprint(r.IntN(1000))
		default:
			b := make([]byte, r.IntN(40))
			for i := range b {
				b[i] = alpha[r.IntN(len(alpha))]
			}
			k = string(b)
		}
		if len(k) > 1024 {
			k = k[:1024]
		}
		set[k], prev = true, k
	}
	keys := make([]string, 0, n)
	for k := range set {
		keys = append(keys, k)
	}
	slicesSort(keys)
	return keys
}

func slicesSort(s []string) { slices.Sort(s) }

// pick returns one of vs, or a random string now and then.
func pick(r *rand.Rand, vs ...string) string {
	if r.IntN(10) == 0 {
		b := make([]byte, r.IntN(70))
		for i := range b {
			b[i] = byte(r.IntN(256))
		}
		return string(b)
	}
	return vs[r.IntN(len(vs))]
}

func hexOf(r *rand.Rand, digits string) string {
	b := make([]byte, 32)
	for i := range b {
		b[i] = digits[r.IntN(len(digits))]
	}
	return string(b)
}

// times stress the zigzag difference: both signs, the int64 extremes
// (whose difference overflows) and clustered neighbours.
func timeOf(r *rand.Rand, prev int64) int64 {
	switch r.IntN(6) {
	case 0:
		return math.MinInt64
	case 1:
		return math.MaxInt64
	case 2:
		return -r.Int64()
	case 3:
		return r.Int64()
	}
	return prev + r.Int64N(2_000_001) - 1_000_000
}

func s3Object(r *rand.Rand, prev *s3.Object) s3.Object {
	const lower, upper = "0123456789abcdef", "0123456789ABCDEF"
	etags := []string{"", hexOf(r, lower), hexOf(r, lower) + "-7", hexOf(r, upper), "\"" + hexOf(r, lower) + "\"", hexOf(r, lower)[:31], prev.ETag}
	sizes := []int64{0, -1, math.MaxInt64, math.MinInt64, r.Int64N(1 << 40), prev.Size}
	return s3.Object{
		Size: sizes[r.IntN(len(sizes))], Mtime: timeOf(r, prev.Mtime), ETag: etags[r.IntN(len(etags))],
		StorageClass:      pick(r, "", "STANDARD", "GLACIER", prev.StorageClass),
		OwnerID:           pick(r, "", strings.Repeat("ab", 32), prev.OwnerID),
		OwnerDisplayName:  pick(r, "", "owner", prev.OwnerDisplayName),
		ChecksumAlgorithm: pick(r, "", "CRC64NVME", prev.ChecksumAlgorithm),
		ChecksumType:      pick(r, "", "FULL_OBJECT", "COMPOSITE", prev.ChecksumType),
	}
}

func gcsObject(r *rand.Rand, prev *gcs.Object) gcs.Object {
	return gcs.Object{Size: r.Int64(), Mtime: timeOf(r, prev.Mtime), ETag: pick(r, "", "CJrtlv3", prev.ETag),
		Generation: r.Int64(), Metageneration: r.Int64N(3), StorageClass: pick(r, "", "STANDARD"),
		MD5: pick(r, "", "1B2M2Y8AsgTpgAmY7PhCfg=="), CRC32C: pick(r, "", "AAAAAA=="), ContentType: pick(r, "", "text/plain")}
}

func fsEntry(r *rand.Rand, _ *localfs.Entry) localfs.Entry {
	e := localfs.Entry{Present: localfs.Presence(r.IntN(1 << 13)), Kind: localfs.Kind(r.IntN(9))}
	for p, v := range map[localfs.Presence]*int64{localfs.HasSize: &e.Size, localfs.HasMtime: &e.Mtime, localfs.HasCtime: &e.Ctime,
		localfs.HasAtime: &e.Atime, localfs.HasBtime: &e.Btime, localfs.HasInode: &e.Inode, localfs.HasDev: &e.Dev, localfs.HasBlocks: &e.Blocks} {
		if e.Has(p) {
			*v = timeOf(r, 0)
		}
	}
	for p, v := range map[localfs.Presence]*uint32{localfs.HasMode: &e.Mode, localfs.HasUID: &e.UID, localfs.HasGID: &e.GID, localfs.HasNlink: &e.Nlink} {
		if e.Has(p) {
			*v = r.Uint32()
		}
	}
	if e.Has(localfs.HasLink) {
		e.LinkTarget = pick(r, "", "../target")
	}
	return e
}

// roundTrip spills random sorted rows of one producer in every frame
// format and checks every row comes back exactly, frame by frame, after
// the reader's buffers were overwritten (no row aliases them).
func roundTrip[P any](t *testing.T, s *schema.Schema[P], gen func(*rand.Rand, *P) P) {
	for seed := range uint64(40) {
		r := rand.New(rand.NewPCG(seed, 7))
		keys := sortedKeys(r, 1+r.IntN(600))
		var frames [][]schema.Row[P]
		var prev P
		for len(keys) > 0 {
			n := 1 + r.IntN(min(len(keys), 200))
			rows := make([]schema.Row[P], n)
			for i := range rows {
				rows[i] = schema.Row[P]{Key: keys[i], Data: gen(r, &prev)}
				prev = rows[i].Data
			}
			frames, keys = append(frames, rows), keys[n:]
		}
		for _, f := range []struct{ plain, compress bool }{{false, false}, {false, true}, {true, false}, {true, true}} {
			got, err := order.SpillRoundTrip(s, t.TempDir(), f.plain, f.compress, frames)
			if err != nil {
				t.Fatalf("seed %d %+v: %v", seed, f, err)
			}
			if !reflect.DeepEqual(got, frames) {
				t.Fatalf("seed %d %+v: rows differ after the round trip", seed, f)
			}
		}
	}
}

// TestDeltaFramesRoundTrip: every producer's rows survive every frame
// format exactly (S3 through its contextual codec, GCS and localfs through
// the stateless fallback), with arbitrary byte keys, empty keys, keys that
// prefix their neighbours either way, null (empty or absent) fields,
// uppercase, quoted, short and multipart ETags, and times whose difference
// crosses zero or overflows.
func TestDeltaFramesRoundTrip(t *testing.T) {
	t.Run("s3", func(t *testing.T) { roundTrip(t, &s3.Schema, s3Object) })
	t.Run("gcs", func(t *testing.T) { roundTrip(t, &gcs.Schema, gcsObject) })
	t.Run("localfs", func(t *testing.T) { roundTrip(t, &localfs.Schema, fsEntry) })
}

// BenchmarkSpillFormats writes and replays 50 frames of 4,096 S3 rows in
// each frame format. The keys are a real list's when SWATH_BENCH_KEYS
// names a sorted key file (the first 204,800 keys), else synthetic paths;
// the payloads are what a real bucket lists: a random MD5 ETag per row
// (incompressible), sizes spread over decades, times a few seconds apart,
// STANDARD and a checksum. Reported: spilled bytes per row and the CPU of a
// write and a replay per row (ns/op divided by the rows).
func BenchmarkSpillFormats(b *testing.B) {
	const frames, per = 50, 4096
	keys := benchKeys(b, frames*per)
	r := rand.New(rand.NewPCG(1, 2))
	var rows [][]schema.Row[s3.Object]
	t := int64(1_700_000_000_000_000)
	for i := range frames {
		f := make([]schema.Row[s3.Object], per)
		for j := range f {
			t += r.Int64N(5_000_000)
			f[j] = schema.Row[s3.Object]{Key: keys[i*per+j], Data: s3.Object{Size: r.Int64N(1 << (10 + r.IntN(25))), Mtime: t,
				ETag: hexOf(r, "0123456789abcdef"), StorageClass: "STANDARD", ChecksumAlgorithm: "CRC64NVME", ChecksumType: "FULL_OBJECT"}}
		}
		rows = append(rows, f)
	}
	for _, f := range []struct {
		name            string
		plain, compress bool
	}{{"plain", true, false}, {"plain+zstd", true, true}, {"delta", false, false}, {"delta+zstd", false, true}} {
		b.Run(f.name, func(b *testing.B) {
			b.ReportAllocs()
			var written int64
			for b.Loop() {
				_, n, err := order.SpillRoundTripBytes(&s3.Schema, b.TempDir(), f.plain, f.compress, rows, false)
				if err != nil {
					b.Fatal(err)
				}
				written = n
			}
			b.ReportMetric(float64(written)/(frames*per), "B/row")
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/(frames*per), "ns/row")
		})
	}
}

func benchKeys(b *testing.B, n int) []string {
	if path := os.Getenv("SWATH_BENCH_KEYS"); path != "" {
		fh, err := os.Open(path)
		if err != nil {
			b.Fatal(err)
		}
		defer fh.Close()
		var keys []string
		sc := bufio.NewScanner(fh)
		for sc.Scan() && len(keys) < n {
			keys = append(keys, sc.Text())
		}
		if len(keys) == n {
			return keys
		}
	}
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("data/year=%d/month=%02d/day=%02d/part-%08d.parquet", 2000+i/100000, i/10000%12+1, i/400%28+1, i)
	}
	slicesSort(keys)
	return keys
}
