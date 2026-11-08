// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"fmt"
	"math/rand/v2"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/parquetout"
	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source/localfs"
	"github.com/varveio/swath/go/internal/source/s3"
)

// TestWriterAdmissionCoversHeap is the writer admission's heap gate: four
// parts open at once (the open part continued as a segment, three more),
// four encoders admitted of six (MaxEncoders 6, as the default at 64
// listing workers, before Admit), values at the schema's longest (1,024-
// byte keys for S3, 24 pages a writer, and one S3 writer alone; 4,096-byte
// keys and link targets for a local path) and random so nothing
// compresses them away; after every page the live heap the writers and
// their codec hold stays within the encoders' admission
// (parquetout.EncoderBytes each). Before (sol review 2, P2) each writer held a codec of its own with
// a zstd state per column goroutine, each about 18 MiB once it has
// compressed a page over one zstd block, against a constant 24 MiB per
// encoder; then (sol review 3) the shared codec's six states all warmed
// before the two beyond the four admitted were charged, and the price
// missed the residency of long runs (135.9 MiB against 122.2 at 24 pages)
// and wide link targets (228.8 against 159.8).
func TestWriterAdmissionCoversHeap(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates hundreds of megabytes")
	}
	writerHeap(t, &s3.Schema, s3Row(1024), 4, 6, 24)
	writerHeap(t, &s3.Schema, s3Row(1024), 1, 1, 24)
	writerHeap(t, &localfs.Schema, fsRow(4096, 4095), 4, 6, 6)
}

// s3Row makes S3 rows with random keys of n bytes.
func s3Row(n int) func(i int, r *rand.Rand) s3.Row {
	return func(i int, r *rand.Rand) s3.Row {
		return s3.Row{Key: randKey(r, i, n), Data: s3.Object{Size: int64(i), Mtime: int64(i) * 1000, ETag: fmt.Sprintf("%032x", i*7919), StorageClass: "STANDARD"}}
	}
}

// fsRow makes local-path rows with random keys of n bytes and, link > 0,
// random link targets of link bytes.
func fsRow(n, link int) func(i int, r *rand.Rand) localfs.Row {
	return func(i int, r *rand.Rand) localfs.Row {
		e := localfs.Entry{Size: int64(i), Mtime: int64(i), Ctime: int64(i), Atime: int64(i), Btime: int64(i), Inode: int64(i),
			Dev: 1, Blocks: 8, Mode: 0o644, UID: 1000, GID: 1000, Nlink: 1, Present: ^localfs.Presence(0), Kind: localfs.KindFile}
		if link > 0 {
			e.LinkTarget, e.Kind = randKey(r, 0, link), localfs.KindSymlink
		} else {
			e.Present &^= localfs.HasLink
		}
		return localfs.Row{Key: randKey(r, i, n), Data: e}
	}
}

// randKey is n random letters, ascending in i.
func randKey(r *rand.Rand, i, n int) string {
	b := make([]byte, n)
	for j := range b {
		b[j] = byte('a' + r.IntN(26))
	}
	copy(b, fmt.Sprintf("%08d", i))
	return string(b)
}

// gcHeap is the live heap after a collection (liveHeap).
func gcHeap() int64 {
	runtime.GC()
	return liveHeap()
}

// writerHeap writes pages of 1,024 rows to writers parts open at once,
// with writers encoders admitted of maxEncoders, and requires the live
// heap after every page within the writers' admission.
func writerHeap[P any](t *testing.T, s *schema.Schema[P], mk func(i int, r *rand.Rand) schema.Row[P], writers, maxEncoders, pages int) {
	t.Helper()
	peak := writerPeak(t, s, mk, writers, maxEncoders, pages)
	admitted := int64(writers) * parquetout.EncoderBytes(s)
	t.Logf("%s: %d writers, %d pages each, held %.1f MiB at most, admitted %.1f MiB", s.ID, writers, pages, float64(peak)/(1<<20), float64(admitted)/(1<<20))
	if peak > admitted {
		t.Fatalf("%s: %d writers held %d bytes, admitted %d", s.ID, writers, peak, admitted)
	}
}

func writerPeak[P any](t *testing.T, s *schema.Schema[P], mk func(i int, r *rand.Rand) schema.Row[P], writers, maxEncoders, pages int) int64 {
	const page = 1024
	r0 := rand.New(rand.NewPCG(1, 2))
	rows := make([]schema.Row[P], writers*pages*page+1)
	for i := range rows {
		rows[i] = mk(i, r0)
	}
	var queued atomic.Int64
	r, err := parquetout.NewRouter(parquetout.Options{Out: t.TempDir(), PartSize: 1 << 40, Encoders: writers, MaxEncoders: maxEncoders,
		Account: func(d int64) { queued.Add(d) }}, s)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Abort()
	ctx := context.Background()
	if err := r.Write(ctx, [][]schema.Row[P]{rows[:1]}); err != nil {
		t.Fatal(err)
	}
	base := gcHeap()
	var segs []*parquetout.Segment[P]
	for w := range writers {
		sg, err := r.OpenSegment(ctx, w == 0)
		if err != nil {
			t.Fatal(err)
		}
		segs = append(segs, sg)
	}
	var peak int64
	for p := range pages {
		for w, sg := range segs {
			at := 1 + (w*pages+p)*page
			if err := sg.Write(ctx, [][]schema.Row[P]{rows[at : at+page]}); err != nil {
				t.Fatal(err)
			}
		}
		for queued.Load() > 0 {
			time.Sleep(time.Millisecond)
		}
		peak = max(peak, gcHeap()-base)
	}
	return peak
}
