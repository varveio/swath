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
// four encoders, keys at the schema's longest (1,024 bytes for S3, 4,096
// for a local path), random so nothing compresses them away; after every
// page the live heap the writers and their codec hold stays within the
// four encoders' admission (parquetout.EncoderBytes each). Before (sol
// review 2, P2) each writer held a codec of its own with a zstd state per
// column goroutine, each about 18 MiB once it has compressed a page over
// one zstd block, against a constant 24 MiB per encoder.
func TestWriterAdmissionCoversHeap(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates hundreds of megabytes")
	}
	s3row := func(i int, k string) s3.Row {
		return s3.Row{Key: k, Data: s3.Object{Size: int64(i), Mtime: int64(i) * 1000, ETag: fmt.Sprintf("%032x", i*7919), StorageClass: "STANDARD"}}
	}
	fsrow := func(i int, k string) localfs.Row {
		return localfs.Row{Key: k, Data: localfs.Entry{Size: int64(i), Mtime: int64(i), Ctime: int64(i), Atime: int64(i), Btime: int64(i), Inode: int64(i),
			Dev: 1, Blocks: 8, Mode: 0o644, UID: 1000, GID: 1000, Nlink: 1, Present: ^localfs.Presence(0), Kind: localfs.KindFile}}
	}
	writerHeap(t, &s3.Schema, s3row, 1024)
	writerHeap(t, &localfs.Schema, fsrow, 4096)
}

// gcHeap is the live heap after a collection (liveHeap).
func gcHeap() int64 {
	runtime.GC()
	return liveHeap()
}

func writerHeap[P any](t *testing.T, s *schema.Schema[P], mk func(i int, key string) schema.Row[P], keyLen int) {
	const writers, pages, page = 4, 6, 1024
	r0 := rand.New(rand.NewPCG(1, 2))
	rows := make([]schema.Row[P], writers*pages*page+1)
	for i := range rows {
		b := make([]byte, keyLen)
		for j := range b {
			b[j] = byte('a' + r0.IntN(26))
		}
		copy(b, fmt.Sprintf("%08d", i)) // ascending
		rows[i] = mk(i, string(b))
	}
	var queued atomic.Int64
	r, err := parquetout.NewRouter(parquetout.Options{Out: t.TempDir(), PartSize: 1 << 40, Encoders: writers,
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
	admitted := writers * parquetout.EncoderBytes(s)
	t.Logf("%s, %d-byte keys: %d writers held %.1f MiB at most, admitted %.1f MiB", s.ID, keyLen, writers, float64(peak)/(1<<20), float64(admitted)/(1<<20))
	if peak > admitted {
		t.Fatalf("%s: %d writers held %d bytes, admitted %d", s.ID, writers, peak, admitted)
	}
}
