// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"encoding/base64"
	"fmt"
	"slices"
	"strings"
	"testing"
)

func TestFilesystemBoundaries(t *testing.T) {
	p := &Provider{}
	if p.filesystemBoundaries() != nil {
		t.Fatal("unfiltered listing claimed boundary coverage")
	}
	p.opt.OneFilesystem = true
	b := p.filesystemBoundaries()
	if b.KeysBase64 == nil || len(b.KeysBase64) != 0 || b.TotalCount != 0 || !b.Complete {
		t.Fatalf("empty set %+v", b)
	}
	key := "bad\xff/"
	p.noteBoundary(key)
	b = p.filesystemBoundaries()
	decoded, err := base64.StdEncoding.DecodeString(b.KeysBase64[0])
	if err != nil || string(decoded) != key || b.TotalCount != 1 || !b.Complete {
		t.Fatalf("byte-exact boundary %+v, decoded %q %v", b, decoded, err)
	}
	b.KeysBase64[0] = "changed"
	if p.filesystemBoundaries().KeysBase64[0] == "changed" {
		t.Fatal("report aliases provider state")
	}
	for _, size := range []int{8, maxKeyBytes - 6} {
		p := &Provider{opt: Options{OneFilesystem: true}}
		stops := min(maxBoundaryKeys, maxBoundaryBytes/base64.StdEncoding.EncodedLen(size+6)) + 2
		for i := 0; i < stops; i++ {
			p.noteBoundary(fmt.Sprintf("%05d", i) + strings.Repeat("a", size) + "/")
		}
		b := p.filesystemBoundaries()
		if b.Complete || b.TotalCount != int64(stops) || len(b.KeysBase64) > maxBoundaryKeys || !slices.IsSorted(b.KeysBase64) {
			t.Fatalf("size=%d: incomplete metadata %+v", size, b)
		}
		encoded := 0
		for _, k := range b.KeysBase64 {
			encoded += len(k)
		}
		if encoded > maxBoundaryBytes || len(b.KeysBase64) == 0 {
			t.Fatalf("retained %d keys, %d bytes", len(b.KeysBase64), encoded)
		}
	}
}
