// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// record encodes one linux_dirent64 with 8-byte aligned length.
func record(ino uint64, typ uint8, name string) []byte {
	n := direntHeader + len(name) + 1
	n = (n + 7) &^ 7
	b := make([]byte, n)
	binary.NativeEndian.PutUint64(b[0:], ino)
	binary.NativeEndian.PutUint16(b[16:], uint16(n))
	b[18] = typ
	copy(b[direntHeader:], name)
	return b
}

func TestParseDirents(t *testing.T) {
	buf := bytes.Join([][]byte{record(1, 4, "."), record(2, 4, ".."), record(3, 8, "file"), record(0, 8, "deleted"), record(5, 4, "dir"), record(6, 8, "日本")}, nil)
	var got []string
	if err := parseDirents(buf, func(ino uint64, typ uint8, name []byte) { got = append(got, string(name)) }); err != nil {
		t.Fatal(err)
	}
	if want := []string{"file", "dir", "日本"}; !equal(got, want) {
		t.Fatalf("got %q", got)
	}
	if err := parseDirents(buf[:len(buf)-3], func(uint64, uint8, []byte) {}); err == nil {
		t.Fatal("truncated buffer accepted")
	}
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func FuzzParseDirents(f *testing.F) {
	f.Add(bytes.Join([][]byte{record(3, 8, "file"), record(5, 4, "dir")}, nil))
	f.Add([]byte{})
	f.Add(make([]byte, 19))
	f.Fuzz(func(t *testing.T, buf []byte) {
		total := 0
		_ = parseDirents(buf, func(ino uint64, typ uint8, name []byte) {
			if ino == 0 || len(name) == 0 || bytes.IndexByte(name, 0) >= 0 {
				t.Fatalf("bad entry ino=%d name=%q", ino, name)
			}
			total += len(name)
		})
		if total > len(buf) {
			t.Fatal("names longer than the buffer")
		}
	})
}
