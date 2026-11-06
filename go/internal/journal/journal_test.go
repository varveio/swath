// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package journal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/dataset"
)

func TestCheck(t *testing.T) {
	src := func(endpoint, canon string) []Field {
		return []Field{{Name: "source", Value: "s3://b/"}, {Name: "endpoint", Value: endpoint, Canonical: canon}, {Name: "region", Value: ""}}
	}
	base := Identity{Tool: "swath-go", Format: 1, SchemaID: "swath/canonical", Provider: "s3", Source: src("http://h:9000", "http://h:9000"), Writer: "w1", PartSize: 64 << 20}
	for _, c := range []struct {
		name  string
		edit  func(*Identity)
		field string
	}{
		{"same", func(*Identity) {}, ""},
		{"tool", func(i *Identity) { i.Tool = "swath" }, "tool"},
		{"format", func(i *Identity) { i.Format = 2 }, "format"},
		{"schema", func(i *Identity) { i.SchemaID = "swath-local/v0" }, "schema"},
		{"provider", func(i *Identity) { i.Provider = "gcs" }, "provider"},
		{"source", func(i *Identity) { i.Source = src("http://h:9000", ""); i.Source[0].Value = "s3://c/" }, "source"},
		{"writer", func(i *Identity) { i.Writer = "w2" }, "writer"},
		{"endpoint", func(i *Identity) { i.Source = src("http://other:9000", "http://other:9000") }, "endpoint"},
		{"endpoint spelling", func(i *Identity) { i.Source = src("HTTP://H:9000/", "http://h:9000") }, ""},
		{"region", func(i *Identity) { i.Source = src("http://h:9000", ""); i.Source[2].Value = "eu-west-1" }, "region"},
		{"field added", func(i *Identity) {
			i.Source = append(src("http://h:9000", ""), Field{Name: "fetch owner", Value: "true"})
		}, "fetch owner"},
		{"field dropped", func(i *Identity) { i.Source = src("http://h:9000", "")[:2] }, "region"},
		{"key cap", func(i *Identity) { i.KeyCap = 4096 }, "key cap"},
		{"part size", func(i *Identity) { i.PartSize = 1 }, "part size"},
	} {
		req := base
		c.edit(&req)
		err := Check(base, req)
		var mm *MismatchError
		switch {
		case c.field == "" && err != nil:
			t.Errorf("%s: %v", c.name, err)
		case c.field != "" && (!errors.As(err, &mm) || mm.Field != c.field):
			t.Errorf("%s: %v", c.name, err)
		}
	}
}

// TestJournalRoundTripAndTornTail: records come back in order with their
// boundaries; a torn tail is ignored and truncated on reopen.
func TestJournalRoundTripAndTornTail(t *testing.T) {
	out := t.TempDir()
	id := Identity{Tool: "swath-go", Format: 1, Provider: "s3", Source: []Field{{Name: "source", Value: "s3://b/"}}}
	j, err := Create(out, id, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		if err := j.Append(Record{Ordinal: i, Part: dataset.PartInfo{Rows: 10, MaxKey: dataset.Key(string(rune('a' + i)))}, RowsTotal: int64(10 * (i + 1))}); err != nil {
			t.Fatal(err)
		}
	}
	j.Close()
	f, _ := os.OpenFile(Path(out), os.O_APPEND|os.O_WRONLY, 0)
	f.Write([]byte{9, 0, 0, 0, 1, 2, 3}) // half a frame
	f.Close()
	st, err := Read(out)
	if err != nil {
		t.Fatal(err)
	}
	if Check(st.Header.Identity, id) != nil || len(st.Records) != 3 || st.Last().RowsTotal != 30 || st.TornBytes != 7 {
		t.Fatalf("%+v", st)
	}
	j, err = Reopen(out, st)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.Append(Record{Ordinal: 3}); err != nil {
		t.Fatal(err)
	}
	j.Close()
	if st, err = Read(out); err != nil || len(st.Records) != 4 || st.TornBytes != 0 {
		t.Fatalf("%+v %v", st, err)
	}
	// A whole last frame whose end never reached the disk (zeros) is torn.
	b, _ := os.ReadFile(Path(out))
	whole := len(b)
	unwritten := append(append([]byte{}, b...), 20, 0, 0, 0, 1, 2, 3, 4)
	unwritten = append(unwritten, make([]byte, 20)...)
	os.WriteFile(Path(out), unwritten, 0o644)
	if st, err = Read(out); err != nil || len(st.Records) != 4 || st.TornBytes != 28 {
		t.Fatalf("unwritten tail: %+v %v", st, err)
	}
	// Bit rot in a whole last frame, and a damaged length that overruns
	// the file over a whole frame, are an acknowledged record damaged:
	// refused, not taken for a torn tail.
	rot := append([]byte{}, b...)
	rot[len(rot)-3] ^= 0xff
	os.WriteFile(Path(out), rot, 0o644)
	if _, err = Read(out); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("bit rot in the last frame: %v", err)
	}
	var offs []int
	for off := 0; off+8 <= whole; off += 8 + int(binary.LittleEndian.Uint32(b[off:off+4])) {
		offs = append(offs, off)
	}
	long := append([]byte{}, b...)
	last := offs[len(offs)-1]
	binary.LittleEndian.PutUint32(long[last:last+4], binary.LittleEndian.Uint32(b[last:last+4])+7)
	os.WriteFile(Path(out), long, 0o644)
	if _, err = Read(out); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("overrunning length of the last frame: %v", err)
	}
}

// TestDamagedHeader: a header that never became durable (an empty or short
// file) is no journal only while no part exists; a header whose length was
// damaged over the records after it is corruption, never "no journal".
func TestDamagedHeader(t *testing.T) {
	out := t.TempDir()
	j, err := Create(out, Identity{Tool: "swath-go"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	j.Append(Record{Ordinal: 0, Part: dataset.PartInfo{MaxKey: "k"}})
	j.Close()
	b, _ := os.ReadFile(Path(out))
	hl := binary.LittleEndian.Uint32(b[:4])
	short := b[:8+hl/2]
	os.WriteFile(Path(out), short, 0o644)
	if _, err := Read(out); !errors.Is(err, ErrNoJournal) {
		t.Fatalf("short header, no parts: %v", err)
	}
	os.WriteFile(Path(out), nil, 0o644)
	if _, err := Read(out); !errors.Is(err, ErrNoJournal) {
		t.Fatalf("empty journal, no parts: %v", err)
	}
	os.MkdirAll(filepath.Join(out, dataset.DataDir), 0o755)
	os.WriteFile(filepath.Join(out, dataset.DataDir, dataset.PartName(0)), []byte("p"), 0o644)
	os.WriteFile(Path(out), short, 0o644)
	if _, err := Read(out); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("short header with a part on disk: %v", err)
	}
	os.Remove(filepath.Join(out, dataset.DataDir, dataset.PartName(0)))
	long := append([]byte{}, b...)
	binary.LittleEndian.PutUint32(long[:4], uint32(len(b)))
	os.WriteFile(Path(out), long, 0o644)
	if _, err := Read(out); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("header length overrunning the records: %v", err)
	}
}

// TestInteriorCorruptionFailsClosed: a bad frame with frames after it is an
// error, not a torn tail; an unknown version is refused; a second process
// cannot take the lock.
func TestInteriorCorruptionFailsClosed(t *testing.T) {
	out := t.TempDir()
	j, err := Create(out, Identity{Tool: "swath-go"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	lock, err := TakeLock(out)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := TakeLock(out); !errors.Is(err, ErrLocked) {
		t.Fatalf("second owner: %v", err)
	}
	lock.Release()
	for i := range 3 {
		j.Append(Record{Ordinal: i, Part: dataset.PartInfo{MaxKey: "k"}})
	}
	j.Close()
	b, _ := os.ReadFile(Path(out))
	hdrLen := 8 + int(b[0]) | int(b[1])<<8
	b[hdrLen+10] ^= 0xff // inside the first record
	os.WriteFile(Path(out), b, 0o644)
	if _, err := Read(out); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("interior corruption: %v", err)
	}
	// A newer version, version 5 (legacy: a capped run could write one,
	// and its identity lacks the S3 route) and version 6 (legacy: its
	// acknowledged part may be at the unclaimed temporary name) are refused.
	for _, v := range []int{9, 5, 6} {
		j, _ = Create(out, Identity{}, nil)
		j.Close()
		b, _ = os.ReadFile(Path(out))
		b2 := []byte(strings.Replace(string(b[8:]), fmt.Sprintf(`"version":%d`, Version), fmt.Sprintf(`"version":%d`, v), 1))
		binary.LittleEndian.PutUint32(b[0:4], uint32(len(b2)))
		binary.LittleEndian.PutUint32(b[4:8], frameCRC(b[:4], b2))
		os.WriteFile(Path(out), append(b[:8], b2...), 0o644)
		var ve *VersionError
		if _, err := Read(out); !errors.As(err, &ve) || ve.Found != v {
			t.Fatalf("version %d: %v", v, err)
		}
	}
}
