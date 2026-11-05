// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package durable

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/dataset"
)

func TestCheck(t *testing.T) {
	base := Identity{Tool: "swath-go", Format: 1, SchemaID: "swath/canonical", Provider: "s3", Bucket: "b", Writer: "w1", PartSize: 64 << 20}
	for _, c := range []struct {
		name  string
		edit  func(*Identity)
		field string
	}{
		{"same", func(*Identity) {}, ""},
		{"tool", func(i *Identity) { i.Tool = "swath" }, "tool"},
		{"format", func(i *Identity) { i.Format = 2 }, "format"},
		{"schema", func(i *Identity) { i.SchemaID = "swath-local/v0" }, "schema"},
		{"bucket", func(i *Identity) { i.Bucket = "c" }, "source"},
		{"prefix", func(i *Identity) { i.Prefix = "p/" }, "source"},
		{"writer", func(i *Identity) { i.Writer = "w2" }, "writer"},
		{"part size", func(i *Identity) { i.PartSize = 1 }, "part size"},
		{"fetch owner", func(i *Identity) { i.FetchOwner = true }, "fetch owner"},
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
	id := Identity{Tool: "swath-go", Format: 1, Provider: "s3", Bucket: "b"}
	j, err := Create(out, id)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		if err := j.Append(Record{Ordinal: i, Part: dataset.Part{Rows: 10, MaxKey: string(rune('a' + i))}, RowsTotal: int64(10 * (i + 1)), Boundaries: []string{string(rune('k' + i))}}); err != nil {
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
	if st.Header.Identity != id || len(st.Records) != 3 || st.Last().RowsTotal != 30 || len(st.Boundaries) != 3 || st.TornBytes != 7 {
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
	// A corrupt frame ends the journal there.
	b, _ := os.ReadFile(Path(out))
	b[len(b)-3] ^= 0xff
	os.WriteFile(Path(out), b, 0o644)
	if st, err = Read(out); err != nil || len(st.Records) != 3 || st.TornBytes == 0 {
		t.Fatalf("%+v %v", st, err)
	}
}

// TestInteriorCorruptionFailsClosed: a bad frame with frames after it is an
// error, not a torn tail; an unknown version is refused; a second process
// cannot take the lock.
func TestInteriorCorruptionFailsClosed(t *testing.T) {
	out := t.TempDir()
	j, err := Create(out, Identity{Tool: "swath-go"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Create(out, Identity{}); !errors.Is(err, ErrLocked) {
		t.Fatalf("second owner: %v", err)
	}
	for i := range 3 {
		j.Append(Record{Ordinal: i, Part: dataset.Part{MaxKey: "k"}})
	}
	j.Close()
	b, _ := os.ReadFile(Path(out))
	hdrLen := 8 + int(b[0]) | int(b[1])<<8
	b[hdrLen+10] ^= 0xff // inside the first record
	os.WriteFile(Path(out), b, 0o644)
	if _, err := Read(out); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("interior corruption: %v", err)
	}
	j, _ = Create(out, Identity{})
	j.Close()
	b, _ = os.ReadFile(Path(out))
	b2 := []byte(strings.Replace(string(b[8:]), `"version":1`, `"version":9`, 1))
	binary.LittleEndian.PutUint32(b[4:8], crc32.Checksum(b2, castagnoli))
	os.WriteFile(Path(out), append(b[:8], b2...), 0o644)
	if _, err := Read(out); err == nil || !strings.Contains(err.Error(), "version 9") {
		t.Fatalf("unknown version: %v", err)
	}
}
