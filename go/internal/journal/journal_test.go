// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package journal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
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
	j, _ = Create(out, Identity{}, nil)
	j.Close()
	b, _ = os.ReadFile(Path(out))
	b2 := []byte(strings.Replace(string(b[8:]), fmt.Sprintf(`"version":%d`, Version), `"version":9`, 1))
	binary.LittleEndian.PutUint32(b[4:8], frameCRC(b[:4], b2))
	os.WriteFile(Path(out), append(b[:8], b2...), 0o644)
	var ve *VersionError
	if _, err := Read(out); !errors.As(err, &ve) || ve.Found != 9 {
		t.Fatalf("unknown version: %v", err)
	}
}
