// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package textout_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source/localfs"
	"github.com/varveio/swath/go/internal/source/s3"
	"github.com/varveio/swath/go/internal/textout"
	"github.com/varveio/swath/go/reader"
)

// awkward is bytes a name may hold: invalid UTF-8, a tab, a line feed, a
// carriage return, a backslash, an escape byte, DEL, a literal \N and
// valid non-ASCII.
const awkward = "a\xff\xfeb\tc\nd\re\\f\x1b\x7f\\Nµ"

var localRows = []localfs.Row{
	{Key: "dir", Data: localfs.Entry{Kind: localfs.KindDir, Size: 4096, Mtime: 1_700_000_000_000_000, Present: localfs.HasSize | localfs.HasMtime}},
	{Key: "dir/" + awkward, Data: localfs.Entry{Kind: localfs.KindSymlink, LinkTarget: "../" + awkward, Size: 0,
		UID: math.MaxUint32, GID: 0, Mode: math.MaxUint32, Inode: -1, Dev: math.MinInt64,
		Present: localfs.HasSize | localfs.HasUID | localfs.HasGID | localfs.HasMode | localfs.HasInode | localfs.HasDev | localfs.HasLink}},
	{Key: "empty-link", Data: localfs.Entry{Kind: localfs.KindSymlink, Present: localfs.HasLink}},
}

// fields splits TSV output into its header and rows of decoded fields
// (nil for a null), through the grammar's decoder.
func fields(t *testing.T, out string) ([]string, [][]*string) {
	t.Helper()
	lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
	header := strings.Split(lines[0], "\t")
	var rows [][]*string
	for _, l := range lines[1:] {
		raw := strings.Split(l, "\t")
		if len(raw) != len(header) {
			t.Fatalf("%q: %d fields, the header has %d", l, len(raw), len(header))
		}
		row := make([]*string, len(raw))
		for i, f := range raw {
			v, ok, err := textout.UnescapeTSV(f)
			if err != nil {
				t.Fatal(err)
			}
			if ok {
				row[i] = &v
			}
		}
		rows = append(rows, row)
	}
	return header, rows
}

// TestLocalRowsRoundTrip: a local tree's rows print: keys and link targets
// of any bytes come back exactly from TSV (through the grammar's decoder)
// and JSONL (NAME_base64 where they are not UTF-8); a null differs from an
// empty string and from zero; unsigned columns print their maxima
// unsigned.
func TestLocalRowsRoundTrip(t *testing.T) {
	header, rows := fields(t, text(t, textout.Config{Format: textout.TSV}, &localfs.Schema, localRows))
	want := append([]string{textout.KeyColumn}, func() []string {
		var n []string
		for _, s := range localfs.Schema.Specs()[1:] {
			n = append(n, s.Name)
		}
		return n
	}()...)
	if strings.Join(header, " ") != strings.Join(want, " ") {
		t.Fatalf("header %q, want %q", header, want)
	}
	col := func(row []*string, name string) *string {
		for i, h := range header {
			if h == name {
				return row[i]
			}
		}
		t.Fatalf("no column %s", name)
		return nil
	}
	str := func(p *string) string {
		if p == nil {
			return "<null>"
		}
		return *p
	}
	for i, r := range localRows {
		if got := str(col(rows[i], "key")); got != r.Key {
			t.Errorf("row %d key %q, want %q", i, got, r.Key)
		}
	}
	for _, c := range []struct {
		row        int
		name, want string
	}{
		{1, "link_target", "../" + awkward}, {2, "link_target", ""}, {0, "link_target", "<null>"},
		{1, "size", "0"}, {2, "size", "<null>"}, {1, "gid", "0"}, {0, "gid", "<null>"},
		{1, "uid", "4294967295"}, {1, "mode", "4294967295"}, {1, "inode", "18446744073709551615"}, {1, "dev", "9223372036854775808"},
		{0, "kind", "DIR"}, {0, "last_modified", "2023-11-14T22:13:20.000000Z"}, {1, "last_modified", "<null>"},
	} {
		if got := str(col(rows[c.row], c.name)); got != c.want {
			t.Errorf("TSV row %d %s: %q, want %q", c.row, c.name, got, c.want)
		}
	}

	out := text(t, textout.Config{Format: textout.JSONL}, &localfs.Schema, localRows)
	lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
	if len(lines) != len(localRows) {
		t.Fatalf("%d lines", len(lines))
	}
	member := func(m map[string]json.RawMessage, name string) string {
		if b64, ok := m[name+textout.Base64Suffix]; ok {
			if _, both := m[name]; both {
				t.Fatalf("%s and %s%s both", name, name, textout.Base64Suffix)
			}
			var s string
			if err := json.Unmarshal(b64, &s); err != nil {
				t.Fatal(err)
			}
			raw, err := base64.StdEncoding.DecodeString(s)
			if err != nil {
				t.Fatal(err)
			}
			return string(raw)
		}
		v, ok := m[name]
		if !ok {
			t.Fatalf("no member %s", name)
		}
		if string(v) == "null" {
			return "<null>"
		}
		var s string
		if json.Unmarshal(v, &s) == nil {
			return s
		}
		return string(v)
	}
	ms := make([]map[string]json.RawMessage, len(lines))
	for i, l := range lines {
		if err := json.Unmarshal([]byte(l), &ms[i]); err != nil {
			t.Fatalf("%q: %v", l, err)
		}
		if got := member(ms[i], "key"); got != localRows[i].Key {
			t.Errorf("JSONL row %d key %q", i, got)
		}
	}
	for _, c := range []struct {
		row        int
		name, want string
	}{
		{1, "link_target", "../" + awkward}, {2, "link_target", ""}, {0, "link_target", "<null>"},
		{1, "size", "0"}, {2, "size", "<null>"}, {1, "uid", "4294967295"}, {1, "inode", "18446744073709551615"},
		{1, "dev", "9223372036854775808"}, {0, "kind", "DIR"}, {0, "last_modified", "2023-11-14T22:13:20.000000Z"},
	} {
		if got := member(ms[c.row], c.name); got != c.want {
			t.Errorf("JSONL row %d %s: %q, want %q", c.row, c.name, got, c.want)
		}
	}
	if !strings.HasPrefix(lines[1], `{"key_base64":"`) || !strings.HasPrefix(lines[0], `{"key":"dir","size":4096,`) {
		t.Errorf("member order: %q", out)
	}
}

// TestLocalTable: a local tree's table has the kind beside the time and
// size (as wide as the widest kind so far), a missing value blank, and
// its keys escaped as in TSV.
func TestLocalTable(t *testing.T) {
	want := "last_modified (local)      size  kind  key\n" +
		"2023-11-14 22:13:20    4.00 KiB  DIR   dir\n" +
		"                            0 B  SYMLINK  dir/a\\xff\\xfeb\\tc\\nd\\re\\\\f\\x1b\\x7f\\\\Nµ\n" +
		"                                 SYMLINK  empty-link\n"
	if got := text(t, textout.Config{Format: textout.Table}, &localfs.Schema, localRows); got != want {
		t.Errorf("\n%q\nwant\n%q", got, want)
	}
}

// TestS3RowAsStored: an S3 object's values print as the schema holds them:
// the ETag without quotes added, the checksum algorithm its one string, the
// owner two flat columns.
func TestS3RowAsStored(t *testing.T) {
	rows := []s3.Row{{Key: "logs/app.log", Data: s3.Object{Size: 434234, Mtime: 1_700_000_000_000_000, ETag: "fba9dede5f27731c9771645a39863328",
		StorageClass: "STANDARD", ChecksumAlgorithm: "CRC64NVME", ChecksumType: "FULL_OBJECT", OwnerID: "abc"}}}
	want := `{"key":"logs/app.log","size":434234,"last_modified":"2023-11-14T22:13:20.000000Z","etag":"fba9dede5f27731c9771645a39863328","storage_class":"STANDARD","owner_id":"abc","owner_display_name":null,"checksum_algorithm":"CRC64NVME","checksum_type":"FULL_OBJECT"}` + "\n"
	if got := text(t, textout.Config{Format: textout.JSONL}, &s3.Schema, rows); got != want {
		t.Errorf("\n%s\nwant\n%s", got, want)
	}
}

// TestProjection: chosen columns print in the order given, in TSV and
// JSONL; a name the schema lacks, or one named twice, is refused before
// any row, the error listing the schema's columns; the table's columns
// are not chosen.
func TestProjection(t *testing.T) {
	rows := []s3.Row{{Key: "k", Data: s3.Object{Size: 3, ETag: "e"}}}
	cols := []string{"etag", "key", "size"}
	if got := text(t, textout.Config{Format: textout.TSV, Columns: cols}, &s3.Schema, rows); got != "etag\tkey\tsize\ne\tk\t3\n" {
		t.Errorf("tsv %q", got)
	}
	if got := text(t, textout.Config{Format: textout.JSONL, Columns: cols}, &s3.Schema, rows); got != `{"etag":"e","key":"k","size":3}`+"\n" {
		t.Errorf("jsonl %q", got)
	}
	for _, c := range []struct {
		cfg  textout.Config
		want string
	}{
		{textout.Config{Format: textout.TSV, Columns: []string{"key", "Size"}},
			`no column "Size" in swath/s3-objects/v1 (its columns: key, size, last_modified, etag, storage_class, owner_id, owner_display_name, checksum_algorithm, checksum_type)`},
		{textout.Config{Format: textout.JSONL, Columns: []string{"key", "key"}}, `column "key" named twice`},
		{textout.Config{Format: textout.Table, Columns: []string{"key"}}, "text output: the table's columns are fixed"},
	} {
		var buf bytes.Buffer
		c.cfg.Stdout = &buf
		o, err := textout.Open(c.cfg)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := textout.NewSink(o, &s3.Schema); err == nil || !strings.HasPrefix(err.Error(), c.want) {
			t.Errorf("%v: %v, want %q", c.cfg.Columns, err, c.want)
		}
		if err := o.Commit(); err != nil || buf.Len() != 0 {
			t.Errorf("%v: wrote %q (%v)", c.cfg.Columns, buf.String(), err)
		}
	}
}

// TestEverySchemaPrints: every schema swath writes (the reader's list,
// which schema.Bind requires a provider's schema to be in) prints a
// header of its column names and a row of every column in TSV, JSONL and
// the table; none ends a column name in the reserved suffix.
func TestEverySchemaPrints(t *testing.T) {
	type payload struct{}
	ids := reader.SchemaIDs()
	if len(ids) < 6 {
		t.Fatalf("schema ids %v", ids)
	}
	for _, id := range ids {
		spec, _ := reader.Schema(id)
		cols := make([]schema.Column[payload], len(spec.Columns))
		for i, c := range spec.Columns {
			switch c.Type {
			case schema.String:
				cols[i].Str = func(*payload) (string, bool) { return "v", true }
			case schema.Binary:
				cols[i].Str = func(*payload) (string, bool) { return "\xff", true }
			case schema.Bool:
				cols[i].Bool = func(*payload) (bool, bool) { return true, true }
			default:
				cols[i].Int = func(*payload) (int64, bool) { return -1, true }
			}
		}
		s := schema.Bind(id, schema.Schema[payload]{Columns: cols, Heap: func(*payload) int64 { return 0 }})
		rows := []schema.Row[payload]{{Key: "k"}}
		header, got := fields(t, text(t, textout.Config{Format: textout.TSV}, &s, rows))
		if want := append([]string{"key"}, spec.ColumnNames()...); strings.Join(header, "\t") != strings.Join(want, "\t") || len(got) != 1 {
			t.Errorf("%s: header %q, want %q; %d rows", id, header, want, len(got))
		}
		for i, f := range got[0] {
			if f == nil {
				t.Errorf("%s: %s null", id, header[i])
			}
		}
		var m map[string]any
		if line := text(t, textout.Config{Format: textout.JSONL}, &s, rows); json.Unmarshal([]byte(line), &m) != nil || len(m) != len(spec.Columns)+1 {
			t.Errorf("%s: JSONL %q", id, line)
		}
		if lines := strings.Split(text(t, textout.Config{Format: textout.Table}, &s, rows), "\n"); len(lines) != 3 || !strings.HasSuffix(lines[1], "  k") {
			t.Errorf("%s: table %q", id, lines)
		}
	}
}
