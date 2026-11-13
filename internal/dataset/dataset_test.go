// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package dataset

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/internal/faults"
	"github.com/varveio/swath/reader"
)

func part(i int, lo, hi string, n int64) Part {
	return Part{Path: TempPath("/x", i), PartInfo: PartInfo{MinKey: Key(lo), MaxKey: Key(hi), Rows: n}}
}

func TestCheck(t *testing.T) {
	if err := Check([]Part{part(0, "a", "c", 2), part(1, "d", "f", 3)}, 5); err != nil {
		t.Fatal(err)
	}
	for name, parts := range map[string][]Part{
		"overlap":  {part(0, "a", "d", 2), part(1, "d", "f", 3)},
		"gap":      {part(0, "a", "c", 2), part(2, "d", "f", 3)},
		"rowcount": {part(0, "a", "c", 2), part(1, "d", "f", 2)},
		"inverted": {part(0, "c", "a", 2), part(1, "d", "f", 3)},
	} {
		var ie *InvariantError
		if err := Check(parts, 5); !errors.As(err, &ie) {
			t.Errorf("%s: gate passed", name)
		}
	}
}

func touch(t *testing.T, p string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func exists(t *testing.T, paths ...string) {
	t.Helper()
	for _, p := range paths {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("%s should survive: %v", p, err)
		}
	}
}

const id = "swath/local-entries/v0"

func TestPrepareNeverDeletesTheTree(t *testing.T) {
	T := t.TempDir()
	src := filepath.Join(T, "src")
	keep := filepath.Join(src, "keep", "file.txt")
	touch(t, keep)
	for name, tc := range map[string]struct {
		out     string
		replace bool
		want    error
	}{
		"ancestor":            {T, true, ErrOutIsRoot},
		"root":                {src, true, ErrOutIsRoot},
		"inside, new":         {filepath.Join(src, "out"), true, ErrInsideRoot},
		"inside, not empty":   {filepath.Join(src, "keep"), false, ErrNotEmpty},
		"foreign, replace":    {filepath.Join(T, "foreign"), true, ErrNotDataset},
		"foreign, no replace": {filepath.Join(T, "foreign"), false, ErrNotEmpty},
	} {
		touch(t, filepath.Join(T, "foreign", "precious.txt"))
		err := Prepare(tc.out, src, id, tc.replace)
		if !errors.Is(err, tc.want) {
			t.Errorf("%s: err = %v, want %v", name, err, tc.want)
		}
		if strings.HasPrefix(name, "inside") && !errors.Is(err, ErrInsideRoot) {
			t.Errorf("%s: err = %v, not %v: --overwrite would be refused", name, err, ErrInsideRoot)
		}
		exists(t, keep, filepath.Join(T, "foreign", "precious.txt"))
	}
	alias := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(T, alias); err == nil {
		if err := Prepare(T, filepath.Join(alias, "src"), id, true); !errors.Is(err, ErrOutIsRoot) {
			t.Errorf("ancestor via alias: %v", err)
		}
		exists(t, keep)
	}
}

func TestReplaceRemovesOnlyOwnEntries(t *testing.T) {
	out := filepath.Join(t.TempDir(), "ds")
	for _, f := range []string{"data/part-00000.parquet", ManifestName, legacySuccess, legacySymlink, ".swath/journal", ".swath/lock", "notes.txt"} {
		touch(t, filepath.Join(out, f))
	}
	if err := Prepare(out, t.TempDir(), id, true); err != nil {
		t.Fatal(err)
	}
	exists(t, filepath.Join(out, "notes.txt"), filepath.Join(out, DataDir), filepath.Join(out, ".swath/lock"))
	for _, gone := range []string{"data/part-00000.parquet", ManifestName, legacySuccess, legacySymlink, ".swath/journal"} {
		if _, err := os.Stat(filepath.Join(out, gone)); !os.IsNotExist(err) {
			t.Errorf("%s survived", gone)
		}
	}
}

// TestPublishFailureLeavesNoManifest: the manifest is the commit; a failed
// publication has none.
func TestPublishFailureLeavesNoManifest(t *testing.T) {
	out := t.TempDir()
	touch(t, filepath.Join(out, DataDir, "x"))
	touch(t, TempPath(out, 0))
	// A non-empty directory where manifest.json goes makes the commit fail.
	touch(t, filepath.Join(out, ManifestName, "blocker"))
	err := Publish(out, Meta{SchemaID: id}, []Part{{Path: TempPath(out, 0), PartInfo: PartInfo{Rows: 1, MinKey: "a", MaxKey: "a"}}}, nil)
	if err == nil {
		t.Fatal("publish succeeded")
	}
	if Committed(out) {
		t.Fatal("a failed publish committed")
	}
}

// TestOverwriteRemovesTheCommitFirst (review item 43): --overwrite
// is a destructive replacement, and its first durable step is to remove the
// previous dataset's commit record: a crash part way through the removal
// leaves a directory with no dataset (and its old parts), never a manifest
// naming parts already removed. Before, data/ went first.
func TestOverwriteRemovesTheCommitFirst(t *testing.T) {
	out := filepath.Join(t.TempDir(), "ds")
	for _, f := range []string{"data/part-00000.parquet", ManifestName, legacySuccess, ".swath/lock"} {
		touch(t, filepath.Join(out, f))
	}
	restore := faults.Set(func(p string) error {
		if p == "overwrite-uncommitted" {
			return faults.ErrCrash
		}
		return nil
	})
	err := Prepare(out, t.TempDir(), id, true)
	restore()
	if !errors.Is(err, faults.ErrCrash) {
		t.Fatalf("the crash point was not reached: %v", err)
	}
	if Committed(out) {
		t.Fatal("the manifest outlived the start of the overwrite")
	}
	if _, err := os.Stat(filepath.Join(out, legacySuccess)); !os.IsNotExist(err) {
		t.Fatal("the earlier layout's commit marker outlived the start of the overwrite")
	}
	exists(t, filepath.Join(out, "data/part-00000.parquet"))
}

// TestOverwriteInterruptedUnjournaled: an unjournaled run (a local
// listing) stopped before its commit leaves data/, error.json and the
// output lock; --overwrite replaces that output (the lock and the outcome
// file are this tool's marks), where before it refused it as not a
// dataset while swath resume found nothing to resume. data/ without a mark
// of ours is still refused.
func TestOverwriteInterruptedUnjournaled(t *testing.T) {
	out := t.TempDir()
	for _, d := range []string{DataDir, Workspace} {
		if err := os.MkdirAll(filepath.Join(out, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	os.WriteFile(filepath.Join(out, DataDir, "part-00000.parquet"), []byte("x"), 0o644)
	os.WriteFile(filepath.Join(out, ErrorName), []byte(`{"class":"deadline"}`), 0o644)
	os.WriteFile(filepath.Join(out, Workspace, LockName), nil, 0o644)
	if Resumable(out) {
		t.Fatal("an unjournaled run reads as resumable")
	}
	if err := Prepare(out, "", "swath/local-files/v1", true); err != nil {
		t.Fatalf("overwrite: %v", err)
	}
	if _, err := os.Stat(filepath.Join(out, ErrorName)); !os.IsNotExist(err) {
		t.Fatal("error.json kept")
	}
	foreign := t.TempDir()
	os.MkdirAll(filepath.Join(foreign, DataDir), 0o755)
	if err := Prepare(foreign, "", "swath/local-files/v1", true); !errors.Is(err, ErrNotDataset) {
		t.Fatalf("data/ without a mark of ours: %v, want ErrNotDataset", err)
	}
}

// TestOverwriteRefusesFormat2: --overwrite replaces a current SeekParquet
// dataset of the run's schema, but refuses a format 2 dataset of builds from
// 2026-11-05 to 2026-11-08 of the same schema id, even beside a journal, and
// leaves it as it was.
func TestOverwriteRefusesFormat2(t *testing.T) {
	const schemaID = "swath/s3-objects/v1"
	current := t.TempDir()
	touch(t, filepath.Join(current, DataDir, PartName(0)))
	os.WriteFile(filepath.Join(current, ManifestName), []byte(`{"format":"`+seekparquet.Format+`","format_version":0,"draft_revision":1,"schema_id":"`+schemaID+`"}`), 0o644)
	if _, err := Usable(current, "", schemaID, true); err != nil {
		t.Fatalf("current dataset: %v", err)
	}
	old := t.TempDir()
	manifest := []byte(`{"format":"swath-dataset","format_version":2,"schema_id":"` + schemaID + `","coverage":{"complete":true},"parts":[]}`)
	part := filepath.Join(old, DataDir, PartName(0))
	touch(t, part)
	touch(t, filepath.Join(old, JournalDir, "journal"))
	if err := os.WriteFile(filepath.Join(old, ManifestName), manifest, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := Prepare(old, "", schemaID, true); !errors.Is(err, ErrNotDataset) {
		t.Fatalf("format 2 dataset: %v, want ErrNotDataset", err)
	}
	if b, err := os.ReadFile(filepath.Join(old, ManifestName)); err != nil || !bytes.Equal(b, manifest) {
		t.Fatalf("manifest changed: %q, %v", b, err)
	}
	for _, kept := range []string{part, filepath.Join(old, JournalDir, "journal")} {
		if _, err := os.Stat(kept); err != nil {
			t.Fatalf("%s: %v", kept, err)
		}
	}
}

// TestManaged: a directory is this tool's by its lock, its journal or a
// manifest that parses as this tool's (format and the swath extension's
// coverage); an unrelated data/ beside a _SUCCESS, another tool's manifest
// with a schema id, or an earlier build's format 2 manifest, is not.
func TestManaged(t *testing.T) {
	for _, tc := range []struct {
		name  string
		files map[string]string
		want  bool
	}{
		{"empty", nil, false},
		{"_SUCCESS beside data/", map[string]string{legacySuccess: "", DataDir + "/x": ""}, false},
		{"data/ alone", map[string]string{DataDir + "/x": ""}, false},
		{"another tool's manifest with a schema id", map[string]string{ManifestName: `{"schema_id":"other/v1"}`}, false},
		{"a schemaId manifest", map[string]string{ManifestName: `{"schemaId":"other"}`}, false},
		{"our format, no coverage", map[string]string{ManifestName: `{"format":"` + seekparquet.Format + `","extensions":{"swath":{}}}`}, false},
		{"our format, null coverage", map[string]string{ManifestName: `{"format":"` + seekparquet.Format + `","extensions":{"swath":{"coverage":null}}}`}, false},
		{"our format, coverage outside the extension", map[string]string{ManifestName: `{"format":"` + seekparquet.Format + `","coverage":{}}`}, false},
		{"another producer's SeekParquet dataset", map[string]string{ManifestName: `{"format":"` + seekparquet.Format + `","extensions":{"other":{"coverage":{}}}}`}, false},
		{"another format with coverage", map[string]string{ManifestName: `{"format":"other","coverage":{}}`}, false},
		{"not JSON", map[string]string{ManifestName: `{`}, false},
		{"our manifest", map[string]string{ManifestName: `{"format":"` + seekparquet.Format + `","extensions":{"swath":{"coverage":{"complete":true}}}}`}, true},
		{"an earlier build's format 2 manifest", map[string]string{ManifestName: `{"format":"swath-dataset","format_version":2,"coverage":{"complete":true}}`}, false},
		{"a lock", map[string]string{Workspace + "/" + LockName: ""}, true},
		{"a journal", map[string]string{JournalDir + "/journal": ""}, true},
	} {
		dir := t.TempDir()
		for name, body := range tc.files {
			p := filepath.Join(dir, filepath.FromSlash(name))
			if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		if got := Managed(dir); got != tc.want {
			t.Errorf("%s: Managed = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestManifestShape: a published manifest has only SeekParquet's
// top-level members, part members and key declaration, with swath's own
// under extensions.swath (version 1 its first member), and reads back as it
// was written; a format 2 manifest of an earlier build, and an
// extensions.swath of another version or none, are refused.
func TestManifestShape(t *testing.T) {
	for _, groups := range []bool{false, true} {
		out := t.TempDir()
		touch(t, filepath.Join(out, DataDir, "x"))
		touch(t, TempPath(out, 0))
		cov := Coverage{Complete: true, StopReason: "completed"}
		part := Part{Path: TempPath(out, 0), PartInfo: PartInfo{Rows: 2, Bytes: 9, MinKey: "a", MaxKey: "b\xff", SHA256: strings.Repeat("0123456789abcdef", 4)}}
		if err := Publish(out, Meta{SchemaID: id, Groups: groups, Source: Source{Kind: "s3", URI: "s3://b/", Container: "b"}, Scope: Scope{Kind: "whole"}, Coverage: cov}, []Part{part}, nil); err != nil {
			t.Fatal(err)
		}
		b, err := os.ReadFile(filepath.Join(out, ManifestName))
		if err != nil {
			t.Fatal(err)
		}
		var compact bytes.Buffer
		if err := json.Compact(&compact, b); err != nil {
			t.Fatal(err)
		}
		var raw map[string]json.RawMessage
		if err := json.Unmarshal(compact.Bytes(), &raw); err != nil {
			t.Fatal(err)
		}
		if got := slices.Sorted(maps.Keys(raw)); !slices.Equal(got, []string{"draft_revision", "extensions", "format", "format_version", "key", "parts", "schema_id"}) {
			t.Fatalf("top-level members %q", got)
		}
		want := fmt.Sprintf(`{"column":"key","unique":%v}`, !groups)
		if string(raw["format"]) != `"seekparquet"` || string(raw["format_version"]) != "0" || string(raw["draft_revision"]) != "1" || string(raw["key"]) != want {
			t.Fatalf("manifest %s", b)
		}
		var parts []map[string]json.RawMessage
		if err := json.Unmarshal(raw["parts"], &parts); err != nil {
			t.Fatal(err)
		}
		if len(parts) != 1 || !slices.Equal(slices.Sorted(maps.Keys(parts[0])), []string{"bytes", "max_key_base64", "min_key_base64", "path", "rows", "sha256"}) ||
			string(parts[0]["max_key_base64"]) != `"Yv8="` {
			t.Fatalf("parts %s", raw["parts"])
		}
		var ext map[string]map[string]json.RawMessage
		if err := json.Unmarshal(raw["extensions"], &ext); err != nil {
			t.Fatal(err)
		}
		if got := slices.Sorted(maps.Keys(ext["swath"])); len(ext) != 1 || !slices.Equal(got, []string{"coverage", "published_at", "scope", "source", "version"}) {
			t.Fatalf("extensions %s", raw["extensions"])
		}
		if !bytes.HasPrefix(raw["extensions"], []byte(`{"swath":{"version":1,`)) {
			t.Fatalf("extensions.swath.version is not its first member, 1: %s", raw["extensions"])
		}
		var m Manifest
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatal(err)
		}
		if !m.Current() || m.MetadataVersion != reader.MetadataVersion || m.Key != (seekparquet.Key{Column: "key", Unique: !groups}) || m.Coverage.StopReason != "completed" || m.Source.Container != "b" ||
			len(m.Parts) != 1 || m.Parts[0].SHA256 != part.SHA256 || m.Parts[0].MaxKey != part.MaxKey || m.Parts[0].Path != "data/part-00000.parquet" {
			t.Fatalf("read back %+v", m)
		}
	}
	legacy := `{"format":"swath-dataset","format_version":2,"schema_id":"x/v1","published_at":"2026-11-05T18:00:00Z",
		"source":{"kind":"s3","uri":"s3://b/","container":"b"},"scope":{"kind":"whole"},"coverage":{"complete":true,"stop_reason":"completed"},
		"parts":[{"path":"data/part-00000.parquet","rows":1,"bytes":5,"min_key_base64":"YQ==","max_key_base64":"YQ==","md5":"ab"}]}`
	var m Manifest
	if err := json.Unmarshal([]byte(legacy), &m); err == nil {
		t.Fatalf("format 2 read as %+v", m)
	}
	// extensions.swath without version 1 (an earlier build's, without the
	// member; a later contract's) is refused; one without the extension
	// reads.
	manifest := func(ext string) string {
		return `{"format":"seekparquet","format_version":0,"draft_revision":1,"schema_id":"x/v1","key":{"column":"key","unique":true},` + ext +
			`"parts":[{"path":"data/part-00000.parquet","rows":1,"bytes":5,"min_key_base64":"YQ==","max_key_base64":"YQ=="}]}`
	}
	for _, sw := range []string{`{"published_at":"2026-11-08T18:00:00Z","coverage":{"complete":true}}`, `{"version":2}`} {
		if err := json.Unmarshal([]byte(manifest(`"extensions":{"swath":`+sw+`},`)), &m); err == nil || !strings.Contains(err.Error(), "unsupported swath metadata version") {
			t.Fatalf("extensions.swath %s: %v, want unsupported swath metadata version", sw, err)
		}
	}
	m = Manifest{}
	if err := json.Unmarshal([]byte(manifest("")), &m); err != nil || !m.Current() || m.MetadataVersion != 0 || m.Parts[0].MaxKey != "a" {
		t.Fatalf("a manifest without extensions.swath read as %+v: %v", m, err)
	}
}

// TestManifestLimitsReadSwathsLargest: every manifest swath can write reads
// back. A part entry written at manifestKeyBytes is within its price in
// ManifestLimits (two base64 bounds and 1 KiB), so MaxParts of them and
// 1 MiB for the rest fit: the proof. (The module's default, 64 MiB, would
// refuse 30,000 one-row parts of 1,024-byte keys, about 82 MB.) A manifest
// of 3,000 such parts reads within ManifestLimits, and the same manifest
// is refused under a limit one byte short of it (ErrLimitExceeded).
func TestManifestLimitsReadSwathsLargest(t *testing.T) {
	part := func(i, keyBytes int) ManifestPart {
		lo := []byte(strings.Repeat("\xff", keyBytes))
		copy(lo, fmt.Sprintf("%08d", 2*i))
		hi := slices.Clone(lo)
		copy(hi, fmt.Sprintf("%08d", 2*i+1))
		return ManifestPart{Path: DataDir + "/" + PartName(i),
			PartInfo: PartInfo{Rows: 1 << 40, Bytes: 1 << 40, MinKey: Key(lo), MaxKey: Key(hi), SHA256: strings.Repeat("f", 64)}}
	}
	manifest := func(parts ...ManifestPart) []byte {
		t.Helper()
		b, err := json.Marshal(Manifest{Format: seekparquet.Format, DraftRevision: int(seekparquet.DraftRevision), SchemaID: "x/v1",
			Key: seekparquet.Key{Column: KeyColumn, Unique: true}, Coverage: Coverage{Exclusions: []string{}, Filters: []string{}}, Parts: parts})
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	entry := int64(len(manifest(part(0, manifestKeyBytes), part(1, manifestKeyBytes))) - len(manifest(part(0, manifestKeyBytes))))
	price := 2*int64(base64.StdEncoding.EncodedLen(manifestKeyBytes)) + 1<<10
	if entry > price || MaxParts*price+1<<20 > ManifestLimits.ManifestBytes || ManifestLimits.Parts < MaxParts {
		t.Fatalf("a part entry at %d-byte keys is %d bytes, priced %d; ManifestLimits %+v", manifestKeyBytes, entry, price, ManifestLimits)
	}
	// Written as JSON directly (MarshalManifest would check it again).
	head := manifest()
	i := bytes.Index(head, []byte(`"parts":[]`))
	if i < 0 {
		t.Fatalf("no empty parts member in %s", head)
	}
	var b bytes.Buffer
	b.Write(head[:i])
	b.WriteString(`"parts":[`)
	for n := range 3_000 {
		p := part(n, 1024)
		if n > 0 {
			b.WriteByte(',')
		}
		b.WriteString(`{"path":"` + p.Path + `","rows":1,"bytes":1,"min_key_base64":"` + base64.StdEncoding.EncodeToString([]byte(p.MinKey)) +
			`","max_key_base64":"` + base64.StdEncoding.EncodeToString([]byte(p.MaxKey)) + `","sha256":"` + p.SHA256 + `"}`)
	}
	b.WriteByte(']')
	b.Write(head[i+len(`"parts":[]`):])
	var m Manifest
	if err := m.UnmarshalJSON(b.Bytes()); err != nil || len(m.Parts) != 3_000 {
		t.Fatalf("3,000 parts: %d read, %v", len(m.Parts), err)
	}
	if _, err := seekparquet.ParseManifest(b.Bytes(), seekparquet.Limits{ManifestBytes: int64(b.Len() - 1)}); !errors.Is(err, seekparquet.ErrLimitExceeded) {
		t.Fatalf("%d bytes under a limit of %d: %v", b.Len(), b.Len()-1, err)
	}
}

// TestManifestObjectBytes: extensions.swath.committed_object_bytes is
// written when known, 0 included, and left out when unknown; it reads back
// as written, and a manifest read and written again keeps it byte for byte.
// A part's own count stays out of the manifest's parts (SeekParquet's
// members only).
func TestManifestObjectBytes(t *testing.T) {
	n := func(v int64) *int64 { return &v }
	for _, want := range []*int64{nil, n(0), n(12345)} {
		m := Manifest{Format: seekparquet.Format, FormatVersion: int(seekparquet.FormatVersion), DraftRevision: int(seekparquet.DraftRevision), SchemaID: id,
			Key: seekparquet.Key{Column: KeyColumn, Unique: true}, Source: Source{Kind: "s3", URI: "s3://b/"}, Scope: Scope{Kind: "whole"},
			Coverage: Coverage{Complete: true, StopReason: "completed", Exclusions: []string{}, Filters: []string{}}, CommittedObjectBytes: want,
			Parts: []ManifestPart{{Path: "data/part-00000.parquet", PartInfo: PartInfo{Rows: 2, Bytes: 9, MinKey: "a", MaxKey: "b", SHA256: strings.Repeat("0123456789abcdef", 4)}}}}
		b, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		var raw struct {
			Extensions map[string]map[string]json.RawMessage `json:"extensions"`
			Parts      []map[string]json.RawMessage          `json:"parts"`
		}
		if err := json.Unmarshal(b, &raw); err != nil {
			t.Fatal(err)
		}
		got, has := raw.Extensions["swath"]["committed_object_bytes"]
		if has != (want != nil) || has && string(got) != fmt.Sprint(*want) {
			t.Fatalf("%v: committed_object_bytes %s (present %v)", want, got, has)
		}
		if _, has := raw.Parts[0]["object_bytes"]; has {
			t.Fatalf("a part carries object_bytes: %s", b)
		}
		var back Manifest
		if err := json.Unmarshal(b, &back); err != nil {
			t.Fatal(err)
		}
		if (back.CommittedObjectBytes == nil) != (want == nil) || want != nil && *back.CommittedObjectBytes != *want {
			t.Fatalf("%v: read back %v", want, back.CommittedObjectBytes)
		}
		if tot := back.Totals(); tot.Rows != 2 || tot.Bytes != 9 || (tot.CommittedObjectBytes == nil) != (want == nil) {
			t.Fatalf("totals %+v", tot)
		}
		again, err := json.Marshal(back)
		if err != nil || !bytes.Equal(again, b) {
			t.Fatalf("rewritten: %v\n%s\n%s", err, again, b)
		}
	}
}

// TestManifestFilesystem: extensions.swath.filesystem is written when the
// source measured it, zero free space included, and left out otherwise; it
// reads back as written, and a manifest read and written again keeps it
// byte for byte.
func TestManifestFilesystem(t *testing.T) {
	at := time.Date(2026, 11, 13, 10, 0, 0, 123456000, time.UTC)
	for _, want := range []*FilesystemSpace{nil, {TotalBytes: 1 << 40, FreeBytes: 0, AvailableBytes: 0, MeasuredAt: at}, {TotalBytes: 100, FreeBytes: 500, AvailableBytes: 80, MeasuredAt: at}} {
		meta := Meta{SchemaID: id, Source: Source{Kind: "local-fs", URI: "/data"}, Scope: Scope{Kind: "whole"},
			Coverage: Coverage{Complete: true, StopReason: "completed"}, Filesystem: want}
		m := newManifest(meta, nil, func(int, Part) string { return "" })
		b, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		var raw struct {
			Extensions map[string]map[string]json.RawMessage `json:"extensions"`
		}
		if err := json.Unmarshal(b, &raw); err != nil {
			t.Fatal(err)
		}
		got, has := raw.Extensions["swath"]["filesystem"]
		if has != (want != nil) {
			t.Fatalf("%+v: filesystem %s (present %v)", want, got, has)
		}
		if want != nil {
			wantJSON := fmt.Sprintf(`{"total_bytes":%d,"free_bytes":%d,"available_bytes":%d,"measured_at":"2026-11-13T10:00:00.123456Z"}`, want.TotalBytes, want.FreeBytes, want.AvailableBytes)
			if string(got) != wantJSON {
				t.Fatalf("filesystem %s, want %s", got, wantJSON)
			}
		}
		var back Manifest
		if err := json.Unmarshal(b, &back); err != nil {
			t.Fatal(err)
		}
		if (back.Filesystem == nil) != (want == nil) || want != nil && *back.Filesystem != *want {
			t.Fatalf("%+v: read back %+v", want, back.Filesystem)
		}
		again, err := json.Marshal(back)
		if err != nil || !bytes.Equal(again, b) {
			t.Fatalf("rewritten: %v\n%s\n%s", err, again, b)
		}
	}
}

// TestObjectBytes: a dataset's object bytes are its parts' counts summed,
// unknown when one part has none; a dataset of no part holds 0 when its
// schema has a size column.
func TestObjectBytes(t *testing.T) {
	n := func(v int64) *int64 { return &v }
	str := func(v *int64) string {
		if v == nil {
			return "nil"
		}
		return fmt.Sprint(*v)
	}
	for _, c := range []struct {
		parts []Part
		sized bool
		want  string
	}{
		{nil, true, "0"},
		{nil, false, "nil"},
		{[]Part{{ObjectBytes: n(0)}}, true, "0"},
		{[]Part{{ObjectBytes: n(3)}, {ObjectBytes: n(4)}}, true, "7"},
		{[]Part{{ObjectBytes: n(3)}, {}}, true, "nil"}, // a retained part of an earlier build's
		{[]Part{{}, {}}, false, "nil"},
	} {
		if got := str(ObjectBytes(c.parts, c.sized)); got != c.want {
			t.Errorf("%+v sized %v: %s, want %s", c.parts, c.sized, got, c.want)
		}
	}
}
