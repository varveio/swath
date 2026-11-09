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

	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/go/internal/faults"
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

const id = "swath-local/v0"

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
// under extensions.swath (the parts' MD5 digests included), and reads back
// as it was written; a format 2 manifest of an earlier build is refused.
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
		if got := slices.Sorted(maps.Keys(ext["swath"])); len(ext) != 1 || !slices.Equal(got, []string{"coverage", "published_at", "scope", "source"}) {
			t.Fatalf("extensions %s", raw["extensions"])
		}
		var m Manifest
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatal(err)
		}
		if !m.Current() || m.Key != (seekparquet.Key{Column: "key", Unique: !groups}) || m.Coverage.StopReason != "completed" || m.Source.Container != "b" ||
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
	// An earlier build's SeekParquet manifest: MD5 digests under the
	// extension, none in the parts.
	earlier := `{"format":"seekparquet","format_version":0,"draft_revision":1,"schema_id":"x/v1","key":{"column":"key","unique":true},
		"extensions":{"swath":{"published_at":"2026-11-08T18:00:00Z","source":{"kind":"s3","uri":"s3://b/","container":"b"},"scope":{"kind":"whole"},
		"coverage":{"complete":true,"stop_reason":"completed"},"part_md5":["0123456789abcdef0123456789abcdef"]}},
		"parts":[{"path":"data/part-00000.parquet","rows":1,"bytes":5,"min_key_base64":"YQ==","max_key_base64":"YQ=="}]}`
	if err := json.Unmarshal([]byte(earlier), &m); err != nil || !m.Current() || !m.Coverage.Complete || m.Parts[0].SHA256 != "" || m.Parts[0].MaxKey != "a" {
		t.Fatalf("an earlier build's manifest read as %+v: %v", m, err)
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
