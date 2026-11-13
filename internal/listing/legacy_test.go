// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/varveio/swath/internal/dataset"
	"github.com/varveio/swath/internal/dataset/datasettest"
	"github.com/varveio/swath/internal/journal"
)

// legacyLayout turns the output of a run crashed after its third record
// into what a version 6 build (before the claimed-part protocol) left at
// the same point: the recorded, not yet renamed part at its unclaimed
// temporary name, and a version 6 header with the identity (writer recipe
// included) unchanged. It returns the frame offsets of the journal.
func legacyLayout(t *testing.T, out string) []int {
	t.Helper()
	if err := os.Rename(dataset.ClaimPath(out, 2), dataset.TempPath(out, 2)); err != nil {
		t.Fatal(err)
	}
	path := journal.Path(out)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	size := binary.LittleEndian.Uint32(b[:4])
	cur := fmt.Sprintf(`"version":%d`, journal.Version)
	// The same length, so the frame's length stands: JSON's whitespace pads
	// a one-digit version.
	six := `"version":` + strings.Repeat(" ", len(cur)-len(`"version":6`)) + `6`
	hdr := []byte(strings.Replace(string(b[8:8+size]), cur, six, 1))
	if len(hdr) != int(size) || !bytes.Contains(hdr, []byte(six)) {
		t.Fatalf("the header does not hold %s", cur)
	}
	sum := crc32.Update(crc32.Checksum(b[:4], crc32.MakeTable(crc32.Castagnoli)), crc32.MakeTable(crc32.Castagnoli), hdr)
	binary.LittleEndian.PutUint32(b[4:8], sum)
	copy(b[8:], hdr)
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
	var offs []int
	for off := 0; off+8 <= len(b); off += 8 + int(binary.LittleEndian.Uint32(b[off:off+4])) {
		offs = append(offs, off)
	}
	return offs
}

// snapshot is every file under data/ and .swath/parts/ plus the journal,
// by relative path.
func snapshot(t *testing.T, out string) map[string][]byte {
	t.Helper()
	got := map[string][]byte{}
	for _, d := range []string{dataset.DataDir, filepath.Join(dataset.Workspace, dataset.PartsDir)} {
		filepath.WalkDir(filepath.Join(out, d), func(p string, e fs.DirEntry, err error) error {
			if err == nil && !e.IsDir() {
				got[p], _ = os.ReadFile(p)
			}
			return nil
		})
	}
	got["journal"], _ = os.ReadFile(journal.Path(out))
	return got
}

// TestLegacyTemporaryProtocolIsRefusedOrRecovered: a version 6 journal
// (written before the claimed-part protocol) whose last record's part is
// still at its unclaimed temporary name is refused as a version mismatch
// before recovery reads, moves or removes any part or touches the journal:
// with the last record intact (unchanged source), and cut by five bytes or
// lost whole after a row of its part and then the rest of the suffix is
// deleted from the source, at two attempts each. The acknowledged part keeps its bytes,
// no replacement part or record is written and no manifest appears
// (durability review: version 7 recovery would remove that part as unrecorded).
func TestLegacyTemporaryProtocolIsRefusedOrRecovered(t *testing.T) {
	datasettest.RequireJournal(t)
	for _, damage := range []string{"intact", "five-byte truncation", "whole last frame removed"} {
		url, b := crashBackend(t)
		out := filepath.Join(t.TempDir(), "ds")
		crashAtRecorded(t, url, out)
		st, err := journal.Read(out)
		if err != nil || len(st.Records) != 3 {
			t.Fatalf("%s: %v", damage, err)
		}
		acked, prev := st.Records[2].Part, st.Records[1].Part
		offs := legacyLayout(t, out)
		path := journal.Path(out)
		jb, _ := os.ReadFile(path)
		switch damage {
		case "five-byte truncation":
			jb = jb[:len(jb)-5]
		case "whole last frame removed":
			jb = jb[:offs[len(offs)-1]]
		}
		os.WriteFile(path, jb, 0o644)
		before := snapshot(t, out)
		changes := []string{"unchanged"}
		if damage != "intact" {
			changes = []string{"deleted row", "deleted suffix"}
		}
		for _, change := range changes {
			switch change {
			case "deleted row":
				if _, err := b.DeleteObject("bkt", string(acked.MinKey)); err != nil {
					t.Fatal(err)
				}
			case "deleted suffix":
				for i := range 8000 {
					if k := fmt.Sprintf("d%02d/sub%d/obj+%06d", i%9, i%4, i); k > string(prev.MaxKey) {
						b.DeleteObject("bkt", k)
					}
				}
			}
			for attempt := range 2 {
				_, err := Run(context.Background(), crashConfig(url, out, true))
				var ve *journal.VersionError
				if !errors.As(err, &ve) || ve.Found != 6 || ve.Want != journal.Version {
					t.Fatalf("%s, %s, attempt %d: not refused as version 6: %v", damage, change, attempt, err)
				}
				got, err := measure(dataset.TempPath(out, 2))
				if err != nil || got.SHA256 != acked.SHA256 || got.Bytes != acked.Bytes {
					t.Fatalf("%s, %s, attempt %d: the acknowledged part is not kept: %v %+v", damage, change, attempt, err, got)
				}
				after := snapshot(t, out)
				if len(after) != len(before) {
					t.Fatalf("%s, %s, attempt %d: files changed: %d before, %d after", damage, change, attempt, len(before), len(after))
				}
				for p, v := range before {
					if !bytes.Equal(after[p], v) {
						t.Fatalf("%s, %s, attempt %d: %s changed", damage, change, attempt, p)
					}
				}
				for _, p := range []string{filepath.Join(out, dataset.ManifestName), filepath.Join(out, dataset.Workspace, dataset.SuspectDir)} {
					if _, err := os.Stat(p); err == nil {
						t.Fatalf("%s, %s, attempt %d: %s exists", damage, change, attempt, p)
					}
				}
			}
		}
	}
}

// setSourceIdentity rewrites the journal header's source fields in out (the
// frame's length and CRC with it).
func setSourceIdentity(t *testing.T, out string, fields []journal.Field) {
	t.Helper()
	path := journal.Path(out)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	size := binary.LittleEndian.Uint32(b[:4])
	var h journal.Header
	if err := json.Unmarshal(b[8:8+size], &h); err != nil || h.Type != "header" {
		t.Fatalf("header: %v", err)
	}
	h.Identity.Source = fields
	hdr, err := json.Marshal(h)
	if err != nil {
		t.Fatal(err)
	}
	tab := crc32.MakeTable(crc32.Castagnoli)
	frame := binary.LittleEndian.AppendUint32(nil, uint32(len(hdr)))
	frame = binary.LittleEndian.AppendUint32(frame, crc32.Update(crc32.Checksum(frame, tab), tab, hdr))
	frame = append(frame, hdr...)
	if err := os.WriteFile(path, append(frame, b[8+size:]...), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestLegacyS3IdentityIsRefused: a journal an earlier build wrote records
// the S3 identity as the resolved route (source, route, region, fetch
// owner), not the endpoint inputs. Its resume is refused for the endpoint,
// "(not recorded)", nothing synthesized in its place, before recovery reads,
// moves or removes any part: the run crashed with its third part recorded
// and still at its claimed temporary name (which recovery would publish),
// and after two refused attempts that part, data/, the journal and the
// workspace are byte for byte as they were, with no manifest. The same
// dataset with the current identity then resumes to the uninterrupted one.
func TestLegacyS3IdentityIsRefused(t *testing.T) {
	datasettest.RequireJournal(t)
	url := crashBucket(t)
	ref := filepath.Join(t.TempDir(), "ref")
	if _, err := Run(context.Background(), crashConfig(url, ref, false)); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "ds")
	crashAtRecorded(t, url, out)
	st, err := journal.Read(out)
	if err != nil || len(st.Records) != 3 {
		t.Fatalf("%v, %d records", err, len(st.Records))
	}
	current, acked := st.Header.Identity.Source, st.Records[2].Part
	if _, err := os.Stat(dataset.ClaimPath(out, 2)); err != nil {
		t.Fatalf("the recorded part is not at its claimed name: %v", err)
	}
	route := url + "/bkt"
	setSourceIdentity(t, out, []journal.Field{
		{Name: "source", Value: "s3://bkt/"},
		{Name: "route", Value: route, Canonical: route},
		{Name: "region", Value: "us-east-1"},
		{Name: "fetch owner", Value: "false"},
	})
	before := snapshot(t, out)
	for attempt := range 2 {
		_, err := Run(context.Background(), crashConfig(url, out, true))
		var mm *journal.MismatchError
		if !errors.As(err, &mm) || mm.Field != "endpoint" || mm.Stored != "(not recorded)" {
			t.Fatalf("attempt %d: %v, want an endpoint (not recorded) mismatch", attempt, err)
		}
		after := snapshot(t, out)
		if len(after) != len(before) {
			t.Fatalf("attempt %d: files changed: %d before, %d after", attempt, len(before), len(after))
		}
		for p, v := range before {
			if !bytes.Equal(after[p], v) {
				t.Fatalf("attempt %d: %s changed", attempt, p)
			}
		}
		if got, err := measure(dataset.ClaimPath(out, 2)); err != nil || got.SHA256 != acked.SHA256 || got.Bytes != acked.Bytes {
			t.Fatalf("attempt %d: the recorded part is not kept at its claimed name: %v %+v", attempt, err, got)
		}
		if _, err := os.Stat(filepath.Join(out, dataset.ManifestName)); err == nil {
			t.Fatalf("attempt %d: a manifest exists", attempt)
		}
	}
	setSourceIdentity(t, out, current)
	if _, err := Run(context.Background(), crashConfig(url, out, true)); err != nil {
		t.Fatalf("the restored identity does not resume: %v", err)
	}
	sameDataset(t, "legacy refused, then resumed", ref, out)
}
