// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package sortout

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/varveio/swath/tools/swath-local-spike/pqout"
	"github.com/varveio/swath/tools/swath-local-spike/record"
)

// SummaryName is swath-local's own run report. It deliberately avoids
// Swath's reserved _swath_summary.json, whose schema Varve's reader checks.
const SummaryName = "_swath_local_summary.json"

// ownEntries are the only root entries a swath-local dataset owns; --overwrite
// removes these and nothing else (plus their .tmp siblings).
var ownEntries = []string{"data", "_staging", "manifest.json", "_SUCCESS", SummaryName,
	"_swath_summary.json" /* pre-rename spike output */, ".swath-state.json", "symlink.txt"}

// Prepare validates out against root and creates <out>/data.
//
//   - out may not be root or an ancestor of root (cleaned absolute paths and
//     device/inode identity of root's ancestors, physical and as given).
//   - A non-empty out is refused unless overwrite, and overwrite is refused
//     unless out holds a previous swath-local dataset (manifest.json with our
//     schemaId, or _SUCCESS plus data/) and out is not inside root.
//   - Overwrite removes only the dataset's own entries, never other content.
//
// root must already be validated as an existing directory.
func Prepare(out, root string, overwrite bool) error {
	absOut, err := filepath.Abs(out)
	if err != nil {
		return err
	}
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	if absOut == absRoot || isUnder(absRoot, absOut) {
		return fmt.Errorf("--out %s is the listed root or one of its ancestors", out)
	}
	outFi, statErr := os.Stat(absOut)
	if statErr == nil {
		if same, err := ancestorOrSelfIs(absRoot, outFi); err != nil {
			return err
		} else if same {
			return fmt.Errorf("--out %s is the listed root or one of its ancestors (same directory identity)", out)
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return statErr
	}
	if overwrite {
		if inside, err := insideRoot(absRoot, absOut); err != nil {
			return err
		} else if inside {
			return fmt.Errorf("refusing --overwrite: --out %s is inside the listed root", out)
		}
	}
	ents, err := os.ReadDir(absOut)
	if err == nil && len(ents) > 0 {
		if !overwrite {
			return fmt.Errorf("%s is not empty (use --overwrite to replace a previous swath-local dataset)", out)
		}
		if !isOwnDataset(absOut) {
			return fmt.Errorf("refusing --overwrite: %s does not hold a previous swath-local dataset", out)
		}
		for _, name := range ownEntries {
			for _, n := range []string{name, name + ".tmp"} {
				if err := os.RemoveAll(filepath.Join(absOut, n)); err != nil {
					return err
				}
			}
		}
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return os.MkdirAll(filepath.Join(absOut, "data"), 0o755)
}

// isUnder reports whether path is strictly below dir (cleaned absolute paths).
func isUnder(path, dir string) bool {
	rel, err := filepath.Rel(dir, path)
	return err == nil && rel != "." && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// ancestorOrSelfIs reports whether target is root or any ancestor of root,
// by directory identity, along both the given and the symlink-resolved path.
func ancestorOrSelfIs(root string, target os.FileInfo) (bool, error) {
	chains := []string{root}
	if real, err := filepath.EvalSymlinks(root); err == nil && real != root {
		chains = append(chains, real)
	}
	for _, p := range chains {
		for cur := p; ; {
			if fi, err := os.Stat(cur); err == nil && os.SameFile(fi, target) {
				return true, nil
			}
			parent := filepath.Dir(cur)
			if parent == cur {
				break
			}
			cur = parent
		}
	}
	return false, nil
}

// LocateInside reports whether out (which must exist) lies inside root, by
// walking out's physical ancestors and comparing directory identity with
// root. rel is out's '/'-separated path below root (its key without the
// trailing slash).
func LocateInside(root, out string) (rel string, inside bool, err error) {
	rootFi, err := os.Stat(root)
	if err != nil {
		return "", false, err
	}
	cur, err := filepath.EvalSymlinks(out)
	if err != nil {
		return "", false, err
	}
	var parts []string
	for {
		fi, err := os.Stat(cur)
		if err != nil {
			return "", false, err
		}
		if os.SameFile(fi, rootFi) {
			if len(parts) == 0 {
				return "", false, nil // out is root itself: handled by Prepare's check
			}
			slices.Reverse(parts)
			return strings.Join(parts, "/"), true, nil
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			return "", false, nil
		}
		parts = append(parts, filepath.Base(cur))
		cur = parent
	}
}

// insideRoot reports whether out (existing or not) lies below root, judged by
// directory identity of out's nearest existing ancestor-or-self.
func insideRoot(root, out string) (bool, error) {
	a := out
	for {
		if _, err := os.Lstat(a); err == nil {
			break
		}
		parent := filepath.Dir(a)
		if parent == a {
			return false, nil
		}
		a = parent
	}
	if a != out {
		rfi, err := os.Stat(root)
		if err != nil {
			return false, err
		}
		if afi, err := os.Stat(a); err == nil && os.SameFile(afi, rfi) {
			return true, nil // out would be created directly or deeper under root
		}
	}
	_, inside, err := LocateInside(root, a)
	return inside, err
}

func isOwnDataset(out string) bool {
	if b, err := os.ReadFile(filepath.Join(out, "manifest.json")); err == nil {
		var m struct {
			SchemaID string `json:"schemaId"`
		}
		if json.Unmarshal(b, &m) == nil && m.SchemaID == record.SchemaID {
			return true
		}
	}
	_, e1 := os.Stat(filepath.Join(out, "_SUCCESS"))
	fi, e2 := os.Stat(filepath.Join(out, "data"))
	return e1 == nil && e2 == nil && fi.IsDir()
}

// CleanupFailed removes a failed run's scratch: _staging/ and unpublished
// data/*.parquet.tmp parts. Nothing was published (no _SUCCESS).
func CleanupFailed(out string) {
	_ = os.RemoveAll(filepath.Join(out, "_staging"))
	if tmps, err := filepath.Glob(filepath.Join(out, "data", "*.parquet.tmp")); err == nil {
		for _, t := range tmps {
			_ = os.Remove(t)
		}
	}
}

// ManifestFile mirrors Swath's consumer manifest files[] entry.
type ManifestFile struct {
	Key         string `json:"key"`
	Size        int64  `json:"size"`
	MD5checksum string `json:"MD5checksum"`
	RowCount    int64  `json:"rowCount"`
	MinKey      string `json:"minKey"`
	MaxKey      string `json:"maxKey"`
}

// Manifest mirrors io.varve.swath.output.parquet.Manifest (field order kept),
// plus additive local fields after files[].
type Manifest struct {
	SourceBucket      string         `json:"sourceBucket"`
	Version           string         `json:"version"`
	CreationTimestamp int64          `json:"creationTimestamp"`
	FileFormat        string         `json:"fileFormat"`
	FileSchema        string         `json:"fileSchema"`
	Sorted            bool           `json:"sorted"`
	SortKey           *string        `json:"sortKey"`
	Files             []ManifestFile `json:"files"`
	// Additive, swath-local only.
	SchemaID   string `json:"schemaId"`
	SourceKind string `json:"sourceKind"`
	SourceRoot string `json:"sourceRoot"`
	TotalRows  int64  `json:"totalRows"`
}

// Publish renames parts into place in name order, then writes manifest.json,
// .swath-state.json, symlink.txt, _swath_local_summary.json and finally _SUCCESS,
// each atomically (tmp, fsync, rename, directory fsync).
func Publish(out, root string, args []string, parts []pqout.PartInfo, summary any) (Manifest, error) {
	dataDir := filepath.Join(out, "data")
	sortKey := pqout.SortOrderValue
	m := Manifest{
		SourceBucket: "", Version: "1", CreationTimestamp: time.Now().UnixMilli(), FileFormat: "Parquet",
		FileSchema: pqout.MessageString(), Sorted: true, SortKey: &sortKey, Files: []ManifestFile{},
		SchemaID: record.SchemaID, SourceKind: "local-fs", SourceRoot: root,
	}
	var symlink strings.Builder
	for i, p := range parts {
		final := filepath.Join(dataDir, PartName(i))
		if err := os.Rename(p.Path, final); err != nil {
			return m, err
		}
		key := "data/" + PartName(i)
		m.Files = append(m.Files, ManifestFile{Key: key, Size: p.Bytes, MD5checksum: p.MD5, RowCount: p.Rows, MinKey: p.MinKey, MaxKey: p.MaxKey})
		m.TotalRows += p.Rows
		symlink.WriteString(key + "\n")
	}
	if err := syncDir(dataDir); err != nil {
		return m, err
	}
	if err := atomicJSON(out, "manifest.json", m); err != nil {
		return m, err
	}
	h := sha256.Sum256([]byte(strings.Join(args, "\x00")))
	state := map[string]any{"args_hash": hex.EncodeToString(h[:]), "run_id": nil}
	if err := atomicJSON(out, ".swath-state.json", state); err != nil {
		return m, err
	}
	if err := atomicWrite(out, "symlink.txt", []byte(symlink.String())); err != nil {
		return m, err
	}
	if err := atomicJSON(out, SummaryName, summary); err != nil {
		return m, err
	}
	return m, atomicWrite(out, "_SUCCESS", nil)
}

func atomicJSON(dir, name string, v any) error {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return err
	}
	return atomicWrite(dir, name, b.Bytes())
}

func atomicWrite(dir, name string, data []byte) error {
	tmp := filepath.Join(dir, name+".tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	if err = errors.Join(err, f.Close()); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, name)); err != nil {
		return err
	}
	return syncDir(dir)
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	// Directory fsync is unsupported on some platforms (Windows); not fatal there.
	if err != nil && isUnsupportedDirSync(err) {
		err = nil
	}
	return errors.Join(err, d.Close())
}
