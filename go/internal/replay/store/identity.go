// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"os"
	"path/filepath"

	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/go/internal/replay/legacy/format2"
	"github.com/varveio/swath/go/internal/replay/legacy/javaera"
)

// identityDomain prefixes every fixture identity digest. A Java-era
// fixture's identity is the one the Java replay server reports for it, so
// startup lines and metrics of both servers name the same fixture alike.
const identityDomain = "swath-replay-fixture-identity-v1"

// javaEraIdentity is the restart identity of a resolved Java-era fixture:
// SHA-256 over the domain, the part count (4-byte big-endian), and per part
// in order its name relative to the fixture directory (or the file's own
// directory) with "/" separators, its size and modification time in
// nanoseconds (8-byte big-endian each), and its stamp's record spelling;
// strings are length-prefixed (4-byte big-endian) UTF-8. Relative names keep
// the identity across a copy that preserves size, time and stamps; it is not
// a content digest, since fixtures stay immutable while served.
func javaEraIdentity(path string, files []string, stamps []javaera.Stamp) (string, error) {
	root, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	if info, err := os.Stat(root); err != nil {
		return "", err
	} else if !info.IsDir() {
		root = filepath.Dir(root)
	}
	digest := sha256.New()
	digest.Write([]byte(identityDomain))
	digest.Write(binary.BigEndian.AppendUint32(nil, uint32(len(files))))
	for i, file := range files {
		stamp := "unstamped"
		if i < len(stamps) {
			stamp = stamps[i].String()
		}
		if err := identityFile(digest, root, file, stamp); err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

// seekIdentity is format2Identity's digest over a SeekParquet dataset: the
// manifest's SHA-256, then every listed part with its format version,
// draft revision and schema id in the stamp's place.
func seekIdentity(dir string, meta seekparquet.Metadata) (string, error) {
	root, err := filepath.Abs(dir)
	if err != nil {
		return "", err
	}
	manifest := sha256.Sum256(meta.Manifest)
	digest := sha256.New()
	digest.Write([]byte(identityDomain))
	digest.Write(binary.BigEndian.AppendUint32(nil, uint32(1+len(meta.Parts))))
	if err := identityFile(digest, root, filepath.Join(root, format2.ManifestName), "sha256:"+hex.EncodeToString(manifest[:])); err != nil {
		return "", err
	}
	stamp := fmt.Sprintf("seekparquet/%d/%d/%s", meta.Version.FormatVersion, meta.Version.DraftRevision, meta.SchemaID)
	for _, part := range meta.Parts {
		if err := identityFile(digest, root, filepath.Join(root, filepath.FromSlash(part.Path)), stamp); err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

// format2Identity is the same digest over manifest.json, with the SHA-256 of
// its bytes in the stamp's place, then every listed part, with its footer
// profile, version and schema id in the stamp's place: the manifest is the
// commit, so its digest pins the dataset.
func format2Identity(dir string, dataset *format2.Dataset) (string, error) {
	root, err := filepath.Abs(dir)
	if err != nil {
		return "", err
	}
	manifest := sha256.Sum256(dataset.ManifestBytes)
	digest := sha256.New()
	digest.Write([]byte(identityDomain))
	digest.Write(binary.BigEndian.AppendUint32(nil, uint32(1+len(dataset.Manifest.Parts))))
	if err := identityFile(digest, root, filepath.Join(root, format2.ManifestName), "sha256:"+hex.EncodeToString(manifest[:])); err != nil {
		return "", err
	}
	stamp := fmt.Sprintf("%s/%d/%s", format2.Format, format2.Version, dataset.Manifest.SchemaID)
	for _, part := range dataset.Manifest.Parts {
		if err := identityFile(digest, root, filepath.Join(root, filepath.FromSlash(part.Path)), stamp); err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

func identityFile(digest hash.Hash, root, file, stamp string) error {
	abs, err := filepath.Abs(file)
	if err != nil {
		return err
	}
	rel, err := filepath.Rel(root, abs)
	if err != nil {
		return err
	}
	info, err := os.Stat(abs)
	if err != nil {
		return err
	}
	identityField(digest, filepath.ToSlash(rel))
	digest.Write(binary.BigEndian.AppendUint64(nil, uint64(info.Size())))
	digest.Write(binary.BigEndian.AppendUint64(nil, uint64(info.ModTime().UnixNano())))
	identityField(digest, stamp)
	return nil
}

func identityField(digest hash.Hash, value string) {
	digest.Write(binary.BigEndian.AppendUint32(nil, uint32(len(value))))
	digest.Write([]byte(value))
}
