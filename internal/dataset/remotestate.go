// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package dataset

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/varveio/swath/internal/faults"
)

// A remote dataset's work-directory state is read and written only through
// the locked work directory's os.Root, as recovery's other paths are: no
// path of it leaves the directory, and every directory and file is checked
// without following a link (NeedIn) before it is read, appended to,
// replaced or synced. A link, or a file where a directory belongs, is
// refused (ErrUnsafeLayout).

// Paths of the remote state, relative to the work directory.
var (
	RemoteRel    = filepath.Join(Workspace, RemoteDir)
	ClaimsRel    = filepath.Join(Workspace, RemoteDir, ClaimsDir)
	AllocatedRel = filepath.Join(Workspace, RemoteDir, AllocatedName)
	CandidateRel = filepath.Join(Workspace, RemoteDir, CandidateName)
)

// claimRel is part i's claim receipt.
func claimRel(i int) string { return filepath.Join(ClaimsRel, PartName(i)+".json") }

// CheckRemoteLayout checks the remote state of the work directory r, by
// lstat: the remote and claims directories are directories, the allocation
// log, the candidate and the commit marker regular files, where they
// exist, and every entry of the claims directory a regular file.
func CheckRemoteLayout(r *os.Root) error {
	for _, c := range []struct {
		rel string
		dir bool
	}{{RemoteRel, true}, {ClaimsRel, true}, {AllocatedRel, false}, {CandidateRel, false}, {RemoteCommitName, false}} {
		if _, err := NeedIn(r, c.rel, c.dir, true); err != nil {
			return err
		}
	}
	d, err := r.Open(ClaimsRel)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	ents, err := d.ReadDir(-1)
	d.Close()
	if err != nil {
		return err
	}
	for _, e := range ents {
		if _, err := NeedIn(r, filepath.Join(ClaimsRel, e.Name()), false, false); err != nil {
			return err
		}
	}
	return nil
}

// remoteDirs checks the remote and claims directories before a mutation.
func remoteDirs(r *os.Root) error {
	for _, d := range []string{Workspace, RemoteRel, ClaimsRel} {
		if _, err := NeedIn(r, d, true, false); err != nil {
			return err
		}
	}
	return nil
}

// PrepareWorkspaceIn makes the work directory r a remote run's workspace:
// the remote and claims directories, created through r and synced with
// their parents.
func PrepareWorkspaceIn(r *os.Root) error {
	if _, err := NeedIn(r, Workspace, true, false); err != nil {
		return err
	}
	for _, d := range []string{RemoteRel, ClaimsRel} {
		if err := r.Mkdir(d, 0o755); err != nil && !errors.Is(err, fs.ErrExist) {
			return fmt.Errorf("create work directory: %w", err)
		}
		if _, err := NeedIn(r, d, true, false); err != nil {
			return err
		}
	}
	for _, d := range []string{ClaimsRel, RemoteRel, Workspace, "."} {
		if err := SyncDirIn(r, d); err != nil {
			return fmt.Errorf("sync work directory: %w", err)
		}
	}
	return nil
}

// ReplaceIn replaces rel, under r, with data durably: a temporary file of
// its own beside it (created exclusively), synced, renamed over rel, and
// the directory synced, all through r. The directory must be a directory
// and an existing rel a regular file, by lstat.
func ReplaceIn(r *os.Root, rel string, data []byte) error {
	dir := filepath.Dir(rel)
	if _, err := NeedIn(r, dir, true, false); err != nil {
		return err
	}
	if _, err := NeedIn(r, rel, false, true); err != nil {
		return err
	}
	var rnd [8]byte
	_, _ = rand.Read(rnd[:])
	tmp := filepath.Join(dir, "."+filepath.Base(rel)+"."+hex.EncodeToString(rnd[:])+".tmp")
	f, err := r.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("write %s: %w", filepath.Base(rel), err)
	}
	_, err = faults.Write(f, data)
	if err == nil {
		err = faults.Sync(f)
	}
	if err = errors.Join(err, f.Close()); err == nil {
		err = r.Rename(tmp, rel)
	}
	if err != nil {
		_ = r.Remove(tmp)
		return fmt.Errorf("write %s: %w", filepath.Base(rel), err)
	}
	if err := SyncDirIn(r, dir); err != nil {
		return fmt.Errorf("sync after %s: %w", filepath.Base(rel), err)
	}
	return nil
}

// readIn reads rel under r, a regular file by lstat (nil, nil when
// absent).
func readIn(r *os.Root, rel string) ([]byte, error) {
	fi, err := NeedIn(r, rel, false, true)
	if err != nil || fi == nil {
		return nil, err
	}
	f, err := OpenRegularIn(r, rel, os.O_RDONLY)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(f)
}

// Allocations is the allocation log: each part name is durable in it
// before its upload begins ("A\tNAME"), and each upload's creation
// identity once the upload completes ("C\tNAME\tGENERATION"), so cleanup
// deletes only what this run created, by the generation it created, and
// recovery knows every name the run may have written, without listing the
// bucket. A line is acknowledged once synced; a torn last line is
// truncated (and synced) when the log is opened, before anything is
// appended.
type Allocations struct {
	mu sync.Mutex
	f  *os.File
}

// OpenAllocations opens the allocation log of the work directory r for
// appending, creating it if absent.
func OpenAllocations(r *os.Root) (*Allocations, error) {
	if err := remoteDirs(r); err != nil {
		return nil, fmt.Errorf("allocation log: %w", err)
	}
	fi, err := NeedIn(r, AllocatedRel, false, true)
	if err != nil {
		return nil, fmt.Errorf("allocation log: %w", err)
	}
	var f *os.File
	if fi == nil {
		f, err = r.OpenFile(AllocatedRel, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
		if err == nil {
			err = SyncDirIn(r, RemoteRel)
		}
	} else {
		f, err = OpenRegularIn(r, AllocatedRel, os.O_RDWR)
	}
	if err != nil {
		if f != nil {
			f.Close()
		}
		return nil, fmt.Errorf("allocation log: %w", err)
	}
	b, err := io.ReadAll(f)
	if err == nil {
		if keep := int64(bytes.LastIndexByte(b, '\n') + 1); keep < int64(len(b)) {
			// A torn last line (an append that never completed): cut it, and
			// make the cut durable, before the next line follows it.
			err = f.Truncate(keep)
			if err == nil {
				err = faults.Sync(f)
			}
		}
	}
	if err == nil {
		_, err = f.Seek(0, io.SeekEnd)
	}
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("allocation log: %w", err)
	}
	return &Allocations{f: f}, nil
}

func (a *Allocations) append(line string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, err := faults.Write(a.f, []byte(line)); err != nil {
		return fmt.Errorf("allocation log: %w", err)
	}
	if err := faults.Sync(a.f); err != nil {
		return fmt.Errorf("allocation log: %w", err)
	}
	return nil
}

// Allocate records name, durably, before its upload begins.
func (a *Allocations) Allocate(name string) error { return a.append("A\t" + name + "\n") }

// Created records, durably, that this run created name at identity.
func (a *Allocations) Created(name, identity string) error {
	return a.append("C\t" + name + "\t" + identity + "\n")
}

// Close closes the log.
func (a *Allocations) Close() error {
	if a == nil {
		return nil
	}
	return a.f.Close()
}

// Ledger is what the allocation log says: the names allocated, in order,
// each once, and the identity each completed upload created.
type Ledger struct {
	Names   []string
	Created map[string]string
}

// ReadAllocations reads the allocation log of the work directory r. A torn
// last line was never acknowledged and is left out; a line of another
// shape is corruption.
func ReadAllocations(r *os.Root) (Ledger, error) {
	l := Ledger{Created: map[string]string{}}
	b, err := readIn(r, AllocatedRel)
	if err != nil {
		return l, fmt.Errorf("allocation log: %w", err)
	}
	b = b[:bytes.LastIndexByte(b, '\n')+1]
	seen := map[string]bool{}
	for line := range strings.SplitSeq(strings.TrimSuffix(string(b), "\n"), "\n") {
		if line == "" {
			continue
		}
		f := strings.Split(line, "\t")
		switch {
		case len(f) == 2 && f[0] == "A" && f[1] != "":
			if !seen[f[1]] {
				seen[f[1]] = true
				l.Names = append(l.Names, f[1])
			}
		case len(f) == 3 && f[0] == "C" && seen[f[1]] && f[2] != "":
			l.Created[f[1]] = f[2]
		default:
			return l, fmt.Errorf("allocation log: a damaged line %q", line)
		}
	}
	return l, nil
}

// Evidence reports whether the work directory r holds remote state that
// only a run whose journal header was durable could have written: an
// allocated name, a claim receipt or a candidate manifest.
func Evidence(r *os.Root) (bool, error) {
	if err := CheckRemoteLayout(r); err != nil {
		return false, err
	}
	l, err := ReadAllocations(r)
	if err != nil || len(l.Names) > 0 {
		return true, err
	}
	claims, err := ReadClaims(r)
	if err != nil || len(claims) > 0 {
		return true, err
	}
	cand, err := ReadCandidate(r)
	return cand != nil, err
}

// WriteClaim makes part's receipt durable (replacing an earlier one of its
// ordinal: a reproduction of the same bytes).
func WriteClaim(r *os.Root, c ClaimReceipt) error {
	if err := faults.At("claim-receipt"); err != nil {
		return err
	}
	if err := remoteDirs(r); err != nil {
		return fmt.Errorf("claim part %d: %w", c.Ordinal, err)
	}
	b, err := EncodeJSON(c)
	if err == nil {
		err = ReplaceIn(r, claimRel(c.Ordinal), b)
	}
	if err != nil {
		return fmt.Errorf("claim part %d: %w", c.Ordinal, err)
	}
	return faults.At("claimed")
}

// ReadClaims returns the work directory's receipts by ordinal. A temporary
// file of an interrupted replacement is not a receipt; a name of another
// shape, a link, or a receipt that does not parse or names another
// ordinal is corruption.
func ReadClaims(r *os.Root) (map[int]ClaimReceipt, error) {
	fi, err := NeedIn(r, ClaimsRel, true, true)
	if err != nil || fi == nil {
		return nil, err
	}
	d, err := r.Open(ClaimsRel)
	if err != nil {
		return nil, err
	}
	ents, err := d.ReadDir(-1)
	d.Close()
	if err != nil {
		return nil, err
	}
	out := map[int]ClaimReceipt{}
	for _, e := range ents {
		name := e.Name()
		if strings.HasPrefix(name, ".") && strings.HasSuffix(name, ".tmp") {
			continue
		}
		i, ok := PartOrdinal(strings.TrimSuffix(name, ".json"))
		if !ok || !strings.HasSuffix(name, ".json") {
			return nil, fmt.Errorf("claim receipts: unexpected entry %s in %s", name, ClaimsRel)
		}
		b, err := readIn(r, filepath.Join(ClaimsRel, name))
		if err != nil {
			return nil, err
		}
		var c ClaimReceipt
		if err := json.Unmarshal(b, &c); err != nil || c.Ordinal != i || c.Object == "" {
			return nil, fmt.Errorf("claim receipts: %s is damaged", name)
		}
		out[i] = c
	}
	return out, nil
}

// SaveCandidate makes the candidate manifest durable.
func SaveCandidate(r *os.Root, b []byte) error {
	if err := remoteDirs(r); err != nil {
		return err
	}
	return ReplaceIn(r, CandidateRel, b)
}

// ReadCandidate returns the candidate manifest saved in the work directory
// r (nil when there is none).
func ReadCandidate(r *os.Root) ([]byte, error) { return readIn(r, CandidateRel) }

// RemoteCommitRecord is the commit marker: the run in the work directory
// committed its dataset. Finalized says the commit's aftermath is done:
// the orphans this run created deleted, the run report put beside the
// manifest; a resume of a run committed but not finalized finishes it.
type RemoteCommitRecord struct {
	Destination      string `json:"destination"`
	ManifestIdentity string `json:"manifest_identity,omitempty"`
	ManifestSHA256   string `json:"manifest_sha256"`
	Finalized        bool   `json:"finalized"`
}

// WriteCommitMarker records a commit in the work directory r, durably.
func WriteCommitMarker(r *os.Root, rec RemoteCommitRecord) error {
	b, err := EncodeJSON(rec)
	if err != nil {
		return err
	}
	return ReplaceIn(r, RemoteCommitName, b)
}

// ReadCommitMarker returns the work directory's commit marker (nil when
// there is none).
func ReadCommitMarker(r *os.Root) (*RemoteCommitRecord, error) {
	b, err := readIn(r, RemoteCommitName)
	if err != nil || b == nil {
		return nil, err
	}
	return parseMarker(b)
}

// ErrBadMarker: the work directory's commit marker is not one: its JSON
// is not a commit record (another file written at its name).
var ErrBadMarker = errors.New("the commit marker is not a commit record")

// parseMarker reads a commit record strictly: these members only, a
// destination, and a manifest SHA-256 of 64 lowercase hex digits.
func parseMarker(b []byte) (*RemoteCommitRecord, error) {
	var rec RemoteCommitRecord
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&rec); err != nil {
		return nil, fmt.Errorf("%s: %w: %v", RemoteCommitName, ErrBadMarker, err)
	}
	if _, err := hex.DecodeString(rec.ManifestSHA256); err != nil || len(rec.ManifestSHA256) != 64 || strings.ToLower(rec.ManifestSHA256) != rec.ManifestSHA256 || rec.Destination == "" {
		return nil, fmt.Errorf("%s: %w", RemoteCommitName, ErrBadMarker)
	}
	return &rec, nil
}

// markerAt reads dir's commit marker by pathname (a read-only look).
func markerAt(dir string) (*RemoteCommitRecord, error) {
	b, err := os.ReadFile(filepath.Join(dir, RemoteCommitName))
	if err != nil {
		return nil, err
	}
	return parseMarker(b)
}

// Unfinalized reports whether dir is the work directory of a remote run
// committed and not finalized (a read-only look, for the command's
// early refusal of a complete run).
func Unfinalized(dir string) bool {
	rec, err := markerAt(dir)
	return err == nil && !rec.Finalized
}

// CommittedCandidate returns the manifest the work directory dir committed:
// its candidate, when the commit marker records that candidate's SHA-256
// (a read-only look by pathname, for the command's account of a complete
// run).
func CommittedCandidate(dir string) ([]byte, error) {
	r, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	rec, err := ReadCommitMarker(r)
	if err != nil || rec == nil {
		return nil, errors.Join(err, fs.ErrNotExist)
	}
	b, err := ReadCandidate(r)
	if err != nil || b == nil {
		return nil, errors.Join(err, fs.ErrNotExist)
	}
	if sum := sha256.Sum256(b); hex.EncodeToString(sum[:]) != rec.ManifestSHA256 {
		return nil, fmt.Errorf("%s: the candidate is not the manifest the marker records", CandidateRel)
	}
	return b, nil
}
