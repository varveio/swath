// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package dataset

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	seekparquet "github.com/varveio/seekparquet/go"
	"google.golang.org/api/googleapi"

	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/objfile"
)

// A remote dataset is written straight to an object store: its parts are
// uploaded as they close, each under a name no other run or attempt uses,
// and the manifest, created only if absent, is the commit. Nothing is
// renamed, overwritten or listed. Its local workspace (the work directory)
// keeps what a resume needs: the lock, the journal and spill (Workspace),
// and under RemoteDir the names allocated before their uploads began, a
// claim receipt for each part the journal records or is about to, and the
// candidate manifest. The commit marker (RemoteCommitName) records the
// commit in the work directory.

// RemoteLayout is the remote layout's version (object names, receipts,
// commit protocol), frozen in a resume's identity.
const RemoteLayout = "swath/remote-1"

// Work-directory names of a remote dataset.
const (
	// RemoteDir, inside the workspace, holds the remote dataset's state.
	RemoteDir = "remote"
	// AllocatedName is the log of part names allocated, one a line, each
	// durable before its upload begins.
	AllocatedName = "allocated"
	// ClaimsDir holds the claim receipts (ClaimReceipt), one a part.
	ClaimsDir = "claims"
	// CandidateName is the manifest a commit creates, saved first.
	CandidateName = "manifest.candidate.json"
	// RemoteCommitName, in the work directory, records a commit
	// (RemoteCommitRecord): the run is complete.
	RemoteCommitName = "commit.json"
)

// RemotePartName is the object name of part i of attempt of run, relative
// to the dataset root: data/RUN/ATTEMPT/part-%05d.parquet. Two runs, and two
// attempts of one run, never share a name, so a part is never replaced and
// a losing committer's parts never touch the winner's.
func RemotePartName(run, attempt string, i int) string {
	return DataDir + "/" + run + "/" + attempt + "/" + PartName(i)
}

// Remote is a remote dataset's root: its location, the endpoint it is
// reached at ("" for the provider's default), and a reader and a writer of
// the names under its prefix.
type Remote struct {
	Location objfile.Location
	Endpoint string
	Opener   objfile.Opener
	Writer   objfile.Writer
}

// URI is the root's canonical URI, and " at " its endpoint when set.
func (r *Remote) URI() string {
	if r.Endpoint != "" {
		return r.Location.String() + " at " + r.Endpoint
	}
	return r.Location.String()
}

func remotePath(ws string, name ...string) string {
	return filepath.Join(append([]string{ws, Workspace, RemoteDir}, name...)...)
}

// CandidatePath is where the candidate manifest is saved.
func CandidatePath(ws string) string { return remotePath(ws, CandidateName) }

// ClaimPathOf is part i's claim receipt.
func ClaimPathOf(ws string, i int) string { return remotePath(ws, ClaimsDir, PartName(i)+".json") }

// PrepareWorkspace makes ws (a work directory Usable allowed) a remote
// run's workspace: the claims directory, synced with its parents.
func PrepareWorkspace(ws string) error {
	if err := os.MkdirAll(remotePath(ws, ClaimsDir), 0o755); err != nil {
		return fmt.Errorf("create work directory: %w", err)
	}
	if err := errors.Join(syncDir(remotePath(ws)), syncDir(filepath.Join(ws, Workspace)), syncDir(ws), syncDir(filepath.Dir(ws))); err != nil {
		return fmt.Errorf("sync work directory: %w", err)
	}
	return nil
}

// Allocations is the log of part names allocated: each name is durable in
// it before its upload begins, so recovery and cleanup know every name a
// run may have created, without listing the bucket.
type Allocations struct {
	mu     sync.Mutex
	f      *os.File
	synced bool // the file's entry is durable
}

// OpenAllocations opens ws's allocation log for appending.
func OpenAllocations(ws string) (*Allocations, error) {
	f, err := os.OpenFile(remotePath(ws, AllocatedName), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("allocation log: %w", err)
	}
	return &Allocations{f: f}, nil
}

// Allocate appends name and returns once it is durable.
func (a *Allocations) Allocate(name string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, err := faults.Write(a.f, []byte(name+"\n")); err != nil {
		return fmt.Errorf("allocation log: %w", err)
	}
	if err := faults.Sync(a.f); err != nil {
		return fmt.Errorf("allocation log: %w", err)
	}
	if !a.synced {
		if err := syncDir(filepath.Dir(a.f.Name())); err != nil {
			return fmt.Errorf("allocation log: %w", err)
		}
		a.synced = true
	}
	return nil
}

// Close closes the log.
func (a *Allocations) Close() error {
	if a == nil {
		return nil
	}
	return a.f.Close()
}

// ReadAllocations returns the names ws's log holds, in order, each once. A
// last line without its newline was never acknowledged (its upload never
// began) and is left out.
func ReadAllocations(ws string) ([]string, error) {
	b, err := os.ReadFile(remotePath(ws, AllocatedName))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("allocation log: %w", err)
	}
	if i := bytes.LastIndexByte(b, '\n'); i+1 < len(b) {
		b = b[:i+1] // a torn last line
	}
	var names []string
	seen := map[string]bool{}
	for line := range strings.SplitSeq(string(b), "\n") {
		if line != "" && !seen[line] {
			seen[line] = true
			names = append(names, line)
		}
	}
	return names, nil
}

// UploadStats account a remote dataset's uploads.
type UploadStats struct {
	Uploads atomic.Int64 // completed
	Aborted atomic.Int64 // ended without an object
	Bytes   atomic.Int64 // bytes handed to uploads
	// WaitNs is the time encoders spent in upload writes and completions:
	// a write blocks while the previous chunk is in flight.
	WaitNs  atomic.Int64
	Retries atomic.Int64

	mu      sync.Mutex
	classes map[string]int64
}

// Retried counts one retried attempt of err (objfile/gcs WriteOptions.Retried).
func (s *UploadStats) Retried(_ string, err error) {
	s.Retries.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.classes == nil {
		s.classes = map[string]int64{}
	}
	s.classes[FailureClass(err)]++
}

// Classes are the retried attempts by FailureClass.
func (s *UploadStats) Classes() map[string]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.classes) == 0 {
		return nil
	}
	m := make(map[string]int64, len(s.classes))
	for k, v := range s.classes {
		m[k] = v
	}
	return m
}

// FailureClass names a store failure for a report: http_STATUS for a
// service answer, timeout, connection, or other.
func FailureClass(err error) string {
	var ge *googleapi.Error
	var ne net.Error
	switch {
	case errors.As(err, &ge):
		return "http_" + strconv.Itoa(ge.Code)
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.As(err, &ne) && ne.Timeout():
		return "timeout"
	case errors.As(err, &ne), errors.Is(err, io.ErrUnexpectedEOF):
		return "connection"
	}
	return "other"
}

// RemoteParts makes a remote dataset's part artifacts (parquetout's
// Options.Create): each part's name is allocated durably, then uploaded
// CreateOnly under the run's and the attempt's names.
type RemoteParts struct {
	Ctx          context.Context // cancelled when the run fails: uploads in flight are aborted
	Remote       *Remote
	Run, Attempt string
	Alloc        *Allocations
	Stats        *UploadStats
}

// Create starts part ordinal's upload.
func (rp *RemoteParts) Create(ordinal int) (Artifact, error) {
	name := RemotePartName(rp.Run, rp.Attempt, ordinal)
	if err := rp.Alloc.Allocate(name); err != nil {
		return nil, fmt.Errorf("create part: %w", err)
	}
	if err := faults.At("allocated"); err != nil {
		return nil, err
	}
	u, err := rp.Remote.Writer.NewWriter(rp.Ctx, name, objfile.CreateOnly)
	if err != nil {
		return nil, fmt.Errorf("create part: %w", err)
	}
	return &remoteArtifact{u: u, name: name, stats: rp.Stats}, nil
}

// uploadFaultEvery spaces the upload-chunk crash point: at an upload's
// first write, then each time its bytes cross a multiple of it.
const uploadFaultEvery = 64 << 10

// remoteArtifact is a part's upload as the part writer sees it: Sync
// completes the upload, Close aborts it unless Sync completed it.
type remoteArtifact struct {
	u        objfile.Upload
	name     string
	identity string
	stats    *UploadStats
	n, next  int64
	done     bool
}

func (a *remoteArtifact) Name() string     { return a.name }
func (a *remoteArtifact) Identity() string { return a.identity }

func (a *remoteArtifact) Write(p []byte) (int, error) {
	if a.n+int64(len(p)) >= a.next {
		a.next += uploadFaultEvery
		if err := faults.At("upload-chunk"); err != nil {
			return 0, err
		}
	}
	t := time.Now()
	n, err := a.u.Write(p)
	a.stats.WaitNs.Add(int64(time.Since(t)))
	a.stats.Bytes.Add(int64(n))
	a.n += int64(n)
	return n, err
}

func (a *remoteArtifact) Sync() error {
	t := time.Now()
	err := a.u.Close()
	a.stats.WaitNs.Add(int64(time.Since(t)))
	if err != nil {
		return fmt.Errorf("upload %s: %w", a.name, err)
	}
	r, _ := a.u.Receipt()
	a.done, a.identity = true, r.Identity
	a.stats.Uploads.Add(1)
	return faults.At("upload-completed")
}

func (a *remoteArtifact) Close() error {
	if a.done {
		return nil
	}
	a.stats.Aborted.Add(1)
	return a.u.Abort()
}

// ClaimReceipt is a part's claim, durable before the journal record that
// names it: the object, its identity and its PartInfo, and what a resume
// needs to reproduce it byte for byte when that record was lost (its rows
// cut, columns and created_by). Receipts are kept until the commit.
type ClaimReceipt struct {
	Ordinal   int      `json:"ordinal"`
	Object    string   `json:"object"`
	Identity  string   `json:"identity"`
	Part      PartInfo `json:"part"`
	Columns   []string `json:"columns"`
	CreatedBy string   `json:"created_by"`
}

// WriteClaim makes part's receipt durable (replacing an earlier one of its
// ordinal: a reproduction of the same bytes).
func WriteClaim(ws string, c ClaimReceipt) error {
	if err := faults.At("claim-receipt"); err != nil {
		return err
	}
	if err := ReplaceJSON(ClaimPathOf(ws, c.Ordinal), c, Durable); err != nil {
		return fmt.Errorf("claim part %d: %w", c.Ordinal, err)
	}
	return faults.At("claimed")
}

// ReadClaims returns ws's receipts by ordinal. A temporary file of an
// interrupted replacement is not a receipt; a name of another shape, or a
// receipt that does not parse or names another ordinal, is corruption.
func ReadClaims(ws string) (map[int]ClaimReceipt, error) {
	dir := remotePath(ws, ClaimsDir)
	ents, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
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
		if !ok || !strings.HasSuffix(name, ".json") || !e.Type().IsRegular() {
			return nil, fmt.Errorf("claim receipts: unexpected entry %s in %s", name, dir)
		}
		b, err := os.ReadFile(filepath.Join(dir, name))
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

// newManifest is the manifest of parts (already gated), part i at
// path(i, part), published now.
func newManifest(meta Meta, parts []Part, path func(i int, p Part) string) Manifest {
	m := Manifest{Format: seekparquet.Format, FormatVersion: int(seekparquet.FormatVersion), DraftRevision: int(seekparquet.DraftRevision), SchemaID: meta.SchemaID,
		Key: seekparquet.Key{Column: KeyColumn, Unique: !meta.Groups}, Source: meta.Source, Scope: meta.Scope, Coverage: meta.Coverage, Parts: []ManifestPart{}}
	if m.Coverage.Exclusions == nil {
		m.Coverage.Exclusions = []string{}
	}
	if m.Coverage.Filters == nil {
		m.Coverage.Filters = []string{}
	}
	for i, p := range parts {
		m.Parts = append(m.Parts, ManifestPart{Path: path(i, p), PartInfo: p.PartInfo})
	}
	m.PublishedAt = time.Now().UTC()
	return m
}

// Commit outcomes of a remote dataset (RemoteCommit.Outcome).
const (
	// CommitCreated: the manifest's creation was answered.
	CommitCreated = "created"
	// CommitConfirmed: the creation's answer was lost or refused, and the
	// manifest read back is the candidate: this run created it.
	CommitConfirmed = "confirmed"
)

// RemoteCommit is a remote commit's account.
type RemoteCommit struct {
	Outcome  string // CommitCreated or CommitConfirmed once committed
	Identity string // the manifest's generation
	SHA256   string // the manifest's bytes
	// MarkerErr is the commit marker's write failure: the dataset stands,
	// but a resume of the work directory learns of the commit by reading
	// the manifest back.
	MarkerErr error
}

// ErrLostRace: another run created the manifest first, with other
// content. Nothing of it is touched; this run's parts are orphans.
var ErrLostRace = errors.New("another run committed a different dataset at the destination first")

// LostRaceError is ErrLostRace with the winner's manifest, whose parts
// cleanup must not touch.
type LostRaceError struct{ Winner []byte }

func (e *LostRaceError) Error() string { return ErrLostRace.Error() }
func (e *LostRaceError) Unwrap() error { return ErrLostRace }

// ErrCommitUnresolved: the manifest's creation was neither confirmed nor
// refuted (the creation failed and the read-back failed too). Nothing is
// removed; a resume reads it back again.
var ErrCommitUnresolved = errors.New("the manifest's creation could not be confirmed or refuted")

// ManifestState is what the destination's manifest is, against a
// candidate.
type ManifestState int

const (
	ManifestUnknown ManifestState = iota // the read failed
	ManifestAbsent
	ManifestOurs  // byte for byte the candidate
	ManifestOther // another manifest
)

// ResolveManifest reads the destination's manifest and compares it with
// candidate: its state, and its bytes when it is another (bounded by
// ManifestLimits).
func ResolveManifest(ctx context.Context, r *Remote, candidate []byte) (ManifestState, []byte, string, error) {
	obj, err := r.Opener.Open(ctx, ManifestName)
	if errors.Is(err, fs.ErrNotExist) {
		return ManifestAbsent, nil, "", nil
	}
	if err != nil {
		return ManifestUnknown, nil, "", err
	}
	defer obj.Close()
	if obj.Size() > ManifestLimits.ManifestBytes {
		return ManifestOther, nil, obj.Identity(), nil
	}
	b, err := io.ReadAll(io.NewSectionReader(obj.WithContext(ctx), 0, obj.Size()))
	if err != nil {
		return ManifestUnknown, nil, "", err
	}
	if bytes.Equal(b, candidate) {
		return ManifestOurs, nil, obj.Identity(), nil
	}
	return ManifestOther, b, obj.Identity(), nil
}

// PublishRemote commits parts (already gated, uploaded and, when the run
// is journaled, recorded) at r: the manifest naming them by their object
// names is saved durably in ws as the candidate, then created at the
// destination only if no manifest is there. That creation is the commit;
// commit, if set, runs it (install) as Publish's does. A creation whose
// answer is lost or refused is resolved by reading the manifest back:
// the candidate is this run's commit, another manifest a lost race
// (*LostRaceError), and a read that fails ErrCommitUnresolved. The
// manifest is never replaced or deleted. Once committed, the commit
// marker is written in ws (its failure is RemoteCommit.MarkerErr).
func PublishRemote(ctx context.Context, ws string, r *Remote, meta Meta, parts []Part, commit func(install func() error) error) (RemoteCommit, error) {
	var rc RemoteCommit
	m := newManifest(meta, parts, func(_ int, p Part) string { return p.Path })
	b, err := EncodeJSON(m)
	if err != nil {
		return rc, err
	}
	sum := sha256.Sum256(b)
	rc.SHA256 = hex.EncodeToString(sum[:])
	if err := Replace(CandidatePath(ws), b, Durable); err != nil {
		return rc, fmt.Errorf("save candidate manifest: %w", err)
	}
	if err := faults.At("candidate-saved"); err != nil {
		return rc, err
	}
	install := func() error {
		if err := faults.At("manifest-put"); err != nil {
			return err
		}
		rec, perr := r.Writer.PutIfAbsent(ctx, ManifestName, b)
		if perr == nil {
			rc.Outcome, rc.Identity = CommitCreated, rec.Identity
		} else {
			state, other, id, rerr := ResolveManifest(ctx, r, b)
			switch state {
			case ManifestOurs:
				rc.Outcome, rc.Identity = CommitConfirmed, id
			case ManifestOther:
				return &LostRaceError{Winner: other}
			case ManifestAbsent:
				if errors.Is(perr, objfile.ErrExists) {
					// Refused as existing, and gone when read: not ours to
					// judge.
					return fmt.Errorf("%w: the creation was refused as existing (%v) and no manifest is there now", ErrCommitUnresolved, perr)
				}
				return fmt.Errorf("create manifest: %w", perr) // not created: a resume commits again
			default:
				return fmt.Errorf("%w: create: %v; read back: %v", ErrCommitUnresolved, perr, rerr)
			}
		}
		if err := faults.At("manifest-created"); err != nil {
			return err
		}
		rc.MarkerErr = WriteCommitMarker(ws, RemoteCommitRecord{Destination: r.URI(), ManifestIdentity: rc.Identity, ManifestSHA256: rc.SHA256})
		return nil
	}
	if commit == nil {
		commit = func(install func() error) error { return install() }
	}
	if err := commit(install); err != nil {
		return rc, err
	}
	return rc, faults.At("manifest")
}

// RemoteCommitRecord is the commit marker: the run in the work directory
// committed its dataset.
type RemoteCommitRecord struct {
	Destination      string    `json:"destination"`
	ManifestIdentity string    `json:"manifest_identity,omitempty"`
	ManifestSHA256   string    `json:"manifest_sha256"`
	CommittedAt      time.Time `json:"committed_at"`
}

// WriteCommitMarker records a commit in ws, durably.
func WriteCommitMarker(ws string, rec RemoteCommitRecord) error {
	if rec.CommittedAt.IsZero() {
		rec.CommittedAt = time.Now().UTC()
	}
	return ReplaceJSON(filepath.Join(ws, RemoteCommitName), rec, Durable)
}

// ReadCandidate returns the candidate manifest saved in ws (nil when
// there is none).
func ReadCandidate(ws string) ([]byte, error) {
	b, err := os.ReadFile(CandidatePath(ws))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	return b, err
}

// CleanupStats account orphan cleanup.
type CleanupStats struct {
	Deleted, Gone, Failed int64
}

// DeleteOwned deletes the objects at names (names this run allocated)
// that keep does not hold: each at the generation it is found at, so an
// object written there since is never removed. A name with no object is
// counted gone; a failure is counted and left.
func DeleteOwned(ctx context.Context, r *Remote, names []string, keep map[string]bool) CleanupStats {
	var st CleanupStats
	for _, n := range names {
		if keep[n] {
			continue
		}
		obj, err := r.Opener.Open(ctx, n)
		if errors.Is(err, fs.ErrNotExist) {
			st.Gone++
			continue
		}
		if err != nil {
			st.Failed++
			continue
		}
		id := obj.Identity()
		_ = obj.Close()
		switch err := r.Writer.DeleteIf(ctx, n, id); {
		case err == nil:
			st.Deleted++
		case errors.Is(err, fs.ErrNotExist):
			st.Gone++
		default:
			st.Failed++
		}
	}
	return st
}

// ManifestParts are the part paths of a manifest's bytes (nil if they do
// not parse).
func ManifestParts(b []byte) map[string]bool {
	var m Manifest
	if json.Unmarshal(b, &m) != nil {
		return nil
	}
	keep := map[string]bool{}
	for _, p := range m.Parts {
		keep[p.Path] = true
	}
	return keep
}

// VerifyObject checks a retained remote part against its record: the
// object at name must still be the version recorded (identity), and its
// bytes, read through that pinned version as a stream, must have the
// recorded size and SHA-256. Existence or an identity alone is not enough.
// progress, if set, is told the bytes read.
func VerifyObject(ctx context.Context, r *Remote, name, identity string, want PartInfo, progress func(int64)) error {
	obj, err := r.Opener.Open(ctx, name)
	if err != nil {
		return err
	}
	defer obj.Close()
	if identity != "" && obj.Identity() != identity {
		return fmt.Errorf("%w: %s is version %s; the journal recorded %s", ErrRemoteAltered, name, obj.Identity(), identity)
	}
	h := sha256.New()
	rd := obj.WithContext(ctx)
	buf := make([]byte, 4<<20)
	var off int64
	for {
		n, err := rd.ReadAt(buf, off)
		h.Write(buf[:n])
		off += int64(n)
		if progress != nil {
			progress(int64(n))
		}
		if err == io.EOF || off >= obj.Size() {
			break
		}
		if err != nil {
			return err
		}
	}
	if got := hex.EncodeToString(h.Sum(nil)); off != want.Bytes || got != want.SHA256 {
		return fmt.Errorf("%w: %s is %d bytes, sha256 %s; the journal says %d bytes, sha256 %s", ErrRemoteAltered, name, off, got, want.Bytes, want.SHA256)
	}
	return nil
}

// ErrRemoteAltered: a retained remote part is not the object its record
// names.
var ErrRemoteAltered = errors.New("retained remote part does not match its journal record")
