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
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
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
// reached at ("" for the provider's default), its region (S3's, as
// resolved; "" for GCS), and a reader and a writer of the names under its
// prefix.
type Remote struct {
	Location objfile.Location
	Endpoint string
	Region   string
	Opener   objfile.Opener
	Writer   objfile.Writer
	// Emulator says Endpoint is an emulator's host, reached without
	// credentials (STORAGE_EMULATOR_HOST, resolved when it was opened).
	Emulator bool
}

// URI is the root's canonical URI, and " at " its endpoint when set.
func (r *Remote) URI() string {
	if r.Endpoint != "" {
		return r.Location.String() + " at " + r.Endpoint
	}
	return r.Location.String()
}

// UploadStats account a remote dataset's uploads.
type UploadStats struct {
	Uploads atomic.Int64 // completed
	Aborted atomic.Int64 // ended without an object
	// AbortsUnconfirmed are aborts the store did not confirm (an S3
	// multipart upload whose abort failed or that is still listed: its
	// parts stay until an abort succeeds or the bucket's lifecycle rule
	// removes them). GCS and Azure aborts send nothing to confirm.
	AbortsUnconfirmed atomic.Int64
	Bytes             atomic.Int64 // bytes handed to uploads
	// WaitNs is the time encoders spent in upload writes and completions:
	// a write blocks while the previous chunk is in flight.
	WaitNs  atomic.Int64
	Retries atomic.Int64

	mu      sync.Mutex
	classes map[string]int64
}

// Retried counts one retried attempt of err (the Retried of the
// objfile/gcs, objfile/s3 and objfile/azblob WriteOptions).
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
// service answer (GCS's, an AWS SDK response error, or an azcore one),
// timeout, connection, or other.
func FailureClass(err error) string {
	var ge *googleapi.Error
	var ae *azcore.ResponseError
	var re interface{ HTTPStatusCode() int }
	var ne net.Error
	switch {
	case errors.As(err, &ge):
		return "http_" + strconv.Itoa(ge.Code)
	case errors.As(err, &ae):
		return "http_" + strconv.Itoa(ae.StatusCode)
	case errors.As(err, &re) && re.HTTPStatusCode() > 0:
		return "http_" + strconv.Itoa(re.HTTPStatusCode())
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
	return &remoteArtifact{u: u, name: name, stats: rp.Stats, alloc: rp.Alloc}, nil
}

// uploadFaultEvery spaces the upload-chunk crash point: at an upload's
// first write, then each time its bytes cross a multiple of it.
const uploadFaultEvery = 64 << 10

// remoteArtifact is a part's upload as the part writer sees it: Sync
// completes the upload, Close aborts it unless Sync completed it.
type remoteArtifact struct {
	u        objfile.Upload
	alloc    *Allocations
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
	// The creation identity, durable: cleanup deletes this generation and
	// no other (an object at this name that a record does not name is
	// never proven ours).
	if err := a.alloc.Created(a.name, r.Identity); err != nil {
		return fmt.Errorf("upload %s: %w", a.name, err)
	}
	return faults.At("upload-completed")
}

func (a *remoteArtifact) Close() error {
	if a.done {
		return nil
	}
	a.stats.Aborted.Add(1)
	err := a.u.Abort()
	if err != nil {
		a.stats.AbortsUnconfirmed.Add(1)
	}
	return err
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

// ErrCandidateMismatch: the work directory holds a candidate manifest an
// earlier attempt tried to create, naming other parts than this attempt
// would commit (its journal lost the final record, and a part was written
// again). That creation may still be pending, so the candidate is never
// replaced: the run is refused, as unresolved, for the operator to settle
// (by reading the destination's manifest once no request can be pending).
var ErrCandidateMismatch = errors.New("the saved candidate manifest names other parts than this attempt would commit")

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

// ResolveManifest reads the destination's manifest, whole in one request
// (objfile.ReadObject), and compares it with candidate: its state, and its
// bytes when it is another (bounded by ManifestLimits).
func ResolveManifest(ctx context.Context, r *Remote, candidate []byte) (ManifestState, []byte, string, error) {
	b, id, err := objfile.ReadObject(ctx, r.Opener, ManifestName, ManifestLimits.ManifestBytes)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return ManifestAbsent, nil, "", nil
	case errors.Is(err, objfile.ErrTooLarge):
		return ManifestOther, nil, id, nil
	case err != nil:
		return ManifestUnknown, nil, "", err
	case bytes.Equal(b, candidate):
		return ManifestOurs, nil, id, nil
	}
	return ManifestOther, b, id, nil
}

// PublishRemote commits parts (already gated, uploaded and, when the run
// is journaled, recorded) at r: the manifest naming them by their object
// names is saved durably in the work directory ws as the candidate, then
// created at the destination only if no manifest is there. That creation
// is the commit; commit, if set, runs it (install) as Publish's does.
//
// prior is a candidate an earlier attempt saved: when it names exactly
// these parts, its bytes are created again (the same manifest, its
// publication time included), so a creation still in flight from that
// attempt and this one are the same commit; when it names other parts the
// commit is refused (ErrCandidateMismatch, unresolved), never replaced.
//
// A creation that is not answered with success is resolved by reading the
// manifest back: the candidate's bytes are this run's commit; other bytes
// a lost race (*LostRaceError). Absence settles nothing after an
// unresolved failure (objfile.ErrUnresolved: a request may still be in
// flight): the same bytes are created again, a few times, and if that
// never resolves the commit is ErrCommitUnresolved. Only a creation the
// store refused outright (absent afterwards) is no commit. A read that
// fails is ErrCommitUnresolved too. Nothing is removed on the way: the
// manifest is never replaced or deleted, and the parts and the candidate
// stay. Once committed, the commit marker is written in ws, not finalized
// (its failure is RemoteCommit.MarkerErr).
func PublishRemote(ctx context.Context, ws *os.Root, r *Remote, meta Meta, parts []Part, prior []byte, commit func(install func() error) error) (RemoteCommit, error) {
	var rc RemoteCommit
	b := prior
	switch {
	case b != nil && !sameParts(b, parts):
		// An earlier attempt's creation of other bytes may still land: this
		// run cannot prove it will not, so it never replaces the candidate.
		return rc, fmt.Errorf("%w: %w", ErrCommitUnresolved, ErrCandidateMismatch)
	case b == nil:
		var err error
		if b, err = EncodeJSON(newManifest(meta, parts, func(_ int, p Part) string { return p.Path })); err != nil {
			return rc, err
		}
	}
	sum := sha256.Sum256(b)
	rc.SHA256 = hex.EncodeToString(sum[:])
	if err := SaveCandidate(ws, b); err != nil {
		return rc, fmt.Errorf("save candidate manifest: %w", err)
	}
	if err := faults.At("candidate-saved"); err != nil {
		return rc, err
	}
	install := func() error {
		if err := faults.At("manifest-put"); err != nil {
			return err
		}
		outcome, id, err := createManifest(ctx, r, b)
		if err != nil {
			return err
		}
		rc.Outcome, rc.Identity = outcome, id
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

// createAttempts bounds the creations of one candidate in one commit.
const createAttempts = 3

// createManifest creates the manifest b, resolving every failure by a
// read-back (PublishRemote). Uncertainty is kept across attempts: once any
// creation failed ambiguously, a later refusal does not prove that the
// earlier one will not land, so the outcome stays unresolved unless the
// manifest is read back.
func createManifest(ctx context.Context, r *Remote, b []byte) (outcome, identity string, err error) {
	rec, perr := r.Writer.PutIfAbsent(ctx, ManifestName, b)
	if perr == nil {
		return CommitCreated, rec.Identity, nil
	}
	uncertain := ambiguous(perr)
	for attempt := 1; ; attempt++ {
		state, other, id, rerr := ResolveManifest(ctx, r, b)
		switch state {
		case ManifestOurs:
			return CommitConfirmed, id, nil
		case ManifestOther:
			return "", "", &LostRaceError{Winner: other}
		case ManifestUnknown:
			return "", "", fmt.Errorf("%w: create: %v; read back: %v", ErrCommitUnresolved, perr, objfile.RedactError(rerr))
		}
		// Absent. A conflict with another conditional write created
		// nothing and may be made again; a refusal settles the commit only
		// if no creation of these bytes is still possibly in flight.
		if !uncertain && !errors.Is(perr, objfile.ErrConflict) {
			return "", "", fmt.Errorf("create manifest: %w", perr)
		}
		if attempt >= createAttempts || ctx.Err() != nil {
			return "", "", fmt.Errorf("%w: the creation failed (%v) and no manifest is there yet; it may still appear", ErrCommitUnresolved, perr)
		}
		// The same bytes again: whichever creation lands, it is this one.
		if rec, perr = r.Writer.PutIfAbsent(ctx, ManifestName, b); perr == nil {
			return CommitCreated, rec.Identity, nil
		}
		uncertain = uncertain || ambiguous(perr)
	}
}

// ambiguous reports whether a failed creation may still create its
// object: the Writer says so (objfile.ErrUnresolved), it was refused as
// existing (a retry of its own success, or a racer's), or the error is a
// cancellation, a deadline or a transport failure a Writer did not
// classify. Only a refusal the store answered is definite.
func ambiguous(err error) bool {
	var ne net.Error
	return errors.Is(err, objfile.ErrUnresolved) || errors.Is(err, objfile.ErrExists) ||
		errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) ||
		errors.Is(err, io.ErrUnexpectedEOF) || errors.As(err, &ne)
}

// sameParts reports whether the manifest b names exactly parts.
func sameParts(b []byte, parts []Part) bool {
	var m Manifest
	if json.Unmarshal(b, &m) != nil || len(m.Parts) != len(parts) {
		return false
	}
	for i, p := range parts {
		if m.Parts[i].Path != p.Path || m.Parts[i].PartInfo != p.PartInfo {
			return false
		}
	}
	return true
}

// CleanupStats account orphan cleanup: deleted; gone (no object, or
// another generation there now, which is not this run's); failed; and
// unproven: names allocated with no creation recorded (an upload that
// failed or never completed, or completed unrecorded), which are left.
type CleanupStats struct {
	Deleted, Gone, Failed, Unproven int64
}

// DeleteOwned deletes the objects this run created (l.Created: the name
// and the generation its upload created) that keep does not hold, each
// only at that generation (DeleteIf). An allocated name with no recorded
// creation is never touched: whatever is there is not proven this run's.
func DeleteOwned(ctx context.Context, r *Remote, l Ledger, keep map[string]bool) CleanupStats {
	var st CleanupStats
	for _, n := range l.Names {
		if keep[n] {
			continue
		}
		id, ok := l.Created[n]
		if !ok {
			st.Unproven++
			continue
		}
		switch err := r.Writer.DeleteIf(ctx, n, id); {
		case err == nil:
			st.Deleted++
		case errors.Is(err, fs.ErrNotExist), errors.Is(err, objfile.ErrObjectChanged):
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
// progress, if set, is told the bytes read. The object is opened at the
// recorded size, with no metadata request (objfile.Opener.OpenSized): its
// first read checks that size and pins the version, which must be the
// recorded one before any more is read; another size is ErrRemoteAltered.
func VerifyObject(ctx context.Context, r *Remote, name, identity string, want PartInfo, progress func(int64)) error {
	open := r.Opener.Open
	if want.Bytes > 0 {
		open = func(ctx context.Context, name string) (objfile.Object, error) {
			return r.Opener.OpenSized(ctx, name, want.Bytes)
		}
	}
	obj, err := open(ctx, name)
	if err != nil {
		return err
	}
	defer obj.Close()
	checkIdentity := func() error {
		if identity != "" && obj.Identity() != identity {
			return fmt.Errorf("%w: %s is version %s; the journal recorded %s", ErrRemoteAltered, name, obj.Identity(), identity)
		}
		return nil
	}
	if obj.Identity() != "" { // pinned at open
		if err := checkIdentity(); err != nil {
			return err
		}
	}
	checked := obj.Identity() != ""
	h := sha256.New()
	buf := make([]byte, 4<<20)
	var off int64
	for {
		// Each read has a deadline of its own, its body included: a stalled
		// response fails the verification instead of holding the resume.
		rctx, cancel := context.WithTimeout(ctx, VerifyReadTimeout)
		n, err := obj.WithContext(rctx).ReadAt(buf, off)
		cancel()
		if sm := (*objfile.SizeMismatchError)(nil); errors.As(err, &sm) {
			return fmt.Errorf("%w: %s is %d bytes; the journal says %d", ErrRemoteAltered, name, sm.Actual, want.Bytes)
		}
		if !checked && obj.Identity() != "" {
			if err := checkIdentity(); err != nil {
				return err
			}
			checked = true
		}
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

// VerifyReadTimeout bounds one read of VerifyObject (4 MiB), its response
// body included.
const VerifyReadTimeout = 2 * time.Minute

// ErrRemoteAltered: a retained remote part is not the object its record
// names.
var ErrRemoteAltered = errors.New("retained remote part does not match its journal record")
