// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/go/internal/buildinfo"
	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/journal"
	"github.com/varveio/swath/go/internal/parquetout"
	"github.com/varveio/swath/go/internal/source"
	"github.com/varveio/swath/go/objfile"
	"github.com/varveio/swath/go/objfile/gcs"
	"github.com/varveio/swath/go/objfile/remote"
	"github.com/varveio/swath/go/objfile/s3"
)

// RemoteOutput is a dataset written straight to an object store (gs:// or
// s3://): its parts are uploaded as they close and its manifest, created
// only if absent, is the commit. OutputConfig.Dir is then the work
// directory: the lock, the journal, spill, the claim receipts and the
// candidate manifest, never a part.
type RemoteOutput struct {
	Location string // gs://BUCKET[/PREFIX] or s3://BUCKET[/PREFIX]
	// Endpoint is the destination's GCS JSON API base, or its
	// S3-compatible endpoint URL ("": the provider's); frozen.
	Endpoint string
	// CredentialsFile is a GCS destination's credentials ("": ADC);
	// Profile an S3 destination's shared config profile ("": the AWS
	// SDK's chain). Resolved again on resume.
	CredentialsFile, Profile string
	// Region is an S3 destination's region ("": AWS_REGION,
	// AWS_DEFAULT_REGION or the profile's, else us-east-1); the one
	// resolved is frozen.
	Region string
	// ChunkSize is an upload's buffer, sent as one request: a GCS chunk
	// (0: gcs.DefaultChunkSize), an S3 part (0: s3.DefaultPartSize; at
	// least 5 MiB). It is the memory admitted per encoder beside its
	// writer.
	ChunkSize int
	// CommitTimeout bounds the commit (the manifest's creation and its
	// read-back; 0: DefaultCommitTimeout); FinalizeTimeout the aftermath
	// (orphan cleanup, the run report; 0: DefaultFinalizeTimeout). A commit
	// that runs out of time is unresolved, and keeps everything.
	CommitTimeout, FinalizeTimeout time.Duration
	// Open, if set, opens the destination instead of remote.Create (tests:
	// a fake's client); stats takes the uploads' retries.
	Open func(ctx context.Context, stats *dataset.UploadStats) (*dataset.Remote, func() error, error)
}

// Defaults of RemoteOutput's time bounds.
const (
	DefaultCommitTimeout   = 10 * time.Minute
	DefaultFinalizeTimeout = 10 * time.Minute
)

func orDefault(d, def time.Duration) time.Duration {
	if d > 0 {
		return d
	}
	return def
}

// Remote is the report's account of a remote dataset (output.remote).
type Remote struct {
	Destination string `json:"destination"` // the root's URI (and " at " its endpoint)
	WorkDir     string `json:"work_dir"`
	// Commit is the commit's outcome: created, confirmed (its answer lost
	// or refused, the manifest read back as the candidate), resumed (a
	// resume found the commit made and finished its aftermath), lost_race,
	// unresolved, or "" when the run did not reach it.
	Commit           string `json:"commit,omitempty"`
	Manifest         string `json:"manifest,omitempty"` // the manifest's URI once committed
	ManifestIdentity string `json:"manifest_identity,omitempty"`
	ManifestSHA256   string `json:"manifest_sha256,omitempty"`
	// Uploads completed, uploads aborted, bytes handed to uploads, and the
	// encoders' time in upload writes and completions (a write waits for
	// the chunk before it to be sent).
	Uploads        int64 `json:"uploads"`
	UploadsAborted int64 `json:"uploads_aborted"`
	UploadBytes    int64 `json:"upload_bytes"`
	UploadWaitNs   int64 `json:"upload_wait_ns"`
	// Retries are the attempts the SDK retried, by failure class
	// (http_STATUS, timeout, connection, other).
	Retries      int64            `json:"retries"`
	RetryClasses map[string]int64 `json:"retry_classes,omitempty"`
	// ObjectsVerified and VerifiedBytes are a resume's checks of retained
	// parts (pinned, streaming size and SHA-256).
	ObjectsVerified int64 `json:"objects_verified"`
	// Orphans: objects this run created (by the generation its upload
	// recorded) that no committed manifest names, deleted at that
	// generation after the commit (or after a failure without a journal,
	// or a lost race); gone already or replaced since; left (a failed
	// delete); and unproven: allocated names with no recorded creation,
	// never touched.
	OrphansDeleted  int64 `json:"orphans_deleted"`
	OrphansGone     int64 `json:"orphans_gone"`
	OrphansLeft     int64 `json:"orphans_left"`
	OrphansUnproven int64 `json:"orphans_unproven"`
}

// remoteState is a remote dataset's run: the destination, the uploads'
// account and, once the output is owned, the allocation log and the names
// this attempt's parts take.
type remoteState struct {
	cfg          *RemoteOutput
	d            *dataset.Remote
	stats        *dataset.UploadStats
	closeFn      func() error
	alloc        *dataset.Allocations
	run, attempt string
	verified     int64
	commit       dataset.RemoteCommit
	outcome      string
	orphans      dataset.CleanupStats
}

// ErrDestinationExists: a fresh run's destination already holds a
// manifest. Replacing a remote dataset is not supported.
var ErrDestinationExists = errors.New("the destination already holds a dataset (manifest.json); replacing a remote dataset is not supported: choose another prefix")

// ErrOverlap: the destination is inside the listed namespace, or holds it.
var ErrOverlap = errors.New("the destination overlaps the listed source")

// ParseDestination parses a dataset destination URI (objfile.ParseLocation)
// that this build writes: gs:// or s3://; az:// is refused.
func ParseDestination(raw string) (objfile.Location, error) {
	l, err := objfile.ParseLocation(raw)
	if err != nil {
		return l, err
	}
	if l.Scheme != "gs" && l.Scheme != "s3" {
		return l, fmt.Errorf("writing a dataset to %s:// is not supported yet (gs:// and s3:// only)", l.Scheme)
	}
	return l, nil
}

// Check refuses a destination this build does not write
// (ParseDestination), and options of another provider than the
// destination's: a credentials file is GCS's, a profile and a region
// S3's.
func (r *RemoteOutput) Check() error {
	l, err := ParseDestination(r.Location)
	switch {
	case err != nil:
		return err
	case l.Scheme != "gs" && r.CredentialsFile != "":
		return errors.New("a credentials file (--output-gcs-credentials-file) is for a GCS destination")
	case l.Scheme != "s3" && (r.Profile != "" || r.Region != ""):
		return errors.New("a profile and a region (--output-s3-profile, --output-s3-region) are for an S3 destination")
	}
	return nil
}

// remoteLocation parses the destination, its options checked.
func remoteLocation(r *RemoteOutput) (objfile.Location, error) {
	if err := r.Check(); err != nil {
		return objfile.Location{}, err
	}
	return ParseDestination(r.Location)
}

// destinationIdentity is the destination's part of the resume identity:
// its root as opened (the endpoint resolved, the environment's emulator
// included; S3's region resolved).
func destinationIdentity(d *dataset.Remote) *journal.Destination {
	l := d.Location
	return &journal.Destination{Provider: l.Scheme, Bucket: l.Container, Prefix: l.Prefix, Endpoint: d.Endpoint, Emulator: d.Emulator, Region: d.Region,
		Layout: dataset.RemoteLayout}
}

// sourceSchemes are the destination schemes of the source kinds a
// destination can overlap.
var sourceSchemes = map[string]string{"gcs": "gs", "s3": "s3"}

// checkOverlap refuses a destination the listing would list, or one that
// holds the listed namespace: the same provider and bucket, one prefix a
// prefix of the other (a listing prefix is a key prefix, not a
// directory). Endpoints and regions are not compared: the same bucket name
// is refused on any.
func checkOverlap(loc source.Locator, l objfile.Location) error {
	scheme := sourceSchemes[loc.Kind]
	if scheme != l.Scheme || loc.Container != l.Container {
		return nil
	}
	rest, _ := strings.CutPrefix(loc.Root, scheme+"://"+loc.Container)
	src := strings.TrimPrefix(rest, "/")
	if strings.HasPrefix(l.Prefix, src) || strings.HasPrefix(src, l.Prefix) {
		return fmt.Errorf("%w: %s and %s", ErrOverlap, l.String(), loc.Root)
	}
	return nil
}

// openRemote opens the destination's client (no request yet).
func openRemote(ctx context.Context, r *RemoteOutput) (*remoteState, error) {
	rs := &remoteState{cfg: r, stats: &dataset.UploadStats{}}
	var err error
	if r.Open != nil {
		rs.d, rs.closeFn, err = r.Open(ctx, rs.stats)
		return rs, err
	}
	l, err := remoteLocation(r)
	if err != nil {
		return nil, err
	}
	// The route is resolved once (the flag, else for GCS the environment's
	// emulator; S3 never takes one from the environment) and given to the
	// client explicitly: the identity freezes it, so a resume under another
	// environment is refused.
	endpoint, emulator := r.Endpoint, false
	if l.Scheme == "gs" {
		endpoint, emulator = remote.ResolveEndpoint(r.Endpoint)
	}
	dst, err := remote.Create(ctx, r.Location, remote.CreateOptions{CredentialsFile: r.CredentialsFile, Endpoint: endpoint, Emulator: emulator,
		Region: r.Region, Profile: r.Profile,
		Write:   gcs.WriteOptions{ChunkSize: r.ChunkSize, Retried: rs.stats.Retried},
		S3Write: s3.WriteOptions{PartSize: int64(r.ChunkSize), Retried: rs.stats.Retried}})
	if err != nil {
		return nil, fmt.Errorf("destination: %w", err)
	}
	rs.d = &dataset.Remote{Location: l, Endpoint: endpoint, Emulator: emulator, Region: dst.Region(), Opener: dst, Writer: dst.Writer()}
	rs.closeFn = dst.Close
	return rs, nil
}

func (rs *remoteState) close() {
	if rs == nil {
		return
	}
	_ = rs.alloc.Close()
	if rs.closeFn != nil {
		_ = rs.closeFn()
	}
}

// uploadBytes is the memory admitted per encoder for its upload: one
// chunk, or one part.
func uploadBytes(r *RemoteOutput) int64 {
	switch {
	case r.ChunkSize > 0:
		return int64(r.ChunkSize)
	case strings.HasPrefix(r.Location, "s3://"):
		return s3.DefaultPartSize
	}
	return gcs.DefaultChunkSize
}

// checkFresh refuses a fresh run whose destination holds a manifest, within
// the commit's time bound.
func (rs *remoteState) checkFresh(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, orDefault(rs.cfg.CommitTimeout, DefaultCommitTimeout))
	defer cancel()
	state, _, _, err := dataset.ResolveManifest(ctx, rs.d, nil)
	switch state {
	case dataset.ManifestAbsent:
		return nil
	case dataset.ManifestUnknown:
		return fmt.Errorf("destination: reading %smanifest.json: %w", rs.d.Location.String(), err)
	}
	return fmt.Errorf("%s: %w", rs.d.URI(), ErrDestinationExists)
}

// preflight is a resume's first look at the destination, within the
// commit's time bound, when the run saved a candidate manifest (it reached
// its commit): the destination's
// manifest is the candidate (the commit happened: the marker is written
// now, not finalized), another one (a lost race: this run's own uploads
// are deleted, as the commit's loser deletes them), absent (the commit is
// made again, with the same candidate), or unknown (unresolved).
func (rs *remoteState) preflight(ctx context.Context, r *os.Root) error {
	cand, err := dataset.ReadCandidate(r)
	if err != nil || cand == nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, orDefault(rs.cfg.CommitTimeout, DefaultCommitTimeout))
	defer cancel()
	state, other, id, err := dataset.ResolveManifest(ctx, rs.d, cand)
	switch state {
	case dataset.ManifestAbsent:
		return nil
	case dataset.ManifestOurs:
		if err := dataset.WriteCommitMarker(r, dataset.RemoteCommitRecord{Destination: rs.d.URI(), ManifestIdentity: id, ManifestSHA256: sha(cand)}); err != nil {
			return fmt.Errorf("the run committed (the destination's manifest is its candidate) but its commit marker was not written: %w", err)
		}
		return nil // committed: the caller finalizes
	case dataset.ManifestOther:
		// Lost: this run's own uploads go, as the commit's loser cleans
		// them; a failed delete is tried again by the next resume.
		rs.outcome = "lost_race"
		cctx, ccancel := context.WithTimeout(context.WithoutCancel(ctx), orDefault(rs.cfg.FinalizeTimeout, DefaultFinalizeTimeout))
		rs.cleanupOrphans(cctx, r, dataset.ManifestParts(other))
		ccancel()
		return &dataset.LostRaceError{Winner: other}
	}
	return fmt.Errorf("%w: read back: %v", dataset.ErrCommitUnresolved, objfile.RedactError(err))
}

// finalizeCommitted finishes the aftermath of a commit an earlier attempt
// made (its marker in r, not finalized): the manifest is read back and
// checked against the marker, then finalize runs with a report of this
// invocation. It returns ErrComplete once finalized, or the finalization's
// error (the marker stays unfinalized, and a later resume tries again).
func (rs *remoteState) finalizeCommitted(ctx context.Context, cfg Config, r *os.Root) error {
	rec, err := dataset.ReadCommitMarker(r)
	if err != nil {
		return fmt.Errorf("resume: %w", err)
	}
	if rec == nil || rec.Finalized {
		return ErrComplete
	}
	ctx, cancel := context.WithTimeout(ctx, orDefault(rs.cfg.FinalizeTimeout, DefaultFinalizeTimeout))
	defer cancel()
	obj, err := rs.d.Opener.Open(ctx, dataset.ManifestName)
	if err != nil {
		return fmt.Errorf("resume: the committed manifest: %w", objfile.RedactError(err))
	}
	b, err := io.ReadAll(io.NewSectionReader(obj.WithContext(ctx), 0, obj.Size()))
	obj.Close()
	if err != nil {
		return fmt.Errorf("resume: the committed manifest: %w", objfile.RedactError(err))
	}
	if sha(b) != rec.ManifestSHA256 {
		return fmt.Errorf("%w: the destination's manifest is not the one this run committed (sha256 %s, recorded %s)", journal.ErrCorrupt, sha(b), rec.ManifestSHA256)
	}
	rs.outcome = "resumed"
	rs.commit = dataset.RemoteCommit{Outcome: "resumed", Identity: rec.ManifestIdentity, SHA256: rec.ManifestSHA256}
	rep := Report{ReportVersion: ReportVersion, Build: buildinfo.Read(), Attempt: cfg.Attempt, Timings: Timings{StartedAt: time.Now().UTC()},
		Invocation: Invocation{Argv: cfg.Argv, Out: cfg.Output.Dir, Resume: true},
		Outcome:    Outcome{Status: StatusCommitted, Class: ClassCompleted, Complete: true, StopReason: "completed"}}
	if err := rs.finalize(ctx, cfg, r, *rec, dataset.ManifestParts(b), &rep); err != nil {
		return err
	}
	return ErrComplete
}

// finalize is a commit's aftermath, idempotent: the objects this run
// created that the manifest does not name (keep) are deleted by the
// generations their uploads recorded; the run report is created beside
// the manifest only if absent; then, if every such object is gone, the
// marker records the run finalized. A cleanup that left one leaves the
// marker unfinalized, for a resume to try again.
func (rs *remoteState) finalize(ctx context.Context, cfg Config, r *os.Root, rec dataset.RemoteCommitRecord, keep map[string]bool, rep *Report) error {
	if keep == nil {
		return fmt.Errorf("finalize: the committed manifest does not parse")
	}
	cleaned := rs.cleanupOrphans(ctx, r, keep)
	rep.Output.Remote = rs.report(cfg.Output.Dir)
	if err := faults.At("run-report"); err != nil {
		return err
	}
	b, err := dataset.EncodeJSON(*rep)
	if err != nil {
		return err
	}
	if _, err := rs.d.Writer.PutIfAbsent(ctx, dataset.ReportName, b); err != nil && !errors.Is(err, objfile.ErrExists) {
		return &dataset.ReportError{Err: err}
	}
	if !cleaned {
		return fmt.Errorf("finalize: %d orphans could not be deleted (or the allocation log could not be read); a resume of the work directory tries again", rs.orphans.Failed)
	}
	if err := faults.At("finalized"); err != nil {
		return err
	}
	rec.Finalized = true
	return dataset.WriteCommitMarker(r, rec)
}

// attach makes jr's run write rs: the allocation log, the run's and the
// attempt's names.
func (jr *runJournal) attachRemote(cfg Config, rs *remoteState) error {
	jr.remote = rs
	r, err := jr.rooted()
	if err != nil {
		return err
	}
	if err := dataset.CheckRemoteLayout(r); err != nil {
		return err
	}
	a, err := dataset.OpenAllocations(r)
	if err != nil {
		return err
	}
	rs.alloc = a
	if jr.j != nil {
		rs.run = jr.j.RunUUID()
	} else {
		rs.run = randomID()
	}
	if cfg.Attempt != nil && cfg.Attempt.ID != "" {
		rs.attempt = cfg.Attempt.ID
	} else {
		rs.attempt = randomID()
	}
	return nil
}

func randomID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func sha(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// prepareOutput prepares a fresh run's output: the dataset directory
// (dataset.Prepare), or a remote dataset's work directory, through its
// root once the lock is verified.
func prepareOutput(cfg Config, path string, id journal.Identity, lock *journal.Lock) error {
	if cfg.Output.Remote == nil {
		return dataset.Prepare(cfg.Output.Dir, path, id.SchemaID, cfg.Output.Replace)
	}
	if !cfg.Output.Replace {
		if _, err := dataset.Usable(cfg.Output.Dir, path, id.SchemaID, false); err != nil {
			return err
		}
	}
	r, err := os.OpenRoot(cfg.Output.Dir)
	if err != nil {
		return err
	}
	defer r.Close()
	if err := lock.Verify(); err != nil {
		return err
	}
	if cfg.Output.Replace {
		// The restart of a run whose journal header never became durable
		// and that left no remote state (recoverLocked refuses one that
		// did): its workspace is emptied, the lock kept.
		if err := resetWorkspace(r); err != nil {
			return err
		}
	}
	return dataset.PrepareWorkspaceIn(r)
}

// resetWorkspace empties a work directory's workspace through its root,
// the lock kept, and removes its outcome file.
func resetWorkspace(r *os.Root) error {
	d, err := r.Open(dataset.Workspace)
	if err != nil {
		return err
	}
	ents, err := d.ReadDir(-1)
	d.Close()
	if err != nil {
		return err
	}
	for _, e := range ents {
		if e.Name() != dataset.LockName {
			if err := r.RemoveAll(filepath.Join(dataset.Workspace, e.Name())); err != nil {
				return fmt.Errorf("remove previous run: %w", err)
			}
		}
	}
	if err := r.Remove(dataset.ErrorName); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

// recoverRemote is a remote resume's recovery: every recorded part's
// object must be the version recorded, with the recorded size and SHA-256
// (read whole, pinned); a claim receipt at the ordinal after the last
// record is the suspect, which the resume must reproduce byte for byte
// (its record may have been acknowledged and lost); a receipt further on
// means an acknowledged record was lost, and is refused. Nothing local is
// renamed and nothing remote is deleted.
func recoverRemote(v verifier, rs *remoteState, r *os.Root, st *journal.State, lock *journal.Lock) (*suspect, error) {
	if err := journal.SyncIn(r); err != nil {
		return nil, fmt.Errorf("resume: %w", err)
	}
	claims, err := dataset.ReadClaims(r)
	if err != nil {
		return nil, fmt.Errorf("resume: %w", err)
	}
	n := len(st.Records)
	for i := range claims {
		if i > n {
			return nil, fmt.Errorf("%w: a claim receipt for part %d but the journal holds %d records (%d damaged bytes); refusing to recover",
				journal.ErrCorrupt, i, n, st.TornBytes)
		}
	}
	for i, rec := range st.Records {
		if rec.Object == "" {
			return nil, fmt.Errorf("%w: record %d names no object", journal.ErrCorrupt, i)
		}
		if c, ok := claims[i]; ok && (c.Part.Bytes != rec.Part.Bytes || c.Part.SHA256 != rec.Part.SHA256) {
			return nil, fmt.Errorf("%w: part %d's claim receipt (sha256 %s) is not its record's (sha256 %s)", journal.ErrCorrupt, i, c.Part.SHA256, rec.Part.SHA256)
		}
		if err := faults.At("remote-verify"); err != nil {
			return nil, err
		}
		err := objfile.RedactError(dataset.VerifyObject(v.ctx, rs.d, rec.Object, rec.Identity, rec.Part, func(b int64) {
			if v.pr != nil {
				v.pr.verified.Add(b)
			}
		}))
		switch {
		case errors.Is(err, fs.ErrNotExist), errors.Is(err, objfile.ErrObjectChanged):
			return nil, fmt.Errorf("resume: part %d (%s) is journaled but %w: %v", i, rec.Object, dataset.ErrRemoteAltered, err)
		case err != nil:
			return nil, fmt.Errorf("resume: part %d: %w", i, err)
		}
		rs.verified++
	}
	if err := lock.Verify(); err != nil {
		return nil, err
	}
	final := n > 0 && st.Records[n-1].Final
	c, ok := claims[n]
	if !ok || final {
		return nil, faults.At("recovered")
	}
	sus := &suspect{ordinal: n, path: filepath.Join(dataset.ClaimsRel, dataset.PartName(n)+".json"), info: dataset.PartInfo{Bytes: c.Part.Bytes, SHA256: c.Part.SHA256},
		footer: seekparquet.Footer{Rows: c.Part.Rows, Columns: c.Columns, CreatedBy: c.CreatedBy}}
	return sus, faults.At("recovered")
}

// publishRemote is publish for a remote part, already uploaded (the
// upload completed in the part writer's Seal): the claim receipt is made
// durable, then the record naming the object is appended and synced; K
// moves only then. The object stays where it was uploaded.
func (jr *runJournal) publishRemote(ordinal int, p dataset.Part, limit, rows int64, final bool) (dataset.Part, error) {
	if err := faults.At("part-closed"); err != nil {
		return p, err
	}
	sus := jr.suspect
	if sus != nil && ordinal == sus.ordinal && (p.Bytes != sus.info.Bytes || p.SHA256 != sus.info.SHA256) {
		return p, fmt.Errorf("%w: part %d is %d bytes, sha256 %s now; its claim receipt (%s) records %d bytes, sha256 %s",
			ErrAmbiguousTail, ordinal, p.Bytes, p.SHA256, sus.path, sus.info.Bytes, sus.info.SHA256)
	}
	if err := jr.verify(); err != nil {
		return p, err
	}
	if err := dataset.WriteClaim(jr.root, dataset.ClaimReceipt{Ordinal: ordinal, Object: p.Path, Identity: p.Identity, Part: p.PartInfo,
		Columns: p.Columns, CreatedBy: p.CreatedBy}); err != nil {
		return p, err
	}
	if err := jr.j.Append(journal.Record{Part: p.PartInfo, Ordinal: ordinal, Limit: limit, RowsTotal: rows, Final: final,
		Object: p.Path, Identity: p.Identity}); err != nil {
		return p, err
	}
	jr.acked.parts.Add(1)
	jr.acked.bytes.Add(p.Bytes)
	jr.acked.rows.Store(rows)
	jr.cache.SetK(string(p.MaxKey))
	if sus != nil && ordinal == sus.ordinal {
		jr.suspect = nil // reproduced: the receipt now names the new object
	}
	return p, faults.At("recorded")
}

// remoteRouter makes the router upload its parts.
func (jr *runJournal) remoteRouter(ctx context.Context, o *parquetout.Options) {
	rs := jr.remote
	rp := &dataset.RemoteParts{Ctx: ctx, Remote: rs.d, Run: rs.run, Attempt: rs.attempt, Alloc: rs.alloc, Stats: rs.stats}
	o.Create = rp.Create
}

// cleanupOrphans deletes the objects this run created that keep (a
// committed manifest's parts) does not name, and reports whether none was
// left behind by a failure.
func (rs *remoteState) cleanupOrphans(ctx context.Context, r *os.Root, keep map[string]bool) bool {
	l, err := dataset.ReadAllocations(r)
	if err != nil {
		rs.orphans.Failed++
		return false
	}
	st := dataset.DeleteOwned(ctx, rs.d, l, keep)
	rs.orphans.Deleted += st.Deleted
	rs.orphans.Gone += st.Gone
	rs.orphans.Failed += st.Failed
	rs.orphans.Unproven += st.Unproven
	return st.Failed == 0
}

// report is the destination's account now.
func (rs *remoteState) report(ws string) *Remote {
	if rs == nil {
		return nil
	}
	r := &Remote{Destination: rs.d.URI(), WorkDir: ws, Commit: rs.outcome,
		Uploads: rs.stats.Uploads.Load(), UploadsAborted: rs.stats.Aborted.Load(), UploadBytes: rs.stats.Bytes.Load(), UploadWaitNs: rs.stats.WaitNs.Load(),
		Retries: rs.stats.Retries.Load(), RetryClasses: rs.stats.Classes(), ObjectsVerified: rs.verified,
		OrphansDeleted: rs.orphans.Deleted, OrphansGone: rs.orphans.Gone, OrphansLeft: rs.orphans.Failed, OrphansUnproven: rs.orphans.Unproven}
	if rs.outcome == dataset.CommitCreated || rs.outcome == dataset.CommitConfirmed || rs.outcome == "resumed" {
		r.Manifest = rs.d.Location.String() + dataset.ManifestName
		r.ManifestIdentity, r.ManifestSHA256 = rs.commit.Identity, rs.commit.SHA256
	}
	return r
}

// commitRemote is commit's publication for a remote dataset: the manifest
// created at the destination, within the commit's time bound, with the
// candidate an earlier attempt saved when it names the same parts. A lost
// race cleans this run's own uploads; an unresolved commit keeps
// everything.
func (a *attempt) commitRemote(meta dataset.Meta, parts []dataset.Part, commit func(install func() error) error, committed *bool) error {
	cfg, rs, r := a.cfg, a.jr.remote, a.jr.root
	prior, err := dataset.ReadCandidate(r)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(a.ctx), orDefault(rs.cfg.CommitTimeout, DefaultCommitTimeout))
	defer cancel()
	rc, err := dataset.PublishRemote(ctx, r, rs.d, meta, parts, prior, commit)
	rs.commit = rc
	var lost *dataset.LostRaceError
	switch {
	case *committed:
		rs.outcome = rc.Outcome
	case errors.As(err, &lost):
		rs.outcome = "lost_race"
		// Only this run's own uploads, by the generations they created;
		// the winner's manifest is consulted anyway.
		cctx, ccancel := context.WithTimeout(context.WithoutCancel(a.ctx), orDefault(rs.cfg.FinalizeTimeout, DefaultFinalizeTimeout))
		rs.cleanupOrphans(cctx, r, dataset.ManifestParts(lost.Winner))
		ccancel()
	case errors.Is(err, dataset.ErrCommitUnresolved):
		rs.outcome = "unresolved"
	}
	if err != nil {
		return err
	}
	if rc.MarkerErr != nil && cfg.Warn != nil {
		cfg.Warn("the dataset is committed but the work directory's commit marker was not written: " + rc.MarkerErr.Error())
	}
	return nil
}

// finalizeRemote is the committing run's aftermath (finalize), within its
// time bound: a failure is a warning, the dataset stands, and a resume of
// the work directory finishes it.
func (a *attempt) finalizeRemote(parts []dataset.Part, rep *Report) error {
	cfg, rs := a.cfg, a.jr.remote
	ctx, cancel := context.WithTimeout(context.WithoutCancel(a.ctx), orDefault(rs.cfg.FinalizeTimeout, DefaultFinalizeTimeout))
	defer cancel()
	keep := map[string]bool{}
	for _, p := range parts {
		keep[p.Path] = true
	}
	rec := dataset.RemoteCommitRecord{Destination: rs.d.URI(), ManifestIdentity: rs.commit.Identity, ManifestSHA256: rs.commit.SHA256}
	err := rs.finalize(ctx, cfg, a.jr.root, rec, keep, rep)
	if errors.Is(err, faults.ErrCrash) {
		return err // tests: killed in the aftermath
	}
	if err != nil && cfg.Warn != nil {
		cfg.Warn("the dataset is committed; its aftermath (orphan cleanup, run report) is not finished, and swath resume of the work directory finishes it: " + err.Error())
	}
	return nil
}

// failRemote cleans a failed remote run's uploads when nothing can resume
// them (no journal): every object it created, by its generation.
func (a *attempt) failRemote(err error) {
	if a.jr == nil || a.jr.remote == nil || a.jr.journaled() || errors.Is(err, faults.ErrCrash) || a.jr.root == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(a.ctx), orDefault(a.jr.remote.cfg.FinalizeTimeout, DefaultFinalizeTimeout))
	defer cancel()
	a.jr.remote.cleanupOrphans(ctx, a.jr.root, nil)
}
