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
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/journal"
	"github.com/varveio/swath/go/internal/parquetout"
	"github.com/varveio/swath/go/internal/source"
	"github.com/varveio/swath/go/objfile"
	"github.com/varveio/swath/go/objfile/gcs"
	"github.com/varveio/swath/go/objfile/remote"
)

// RemoteOutput is a dataset written straight to an object store (gs://
// only, for now): its parts are uploaded as they close and its manifest,
// created only if absent, is the commit. OutputConfig.Dir is then the work
// directory: the lock, the journal, spill, the claim receipts and the
// candidate manifest, never a part.
type RemoteOutput struct {
	Location        string // gs://BUCKET[/PREFIX]
	Endpoint        string // the destination's JSON API base ("": the default); frozen
	CredentialsFile string // the destination's credentials ("": ADC); resolved again on resume
	// ChunkSize is an upload's chunk (0: gcs.DefaultChunkSize), and the
	// memory admitted per encoder beside its writer.
	ChunkSize int
	// Open, if set, opens the destination instead of remote.Create (tests:
	// a fake's client); stats takes the uploads' retries.
	Open func(ctx context.Context, stats *dataset.UploadStats) (*dataset.Remote, func() error, error)
}

// Remote is the report's account of a remote dataset (output.remote).
type Remote struct {
	Destination string `json:"destination"` // the root's URI (and " at " its endpoint)
	WorkDir     string `json:"work_dir"`
	// Commit is the commit's outcome: created, confirmed (its answer lost
	// or refused, the manifest read back as the candidate), lost_race,
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
	// Orphans: objects this run created and no committed manifest names,
	// deleted by generation after the commit (or after a failure without a
	// journal, or a lost race), gone already, or left (a failed delete).
	OrphansDeleted int64 `json:"orphans_deleted"`
	OrphansGone    int64 `json:"orphans_gone"`
	OrphansLeft    int64 `json:"orphans_left"`
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
// that this build writes: gs:// only; s3:// and az:// are refused.
func ParseDestination(raw string) (objfile.Location, error) {
	l, err := objfile.ParseLocation(raw)
	if err != nil {
		return l, err
	}
	if l.Scheme != "gs" {
		return l, fmt.Errorf("writing a dataset to %s:// is not supported yet (gs:// only)", l.Scheme)
	}
	return l, nil
}

// remoteLocation parses the destination.
func remoteLocation(r *RemoteOutput) (objfile.Location, error) { return ParseDestination(r.Location) }

// destinationIdentity is the destination's part of the resume identity.
func destinationIdentity(r *RemoteOutput) (*journal.Destination, error) {
	l, err := remoteLocation(r)
	if err != nil {
		return nil, err
	}
	return &journal.Destination{Provider: l.Scheme, Bucket: l.Container, Prefix: l.Prefix, Endpoint: r.Endpoint, Layout: dataset.RemoteLayout}, nil
}

// checkOverlap refuses a destination the listing would list, or one that
// holds the listed namespace: the same bucket, one prefix a prefix of the
// other (a listing prefix is a key prefix, not a directory). Endpoints are
// not compared: the same bucket name is refused on any.
func checkOverlap(loc source.Locator, d *journal.Destination) error {
	if loc.Kind != "gcs" || loc.Container != d.Bucket {
		return nil
	}
	rest, _ := strings.CutPrefix(loc.Root, "gs://"+loc.Container)
	src := strings.TrimPrefix(rest, "/")
	if strings.HasPrefix(d.Prefix, src) || strings.HasPrefix(src, d.Prefix) {
		return fmt.Errorf("%w: gs://%s/%s and %s", ErrOverlap, d.Bucket, d.Prefix, loc.Root)
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
	dst, err := remote.Create(ctx, r.Location, remote.CreateOptions{CredentialsFile: r.CredentialsFile, Endpoint: r.Endpoint,
		Write: gcs.WriteOptions{ChunkSize: r.ChunkSize, Retried: rs.stats.Retried}})
	if err != nil {
		return nil, fmt.Errorf("destination: %w", err)
	}
	rs.d = &dataset.Remote{Location: l, Endpoint: r.Endpoint, Opener: dst, Writer: dst.Writer()}
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

// uploadBytes is the memory admitted per encoder for its upload: one chunk.
func uploadBytes(r *RemoteOutput) int64 {
	if r.ChunkSize > 0 {
		return int64(r.ChunkSize)
	}
	return gcs.DefaultChunkSize
}

// checkFresh refuses a fresh run whose destination holds a manifest.
func (rs *remoteState) checkFresh(ctx context.Context) error {
	state, _, _, err := dataset.ResolveManifest(ctx, rs.d, nil)
	switch state {
	case dataset.ManifestAbsent:
		return nil
	case dataset.ManifestUnknown:
		return fmt.Errorf("destination: reading %smanifest.json: %w", rs.d.Location.String(), err)
	}
	return fmt.Errorf("%s: %w", rs.d.URI(), ErrDestinationExists)
}

// preflight is a resume's first look at the destination, when the run
// saved a candidate manifest (it reached its commit): the destination's
// manifest is the candidate (the commit happened; the marker is written
// now and the run is complete), another one (a lost race), or absent (the
// commit is made again).
func (rs *remoteState) preflight(ctx context.Context, ws string) error {
	cand, err := dataset.ReadCandidate(ws)
	if err != nil || cand == nil {
		return err
	}
	state, other, id, err := dataset.ResolveManifest(ctx, rs.d, cand)
	switch state {
	case dataset.ManifestAbsent:
		return nil
	case dataset.ManifestOurs:
		if err := dataset.WriteCommitMarker(ws, dataset.RemoteCommitRecord{Destination: rs.d.URI(), ManifestIdentity: id, ManifestSHA256: sha(cand)}); err != nil {
			return fmt.Errorf("the run committed (the destination's manifest is its candidate) but its commit marker was not written: %w", err)
		}
		return ErrComplete
	case dataset.ManifestOther:
		return &dataset.LostRaceError{Winner: other}
	}
	return fmt.Errorf("%w: read back: %v", dataset.ErrCommitUnresolved, err)
}

// attach makes jr's run write rs: the allocation log, the run's and the
// attempt's names.
func (jr *runJournal) attachRemote(cfg Config, rs *remoteState) error {
	jr.remote = rs
	a, err := dataset.OpenAllocations(cfg.Output.Dir)
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
// (dataset.Prepare), or a remote dataset's work directory.
func prepareOutput(cfg Config, path string, id journal.Identity) error {
	if cfg.Output.Remote == nil {
		return dataset.Prepare(cfg.Output.Dir, path, id.SchemaID, cfg.Output.Replace)
	}
	if cfg.Output.Replace {
		// The restart of a run whose journal header never became durable
		// (nothing was uploaded): its workspace is emptied, the lock kept.
		if err := resetWorkspace(cfg.Output.Dir); err != nil {
			return err
		}
	} else if _, err := dataset.Usable(cfg.Output.Dir, path, id.SchemaID, false); err != nil {
		return err
	}
	return dataset.PrepareWorkspace(cfg.Output.Dir)
}

// resetWorkspace empties a work directory's workspace, the lock kept, and
// removes its outcome file.
func resetWorkspace(ws string) error {
	ents, _ := os.ReadDir(filepath.Join(ws, dataset.Workspace))
	for _, e := range ents {
		if e.Name() != dataset.LockName {
			if err := os.RemoveAll(filepath.Join(ws, dataset.Workspace, e.Name())); err != nil {
				return fmt.Errorf("remove previous run: %w", err)
			}
		}
	}
	if err := os.Remove(filepath.Join(ws, dataset.ErrorName)); err != nil && !errors.Is(err, fs.ErrNotExist) {
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
func recoverRemote(v verifier, rs *remoteState, ws string, st *journal.State, lock *journal.Lock) (*suspect, error) {
	if err := journal.Sync(ws); err != nil {
		return nil, fmt.Errorf("resume: %w", err)
	}
	claims, err := dataset.ReadClaims(ws)
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
		err := dataset.VerifyObject(v.ctx, rs.d, rec.Object, rec.Identity, rec.Part, func(b int64) {
			if v.pr != nil {
				v.pr.verified.Add(b)
			}
		})
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
	sus := &suspect{ordinal: n, path: dataset.ClaimPathOf(ws, n), info: dataset.PartInfo{Bytes: c.Part.Bytes, SHA256: c.Part.SHA256},
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
	if err := dataset.WriteClaim(jr.out, dataset.ClaimReceipt{Ordinal: ordinal, Object: p.Path, Identity: p.Identity, Part: p.PartInfo,
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

// cleanupOrphans deletes the objects this run allocated that keep (a
// committed manifest's parts) does not name.
func (rs *remoteState) cleanupOrphans(ctx context.Context, ws string, keep map[string]bool) {
	names, err := dataset.ReadAllocations(ws)
	if err != nil {
		rs.orphans.Failed++
		return
	}
	st := dataset.DeleteOwned(ctx, rs.d, names, keep)
	rs.orphans.Deleted += st.Deleted
	rs.orphans.Gone += st.Gone
	rs.orphans.Failed += st.Failed
}

// report is the destination's account now.
func (rs *remoteState) report(ws string) *Remote {
	if rs == nil {
		return nil
	}
	r := &Remote{Destination: rs.d.URI(), WorkDir: ws, Commit: rs.outcome,
		Uploads: rs.stats.Uploads.Load(), UploadsAborted: rs.stats.Aborted.Load(), UploadBytes: rs.stats.Bytes.Load(), UploadWaitNs: rs.stats.WaitNs.Load(),
		Retries: rs.stats.Retries.Load(), RetryClasses: rs.stats.Classes(), ObjectsVerified: rs.verified,
		OrphansDeleted: rs.orphans.Deleted, OrphansGone: rs.orphans.Gone, OrphansLeft: rs.orphans.Failed}
	if rs.outcome == dataset.CommitCreated || rs.outcome == dataset.CommitConfirmed {
		r.Manifest = rs.d.Location.String() + dataset.ManifestName
		r.ManifestIdentity, r.ManifestSHA256 = rs.commit.Identity, rs.commit.SHA256
	}
	return r
}

// commitRemote is commit's publication for a remote dataset: the manifest
// created at the destination, the orphans cleaned, the run report put
// beside it.
func (a *attempt) commitRemote(meta dataset.Meta, parts []dataset.Part, commit func(install func() error) error, committed *bool) error {
	cfg, rs := a.cfg, a.jr.remote
	ctx := context.WithoutCancel(a.ctx)
	rc, err := dataset.PublishRemote(ctx, cfg.Output.Dir, rs.d, meta, parts, commit)
	rs.commit = rc
	var lost *dataset.LostRaceError
	switch {
	case *committed:
		rs.outcome = rc.Outcome
	case errors.As(err, &lost):
		rs.outcome = "lost_race"
		// Every name this run allocated is its own; the winner's manifest
		// is consulted anyway, so a part it names is never touched.
		if !errors.Is(err, faults.ErrCrash) {
			rs.cleanupOrphans(ctx, cfg.Output.Dir, dataset.ManifestParts(lost.Winner))
		}
	case errors.Is(err, dataset.ErrCommitUnresolved):
		rs.outcome = "unresolved"
	}
	if err != nil {
		return err
	}
	if rc.MarkerErr != nil && cfg.Warn != nil {
		cfg.Warn("the dataset is committed but the work directory's commit marker was not written: " + rc.MarkerErr.Error())
	}
	keep := map[string]bool{}
	for _, p := range parts {
		keep[p.Path] = true
	}
	rs.cleanupOrphans(ctx, cfg.Output.Dir, keep)
	return nil
}

// writeRemoteReport puts the run report beside the committed manifest,
// created only if absent: only the run that committed writes it.
func (a *attempt) writeRemoteReport(rep Report) error {
	rs := a.jr.remote
	if err := faults.At("run-report"); err != nil {
		return &dataset.ReportError{Err: err}
	}
	b, err := dataset.EncodeJSON(rep)
	if err != nil {
		return &dataset.ReportError{Err: err}
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(a.ctx), 5*time.Minute)
	defer cancel()
	if _, err := rs.d.Writer.PutIfAbsent(ctx, dataset.ReportName, b); err != nil {
		return &dataset.ReportError{Err: err}
	}
	return nil
}

// failRemote cleans a failed remote run's uploads when nothing can resume
// them (no journal): every name it allocated.
func (a *attempt) failRemote(err error) {
	if a.jr == nil || a.jr.remote == nil || a.jr.journaled() || errors.Is(err, faults.ErrCrash) {
		return
	}
	a.jr.remote.cleanupOrphans(context.WithoutCancel(a.ctx), a.cfg.Output.Dir, nil)
}
