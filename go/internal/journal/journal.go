// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package durable keeps a run's progress on disk so an interrupted listing
// resumes without re-listing published keys and without skipping any.
//
// The journal is about committed output only. Before a part is renamed
// into data/, one record is appended and fsynced: the part (ordinal and
// dataset.PartInfo: rows, size, MD5, min and max key) and the writer state
// needed to continue (the router's frozen part limit, the running row
// total). A resume keeps the published parts and has the source restart
// after K, the last published part's max key, from its normal start-up:
// no scheduling history is kept (a restart may ramp up more slowly, never
// list less).
//
// The journal (out/.swath/journal) is a sequence of frames: a 4-byte
// little-endian payload length, a 4-byte CRC-32C of the payload, and the
// payload (JSON). The first frame is the header (run identity). A torn
// tail (a crash mid-append) ends the journal at the last whole frame; such
// a frame was never acknowledged, so nothing depends on it. Damage that
// cannot be told from a torn tail is refused (see decode).
package journal

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/faults"
)

// Dir and Name locate the journal inside the dataset directory.
const (
	Dir  = ".swath"
	Name = "journal"
)

// Path is the journal path of dataset directory out.
func Path(out string) string { return filepath.Join(out, Dir, Name) }

// Witness identifies the unfinished run in out as this process sees it now:
// a digest of the journal file and of the outcome file (error.json), or ""
// when out holds no journal file (no unfinished run to settle). An
// invocation that failed before it owned out records its outcome only if
// the witness it took before is unchanged once it holds the lock: a run
// another process completed, replaced or continued meanwhile reads
// differently, and its outcome is not this invocation's to replace.
func Witness(out string) string {
	j, err := os.ReadFile(Path(out))
	if err != nil {
		return ""
	}
	h := sha256.New()
	fmt.Fprintf(h, "%d\n", len(j))
	h.Write(j)
	if o, err := os.ReadFile(filepath.Join(out, dataset.ErrorName)); err == nil {
		fmt.Fprintf(h, "outcome %d\n", len(o))
		h.Write(o)
	} else {
		fmt.Fprintf(h, "no outcome: %v", errors.Is(err, os.ErrNotExist))
	}
	return hex.EncodeToString(h.Sum(nil))
}

const maxFrame = 64 << 20

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Header is the journal's first frame.
type Header struct {
	Type     string    `json:"type"` // "header"
	Version  int       `json:"version"`
	Identity Identity  `json:"identity"`
	Created  time.Time `json:"created"`
	// Argv is the command that started the run; swath resume replays it.
	Argv []string `json:"argv,omitempty"`
}

// Record is one durable step: a part published.
type Record struct {
	Type      string           `json:"type"` // "part"
	Ordinal   int              `json:"ordinal"`
	Part      dataset.PartInfo `json:"part"`
	Limit     int64            `json:"limit"`           // the router's frozen logical bytes per part (0 before part 0 closes)
	RowsTotal int64            `json:"rows_total"`      // rows in parts 0..Ordinal
	Final     bool             `json:"final,omitempty"` // the last part: the listing is complete
}

// State is what a journal says about a run.
type State struct {
	Header    Header
	Records   []Record
	TornBytes int64 // bytes after the last whole frame (ignored)
}

// Last is the last record, or nil.
func (s *State) Last() *Record {
	if len(s.Records) == 0 {
		return nil
	}
	return &s.Records[len(s.Records)-1]
}

// Journal appends records; Append returns once the record is on disk. Its
// one caller is the ordered committer (the part router publishes parts in
// ordinal order, one at a time); the mutex only makes misuse safe. The
// caller holds the output's Lock.
type Journal struct {
	mu sync.Mutex
	f  *os.File
}

// Create starts a new journal at Path(out), replacing any old one, with
// the header written and synced.
func Create(out string, id Identity, argv []string) (*Journal, error) {
	if err := os.MkdirAll(filepath.Join(out, Dir), 0o755); err != nil {
		return nil, fmt.Errorf("journal: %w", err)
	}
	f, err := os.OpenFile(Path(out), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("journal: %w", err)
	}
	j := start(f)
	if err := j.append(Header{Type: "header", Version: Version, Identity: id, Created: time.Now().UTC(), Argv: argv}); err != nil {
		j.Close()
		return nil, err
	}
	// The journal's entry in .swath, and .swath's entry in the output.
	if err := syncDir(filepath.Join(out, Dir)); err != nil {
		j.Close()
		return nil, err
	}
	if err := syncDir(out); err != nil {
		j.Close()
		return nil, err
	}
	return j, nil
}

// Reopen continues an existing journal: it truncates a torn tail and
// appends after the last whole frame.
func Reopen(out string, st *State) (*Journal, error) {
	f, err := os.OpenFile(Path(out), os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("journal: %w", err)
	}
	end, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("journal: %w", err)
	}
	if st.TornBytes > 0 {
		if err := f.Truncate(end - st.TornBytes); err != nil {
			f.Close()
			return nil, fmt.Errorf("journal: truncate torn tail: %w", err)
		}
		if _, err := f.Seek(end-st.TornBytes, io.SeekStart); err != nil {
			f.Close()
			return nil, fmt.Errorf("journal: %w", err)
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return nil, fmt.Errorf("journal: %w", err)
		}
	}
	return start(f), nil
}

// LockName is the output directory's lock file, inside the workspace.
// Replacing a previous run empties the workspace but never removes the
// lock (dataset.Prepare), so a live owner's lock is never unlinked.
var LockName = filepath.Join(dataset.Workspace, dataset.LockName)

// Lock is exclusive ownership of an output directory: taken before the
// directory is prepared, recovered or written, held until the run ends.
type Lock struct{ f *os.File }

// TakeLock creates out if needed and takes its lock (non-blocking).
func TakeLock(out string) (*Lock, error) {
	if err := os.MkdirAll(filepath.Join(out, dataset.Workspace), 0o755); err != nil {
		return nil, fmt.Errorf("output: %w", err)
	}
	f, err := os.OpenFile(filepath.Join(out, LockName), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("output lock: %w", err)
	}
	if err := lockFile(f); err != nil {
		f.Close()
		return nil, fmt.Errorf("%w (%s): %v", ErrLocked, out, err)
	}
	return &Lock{f}, nil
}

// Release releases the lock.
func (l *Lock) Release() {
	if l != nil && l.f != nil {
		l.f.Close()
		l.f = nil
	}
}

func start(f *os.File) *Journal { return &Journal{f: f} }

// Append writes rec and returns after it is synced.
func (j *Journal) Append(rec Record) error {
	rec.Type = "part"
	return j.append(rec)
}

func (j *Journal) append(v any) error {
	payload, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("journal: %w", err)
	}
	if len(payload) > maxFrame {
		return fmt.Errorf("journal: record of %d bytes exceeds %d", len(payload), maxFrame)
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	var hdr [8]byte
	binary.LittleEndian.PutUint32(hdr[:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(hdr[4:], frameCRC(hdr[:4], payload))
	err = faults.At("journal-append")
	if err == nil {
		_, err = faults.Write(j.f, append(hdr[:], payload...))
	}
	if err == nil {
		err = faults.At("journal-sync")
	}
	if err == nil {
		err = faults.Sync(j.f)
	}
	if err != nil {
		return fmt.Errorf("journal: %w", err)
	}
	return nil
}

// Close closes the file.
func (j *Journal) Close() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.f.Close()
}

// Version is the journal format this tool writes and reads. Version 7
// marks the claimed-part protocol: the part about to be recorded is at
// dataset.ClaimPath, and recovery protects only that name. A version 6
// journal's acknowledged part may still be at dataset.TempPath, which
// version 7 recovery would remove as unrecorded, so it is refused before
// recovery touches anything (see durability.md).
const Version = 7

// frameCRC covers the length field and the payload, so a damaged length
// cannot pass for a different frame boundary.
func frameCRC(length, payload []byte) uint32 {
	return crc32.Update(crc32.Checksum(length, castagnoli), castagnoli, payload)
}

// MaxFrame is the largest record accepted, on append and on read.
const MaxFrame = maxFrame

// VersionError is a journal of a version this build does not read: an
// interrupted run of another build.
type VersionError struct{ Found, Want int }

func (e *VersionError) Error() string {
	return fmt.Sprintf("journal version %d is not supported (this tool writes %d)", e.Found, e.Want)
}

// Errors.
var (
	ErrNoJournal = errors.New("no journal")
	ErrCorrupt   = errors.New("journal corrupt")
	ErrLocked    = errors.New("another swath process owns this output directory")
)

// Read loads the journal of dataset directory out. Damage it cannot tell
// from a torn append is ErrCorrupt; ErrNoJournal means no journal file, or
// a header that never became durable in an output with no part anywhere.
func Read(out string) (*State, error) {
	f, err := os.Open(Path(out))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNoJournal
	}
	if err != nil {
		return nil, fmt.Errorf("journal: %w", err)
	}
	defer f.Close()
	st, err := decode(bufio.NewReader(f))
	if errors.Is(err, errTornHeader) {
		// Create syncs the header before any part exists, so a part on
		// disk proves the header was durable and has been damaged since.
		if n := partFiles(out); n > 0 {
			return nil, fmt.Errorf("%w: the header is damaged but %d part files exist", ErrCorrupt, n)
		}
		return nil, ErrNoJournal
	}
	return st, err
}

// partFiles counts the files in data/ and in the temporary parts directory.
func partFiles(out string) int {
	n := 0
	for _, d := range []string{filepath.Join(out, dataset.DataDir), filepath.Join(out, dataset.Workspace, dataset.PartsDir)} {
		ents, _ := os.ReadDir(d)
		n += len(ents)
	}
	return n
}

// errTornHeader: the header frame is incomplete (Read decides what it means).
var errTornHeader = errors.New("journal header incomplete")

// maxLengthProbe bounds the search for a frame whose length field was
// damaged: records and headers are far smaller.
const maxLengthProbe = 64 << 10

// decode reads frames to the end. Only an append that never completed is a
// torn tail, and it can only be the last frame: fewer bytes than its length
// field says, or a whole frame whose payload ends in bytes that never
// reached the disk, which read as zeros (a payload is JSON and ends in '}').
// Everything else is corruption (ErrCorrupt): a bad checksum with data after
// it, a whole last frame with a bad checksum whose last byte was written (a
// '}' or any other nonzero byte: an acknowledged record damaged since, its
// closing byte included; its part may be durable in .swath/parts and
// nothing else names it), a length field over the frame limit, or one that
// overruns the file when the bytes there hold a whole frame under a smaller
// length.
func decode(r io.Reader) (*State, error) {
	st := &State{}
	var hdr [8]byte
	first := true
	torn := func(n int64) (*State, error) {
		st.TornBytes += n
		if first {
			return nil, errTornHeader
		}
		return st, nil
	}
	for {
		n, err := io.ReadFull(r, hdr[:])
		if err == io.EOF {
			if first {
				return nil, errTornHeader
			}
			return st, nil
		}
		if err != nil {
			return torn(int64(n))
		}
		size, sum := binary.LittleEndian.Uint32(hdr[:4]), binary.LittleEndian.Uint32(hdr[4:])
		if size > maxFrame {
			return nil, fmt.Errorf("%w: frame %d claims %d bytes", ErrCorrupt, len(st.Records)+1, size)
		}
		payload := make([]byte, size)
		m, err := io.ReadFull(r, payload)
		if err != nil {
			if wholeUnder(payload[:m], sum) {
				return nil, fmt.Errorf("%w: frame %d's length overruns the file but a whole frame is there", ErrCorrupt, len(st.Records)+1)
			}
			return torn(8 + int64(m))
		}
		if frameCRC(hdr[:4], payload) != sum {
			rest, _ := io.Copy(io.Discard, r)
			if rest > 0 {
				return nil, fmt.Errorf("%w: frame %d has a bad checksum and %d bytes follow it", ErrCorrupt, len(st.Records)+1, rest)
			}
			if size > 0 && payload[size-1] != 0 {
				return nil, fmt.Errorf("%w: the last frame (%d) was written to its end and is damaged", ErrCorrupt, len(st.Records)+1)
			}
			return torn(8 + int64(m))
		}
		if first {
			if err := json.Unmarshal(payload, &st.Header); err != nil || st.Header.Type != "header" {
				return nil, fmt.Errorf("%w: first frame is not a header", ErrCorrupt)
			}
			if st.Header.Version != Version {
				return nil, &VersionError{Found: st.Header.Version, Want: Version}
			}
			first = false
			continue
		}
		var rec Record
		if err := json.Unmarshal(payload, &rec); err != nil || rec.Type != "part" {
			return nil, fmt.Errorf("%w: bad record %d", ErrCorrupt, len(st.Records))
		}
		if rec.Ordinal != len(st.Records) {
			return nil, fmt.Errorf("%w: record for part %d after %d records", ErrCorrupt, rec.Ordinal, len(st.Records))
		}
		st.Records = append(st.Records, rec)
	}
}

// wholeUnder reports whether b starts with a whole frame payload under some
// length other than the recorded one: the checksum covers the length, so a
// match proves the frame was written whole and only its length is damaged.
func wholeUnder(b []byte, sum uint32) bool {
	var l [4]byte
	for i := range min(len(b), maxLengthProbe) {
		if b[i] != '}' {
			continue
		}
		binary.LittleEndian.PutUint32(l[:], uint32(i+1))
		if frameCRC(l[:], b[:i+1]) == sum {
			return true
		}
	}
	return false
}

// Sync makes the journal's accepted frames durable. A recovery calls it
// before acting on them: a whole frame that survived a crash may never
// have been synced.
func Sync(out string) error {
	f, err := os.Open(Path(out))
	if err != nil {
		return fmt.Errorf("journal: %w", err)
	}
	defer f.Close()
	if err := f.Sync(); err != nil {
		return fmt.Errorf("journal: %w", err)
	}
	return nil
}

// Dump writes the journal of dataset directory out as JSON lines: the
// header, each record, and a final line on any torn tail.
func Dump(w io.Writer, out string) error {
	st, err := Read(out)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(w)
	if err := enc.Encode(st.Header); err != nil {
		return err
	}
	for _, r := range st.Records {
		if err := enc.Encode(r); err != nil {
			return err
		}
	}
	if st.TornBytes > 0 {
		return enc.Encode(map[string]any{"type": "torn_tail", "bytes": st.TornBytes})
	}
	return nil
}
