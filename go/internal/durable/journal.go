// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package durable keeps a run's progress on disk so an interrupted listing
// resumes without re-listing published keys and without skipping any.
//
// Durability is per part. Before a part is renamed into data/, one record
// is appended to an append-only journal and fsynced: the part (ordinal,
// rows, size, MD5, min and max key), the router's frozen part limit, and
// the range boundaries created since the previous record. A resume keeps
// the published parts, drops nothing it has not listed, and re-lists
// (K, ⊤] where K is the last published part's max key, tiled by the
// recorded boundaries so it starts as parallel as the run was. Boundaries
// only shape that tiling: a split the journal never saw costs parallelism
// on resume, never correctness.
//
// The journal (out/.swath/journal) is a sequence of frames: a 4-byte
// little-endian payload length, a 4-byte CRC-32C of the payload, and the
// payload (JSON). The first frame is the header (run identity). A torn
// tail (a crash mid-append) ends the journal at the last whole frame; such
// a frame was never acknowledged, so nothing depends on it.
package durable

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
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

const maxFrame = 64 << 20

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Header is the journal's first frame.
type Header struct {
	Type     string    `json:"type"` // "header"
	Version  int       `json:"version"`
	Identity Identity  `json:"identity"`
	Created  time.Time `json:"created"`
}

// Record is one durable step: a part published.
type Record struct {
	Type       string       `json:"type"` // "part"
	Part       dataset.Part `json:"part"`
	Ordinal    int          `json:"ordinal"`
	Limit      int64        `json:"limit"`      // the router's frozen logical bytes per part (0 before part 0 closes)
	RowsTotal  int64        `json:"rows_total"` // rows in parts 0..Ordinal
	Boundaries []string     `json:"boundaries,omitempty"`
}

// State is what a journal says about a run.
type State struct {
	Header     Header
	Records    []Record
	Boundaries []string // every recorded boundary, in record order
	TornBytes  int64    // bytes after the last whole frame (ignored)
}

// Last is the last record, or nil.
func (s *State) Last() *Record {
	if len(s.Records) == 0 {
		return nil
	}
	return &s.Records[len(s.Records)-1]
}

// Journal appends records with one writer goroutine; Append returns once
// the record is on disk. The caller holds the output's Lock.
type Journal struct {
	f    *os.File
	reqs chan req
	done chan struct{}
}

type req struct {
	payload []byte
	ack     chan error
}

// Create starts a new journal at Path(out), replacing any old one, with
// the header written and synced.
func Create(out string, id Identity) (*Journal, error) {
	if err := os.MkdirAll(filepath.Join(out, Dir), 0o755); err != nil {
		return nil, fmt.Errorf("journal: %w", err)
	}
	f, err := os.OpenFile(Path(out), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("journal: %w", err)
	}
	j := start(f)
	if err := j.append(Header{Type: "header", Version: Version, Identity: id, Created: time.Now().UTC()}); err != nil {
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
	}
	return start(f), nil
}

// LockName is the output directory's lock file. It lives outside .swath, so
// replacing a previous run's entries never removes a live owner's lock.
const LockName = ".swath.lock"

// Lock is exclusive ownership of an output directory: taken before the
// directory is prepared, recovered or written, held until the run ends.
type Lock struct{ f *os.File }

// TakeLock creates out if needed and takes its lock (non-blocking).
func TakeLock(out string) (*Lock, error) {
	if err := os.MkdirAll(out, 0o755); err != nil {
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

func start(f *os.File) *Journal {
	j := &Journal{f: f, reqs: make(chan req), done: make(chan struct{})}
	go j.writer()
	return j
}

// writer is the only goroutine that touches the file.
func (j *Journal) writer() {
	defer close(j.done)
	var hdr [8]byte
	for r := range j.reqs {
		binary.LittleEndian.PutUint32(hdr[:4], uint32(len(r.payload)))
		binary.LittleEndian.PutUint32(hdr[4:], crc32.Checksum(r.payload, castagnoli))
		err := faults.At("journal-append")
		if err == nil {
			_, err = j.f.Write(append(hdr[:], r.payload...))
		}
		if err == nil {
			err = faults.At("journal-sync")
		}
		if err == nil {
			err = j.f.Sync()
		}
		if err != nil {
			err = fmt.Errorf("journal: %w", err)
		}
		r.ack <- err
	}
}

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
	ack := make(chan error, 1)
	j.reqs <- req{payload, ack}
	return <-ack
}

// Close stops the writer, closes the file and releases the lock.
func (j *Journal) Close() error {
	close(j.reqs)
	<-j.done
	return j.f.Close()
}

// Version is the journal format this tool writes and reads.
const Version = 1

// Errors.
var (
	ErrNoJournal = errors.New("no journal")
	ErrCorrupt   = errors.New("journal corrupt")
	ErrLocked    = errors.New("another swath process owns this output directory")
)

// Read loads the journal of dataset directory out.
func Read(out string) (*State, error) {
	f, err := os.Open(Path(out))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNoJournal
	}
	if err != nil {
		return nil, fmt.Errorf("journal: %w", err)
	}
	defer f.Close()
	return decode(bufio.NewReader(f))
}

// decode reads frames until the end or the first incomplete or corrupt
// frame (a torn tail).
func decode(r io.Reader) (*State, error) {
	st := &State{}
	var hdr [8]byte
	first := true
	for {
		n, err := io.ReadFull(r, hdr[:])
		if err == io.EOF {
			return st, finish(st, first)
		}
		if err != nil {
			st.TornBytes += int64(n)
			rest, _ := io.Copy(io.Discard, r)
			st.TornBytes += rest
			return st, finish(st, first)
		}
		size, sum := binary.LittleEndian.Uint32(hdr[:4]), binary.LittleEndian.Uint32(hdr[4:])
		if size > maxFrame {
			return nil, fmt.Errorf("journal: frame of %d bytes", size)
		}
		payload := make([]byte, size)
		m, err := io.ReadFull(r, payload)
		if err != nil {
			// A torn tail: the last append did not complete. It was never
			// acknowledged, so nothing depends on it.
			st.TornBytes += 8 + int64(m)
			return st, finish(st, first)
		}
		if crc32.Checksum(payload, castagnoli) != sum {
			rest, _ := io.Copy(io.Discard, r)
			if rest > 0 {
				// A whole frame with a bad checksum and data after it is
				// corruption, not a torn append: refuse to guess.
				return nil, fmt.Errorf("%w: frame %d has a bad checksum and %d bytes follow it", ErrCorrupt, len(st.Records)+1, rest)
			}
			st.TornBytes += 8 + int64(m)
			return st, finish(st, first)
		}
		if first {
			if err := json.Unmarshal(payload, &st.Header); err != nil || st.Header.Type != "header" {
				return nil, fmt.Errorf("%w: first frame is not a header", ErrCorrupt)
			}
			if st.Header.Version != Version {
				return nil, fmt.Errorf("journal version %d is not supported (this tool writes %d)", st.Header.Version, Version)
			}
			first = false
			continue
		}
		var rec Record
		if err := json.Unmarshal(payload, &rec); err != nil || rec.Type != "part" {
			return nil, fmt.Errorf("journal: bad record %d", len(st.Records))
		}
		if rec.Ordinal != len(st.Records) {
			return nil, fmt.Errorf("journal: record for part %d after %d records", rec.Ordinal, len(st.Records))
		}
		st.Records = append(st.Records, rec)
		st.Boundaries = append(st.Boundaries, rec.Boundaries...)
	}
}

func finish(st *State, noHeader bool) error {
	if noHeader {
		// The header never became durable: nothing did.
		return ErrNoJournal
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
