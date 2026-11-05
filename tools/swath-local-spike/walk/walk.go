// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package walk lists a local directory tree in parallel. The unit of work is
// one directory; N workers share a LIFO queue with a pending counter, so the
// walk terminates exactly when no directory is queued or being read.
//
// Engines:
//   - getdents (Linux default): open dir, raw getdents64 (d_type, no stat),
//     then fstatat/statx relative to the open directory fd.
//   - fstatat: the same with os.File.ReadDir(-1) instead of raw getdents64.
//   - parallel-lstat: same parallel walker, full-path os.Lstat per entry.
//   - portable: os.ReadDir (sorted) + DirEntry.Info(); the non-Linux default.
//   - walkdir: single-threaded filepath.WalkDir + os.Lstat baseline.
package walk

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/varveio/swath/tools/swath-local-spike/record"
)

// Fields selects the metadata cost tier.
type Fields int

const (
	FieldsNames Fields = iota // getdents only: key + kind from d_type
	FieldsStat                // one stat per entry (+ readlink per symlink)
	FieldsStatx               // statx per entry (Linux fstatat engine only); adds birth_time
)

func ParseFields(s string) (Fields, error) {
	switch s {
	case "names":
		return FieldsNames, nil
	case "stat", "":
		return FieldsStat, nil
	case "statx":
		return FieldsStatx, nil
	}
	return 0, fmt.Errorf("unknown --fields %q (names|stat|statx)", s)
}

func (f Fields) String() string { return [...]string{"names", "stat", "statx"}[f] }

// Engine names.
const (
	EngineFstatat       = "fstatat"
	EngineGetdents      = "getdents"
	EngineParallelLstat = "parallel-lstat"
	EnginePortable      = "portable"
	EngineWalkDir       = "walkdir"
)

// Options configure a walk.
type Options struct {
	Engine     string
	Workers    int
	Fields     Fields
	StatxMask  string // "basic" or "all" (statx tier only)
	NoDirs     bool   // omit kind=DIR rows
	LinkTarget bool   // readlink symlinks (stat/statx tiers)
	BatchRows  int    // rows per Emit batch
	// Linux fd engines only:
	InoSort      bool // stat each directory's entries in d_ino order (getdents engine)
	SplitEntries int  // directories with more entries are statted in chunks by several workers (0 = never, else >= 2)
	// SkipDirKeys are directory keys ("a/b/") neither emitted nor descended,
	// e.g. an --out located inside the root (found by directory identity).
	SkipDirKeys map[string]bool
}

// Sink receives row batches from workers concurrently. It takes ownership of
// rows and may return a slice (len 0) for the worker to reuse, or nil.
type Sink interface {
	Emit(rows []record.Row) []record.Row
}

// Stats are summed over workers at the end of the walk.
type Stats struct {
	Files          int64 `json:"files"` // non-directory entries emitted
	Dirs           int64 `json:"dirs"`  // directories found below the root (rows emitted unless --no-dirs)
	Bytes          int64 `json:"bytes"` // sum of st_size over non-directory entries
	DirsRead       int64 `json:"dirs_read"`
	OpenErrors     int64 `json:"open_errors"`
	ReadDirErrors  int64 `json:"readdir_errors"`
	StatErrors     int64 `json:"stat_errors"`
	Vanished       int64 `json:"vanished"`
	ReadlinkErrors int64 `json:"readlink_errors"`
	InvalidUTF8    int64 `json:"invalid_utf8_names"` // skipped; a directory's whole subtree is skipped
	SplitBatches   int64 `json:"split_batches"`      // extra stat batches from large directories
	SkippedDirs    int64 `json:"skipped_dirs"`       // SkipDirKeys hits (e.g. --out inside the root)
	UnsafeDirs     int64 `json:"unsafe_dirs"`        // portable engines: a child dir became a symlink/non-dir before open
	// dfs-parallel: peak rows read but not yet emitted (reorder buffer).
	ReorderMaxBuffered int64 `json:"reorder_max_buffered_rows,omitempty"`
}

func (s *Stats) add(o *Stats) {
	s.Files += o.Files
	s.Dirs += o.Dirs
	s.Bytes += o.Bytes
	s.DirsRead += o.DirsRead
	s.OpenErrors += o.OpenErrors
	s.ReadDirErrors += o.ReadDirErrors
	s.StatErrors += o.StatErrors
	s.Vanished += o.Vanished
	s.ReadlinkErrors += o.ReadlinkErrors
	s.InvalidUTF8 += o.InvalidUTF8
	s.SplitBatches += o.SplitBatches
	s.SkippedDirs += o.SkippedDirs
	s.UnsafeDirs += o.UnsafeDirs
}

// Errors is the total of all error counters (vanished and invalid names included).
func (s *Stats) Errors() int64 {
	return s.OpenErrors + s.ReadDirErrors + s.StatErrors + s.Vanished + s.ReadlinkErrors + s.InvalidUTF8
}

// DefaultWorkers is min(64, max(4, 3*GOMAXPROCS)): extra workers overlap
// blocking stat/getdents calls (cold cache, network filesystems).
func DefaultWorkers() int { return min(64, max(4, 3*runtime.GOMAXPROCS(0))) }

// DefaultEngine is the platform fast path.
func DefaultEngine() string { return defaultEngine }

// Walk lists root. Errors below the root are counted, never fatal; only an
// unusable root or invalid options return an error.
func Walk(root string, opt Options, sink Sink) (Stats, error) {
	if _, err := statRoot(root); err != nil {
		return Stats{}, err
	}
	if opt.Workers <= 0 {
		opt.Workers = DefaultWorkers()
	}
	if opt.BatchRows <= 0 {
		opt.BatchRows = 4096
	}
	if opt.Engine == "" {
		opt.Engine = defaultEngine
	}
	if opt.SplitEntries < 0 || opt.SplitEntries == 1 {
		return Stats{}, errors.New("--split-entries must be 0 (off) or >= 2")
	}
	if opt.Engine == EngineWalkDir {
		if opt.Fields == FieldsStatx {
			return Stats{}, errors.New("--fields statx requires --engine fstatat or getdents (Linux)")
		}
		return walkDir(root, opt, sink)
	}
	read, err := readerFor(opt)
	if err != nil {
		return Stats{}, err
	}
	return parallel(root, opt, sink, read), nil
}

func statRoot(root string) (fs.FileInfo, error) {
	fi, err := os.Stat(root)
	if err != nil {
		return nil, err
	}
	if !fi.IsDir() {
		return nil, fmt.Errorf("%s is not a directory", root)
	}
	return fi, nil
}

func cleanRoot(root string) string { return filepath.Clean(root) }

// readerFor validates engine options and returns the per-directory reader.
func readerFor(opt Options) (dirReader, error) {
	native := opt.Engine == EngineFstatat || opt.Engine == EngineGetdents
	if opt.Fields == FieldsStatx && !native {
		return nil, errors.New("--fields statx requires --engine fstatat or getdents (Linux)")
	}
	if opt.InoSort && opt.Engine != EngineGetdents {
		return nil, errors.New("--ino-sort requires --engine getdents (os.DirEntry hides d_ino)")
	}
	if opt.SplitEntries < 0 || opt.SplitEntries == 1 {
		return nil, errors.New("--split-entries must be 0 (off) or >= 2")
	}
	switch opt.Engine {
	case EngineFstatat, EngineGetdents:
		if r := nativeReader(opt); r != nil {
			return r, nil
		}
		return nil, fmt.Errorf("engine %s is Linux-only; use portable", opt.Engine)
	case EngineParallelLstat:
		return lstatReader, nil
	case EnginePortable:
		return portableReader, nil
	}
	return nil, fmt.Errorf("unknown engine %q", opt.Engine)
}

// dirJob is one directory to read: its path on disk and its key prefix
// ("" for the root, "a/b/" below it).
type dirJob struct {
	path   string
	prefix string
	batch  *statBatch // non-nil: a stat chunk of a large directory (Linux)
}

// worker holds per-goroutine state; dirReader implementations append rows to
// w.batch (via w.emit) and child directories to w.children.
type worker struct {
	opt      *Options
	sink     Sink
	batch    []record.Row
	children []dirJob
	stats    Stats
	linkBuf  []byte
	// Linux getdents engine scratch.
	direntBuf []byte
	dents     []dent
	// Ordered fast path scratch (reused per directory).
	nameArena []byte
	fdents    []fdent
}

// fdent is a directory entry for the ordered fast path: name and key are
// offsets into a per-worker name arena and a per-directory key arena.
type fdent struct {
	nameOff, nameLen uint32
	keyOff, keyLen   uint32
	pos              uint32 // sorted slot
	ino              uint64
	typ              uint8
}

type dirReader func(w *worker, job dirJob)

func (w *worker) emit(r record.Row) {
	if r.Kind == record.KindDir {
		if w.opt.SkipDirKeys[r.Key] {
			w.stats.SkippedDirs++
			return
		}
		w.stats.Dirs++
		if w.opt.NoDirs {
			return
		}
	} else {
		w.stats.Files++
		w.stats.Bytes += r.Size
	}
	w.batch = append(w.batch, r)
	if len(w.batch) >= w.opt.BatchRows {
		w.flush()
	}
}

func (w *worker) flush() {
	if len(w.batch) == 0 {
		return
	}
	next := w.sink.Emit(w.batch)
	if next == nil {
		next = make([]record.Row, 0, w.opt.BatchRows)
	}
	w.batch = next[:0]
}

// child records a subdirectory to visit; its own row was already emitted.
func (w *worker) child(job dirJob, name string) {
	if w.opt.SkipDirKeys[job.prefix+name+"/"] {
		return
	}
	w.children = append(w.children, dirJob{path: job.path + string(os.PathSeparator) + name, prefix: job.prefix + name + "/"})
}

func validName(w *worker, name string) bool {
	if utf8.ValidString(name) {
		return true
	}
	w.stats.InvalidUTF8++
	return false
}

// queue is a LIFO stack with a pending counter (queued + in progress).
type queue struct {
	mu      sync.Mutex
	cond    *sync.Cond
	stack   []dirJob
	pending int
}

func (q *queue) pop() (dirJob, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.stack) == 0 {
		if q.pending == 0 {
			return dirJob{}, false
		}
		q.cond.Wait()
	}
	j := q.stack[len(q.stack)-1]
	q.stack = q.stack[:len(q.stack)-1]
	return j, true
}

// done pushes children and retires one in-progress job atomically.
func (q *queue) done(children []dirJob) {
	q.mu.Lock()
	q.stack = append(q.stack, children...)
	q.pending += len(children) - 1
	wake := len(children) > 0 || q.pending == 0
	q.mu.Unlock()
	if wake {
		q.cond.Broadcast()
	}
}

func parallel(root string, opt Options, sink Sink, read dirReader) Stats {
	q := &queue{stack: []dirJob{{path: cleanRoot(root), prefix: ""}}, pending: 1}
	q.cond = sync.NewCond(&q.mu)
	workers := make([]*worker, opt.Workers)
	var wg sync.WaitGroup
	for i := range workers {
		w := &worker{opt: &opt, sink: sink, batch: make([]record.Row, 0, opt.BatchRows)}
		workers[i] = w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				job, ok := q.pop()
				if !ok {
					break
				}
				if job.batch == nil {
					w.stats.DirsRead++
				}
				read(w, job)
				q.done(w.children)
				w.children = w.children[:0]
			}
			w.flush()
		}()
	}
	wg.Wait()
	var total Stats
	for _, w := range workers {
		total.add(&w.stats)
	}
	return total
}

func classify(w *worker, err error) {
	if errors.Is(err, fs.ErrNotExist) {
		w.stats.Vanished++
	} else {
		w.stats.StatErrors++
	}
}

// kindOf maps Go file mode type bits to a kind.
func kindOf(m fs.FileMode) record.Kind {
	switch {
	case m.IsRegular():
		return record.KindFile
	case m&fs.ModeDir != 0:
		return record.KindDir
	case m&fs.ModeSymlink != 0:
		return record.KindSymlink
	case m&fs.ModeNamedPipe != 0:
		return record.KindFIFO
	case m&fs.ModeSocket != 0:
		return record.KindSocket
	case m&fs.ModeDevice != 0 && m&fs.ModeCharDevice != 0:
		return record.KindCharDevice
	case m&fs.ModeDevice != 0:
		return record.KindBlockDevice
	}
	return record.KindUnknown
}

// fromFileInfo fills a row from os.FileInfo plus the platform Sys() data.
func fromFileInfo(r *record.Row, fi fs.FileInfo) {
	r.Kind = kindOf(fi.Mode())
	r.Size = fi.Size()
	r.Mtime = fi.ModTime().UnixMicro()
	r.Mode = uint32(fi.Mode().Perm())
	if fi.Mode()&fs.ModeSetuid != 0 {
		r.Mode |= 0o4000
	}
	if fi.Mode()&fs.ModeSetgid != 0 {
		r.Mode |= 0o2000
	}
	if fi.Mode()&fs.ModeSticky != 0 {
		r.Mode |= 0o1000
	}
	r.Present |= record.HasSize | record.HasMtime | record.HasMode
	fillSys(r, fi)
}

// infoRow builds the row for name in job given an Lstat-like result.
func infoRow(w *worker, job dirJob, name string, fi fs.FileInfo, readlink func() (string, error)) {
	r := record.Row{Key: job.prefix + name}
	fromFileInfo(&r, fi)
	if r.Kind == record.KindDir {
		r.Key += "/"
		w.child(job, name)
	} else if r.Kind == record.KindSymlink && w.opt.LinkTarget {
		if t, err := readlink(); err == nil {
			r.LinkTarget = filepath.ToSlash(t)
			r.Present |= record.HasLink
		} else {
			w.stats.ReadlinkErrors++
		}
	}
	w.emit(r)
}

// namesRow builds a names-tier row from the d_type-derived DirEntry type.
func namesRow(w *worker, job dirJob, name string, typ fs.FileMode) {
	r := record.Row{Key: job.prefix + name, Kind: kindOf(typ)}
	if r.Kind == record.KindDir {
		r.Key += "/"
		w.child(job, name)
	}
	w.emit(r)
}

// lstatReader: parallel walker with a full-path os.Lstat per entry.
func lstatReader(w *worker, job dirJob) {
	f, ok := openDirChecked(w, job)
	if !ok {
		return
	}
	ents, err := f.ReadDir(-1)
	f.Close()
	if err != nil {
		w.stats.ReadDirErrors++
	}
	for _, e := range ents {
		name := e.Name()
		if !validName(w, name) {
			continue
		}
		if w.opt.Fields == FieldsNames {
			namesRow(w, job, name, e.Type())
			continue
		}
		p := job.path + string(os.PathSeparator) + name
		fi, err := os.Lstat(p)
		if err != nil {
			classify(w, err)
			continue
		}
		infoRow(w, job, name, fi, func() (string, error) { return os.Readlink(p) })
	}
}

// portableReader: os.ReadDir semantics (entries sorted by name) +
// DirEntry.Info(), via a checked open of the directory.
func portableReader(w *worker, job dirJob) {
	f, ok := openDirChecked(w, job)
	if !ok {
		return
	}
	ents, err := f.ReadDir(-1)
	f.Close()
	if err != nil {
		w.stats.ReadDirErrors++
	}
	slices.SortFunc(ents, func(a, b fs.DirEntry) int { return strings.Compare(a.Name(), b.Name()) })
	for _, e := range ents {
		name := e.Name()
		if !validName(w, name) {
			continue
		}
		if w.opt.Fields == FieldsNames {
			namesRow(w, job, name, e.Type())
			continue
		}
		fi, err := e.Info()
		if err != nil {
			classify(w, err)
			continue
		}
		p := job.path + string(os.PathSeparator) + name
		infoRow(w, job, name, fi, func() (string, error) { return os.Readlink(p) })
	}
}

// openDirChecked opens a directory by path for the path-based engines. Below
// the root it re-checks, after the open, that the path is still a real
// directory (Lstat: not a symlink) and is the same object as the opened fd
// (fstat). This is best-effort protection against a directory being swapped
// for a symlink between the parent's listing and this open; the Linux fd
// engines use O_NOFOLLOW instead. The root itself may be a symlink.
func openDirChecked(w *worker, job dirJob) (*os.File, bool) {
	f, err := os.Open(job.path)
	if err != nil {
		w.stats.OpenErrors++
		return nil, false
	}
	if job.prefix == "" {
		return f, true
	}
	ffi, err1 := f.Stat()
	lfi, err2 := os.Lstat(job.path)
	if err1 != nil || err2 != nil || !ffi.IsDir() || !lfi.IsDir() || lfi.Mode()&fs.ModeSymlink != 0 || !os.SameFile(ffi, lfi) {
		f.Close()
		w.stats.UnsafeDirs++
		return nil, false
	}
	return f, true
}

// walkDir: single-threaded filepath.WalkDir + os.Lstat baseline.
func walkDir(root string, opt Options, sink Sink) (Stats, error) {
	w := &worker{opt: &opt, sink: sink, batch: make([]record.Row, 0, opt.BatchRows)}
	root = filepath.Clean(root)
	skip := len(root) + 1
	if os.IsPathSeparator(root[len(root)-1]) {
		skip = len(root)
	}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if d != nil && d.IsDir() {
				// ReadDir failed for this directory (its row was already emitted).
				w.stats.OpenErrors++
				return nil
			}
			classify(w, err)
			return nil
		}
		if p == root {
			w.stats.DirsRead++
			return nil
		}
		name := d.Name()
		if !validName(w, name) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		rel := p[skip:]
		job := dirJob{path: filepath.Dir(p), prefix: filepath.ToSlash(rel[:len(rel)-len(name)])}
		if d.IsDir() && opt.SkipDirKeys[job.prefix+name+"/"] {
			w.stats.SkippedDirs++
			return filepath.SkipDir
		}
		if d.IsDir() {
			w.stats.DirsRead++
		}
		if opt.Fields == FieldsNames {
			namesRow(w, job, name, d.Type())
		} else {
			fi, err := os.Lstat(p)
			if err != nil {
				classify(w, err)
				return nil
			}
			infoRow(w, job, name, fi, func() (string, error) { return os.Readlink(p) })
		}
		w.children = w.children[:0] // WalkDir does its own descent
		return nil
	})
	w.flush()
	return w.stats, err
}
