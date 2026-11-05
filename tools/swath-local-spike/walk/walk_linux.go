// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package walk

import (
	"errors"
	"os"
	"slices"
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/unix"

	"github.com/varveio/swath/tools/swath-local-spike/record"
)

const defaultEngine = EngineGetdents

// dent is one directory entry from getdents64 (or os.File.ReadDir).
type dent struct {
	name string
	ino  uint64
	typ  uint8 // DT_* value
}

// sharedDir keeps a directory fd open until every stat batch of it is done.
type sharedDir struct {
	fd    int
	close func()
	refs  atomic.Int32
}

func (d *sharedDir) release() {
	if d.refs.Add(-1) == 0 {
		d.close()
	}
}

// statBatch is a slice of one large directory's entries that any worker can stat.
type statBatch struct {
	dir   *sharedDir
	job   dirJob
	dents []dent
}

// nativeReader returns the Linux fd-relative reader for the selected engine.
func nativeReader(opt Options) dirReader {
	var mask uint32
	if opt.Fields == FieldsStatx {
		mask = unix.STATX_ALL
		if opt.StatxMask == "basic" {
			mask = unix.STATX_BASIC_STATS
		}
	}
	raw := opt.Engine == EngineGetdents
	return func(w *worker, job dirJob) {
		if job.batch != nil {
			statEntries(w, job.batch.job, job.batch.dir.fd, job.batch.dents, mask)
			job.batch.dir.release()
			return
		}
		flags := unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC
		if job.prefix != "" { // the root may be a symlink the user asked for; below it never follow
			flags |= unix.O_NOFOLLOW
		}
		var fd int
		var closeDir func()
		var dents []dent
		var err error
		if raw {
			fd, err = unix.Open(job.path, flags, 0)
			if err != nil {
				w.stats.OpenErrors++
				return
			}
			closeDir = func() { unix.Close(fd) }
			dents, err = readGetdents(w, fd)
		} else {
			var f *os.File
			f, err = os.OpenFile(job.path, flags, 0)
			if err != nil {
				w.stats.OpenErrors++
				return
			}
			closeDir = func() { f.Close() }
			dents, fd, err = readOSFile(f)
		}
		if err != nil {
			w.stats.ReadDirErrors++
		}
		if w.opt.InoSort {
			slices.SortFunc(dents, func(a, b dent) int {
				switch {
				case a.ino < b.ino:
					return -1
				case a.ino > b.ino:
					return 1
				}
				return 0
			})
		}
		split := w.opt.SplitEntries
		if split <= 0 || len(dents) <= split || w.opt.Fields == FieldsNames {
			statEntries(w, job, fd, dents, mask)
			closeDir()
			return
		}
		// Large directory: keep the first chunk, hand the rest to other workers.
		chunk := max(1, split/2)
		d := &sharedDir{fd: fd, close: closeDir}
		var chunks [][]dent
		for start := chunk; start < len(dents); start += chunk {
			chunks = append(chunks, dents[start:min(start+chunk, len(dents))])
		}
		d.refs.Store(int32(len(chunks) + 1))
		for _, c := range chunks {
			w.children = append(w.children, dirJob{batch: &statBatch{dir: d, job: job, dents: c}})
		}
		w.stats.SplitBatches += int64(len(chunks))
		statEntries(w, job, fd, dents[:chunk], mask)
		d.release()
	}
}

// readOSFile reads names and d_type through os.File.ReadDir(-1) and returns
// the raw fd (via SyscallConn, which unlike Fd() does not reset blocking mode).
func readOSFile(f *os.File) ([]dent, int, error) {
	ents, err := f.ReadDir(-1)
	fd := -1
	if rc, cerr := f.SyscallConn(); cerr == nil {
		_ = rc.Control(func(u uintptr) { fd = int(u) })
	}
	out := make([]dent, len(ents))
	for i, e := range ents {
		out[i] = dent{name: e.Name(), typ: dtypeOf(e.Type())}
	}
	return out, fd, err
}

// readGetdents parses getdents64 records from a reused 64 KiB buffer
// (the approach of github.com/charlievieth/fastwalk, MIT, re-implemented).
func readGetdents(w *worker, fd int) ([]dent, error) {
	if w.direntBuf == nil {
		w.direntBuf = make([]byte, 64<<10)
	}
	out := w.dents[:0]
	var rerr error
loop:
	for {
		n, err := unix.Getdents(fd, w.direntBuf)
		if err == unix.EINTR {
			continue
		}
		if err != nil {
			rerr = err
			break
		}
		if n <= 0 {
			break
		}
		buf := w.direntBuf[:n]
		for len(buf) > 0 {
			// struct linux_dirent64 { u64 d_ino; s64 d_off; u16 d_reclen; u8 d_type; char d_name[]; }
			reclen := int(*(*uint16)(unsafe.Pointer(&buf[16])))
			if reclen <= 19 || reclen > len(buf) {
				rerr = errors.New("malformed dirent")
				break loop
			}
			rec := buf[:reclen]
			buf = buf[reclen:]
			ino := *(*uint64)(unsafe.Pointer(&rec[0]))
			if ino == 0 {
				continue
			}
			nameBytes := rec[19:]
			for i, c := range nameBytes {
				if c == 0 {
					nameBytes = nameBytes[:i]
					break
				}
			}
			if len(nameBytes) == 1 && nameBytes[0] == '.' || len(nameBytes) == 2 && nameBytes[0] == '.' && nameBytes[1] == '.' {
				continue
			}
			out = append(out, dent{name: string(nameBytes), ino: ino, typ: rec[18]})
		}
	}
	// Return a private copy: w.dents is reused, but stat batches may outlive this call.
	res := slices.Clone(out)
	w.dents = out[:0]
	return res, rerr
}

// statEntries stats (or, for the names tier, types) each entry relative to fd.
func statEntries(w *worker, job dirJob, fd int, dents []dent, statxMask uint32) {
	var st unix.Stat_t
	var stx unix.Statx_t
	for i := range dents {
		name := dents[i].name
		if !validName(w, name) {
			continue
		}
		r := record.Row{Key: job.prefix + name}
		var err error
		switch {
		case w.opt.Fields == FieldsNames && dents[i].typ != unix.DT_UNKNOWN:
			r.Kind = kindFromDT(dents[i].typ)
		case w.opt.Fields == FieldsNames:
			// d_type unavailable on this filesystem: one stat for the type only.
			if err = unix.Fstatat(fd, name, &st, unix.AT_SYMLINK_NOFOLLOW); err == nil {
				r.Kind = kindFromUnixMode(st.Mode)
			}
		case statxMask != 0:
			err = unix.Statx(fd, name, unix.AT_SYMLINK_NOFOLLOW|unix.AT_NO_AUTOMOUNT, int(statxMask), &stx)
			if err == nil {
				fillStatx(&r, &stx)
			}
		default:
			err = unix.Fstatat(fd, name, &st, unix.AT_SYMLINK_NOFOLLOW)
			if err == nil {
				fillStat(&r, &st)
			}
		}
		if err != nil {
			if errors.Is(err, unix.ENOENT) {
				w.stats.Vanished++
			} else {
				w.stats.StatErrors++
			}
			continue
		}
		switch r.Kind {
		case record.KindDir:
			r.Key += "/"
			w.child(job, name)
		case record.KindSymlink:
			if w.opt.LinkTarget && w.opt.Fields != FieldsNames {
				if t, ok := readlinkat(w, fd, name); ok {
					r.LinkTarget = t
					r.Present |= record.HasLink
				}
			}
		}
		w.emit(r)
	}
}

func readlinkat(w *worker, fd int, name string) (string, bool) {
	if w.linkBuf == nil {
		w.linkBuf = make([]byte, 4096)
	}
	for {
		n, err := unix.Readlinkat(fd, name, w.linkBuf)
		if err != nil {
			w.stats.ReadlinkErrors++
			return "", false
		}
		if n < len(w.linkBuf) {
			return string(w.linkBuf[:n]), true
		}
		w.linkBuf = make([]byte, 2*len(w.linkBuf))
	}
}

func kindFromDT(t uint8) record.Kind {
	switch t {
	case unix.DT_REG:
		return record.KindFile
	case unix.DT_DIR:
		return record.KindDir
	case unix.DT_LNK:
		return record.KindSymlink
	case unix.DT_FIFO:
		return record.KindFIFO
	case unix.DT_SOCK:
		return record.KindSocket
	case unix.DT_CHR:
		return record.KindCharDevice
	case unix.DT_BLK:
		return record.KindBlockDevice
	}
	return record.KindUnknown
}

func dtypeOf(m os.FileMode) uint8 {
	switch kindOf(m) {
	case record.KindFile:
		return unix.DT_REG
	case record.KindDir:
		return unix.DT_DIR
	case record.KindSymlink:
		return unix.DT_LNK
	case record.KindFIFO:
		return unix.DT_FIFO
	case record.KindSocket:
		return unix.DT_SOCK
	case record.KindCharDevice:
		return unix.DT_CHR
	case record.KindBlockDevice:
		return unix.DT_BLK
	}
	return unix.DT_UNKNOWN
}

func kindFromUnixMode(m uint32) record.Kind {
	switch m & unix.S_IFMT {
	case unix.S_IFREG:
		return record.KindFile
	case unix.S_IFDIR:
		return record.KindDir
	case unix.S_IFLNK:
		return record.KindSymlink
	case unix.S_IFIFO:
		return record.KindFIFO
	case unix.S_IFSOCK:
		return record.KindSocket
	case unix.S_IFCHR:
		return record.KindCharDevice
	case unix.S_IFBLK:
		return record.KindBlockDevice
	}
	return record.KindUnknown
}

func fillStat(r *record.Row, st *unix.Stat_t) {
	r.Kind = kindFromUnixMode(st.Mode)
	r.Size = st.Size
	r.Mode = st.Mode & 0o7777
	r.UID = st.Uid
	r.GID = st.Gid
	r.Nlink = uint32(st.Nlink)
	r.Inode = int64(st.Ino)
	r.Dev = int64(st.Dev)
	r.Blocks = st.Blocks * 512
	r.Mtime = record.Micros(st.Mtim.Unix())
	r.Ctime = record.Micros(st.Ctim.Unix())
	r.Atime = record.Micros(st.Atim.Unix())
	r.Present |= record.HasStat
}

func fillStatx(r *record.Row, s *unix.Statx_t) {
	r.Kind = kindFromUnixMode(uint32(s.Mode))
	r.Size = int64(s.Size)
	r.Mode = uint32(s.Mode) & 0o7777
	r.UID = s.Uid
	r.GID = s.Gid
	r.Nlink = s.Nlink
	r.Inode = int64(s.Ino)
	r.Dev = int64(unix.Mkdev(s.Dev_major, s.Dev_minor))
	r.Blocks = int64(s.Blocks) * 512
	r.Mtime = record.Micros(s.Mtime.Sec, int64(s.Mtime.Nsec))
	r.Ctime = record.Micros(s.Ctime.Sec, int64(s.Ctime.Nsec))
	r.Atime = record.Micros(s.Atime.Sec, int64(s.Atime.Nsec))
	r.Present |= record.HasStat
	if s.Mask&unix.STATX_BTIME != 0 {
		r.Btime = record.Micros(s.Btime.Sec, int64(s.Btime.Nsec))
		r.Present |= record.HasBtime
	}
}
