// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

// The portable engine reads a directory itself on Windows (readDir), with
// the call os.File.ReadDir makes (GetFileInformationByHandleEx), so that
// each entry keeps what that call returns and Go drops: its space on disk
// (AllocationSize) and its file ID. No call is added per file but at the
// links tier (fillLinks); a directory's own allocation, which the
// enumeration gives as 0, is read through a handle (fillDirBlocks).

// winInfo is what one directory entry says of a file: Go's
// Win32FileAttributeData (attributes, times, length), its allocation size,
// its file ID (0: none, as FileFullDirectoryInfo gives, or a filesystem
// without IDs), for a reparse point its tag, and the serial number of the
// volume its directory is on (HasVolume: read), which is the entry's: an
// entry is opened as itself, never followed (byHandle).
type winInfo struct {
	syscall.Win32FileAttributeData
	AllocationSize int64
	FileID         int64
	ReparseTag     uint32
	Volume         uint32
	HasVolume      bool
}

// winEntry is one entry of readDir: an fs.DirEntry that is its own
// fs.FileInfo, with no call to make for Info.
type winEntry struct {
	name string
	mode fs.FileMode
	info winInfo
}

func (e *winEntry) Name() string               { return e.name }
func (e *winEntry) IsDir() bool                { return e.mode.IsDir() }
func (e *winEntry) Type() fs.FileMode          { return e.mode.Type() }
func (e *winEntry) Info() (fs.FileInfo, error) { return e, nil }
func (e *winEntry) Mode() fs.FileMode          { return e.mode }
func (e *winEntry) Sys() any                   { return &e.info }
func (e *winEntry) Size() int64 {
	return int64(e.info.FileSizeHigh)<<32 | int64(e.info.FileSizeLow)
}
func (e *winEntry) ModTime() time.Time {
	return time.Unix(0, e.info.LastWriteTime.Nanoseconds())
}

// Reparse tags Go's file mode tells apart (os/types_windows.go).
const (
	ioReparseTagDedup      = 0x80000013 // IO_REPARSE_TAG_DEDUP
	ioReparseTagAFUnix     = 0x80000023 // IO_REPARSE_TAG_AF_UNIX
	ioReparseTagMountPoint = 0xA0000003 // IO_REPARSE_TAG_MOUNT_POINT
	nameSurrogateBit       = 0x20000000 // IsReparseTagNameSurrogate
)

// winMode is the file mode Go gives an entry with these attributes and,
// for a reparse point, this tag (os/types_windows.go, fileStat.mode): an
// entry of the enumeration has no file type of its own (pipe, character
// device), which only a handle gives. readDir's entries must have the
// modes os.File.ReadDir gives, kinds included (TestReadDirIsReadDir).
func winMode(attrs, tag uint32) fs.FileMode {
	var m fs.FileMode = 0o666
	if attrs&syscall.FILE_ATTRIBUTE_READONLY != 0 {
		m = 0o444
	}
	reparse := attrs&syscall.FILE_ATTRIBUTE_REPARSE_POINT != 0
	if !(reparse && tag&nameSurrogateBit != 0) && attrs&syscall.FILE_ATTRIBUTE_DIRECTORY != 0 {
		m |= fs.ModeDir | 0o111
	}
	if reparse {
		switch tag {
		case syscall.IO_REPARSE_TAG_SYMLINK:
			m |= fs.ModeSymlink
		case ioReparseTagAFUnix:
			m |= fs.ModeSocket
		case ioReparseTagDedup:
		default:
			m |= fs.ModeIrregular
		}
	}
	return m
}

// fileIDDirInfo is FILE_ID_BOTH_DIR_INFO; fullDirInfo is
// FILE_FULL_DIR_INFO, the class for a filesystem without file IDs. Both
// put a reparse point's tag in EaSize.
type fileIDDirInfo struct {
	NextEntryOffset uint32
	FileIndex       uint32
	CreationTime    syscall.Filetime
	LastAccessTime  syscall.Filetime
	LastWriteTime   syscall.Filetime
	ChangeTime      syscall.Filetime
	EndOfFile       int64
	AllocationSize  int64
	FileAttributes  uint32
	FileNameLength  uint32
	EaSize          uint32
	ShortNameLength int8
	ShortName       [12]uint16
	FileID          int64
	FileName        [1]uint16
}

type fullDirInfo struct {
	NextEntryOffset uint32
	FileIndex       uint32
	CreationTime    syscall.Filetime
	LastAccessTime  syscall.Filetime
	LastWriteTime   syscall.Filetime
	ChangeTime      syscall.Filetime
	EndOfFile       int64
	AllocationSize  int64
	FileAttributes  uint32
	FileNameLength  uint32
	EaSize          uint32
	FileName        [1]uint16
}

// readDirFileIDs lets readDir use FileIdBothDirectoryInfo where the
// filesystem has file IDs; tests clear it to read as on one without.
var readDirFileIDs = true

// dirBufs are readDir's buffers: 64 KiB, the most Windows 8.1 takes, and
// room for the longest name.
var dirBufs = sync.Pool{New: func() any { b := make([]byte, 64<<10); return &b }}

// readDir reads the directory f whole, as f.ReadDir(-1) does: the same
// call, class and entries ("." and ".." left out), in the order the
// filesystem gives them. The class is chosen as Go chooses it, by the
// volume's flags (GetVolumeInformationByHandle, once per directory, as Go
// asks it): FileIdBothDirectoryInfo where the filesystem supports object
// IDs and opening by file ID (NTFS, ReFS), else FileFullDirectoryInfo
// (FAT, exFAT, some network shares), whose entries have no file ID. A
// filesystem that takes neither flag's class (an older network server)
// falls back to the other once. The same call gives the volume's serial
// number, which every entry carries. A read that fails after some entries
// returns them with the error, as ReadDir does.
func readDir(f *os.File) ([]fs.DirEntry, error) {
	h := windows.Handle(f.Fd())
	class := uint32(windows.FileFullDirectoryRestartInfo)
	var flags, serial uint32
	volume := windows.GetVolumeInformationByHandle(h, nil, 0, &serial, nil, &flags, nil, 0) == nil
	if readDirFileIDs && volume && flags&windows.FILE_SUPPORTS_OBJECT_IDS != 0 && flags&windows.FILE_SUPPORTS_OPEN_BY_FILE_ID != 0 {
		class = windows.FileIdBothDirectoryRestartInfo
	}
	bp := dirBufs.Get().(*[]byte)
	defer dirBufs.Put(bp)
	buf := *bp
	var ents []fs.DirEntry
	for first := true; ; first = false {
		err := windows.GetFileInformationByHandleEx(h, class, &buf[0], uint32(len(buf)))
		if err != nil && first && class == windows.FileIdBothDirectoryRestartInfo &&
			(errors.Is(err, windows.ERROR_INVALID_PARAMETER) || errors.Is(err, windows.ERROR_INVALID_LEVEL) || errors.Is(err, windows.ERROR_NOT_SUPPORTED)) {
			class = windows.FileFullDirectoryRestartInfo
			err = windows.GetFileInformationByHandleEx(h, class, &buf[0], uint32(len(buf)))
		}
		switch {
		case errors.Is(err, windows.ERROR_NO_MORE_FILES):
			return ents, nil
		case first && errors.Is(err, windows.ERROR_FILE_NOT_FOUND):
			// An empty root directory (MS-FSA 2.1.5.6.3; go.dev/issue/61159).
			return ents, nil
		case err != nil:
			return ents, &os.PathError{Op: "GetFileInformationByHandleEx", Path: f.Name(), Err: err}
		}
		switch class {
		case windows.FileIdBothDirectoryRestartInfo:
			class = windows.FileIdBothDirectoryInfo
		case windows.FileFullDirectoryRestartInfo:
			class = windows.FileFullDirectoryInfo
		}
		for off := 0; ; {
			p := unsafe.Pointer(&buf[off])
			var next uint32
			var name []uint16
			var e winEntry
			if class == windows.FileIdBothDirectoryInfo {
				d := (*fileIDDirInfo)(p)
				next, name = d.NextEntryOffset, unsafe.Slice(&d.FileName[0], d.FileNameLength/2)
				e.info = winInfo{Win32FileAttributeData: syscall.Win32FileAttributeData{FileAttributes: d.FileAttributes,
					CreationTime: d.CreationTime, LastAccessTime: d.LastAccessTime, LastWriteTime: d.LastWriteTime,
					FileSizeHigh: uint32(d.EndOfFile >> 32), FileSizeLow: uint32(d.EndOfFile)},
					AllocationSize: d.AllocationSize, FileID: d.FileID, ReparseTag: d.EaSize}
			} else {
				d := (*fullDirInfo)(p)
				next, name = d.NextEntryOffset, unsafe.Slice(&d.FileName[0], d.FileNameLength/2)
				e.info = winInfo{Win32FileAttributeData: syscall.Win32FileAttributeData{FileAttributes: d.FileAttributes,
					CreationTime: d.CreationTime, LastAccessTime: d.LastAccessTime, LastWriteTime: d.LastWriteTime,
					FileSizeHigh: uint32(d.EndOfFile >> 32), FileSizeLow: uint32(d.EndOfFile)},
					AllocationSize: d.AllocationSize, ReparseTag: d.EaSize}
			}
			if !(len(name) == 1 && name[0] == '.' || len(name) == 2 && name[0] == '.' && name[1] == '.') {
				if e.info.FileAttributes&syscall.FILE_ATTRIBUTE_REPARSE_POINT == 0 {
					e.info.ReparseTag = 0 // EaSize is the extended attributes' size then
				}
				e.info.Volume, e.info.HasVolume = serial, volume
				e.name = syscall.UTF16ToString(name)
				e.mode = winMode(e.info.FileAttributes, e.info.ReparseTag)
				ents = append(ents, &e)
			}
			if next == 0 {
				break
			}
			off += int(next)
		}
	}
}

// handleLinks: the links tier reads each entry through a handle of its
// own (fillLinks).
const handleLinks = true

// fillRoot fills what a file root's Lstat leaves out (expandFile): its
// space on disk and file ID, read through a handle (byHandle), and at the
// links tier (f) its link count, its volume's serial number as dev, and
// its current length. They are current, where an enumeration's may lag
// (fillSys). The file ID is taken where readDir would take one (the
// volume's flags). A failed read leaves them null.
func fillRoot(path string, f Fields, r *Entry, st *Stats) {
	h, err := byHandle(path, askVolume, false, st)
	if err != nil {
		return
	}
	r.Blocks, r.Present = h.alloc, r.Present|HasBlocks
	if h.fileID != 0 {
		r.Inode, r.Present = h.fileID, r.Present|HasInode
	}
	if f == Links && h.hasVolume {
		r.Size, r.Nlink, r.Dev = h.size, h.links, int64(h.volume)
		r.Present |= HasNlink | HasDev
	}
}

// dirBlocks: a directory's own space on disk is read through a handle
// (fillDirBlocks), one per directory, none per file.
const dirBlocks = true

// fillDirBlocks sets the space on disk of the directory at path, whose
// row the enumeration filled: the enumeration gives a directory an
// allocation of 0 whatever its index takes (128 KiB for one of 600
// entries, TestAllocatedBytes), so it is read through a handle (byHandle)
// instead. A failed read leaves it null, which swath du counts as missing
// (checkLocalTotals: BlocksMayLack).
func fillDirBlocks(path string, r *Entry, st *Stats) {
	h, err := byHandle(path, askStandard, true, st)
	if err != nil {
		r.Blocks, r.Present = 0, r.Present&^HasBlocks
		return
	}
	r.Blocks, r.Present = h.alloc, r.Present|HasBlocks
}

// fillLinks reads, at the links tier, the entry at path, which d (readDir's)
// describes and whose row the enumeration filled, through a handle of its
// own (byHandle): one open, one read and one close per entry, which the
// stat tier does not make. It sets the entry's link count, and its dev to
// its volume's serial number (readDir's, read once per directory), so that
// its file ID (inode) and dev identify it as st_dev and st_ino do; and its
// space on disk and, but for a directory, its length, which are current
// where the enumeration's lag under a hard link's other names. Where the
// enumeration gave a file ID, the handle reads it too, so that a name
// replaced since by another file has no link count beside the other
// file's ID. A failed read, or one that finds a directory where the
// listing had a file or the reverse, or another file ID (the name replaced
// since), keeps what the enumeration said, leaves nlink and dev null (a
// directory's space on disk too, as fillDirBlocks), and is counted
// (Handles.Failed); the entry is listed. On a volume without file IDs
// (FAT, exFAT) inode stays null, and so nothing identifies a file there.
func fillLinks(d fs.DirEntry, path string, r *Entry, st *Stats) {
	dir := r.Kind == KindDir
	ask := askStandard
	if r.Has(HasInode) {
		ask = askFileID
	}
	h, err := byHandle(path, ask, dir, st)
	if err == nil && (r.Kind == KindFile && h.dir || dir && !h.dir || ask == askFileID && h.fileID != r.Inode) {
		err = errHandleChanged
		st.Handles.fail(err)
	}
	if err != nil {
		if dir {
			r.Blocks, r.Present = 0, r.Present&^HasBlocks
		}
		return
	}
	r.Blocks, r.Present = h.alloc, r.Present|HasBlocks
	if !dir {
		r.Size = h.size
	}
	fi, err := d.Info()
	if err != nil {
		return
	}
	if _, w := winData(fi); w != nil && w.HasVolume {
		r.Nlink, r.Dev = h.links, int64(w.Volume)
		r.Present |= HasNlink | HasDev
	}
}

// handleAsk is what byHandle reads of an entry besides its
// FileStandardInfo.
type handleAsk uint8

const (
	askStandard handleAsk = iota // nothing more
	askFileID                    // its file ID (fillLinks: the enumeration gave one)
	askVolume                    // its volume's serial number and flags, and its file ID where they say the volume has them (fillRoot)
)

// handleRead is what a handle to an entry says of it (byHandle): its space
// on disk, length and link count; whether it is a directory; and, where
// asked, its file ID (0: the volume has none) and its volume's serial
// number (hasVolume: read).
type handleRead struct {
	alloc, size int64
	links       uint32
	dir         bool
	fileID      int64
	volume      uint32
	hasVolume   bool
}

// byHandle reads the entry at path, a directory if dir, through a handle
// that asks for attributes alone, opened without following a link or any
// other reparse point, so that one another program holds open, or a
// directory whose listing is denied, is read too, and what ask asks besides.
// It counts the read in st.Handles, and a failure by its reason.
func byHandle(path string, ask handleAsk, dir bool, st *Stats) (handleRead, error) {
	if dir {
		st.Handles.Dirs++
	} else {
		st.Handles.Files++
	}
	h, err := openHandle(path, ask)
	if err != nil {
		st.Handles.fail(err)
	}
	return h, err
}

// openHandle is byHandle's read; tests replace it to fail.
var openHandle = readHandle

func readHandle(path string, ask handleAsk) (handleRead, error) {
	var out handleRead
	p, err := windows.UTF16PtrFromString(longPathW(path))
	if err != nil {
		return out, err
	}
	h, err := windows.CreateFile(p, windows.FILE_READ_ATTRIBUTES, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil,
		windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT, 0)
	if err != nil {
		return out, err
	}
	defer windows.CloseHandle(h)
	var s struct { // FILE_STANDARD_INFO
		AllocationSize, EndOfFile int64
		NumberOfLinks             uint32
		DeletePending, Directory  uint8
	}
	if err := windows.GetFileInformationByHandleEx(h, windows.FileStandardInfo, (*byte)(unsafe.Pointer(&s)), uint32(unsafe.Sizeof(s))); err != nil {
		return out, err
	}
	out.alloc, out.size, out.links, out.dir = s.AllocationSize, s.EndOfFile, s.NumberOfLinks, s.Directory != 0
	var flags uint32
	var bh windows.ByHandleFileInformation
	switch ask {
	case askFileID:
		if err := windows.GetFileInformationByHandle(h, &bh); err != nil {
			return out, err
		}
		out.fileID = int64(bh.FileIndexHigh)<<32 | int64(bh.FileIndexLow)
	case askVolume:
		if windows.GetVolumeInformationByHandle(h, nil, 0, &out.volume, nil, &flags, nil, 0) == nil {
			out.hasVolume = true
			if readDirFileIDs && flags&windows.FILE_SUPPORTS_OBJECT_IDS != 0 && flags&windows.FILE_SUPPORTS_OPEN_BY_FILE_ID != 0 &&
				windows.GetFileInformationByHandle(h, &bh) == nil {
				out.fileID = int64(bh.FileIndexHigh)<<32 | int64(bh.FileIndexLow)
			}
		}
	}
	return out, nil
}

// longPathW gives a path of MAX_PATH or more the extended-length form the
// wide calls take (\\?\, \\?\UNC\), as Go does for its own calls; a
// shorter one is unchanged.
func longPathW(path string) string {
	if len(path) < 248 || strings.HasPrefix(path, `\\?\`) {
		return path
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return path
	}
	if strings.HasPrefix(abs, `\\`) {
		return `\\?\UNC\` + abs[2:]
	}
	return `\\?\` + abs
}
