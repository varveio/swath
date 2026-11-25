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
// (AllocationSize) and its file ID. No call is added per file; a
// directory's own allocation, which the enumeration gives as 0, is read
// through a handle (fillDirBlocks).

// winInfo is what one directory entry says of a file: Go's
// Win32FileAttributeData (attributes, times, length), its allocation size,
// its file ID (0: none, as FileFullDirectoryInfo gives, or a filesystem
// without IDs) and, for a reparse point, its tag.
type winInfo struct {
	syscall.Win32FileAttributeData
	AllocationSize int64
	FileID         int64
	ReparseTag     uint32
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
// falls back to the other once. A read that fails after some entries
// returns them with the error, as ReadDir does.
func readDir(f *os.File) ([]fs.DirEntry, error) {
	h := windows.Handle(f.Fd())
	class := uint32(windows.FileFullDirectoryRestartInfo)
	var flags uint32
	if readDirFileIDs && windows.GetVolumeInformationByHandle(h, nil, 0, nil, nil, &flags, nil, 0) == nil &&
		flags&windows.FILE_SUPPORTS_OBJECT_IDS != 0 && flags&windows.FILE_SUPPORTS_OPEN_BY_FILE_ID != 0 {
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

// fillRoot fills what a file root's Lstat leaves out (expandFile): its
// space on disk and file ID, read through a handle (byHandle). They are
// current, where an enumeration's may lag (fillSys). The file ID is taken
// where readDir would take one (the volume's flags). A failed read leaves
// both null.
func fillRoot(path string, r *Entry) {
	alloc, id, err := byHandle(path, true)
	if err != nil {
		return
	}
	r.Blocks, r.Present = alloc, r.Present|HasBlocks
	if id != 0 {
		r.Inode, r.Present = id, r.Present|HasInode
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
func fillDirBlocks(path string, r *Entry) {
	alloc, _, err := byHandle(path, false)
	if err != nil {
		r.Blocks, r.Present = 0, r.Present&^HasBlocks
		return
	}
	r.Blocks, r.Present = alloc, r.Present|HasBlocks
}

// byHandle reads the space on disk of the entry at path, and with id its
// file ID where the volume has them (0 otherwise), through a handle that
// asks for attributes alone, opened without following a link, so that one
// another program holds open, or a directory whose listing is denied, is
// read too.
func byHandle(path string, id bool) (alloc, fileID int64, err error) {
	p, err := windows.UTF16PtrFromString(longPathW(path))
	if err != nil {
		return 0, 0, err
	}
	h, err := windows.CreateFile(p, windows.FILE_READ_ATTRIBUTES, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil,
		windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT, 0)
	if err != nil {
		return 0, 0, err
	}
	defer windows.CloseHandle(h)
	var s struct { // FILE_STANDARD_INFO
		AllocationSize, EndOfFile int64
		NumberOfLinks             uint32
		DeletePending, Directory  uint8
	}
	if err := windows.GetFileInformationByHandleEx(h, windows.FileStandardInfo, (*byte)(unsafe.Pointer(&s)), uint32(unsafe.Sizeof(s))); err != nil {
		return 0, 0, err
	}
	var flags uint32
	var bh windows.ByHandleFileInformation
	if id && readDirFileIDs && windows.GetVolumeInformationByHandle(h, nil, 0, nil, nil, &flags, nil, 0) == nil &&
		flags&windows.FILE_SUPPORTS_OBJECT_IDS != 0 && flags&windows.FILE_SUPPORTS_OPEN_BY_FILE_ID != 0 &&
		windows.GetFileInformationByHandle(h, &bh) == nil {
		fileID = int64(bh.FileIndexHigh)<<32 | int64(bh.FileIndexLow)
	}
	return s.AllocationSize, fileID, nil
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
