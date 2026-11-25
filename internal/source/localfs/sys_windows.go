// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build windows

package localfs

import (
	"io/fs"
	"syscall"
)

// statLinks: no tier reads a link count, device or inode here.
const statLinks = false

// statBlocks: no tier reads allocated blocks here.
const statBlocks = false

// fillSys: CreationTime is a birth time (Windows has no POSIX ctime through
// FileInfo) and LastAccessTime is the access time. uid, gid, inode, dev, nlink and
// blocks stay null. A junction or a volume mount point is a symbolic link
// (surrogateKind).
func fillSys(r *Entry, fi fs.FileInfo) {
	d, ok := fi.Sys().(*syscall.Win32FileAttributeData)
	if !ok {
		return
	}
	r.Kind = surrogateKind(r.Kind, d.FileAttributes)
	r.Btime = d.CreationTime.Nanoseconds() / 1000
	r.Atime = d.LastAccessTime.Nanoseconds() / 1000
	r.Present |= HasBtime | HasAtime
}

// reparseLinks: a junction or a volume mount point lists as a symbolic link
// (surrogateKind), and the walk never enters one.
const reparseLinks = true

// surrogateDirectory is a directory that is a reparse point.
const surrogateDirectory = syscall.FILE_ATTRIBUTE_REPARSE_POINT | syscall.FILE_ATTRIBUTE_DIRECTORY

// surrogateKind is k, the kind of an entry with the attributes attrs, except
// for a junction or a volume mount point, which is KindSymlink. Both are a
// directory reparse point whose tag is a name surrogate
// (IO_REPARSE_TAG_MOUNT_POINT), which Go reports without the directory bit
// and, unlike a symbolic link's tag, as ModeIrregular: kind unknown. A
// directory reparse point of another tag (a cloud placeholder, say) keeps
// the directory bit and is walked. The target, read as a symbolic link's,
// tells a junction (a path) from a mount point (\\?\Volume{GUID}\).
func surrogateKind(k Kind, attrs uint32) Kind {
	if k == KindUnknown && attrs&surrogateDirectory == surrogateDirectory {
		return KindSymlink
	}
	return k
}

// reparseKind is surrogateKind for fi, the directory enumeration's own
// (os.DirEntry.Info: no call).
func reparseKind(k Kind, fi fs.FileInfo) Kind {
	if d, ok := fi.Sys().(*syscall.Win32FileAttributeData); ok {
		return surrogateKind(k, d.FileAttributes)
	}
	return k
}
