// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package listing

import (
	"strconv"
	"time"
)

// Kind is what an entry is. Object stores have objects and common
// prefixes; local sources have files, directories, symbolic links and other
// entries. The zero Kind is no kind. The numeric values are not stable:
// record a kind by its String form.
type Kind uint8

const (
	// KindObject is an object in an object store.
	KindObject Kind = iota + 1
	// KindPrefix is a common prefix of an object store's delimiter listing:
	// a synthetic entry standing for the keys that begin with it. It is not
	// an object, says nothing about an object at exactly its key, and is
	// never counted.
	KindPrefix
	// KindFile is a regular file.
	KindFile
	// KindDir is a directory. Its key ends in "/".
	KindDir
	// KindSymlink is a symbolic link. It is listed, never followed, and its
	// key has no trailing "/" whatever it points to.
	KindSymlink
	// KindOther is any other local entry: a FIFO, a socket or a device.
	KindOther
)

var kindNames = [...]string{
	KindObject:  "object",
	KindPrefix:  "prefix",
	KindFile:    "file",
	KindDir:     "dir",
	KindSymlink: "symlink",
	KindOther:   "other",
}

// String returns the kind's name: object, prefix, file, dir, symlink or
// other; Kind(N) for a value that is no kind.
func (k Kind) String() string {
	if int(k) < len(kindNames) && kindNames[k] != "" {
		return kindNames[k]
	}
	return "Kind(" + strconv.Itoa(int(k)) + ")"
}

// Entry is one listed entry. An optional value is a pointer: nil is
// unknown, never zero. A file with several hard links is one Entry per
// directory entry; nothing is deduplicated.
type Entry struct {
	// Key is the entry's key. A KindDir's and a KindPrefix's end in "/".
	Key []byte
	// Kind is what the entry is.
	Kind Kind
	// Size is the logical size in bytes. A live source sets it on every
	// KindObject and KindFile; an inventory listed without sizes has none.
	// A KindPrefix has none.
	Size *int64
	// AllocatedBytes is the space a local entry takes on disk: st_blocks ×
	// 512.
	AllocatedBytes *int64
	// LastModified is an object's last-modified time or a local entry's
	// mtime.
	LastModified *time.Time
	// StorageClass is an object's storage class, or an Azure blob's access
	// tier.
	StorageClass *string
	// Device, Inode and Links are a local entry's st_dev, st_ino and
	// st_nlink. They are evidence of the entry as it was listed, not its
	// identity: device numbers can change across mounts and inode numbers
	// are reused.
	Device *uint64
	Inode  *uint64
	Links  *uint32
}
