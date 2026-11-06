// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"bytes"
	"encoding/binary"
	"errors"
)

// direntHeader is the fixed part of struct linux_dirent64: d_ino (8),
// d_off (8), d_reclen (2), d_type (1), followed by the NUL-terminated name.
const direntHeader = 19

var errMalformedDirent = errors.New("malformed directory entry")

// parseDirents calls fn for every entry in a getdents64 buffer, skipping
// "." and ".." and deleted (inode 0) entries. name aliases buf.
func parseDirents(buf []byte, fn func(ino uint64, typ uint8, name []byte)) error {
	for len(buf) > 0 {
		if len(buf) < direntHeader {
			return errMalformedDirent
		}
		reclen := int(binary.NativeEndian.Uint16(buf[16:18]))
		if reclen <= direntHeader || reclen > len(buf) {
			return errMalformedDirent
		}
		rec := buf[:reclen]
		buf = buf[reclen:]
		ino := binary.NativeEndian.Uint64(rec[0:8])
		name := rec[direntHeader:]
		if i := bytes.IndexByte(name, 0); i >= 0 {
			name = name[:i]
		}
		if ino == 0 || len(name) == 0 || string(name) == "." || string(name) == ".." {
			continue
		}
		fn(ino, rec[18], name)
	}
	return nil
}
