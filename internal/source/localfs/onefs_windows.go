// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package localfs

// filesystemMechanism: a junction or a volume mount point is a symbolic
// link to the walk (reparseLinks), which it never enters, with or without
// OneFilesystem. No device or volume is compared: a directory reparse
// point of another tag (a DFS link) is walked, and so is a directory the
// redirector serves from another volume.
const filesystemMechanism = "reparse_point"

func filesystemID(int) ([2]int32, error) { return [2]int32{}, nil }
