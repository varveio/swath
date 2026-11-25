// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package localfs

// filesystemMechanism: a junction or a volume mount point is a symbolic
// link to the walk (reparseLinks), which it never enters, so the walk
// stays on the root's volume with or without OneFilesystem.
const filesystemMechanism = "reparse_point"

func filesystemID(int) ([2]int32, error) { return [2]int32{}, nil }
