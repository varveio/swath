// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build !linux

package walk

// Non-Linux builds have no fd-relative fast path yet: the default engine is
// the portable os.ReadDir + DirEntry.Info() walker. Natural fast paths for
// later: getattrlistbulk(2) on macOS, FindFirstFileEx/GetFileInformationByHandleEx
// directory enumeration on Windows (both return size and times with names).
const defaultEngine = EnginePortable

type dent struct{}
type statBatch struct{}

func nativeReader(Options) dirReader { return nil }
