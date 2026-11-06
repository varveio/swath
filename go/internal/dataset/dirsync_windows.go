// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build windows

package dataset

// DirSyncEnforced reports whether syncDir makes a directory's entries
// durable here.
var DirSyncEnforced = false // a variable for tests

// syncDir does nothing on Windows: the platform has no directory fsync
// through the Go runtime (a read-only directory handle cannot be flushed).
// A process crash loses nothing, since the system cache survives it; after
// a power loss or a system crash, NTFS replays its metadata log to a
// consistent state, which need not include the latest creations and
// renames even when the files' data was synced. A journaled run relies on
// those entries, so it is refused here (listing.ErrUnenforced).
func syncDir(string) error { return nil }
