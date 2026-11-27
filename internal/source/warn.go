// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package source

// The codes of the warnings the pipeline and the providers give (the warn
// functions' code, the warning event's code member under --log-format
// json: docs/reference/log-events.md). The command's own are in
// package command.
const (
	WarnAnonymous            = "anonymous_credentials_ignored" // --anonymous ignores credentials the environment holds
	WarnTLSUnverified        = "tls_unverified"                // --no-verify-ssl
	WarnFlatAssumed          = "namespace_unread"              // Azure Blob: the account's namespace could not be read; listed as flat
	WarnDirectoryTooLarge    = "directory_too_large"           // a local directory past what a listing sorts in memory, left out
	WarnLockUnenforced       = "lock_unenforced"               // the output lock is not enforced on this platform
	WarnDurableSpillOff      = "durable_spill_off"             // durable spill could not be set up
	WarnUnresumableNotMarked = "unresumable_not_marked"        // the journal could not mark the run unresumable
	WarnSeedFromCuts         = "seed_from_cuts_refused"        // --seed-from: the dataset's cut file is not used
	WarnSeedFromUnavailable  = "seed_from_unavailable"         // a resume goes on without its --seed-from prior
	WarnEncodersLimited      = "encoders_limited"              // --encoders asks for more than the memory and descriptors carry
	WarnSpillNotRemoved      = "spill_not_removed"             // spill files or the durable spill directory were not removed
	WarnSpillInMemory        = "spill_in_memory"               // spill is on a memory filesystem (tmpfs, ramfs)
	WarnRunReportUnwritten   = "run_report_unwritten"          // the dataset is committed; run.json is not written
	WarnCutsUnwritten        = "cuts_unwritten"                // the dataset is committed; its cut file is not written
	WarnCommitMarker         = "commit_marker_unwritten"       // a dataset in a bucket: the work directory's commit marker is not written
	WarnAftermath            = "aftermath_unfinished"          // a dataset in a bucket: committed, its orphan cleanup or report not finished
	WarnReplacedNotRemoved   = "replaced_not_removed"          // --overwrite: the replaced dataset's files were not removed
	WarnTreeListing          = "tree_listing"                  // a folder-by-folder listing; a dataset it writes cannot be resumed
	WarnUnorderedRange       = "range_listing_unordered"       // S3: --s3-listing range on a store that does not list in byte order (R2)
)

// WarningCodes are the codes above, for the documentation's check.
var WarningCodes = []string{WarnAnonymous, WarnTLSUnverified, WarnFlatAssumed, WarnDirectoryTooLarge, WarnLockUnenforced,
	WarnDurableSpillOff, WarnUnresumableNotMarked, WarnSeedFromCuts, WarnSeedFromUnavailable, WarnEncodersLimited,
	WarnSpillNotRemoved, WarnSpillInMemory, WarnRunReportUnwritten, WarnCutsUnwritten, WarnCommitMarker, WarnAftermath,
	WarnReplacedNotRemoved, WarnTreeListing, WarnUnorderedRange}
