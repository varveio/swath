// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package sortedparquet reads sorted Parquet listings the way an sstable is
// read: a lower bound is routed to a part and row group by the actual first
// key of every row group, the key column's ColumnIndex and OffsetIndex locate
// the page that can hold it, and only that page onward is decoded, a bounded
// number of rows at a time, in unsigned byte order of the keys.
//
// The package is format-neutral. Open takes the parts in key order and the
// value columns to project; what makes a set of files a sorted listing (the
// footer stamps, a manifest, the completeness rule) is decided by a profile
// before Open: see the javaera and format2 sub-packages. Keys are bytes
// compared unsigned; nothing assumes UTF-8.
//
// What the reader guarantees: every row group's first key is decoded from
// its data page at Open, never taken from statistics, and those keys must
// ascend strictly across row groups and parts; every key page it decodes
// must hold one non-empty key per row of the page, strictly ascending, inside
// the page's ColumnIndex bounds, and after every key decoded before it in the
// same read. Page bounds are used conservatively: a bound at least as long
// as the writer's recorded truncation length may be a truncation, so a page
// is skipped only when a key at or after the target provably cannot be on
// it. A violation found while reading wraps ErrCorrupt; keys out of order
// inside one row group are a *DisorderError (reason row_group_disorder).
//
// What it does not check: keys it never decodes, values beyond their
// Parquet type (a null size or time is returned as null), or that the files
// are unchanged after Open.
//
// It imports only the standard library and parquet-go, so a module outside
// this repository can import it.
package sortedparquet
