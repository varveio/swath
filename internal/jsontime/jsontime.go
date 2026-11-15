// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package jsontime is the one timestamp format of every JSON document swath
// writes for a program to read: the run report and run.json, error.json,
// progress.json, the manifest, the journal and the log events. It is RFC
// 3339 in UTC with exactly six fraction digits, the rows' microsecond
// precision, so the strings have one width and sort as the times do.
package jsontime

import "time"

// Layout is the format: 2026-11-15T12:00:00.000000Z.
const Layout = "2006-01-02T15:04:05.000000Z07:00"

// Format is t in Layout, in UTC.
func Format(t time.Time) string { return t.UTC().Format(Layout) }

// Time is a time.Time that marshals in Layout. It reads any RFC 3339 time
// (time.Time's UnmarshalJSON).
type Time struct{ time.Time }

// Of is t as a Time, in UTC.
func Of(t time.Time) Time { return Time{t.UTC()} }

// MarshalJSON writes t in Layout.
func (t Time) MarshalJSON() ([]byte, error) {
	b := make([]byte, 0, len(Layout)+2)
	b = append(b, '"')
	b = t.UTC().AppendFormat(b, Layout)
	return append(b, '"'), nil
}
