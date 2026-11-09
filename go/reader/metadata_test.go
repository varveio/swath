// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	seekparquet "github.com/varveio/seekparquet/go"
)

func manifestWith(ext string) seekparquet.Metadata {
	m := seekparquet.Metadata{SchemaID: string(S3ObjectsV1)}
	if ext != "" {
		m.Extensions = json.RawMessage(ext)
	}
	return m
}

func TestParseMetadata(t *testing.T) {
	full := manifestWith(`{"swath": {"published_at": "2026-11-08T18:00:00.123456Z",
		"source": {"kind": "s3", "uri": "s3://bucket/logs/", "container": "bucket"},
		"scope": {"kind": "prefix", "prefix": "logs/"},
		"coverage": {"complete": true, "stop_reason": "completed", "skipped_entries": 0, "exclusions": [], "filters": [], "snapshot": false,
			"listing_started_at": "2026-11-08T17:00:00Z", "listing_finished_at": "2026-11-08T17:59:00Z"},
		"part_md5": ["ignored"]}, "other": {"x": 1}}`)
	m, err := ParseMetadata(full)
	if err != nil {
		t.Fatal(err)
	}
	c := m.Coverage
	if m.SchemaID != S3ObjectsV1 || !m.Swath || !m.PublishedAt.Equal(time.Date(2026, 11, 8, 18, 0, 0, 123456000, time.UTC)) ||
		*m.Source.Kind != "s3" || *m.Source.URI != "s3://bucket/logs/" || *m.Source.Container != "bucket" || *m.Scope.Kind != "prefix" || *m.Scope.Prefix != "logs/" ||
		!*c.Complete || *c.StopReason != "completed" || *c.SkippedEntries != 0 || c.Exclusions == nil || c.Filters == nil || len(c.Filters) != 0 || *c.Snapshot ||
		c.ListingStartedAt.Hour() != 17 || c.ListingFinishedAt.Minute() != 59 {
		t.Fatalf("parsed %+v", m)
	}

	// Absent members are nil, present zero values are not.
	m, err = ParseMetadata(manifestWith(`{"swath": {"source": {"kind": "local-fs", "uri": "/data"}, "scope": {"kind": "whole"},
		"coverage": {"complete": false, "skipped_entries": 0}}}`))
	if err != nil {
		t.Fatal(err)
	}
	c = m.Coverage
	if m.PublishedAt != nil || m.Source.Container != nil || m.Scope.Prefix != nil || c.Complete == nil || *c.Complete || c.SkippedEntries == nil ||
		c.StopReason != nil || c.Filters != nil || c.Exclusions != nil || c.Snapshot != nil || c.ListingStartedAt != nil {
		t.Fatalf("partial %+v %+v", m, *c)
	}

	for _, ext := range []string{"", `{}`, `{"other": {}}`} {
		m, err := ParseMetadata(manifestWith(ext))
		if err != nil || m.Swath || m.Source != nil || m.Scope != nil || m.Coverage != nil || m.PublishedAt != nil {
			t.Fatalf("%q: %+v, %v", ext, m, err)
		}
	}

	for _, ext := range []string{
		`{"swath": {"coverage": {"complete": "yes"}}}`,
		`{"swath": {"coverage": {"skipped_entries": 1.5}}}`,
		`{"swath": {"coverage": {"filters": "none"}}}`,
		`{"swath": {"published_at": "yesterday"}}`,
		`{"swath": []}`,
	} {
		_, err := ParseMetadata(manifestWith(ext))
		var se *SemanticError
		if !errors.Is(err, ErrCoverage) || errors.Is(err, ErrSchema) || !errors.As(err, &se) || se.Err == nil {
			t.Errorf("%s: %v, want a coverage error with its cause", ext, err)
		}
	}
}
