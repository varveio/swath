// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"encoding/json"
	"errors"
	"strings"
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
	full := manifestWith(`{"swath": {"version": 1, "published_at": "2026-11-08T18:00:00.123456Z",
		"source": {"kind": "s3", "uri": "s3://bucket/logs/", "container": "bucket"},
		"scope": {"kind": "prefix", "prefix": "logs/"},
		"coverage": {"complete": true, "stop_reason": "completed", "skipped_entries": 0, "exclusions": [], "filters": [], "snapshot": false,
			"listing_started_at": "2026-11-08T17:00:00Z", "listing_finished_at": "2026-11-08T17:59:00Z"},
		"committed_object_bytes": 81234, "part_md5": ["ignored"]}, "other": {"x": 1}}`)
	m, err := ParseMetadata(full)
	if err != nil {
		t.Fatal(err)
	}
	c := m.Coverage
	if m.SchemaID != S3ObjectsV1 || !m.Swath || !m.PublishedAt.Equal(time.Date(2026, 11, 8, 18, 0, 0, 123456000, time.UTC)) ||
		*m.Source.Kind != "s3" || *m.Source.URI != "s3://bucket/logs/" || *m.Source.Container != "bucket" || *m.Scope.Kind != "prefix" || *m.Scope.Prefix != "logs/" ||
		!*c.Complete || *c.StopReason != "completed" || *c.SkippedEntries != 0 || c.Exclusions == nil || c.Filters == nil || len(c.Filters) != 0 || *c.Snapshot ||
		c.ListingStartedAt.Hour() != 17 || c.ListingFinishedAt.Minute() != 59 || m.CommittedObjectBytes == nil || *m.CommittedObjectBytes != 81234 {
		t.Fatalf("parsed %+v", m)
	}

	// committed_object_bytes 0 is a total (a dataset of empty objects, or of none);
	// absent is unknown, never 0.
	if m, err := ParseMetadata(manifestWith(`{"swath": {"version": 1, "committed_object_bytes": 0}}`)); err != nil || m.CommittedObjectBytes == nil || *m.CommittedObjectBytes != 0 {
		t.Fatalf("committed_object_bytes 0: %+v, %v", m, err)
	}

	// Absent members are nil, present zero values are not.
	m, err = ParseMetadata(manifestWith(`{"swath": {"version": 1, "source": {"kind": "local-fs", "uri": "/data"}, "scope": {"kind": "whole"},
		"coverage": {"complete": false, "skipped_entries": 0}}}`))
	if err != nil {
		t.Fatal(err)
	}
	c = m.Coverage
	if m.PublishedAt != nil || m.Source.Container != nil || m.Scope.Prefix != nil || c.Complete == nil || *c.Complete || c.SkippedEntries == nil ||
		c.StopReason != nil || c.Filters != nil || c.Exclusions != nil || c.Snapshot != nil || c.ListingStartedAt != nil || m.CommittedObjectBytes != nil {
		t.Fatalf("partial %+v %+v", m, *c)
	}

	for _, ext := range []string{"", `{}`, `{"other": {}}`} {
		m, err := ParseMetadata(manifestWith(ext))
		if err != nil || m.Swath || m.Source != nil || m.Scope != nil || m.Coverage != nil || m.PublishedAt != nil {
			t.Fatalf("%q: %+v, %v", ext, m, err)
		}
	}

	for _, ext := range []string{
		`{"swath": {"version": 1, "coverage": {"complete": "yes"}}}`,
		`{"swath": {"version": 1, "coverage": {"skipped_entries": 1.5}}}`,
		`{"swath": {"version": 1, "coverage": {"filters": "none"}}}`,
		`{"swath": {"version": 1, "published_at": "yesterday"}}`,
		`{"swath": {"version": 1, "committed_object_bytes": "81234"}}`,
		`{"swath": {"version": 1, "committed_object_bytes": 1.5}}`,
		`{"swath": []}`,
	} {
		_, err := ParseMetadata(manifestWith(ext))
		var se *SemanticError
		if !errors.Is(err, ErrMetadata) || errors.Is(err, ErrSchema) || !errors.As(err, &se) || se.Err == nil {
			t.Errorf("%s: %v, want a metadata error with its cause", ext, err)
		}
	}
}

// TestParseMetadataVersion: extensions.swath must hold version 1, the
// integer; absent, another version, or not an integer is KindSchema
// ("unsupported swath metadata version"), before any other member is read.
func TestParseMetadataVersion(t *testing.T) {
	for _, ext := range []string{
		`{"swath": {"source": {"kind": "s3"}}}`,
		`{"swath": {"version": 2, "coverage": {"complete": "a later contract"}}}`,
		`{"swath": {"version": 0}}`,
		`{"swath": {"version": null}}`,
		`{"swath": {"version": "1"}}`,
		`{"swath": {"version": 1.0}}`,
	} {
		_, err := ParseMetadata(manifestWith(ext))
		if !errors.Is(err, ErrSchema) || errors.Is(err, ErrMetadata) || !strings.Contains(err.Error(), "unsupported swath metadata version") {
			t.Errorf("%s: %v, want an unsupported swath metadata version (schema) error", ext, err)
		}
	}
	if m, err := ParseMetadata(manifestWith(`{"swath": {"version": 1}}`)); err != nil || !m.Swath {
		t.Fatalf("version 1: %+v, %v", m, err)
	}
}
