// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"fmt"
	"time"

	"github.com/varveio/swath/internal/source/objhttp"
)

// ListInput is one ListObjectsV2 request.
type ListInput struct {
	Bucket            string
	Prefix            string
	StartAfter        string
	ContinuationToken string
	Delimiter         string
	MaxKeys           int // 0: the server default (1000)
	FetchOwner        bool
}

// Object is one <Contents> entry. Strings share one arena per page.
type Object struct {
	Key               string
	ETag              string // as sent, quotes included
	Size              int64
	NoSize            bool  // a <Contents> without <Size>: Size is 0, not the object's
	Mtime             int64 // LastModified in Unix microseconds; 0 when absent
	StorageClass      string
	OwnerID           string
	OwnerDisplayName  string
	ChecksumAlgorithm string // the first listed
	ChecksumType      string
}

// Head is one HeadObject response: what it says of the object. A field
// the answer did not carry is nil or empty.
type Head struct {
	Size         *int64     // Content-Length
	LastModified *time.Time // Last-Modified, to the second
	// StorageClass is x-amz-storage-class, which S3 sends for every class
	// but STANDARD.
	StorageClass string
}

// Page is one ListObjectsV2 response.
type Page struct {
	Contents              []Object
	CommonPrefixes        []string
	IsTruncated           bool
	NextContinuationToken string
	// Retained is the bytes the page's strings hold: one arena, kept at
	// its encoded size (URL decoding shrinks the strings in place), which
	// every string of the page keeps alive.
	Retained int
}

// VersionsInput is one ListObjectVersions request.
type VersionsInput struct {
	Bucket    string
	Prefix    string
	Delimiter string
	// KeyMarker: the versions of keys after it. With VersionIDMarker: the
	// versions of KeyMarker after that one, then the keys after it.
	KeyMarker string
	// VersionIDMarker is sent only with a KeyMarker (S3 refuses it alone)
	// and only when not empty (S3 refuses an empty one too).
	VersionIDMarker string
	// MaxKeys: 0 is the server default (1000). It is never sent as 0: S3
	// answers max-keys=0 with an empty page that is not truncated, which
	// would read as the end of the listing.
	MaxKeys int
}

// Version is one <Version> or <DeleteMarker> entry. Strings share one arena
// per page. A delete marker has no ETag, size, storage class or checksums
// (S3 sends none): its kind, not a zero, says they are absent.
type Version struct {
	Object
	VersionID    string // verbatim: "null" is a real ID (written while versioning was off)
	IsLatest     bool
	DeleteMarker bool // a <DeleteMarker>, else a <Version>
}

// VersionPage is one ListObjectVersions response.
type VersionPage struct {
	// Versions holds the <Version> and <DeleteMarker> entries in one slice
	// in wire order: within a key S3 interleaves them newest first, which
	// two separate lists would lose.
	Versions       []Version
	CommonPrefixes []string
	IsTruncated    bool
	// NextKeyMarker is URL-decoded like keys; a truncated page always has
	// one (the parser refuses a page without). NextVersionIDMarker is
	// verbatim, and empty when absent (a truncated page that ends at a
	// common prefix omits it).
	NextKeyMarker       string
	NextVersionIDMarker string
}

// Error is an S3 error response (or an HTTP failure without one).
type Error struct {
	Status    int
	Code      string
	Message   string
	RequestID string
	Region    string // x-amz-bucket-region, when the server names it
	// Renewed: an expired-token answer after which the credentials were
	// renewed, retried once (auth.expired).
	Renewed bool
	Note    string // what swath adds about the answer, or ""
}

// Error is the message for the command's user: a wrong-region answer
// names the flag that sets the region.
func (e *Error) Error() string { return e.text("give --s3-region " + e.Region) }

// Library is the message for a library caller, who sets the region with
// no flag: a wrong-region answer names the bucket's region only.
func (e *Error) Library() string { return e.text("") }

// text is the message, a wrong-region answer's advice to fix it.
func (e *Error) text(advice string) string {
	s := fmt.Sprintf("S3 %d %s", e.Status, e.Code)
	if e.Message != "" {
		s += ": " + e.Message
	}
	switch {
	case e.Region != "" && advice != "":
		s += " (the bucket is in " + e.Region + ": " + advice + ")"
	case e.Region != "":
		s += " (the bucket is in " + e.Region + ")"
	}
	if e.Note != "" {
		s += " (" + e.Note + ")"
	}
	if e.RequestID != "" {
		s += " (request id " + e.RequestID + ")"
	}
	return s
}

// Validate checks the continuation contract, including supplied clients.
// A key-only marker is valid at a prefix boundary; at the last version's
// key it would skip the rest of that key's history.
func (p *VersionPage) Validate() error {
	if !p.IsTruncated {
		return nil
	}
	if p.NextKeyMarker == "" {
		return fmt.Errorf("%w: a truncated ListObjectVersions page without a NextKeyMarker", objhttp.ErrInvalidPage)
	}
	if n := len(p.Versions); n > 0 && p.Versions[n-1].Key == p.NextKeyMarker && p.NextVersionIDMarker == "" {
		return fmt.Errorf("%w: a truncated ListObjectVersions page ending at its NextKeyMarker without a NextVersionIdMarker", objhttp.ErrInvalidPage)
	}
	return nil
}
