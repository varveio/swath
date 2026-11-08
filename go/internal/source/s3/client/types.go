// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package client is a small S3 client for the listing calls swath makes,
// ListObjectsV2 and ListObjectVersions: SigV4 signing (UNSIGNED-PAYLOAD), credentials from the
// environment and the shared AWS files, anonymous access, counted retries
// with jittered exponential backoff, and a response parser that allocates
// per page rather than per object. It replaces the AWS SDK in the binary;
// the SDK remains a test-only oracle.
package client

import "fmt"

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
	Mtime             int64 // LastModified in Unix microseconds; 0 when absent
	StorageClass      string
	OwnerID           string
	OwnerDisplayName  string
	ChecksumAlgorithm string // the first listed
	ChecksumType      string
}

// Page is one ListObjectsV2 response.
type Page struct {
	Contents              []Object
	CommonPrefixes        []string
	IsTruncated           bool
	NextContinuationToken string
	HasContinuation       bool
	KeyCount              int
	EncodingType          string
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
	// Prefix and KeyMarker echo the request's, URL-decoded.
	Prefix       string
	KeyMarker    string
	EncodingType string
}

// Error is an S3 error response (or an HTTP failure without one).
type Error struct {
	Status    int
	Code      string
	Message   string
	RequestID string
	Region    string // x-amz-bucket-region, when the server names it
}

func (e *Error) Error() string {
	s := fmt.Sprintf("S3 %d %s", e.Status, e.Code)
	if e.Message != "" {
		s += ": " + e.Message
	}
	if e.Region != "" {
		s += " (the bucket is in " + e.Region + ")"
	}
	if e.RequestID != "" {
		s += " (request id " + e.RequestID + ")"
	}
	return s
}

// Throttle reports whether the server asked the client to slow down.
func (e *Error) Throttle() bool {
	switch e.Code {
	case "SlowDown", "Throttling", "ThrottlingException", "RequestLimitExceeded", "TooManyRequestsException", "RequestThrottled":
		return true
	}
	return e.Status == 429 || e.Status == 503
}

// Retryable reports whether the request may succeed when repeated.
func (e *Error) Retryable() bool {
	if e.Throttle() || e.Status >= 500 {
		return true
	}
	switch e.Code {
	case "RequestTimeout", "InternalError", "ServiceUnavailable":
		return true
	}
	return false
}

// Validate checks the continuation contract, including supplied clients.
// A key-only marker is valid at a prefix boundary; at the last version's
// key it would skip the rest of that key's history.
func (p *VersionPage) Validate() error {
	if !p.IsTruncated {
		return nil
	}
	if p.NextKeyMarker == "" {
		return fmt.Errorf("%w: a truncated ListObjectVersions page without a NextKeyMarker", ErrInvalidPage)
	}
	if n := len(p.Versions); n > 0 && p.Versions[n-1].Key == p.NextKeyMarker && p.NextVersionIDMarker == "" {
		return fmt.Errorf("%w: a truncated ListObjectVersions page ending at its NextKeyMarker without a NextVersionIdMarker", ErrInvalidPage)
	}
	return nil
}
