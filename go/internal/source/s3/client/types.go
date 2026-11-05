// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package client is a small S3 client for the one call swath makes,
// ListObjectsV2: SigV4 signing (UNSIGNED-PAYLOAD), credentials from the
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
		s += fmt.Sprintf(" (the bucket is in %s; pass -region %s)", e.Region, e.Region)
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
