// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objfile

import "context"

// RequestKind is what a physical request asks the store for.
type RequestKind uint8

const (
	// MetadataRequest asks for an object's size and version: S3
	// HeadObject, GCS attributes, Azure blob properties.
	MetadataRequest RequestKind = iota + 1
	// RangeRequest asks for one byte range of an object.
	RangeRequest
	// WholeRequest asks for a whole object, with no range (ReadObject).
	WholeRequest
)

func (k RequestKind) String() string {
	switch k {
	case MetadataRequest:
		return "metadata"
	case RangeRequest:
		return "range"
	case WholeRequest:
		return "whole"
	}
	return "unknown"
}

// Request describes one physical storage attempt.
type Request struct {
	Kind RequestKind
	// Name is the store's object name, including any Root prefix: not
	// the root-relative name a reader opened.
	Name string
	// Off is a RangeRequest's offset; zero otherwise.
	Off int64
	// Len is the requested payload length: zero for metadata, -1 for a
	// whole object of unknown length.
	Len int64
	// Retry reports another attempt to complete the same underlying
	// work: the rest of a body cut short, or a request the SDK sends
	// again, always of the same pinned version.
	Retry bool
}

// RequestResult describes a completed physical attempt.
type RequestResult struct {
	// Bytes is the response payload consumed, including that of a failed
	// attempt.
	Bytes int64
	// Err is the attempt's failure, nil on success. It includes response
	// validation and body-read failures.
	Err error
}

// Observer receives storage activity under an operation's context
// (WithObserver): the physical requests every store package makes, the
// plans of batches and the bytes a ByteCache supplies, through every
// wrapper of this package. Which requests it sees completely is
// IOReporter's to say. Methods must be concurrency-safe, return promptly,
// and not call back into the objects being observed.
type Observer interface {
	// Begin runs after admission (a Limit slot), immediately before an
	// attempt starts. A non-nil returned function is called exactly once
	// after the response body is consumed or closed, before the operation
	// returns.
	Begin(Request) func(RequestResult)

	// BatchPlanned reports a batch's non-empty uncached input ranges, its
	// planned requests before retries, and the gap bytes they read and
	// discard. Some requests may never start.
	BatchPlanned(ranges, requests, overReadBytes int64)

	// CacheHit reports bytes copied successfully from a ByteCache into
	// caller destinations.
	CacheHit(bytes int64)
}

type observerKey struct{}

// WithObserver replaces ctx's observer. Callers compose observers when
// needed. A nil observer suppresses observation.
func WithObserver(ctx context.Context, observer Observer) context.Context {
	return context.WithValue(ctx, observerKey{}, observer)
}

// ObserverFromContext returns ctx's observer, or nil. Without one,
// observation costs a context lookup and a nil branch.
func ObserverFromContext(ctx context.Context) Observer {
	o, _ := ctx.Value(observerKey{}).(Observer)
	return o
}

// IOReporter optionally describes an Opener's or Object's reporting.
// Wrappers must preserve truthful reporting independently on each
// receiver: every wrapper of this package reports what the Opener or
// Object it wraps reports.
type IOReporter interface {
	// ReportsIO reports whether every physical storage attempt is
	// observable, including SDK retries and response-body recovery.
	// False or an absent capability means observation may be incomplete.
	ReportsIO() bool
}

// reportsIO is what v reports as an IOReporter, false when it is none.
func reportsIO(v any) bool {
	r, ok := v.(IOReporter)
	return ok && r.ReportsIO()
}
