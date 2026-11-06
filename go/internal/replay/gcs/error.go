// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs

import "strconv"

// Kind classifies an answer the route gives instead of a page.
type Kind int

// The kinds, each with its status, envelope reason and message.
const (
	Malformed Kind = iota
	Unsupported
	NotFound
	WrongMethod
	Overload
	FixtureIncompatible
	FixtureDisordered
	ResponseTooLarge
	Internal
)

var kinds = [...]struct {
	status          int
	reason, message string
}{
	Malformed:           {400, "invalid", "Invalid GCS listing request"},
	Unsupported:         {400, "invalid", "Unsupported replay feature"},
	NotFound:            {404, "notFound", "Bucket not found"},
	WrongMethod:         {405, "methodNotAllowed", "Method not allowed"},
	Overload:            {503, "backendError", "Replay server busy"},
	FixtureIncompatible: {500, "internalError", "Fixture incompatible with GCS listing"},
	FixtureDisordered:   {500, "internalError", "Fixture incompatible with GCS listing"},
	ResponseTooLarge:    {500, "internalError", "Replay response too large"},
	Internal:            {500, "internalError", "Internal replay error"},
}

// Status is the kind's HTTP status.
func (k Kind) Status() int { return kinds[k].status }

// Error is a refusal on the route: its kind and the replay's reason, sent
// in x-swath-replay-error (for example invalid_max_results, wrong_method,
// response_budget_exhausted).
type Error struct {
	Kind   Kind
	Reason string
}

func (e *Error) Error() string { return "gcs: " + e.Reason }

// Kind is the kind of a request refusal from Parse or the pager.
func (f *Failure) Kind() Kind {
	if f.Unsupported {
		return Unsupported
	}
	return Malformed
}

// AppendError renders the JSON API error envelope for kind: the status as
// error.code, the kind's message at both levels, domain "global" and the
// kind's reason, in the wire contract's field order.
func AppendError(dst []byte, kind Kind) []byte {
	k := kinds[kind]
	dst = append(dst, `{"error":{"code":`...)
	dst = strconv.AppendInt(dst, int64(k.status), 10)
	dst = append(dst, `,"message":"`+k.message+`","errors":[{"message":"`+k.message+`","domain":"global","reason":"`+k.reason+`"}]}}`...)
	return dst
}
