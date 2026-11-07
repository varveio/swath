// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package azure is the replay server's Azure Blob flat List Blobs route
// (GET /{account}/{container}?restype=container&comp=list): request
// parsing, the listing in Azure's UTF-16 code-unit order over the
// byte-sorted store, markers, and the XML and error bodies. The server
// package owns admission, latency injection and the HTTP exchange.
package azure

// Kind classifies a refusal; Status and Code map it to the Azure wire
// envelope.
type Kind int

// The refusal kinds.
const (
	Malformed Kind = iota
	Unsupported
	NotFound
	WrongMethod
	FixtureIncompatible
	FixtureDisordered
	Overload
	ResponseTooLarge
	Internal
)

// Error is a refusal. Reason is sent in x-swath-replay-error so a client
// can tell a replay refusal from the provider's answer.
type Error struct {
	Kind   Kind
	Reason string
}

func (e *Error) Error() string { return e.Code() + ": " + e.Reason }

func fail(k Kind, reason string) *Error { return &Error{Kind: k, Reason: reason} }

// Status is the HTTP status of the refusal.
func (e *Error) Status() int {
	switch e.Kind {
	case Malformed, Unsupported:
		return 400
	case NotFound:
		return 404
	case WrongMethod:
		return 405
	case Overload:
		return 503
	}
	return 500
}

// Code is the Azure error code, sent in x-ms-error-code and the body.
func (e *Error) Code() string {
	switch e.Kind {
	case Malformed:
		switch e.Reason {
		case "maxresults_out_of_range":
			return "OutOfRangeQueryParameterValue"
		case "invalid_client_request_id", "duplicate_header":
			return "InvalidHeaderValue"
		}
		return "InvalidQueryParameterValue"
	case Unsupported:
		switch e.Reason {
		case "invalid_header_value", "unsupported_accept":
			return "InvalidHeaderValue"
		}
		return "UnsupportedQueryParameter"
	case NotFound:
		return "ContainerNotFound"
	case WrongMethod:
		return "UnsupportedHttpVerb"
	case Overload:
		return "ServerBusy"
	}
	return "InternalError"
}
