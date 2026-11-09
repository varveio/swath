// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/varveio/swath/go/internal/source/objhttp"
)

// The error codes the classification names (x-ms-error-code).
const (
	codeNoAuthentication   = "NoAuthenticationInformation"
	codeAuthFailed         = "AuthenticationFailed"
	codeInvalidAuthInfo    = "InvalidAuthenticationInfo"
	codeAuthorization      = "AuthorizationFailure"
	codePermissionMismatch = "AuthorizationPermissionMismatch"
	codeContainerNotFound  = "ContainerNotFound"
	codeServerBusy         = "ServerBusy"
	codeOperationTimedOut  = "OperationTimedOut"
	codeInternalError      = "InternalError"
)

// The headers and values the client sends or reads.
const (
	headerErrorCode        = "x-ms-error-code"
	headerRetryAfter       = "Retry-After"
	headerHNSEnabled       = "x-ms-is-hns-enabled"
	headerNamespaceEnabled = "x-ms-namespace-enabled"
	headerVersion          = "x-ms-version"
	headerAuthorization    = "Authorization"
	headerUserAgent        = "User-Agent"
	headerLocation         = "Location"
	maxErrorBody           = 64 << 10 // an error body read for its code and message
)

// Error is a non-200 answer.
type Error struct {
	Status int
	// Code is x-ms-error-code, else the decoded XML Code or azcore's
	// body code (JSON included), scrubbed of the SAS and capped at 64 KiB.
	Code    string
	Message string
	Query   string // the request's query without the SAS, on a 400
	// Wait is the answer's Retry-After (0 when absent).
	Wait time.Duration
}

func (e *Error) Error() string {
	s := fmt.Sprintf("Azure %d", e.Status)
	if e.Code != "" {
		s += " " + e.Code
	}
	if e.Message != "" {
		s += ": " + e.Message
	}
	if e.Query != "" {
		s += " (request ?" + e.Query + ")"
	}
	return s
}

// RetryAfter is the service's wait before the next attempt
// (objhttp.RetryAfter).
func (e *Error) RetryAfter() time.Duration { return e.Wait }

// retryAfter reads a Retry-After header: delay seconds or an HTTP date
// (relative to now); 0 when absent, malformed, negative or past.
func retryAfter(v string, now time.Time) time.Duration {
	if v == "" {
		return 0
	}
	if strings.Trim(v, "0123456789") == "" {
		// Delay seconds, saturating: a value past any Duration is the
		// largest one (the caller caps it at the request timeout).
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil || n > uint64(math.MaxInt64/time.Second) {
			return time.Duration(math.MaxInt64/time.Second) * time.Second
		}
		return time.Duration(n) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil && t.After(now) {
		return t.Sub(now) // saturates at the largest Duration
	}
	return 0
}

// Failure keeps Azure's code-sensitive 500 policy. A generic 500 (or 501)
// is terminal even though other providers retry it.
func (e *Error) Failure() objhttp.Failure {
	f := objhttp.Failure{Status: e.Status, Code: e.Code}
	switch e.Status {
	case 408, 429, 502, 503, 504:
		f.Transient = true
	case 500:
		f.Transient = e.Code == codeOperationTimedOut || e.Code == codeInternalError
	}
	switch e.Code {
	case codeNoAuthentication, codeAuthFailed, codeInvalidAuthInfo:
		f.Class = objhttp.ClassAuth
	case codeAuthorization, codePermissionMismatch:
		f.Class = objhttp.ClassAccessDenied
	case codeServerBusy:
		f.Throttle = true
	case codeOperationTimedOut:
		f.Timeout = true
	case codeInternalError, codeContainerNotFound:
	default:
		switch e.Status {
		case 429:
			f.Throttle = true
		case 408:
			f.Timeout = true
		case 401:
			f.Class = objhttp.ClassAuth
		case 403:
			f.Class = objhttp.ClassAccessDenied
		}
	}
	return f
}
