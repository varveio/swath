// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/varveio/swath/go/internal/source/objhttp"
)

// ErrInvalidPage marks a page that parsed but breaks the listing contract
// (names out of order, a repeated marker, an Encoded name that does not
// decode); the range engine wraps it.
var ErrInvalidPage = objhttp.ErrInvalidPage

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
	headerClientRequestID  = "x-ms-client-request-id"
	headerVersion          = "x-ms-version"
	headerDate             = "x-ms-date"
	headerAuthorization    = "Authorization"
	headerAccept           = "Accept"
	headerUserAgent        = "User-Agent"
	headerLocation         = "Location"
	acceptXML              = "application/xml"
	maxErrorBody           = 64 << 10 // an error body read for its code and message
)

// Error is a non-200 answer.
type Error struct {
	Status int
	// Code is x-ms-error-code, or the body's <Code> when the header is
	// absent (some answers omit it).
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

// Throttle reports the service asking the client to slow down.
func (e *Error) Throttle() bool { return e.Status == 429 || e.Code == codeServerBusy }

// Retryable reports an answer worth repeating: 408, 429, 502, 503, 504,
// and a 500 that is OperationTimedOut or InternalError.
func (e *Error) Retryable() bool {
	switch e.Status {
	case 408, 429, 502, 503, 504:
		return true
	case 500:
		return e.Code == codeOperationTimedOut || e.Code == codeInternalError
	}
	return false
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

// Counters are what the client did.
type Counters = objhttp.Counts

// CredentialError is a credential failure (objhttp.CredentialError): a
// configured credential that does not load, or a token Entra ID would not
// issue; transient ones are retried.
type CredentialError = objhttp.CredentialError

// ExhaustedError is a transient failure that outlived its attempts.
type ExhaustedError = objhttp.ExhaustedError

// ErrSyntax marks a 200 whose body did not parse (objhttp.ErrSyntax).
var ErrSyntax = objhttp.ErrSyntax

// Classify returns the class of err (objhttp's vocabulary, with Azure's
// answers mapped by error code first and status second: an
// AuthenticationFailed arrives as a 403).
func Classify(err error) objhttp.Class { return objhttp.Classify(err, answer) }

func answer(err error) (objhttp.Class, bool) {
	var e *Error
	if !errors.As(err, &e) {
		return "", false
	}
	switch e.Code {
	case codeNoAuthentication, codeAuthFailed, codeInvalidAuthInfo:
		return objhttp.ClassAuth, true
	case codeAuthorization, codePermissionMismatch:
		return objhttp.ClassAccessDenied, true
	case codeContainerNotFound:
		return objhttp.ClassNoSuchBucket, true
	case codeServerBusy:
		return objhttp.ClassThrottle, true
	case codeOperationTimedOut:
		return objhttp.ClassTimeout, true
	case codeInternalError:
		return objhttp.ClassServer, true
	}
	switch {
	case e.Status == 429:
		return objhttp.ClassThrottle, true
	case e.Status == 408:
		return objhttp.ClassTimeout, true
	case e.Status >= 500:
		return objhttp.ClassServer, true
	case e.Status/100 == 3:
		return objhttp.ClassRedirect, true
	case e.Status == 401:
		return objhttp.ClassAuth, true
	case e.Status == 403:
		return objhttp.ClassAccessDenied, true
	}
	return objhttp.ClassOther, true
}
