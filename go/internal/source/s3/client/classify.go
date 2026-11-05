// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"errors"
	"fmt"
)

// Class is what kind of failure an error is, for the caller's decision:
// retry the request later or stop the run.
type Class string

const (
	ClassAuth         Class = "auth"           // credentials missing, invalid or expired
	ClassAccessDenied Class = "access_denied"  // the principal may not list this bucket
	ClassNoSuchBucket Class = "no_such_bucket" // the bucket does not exist (or not at this endpoint)
	ClassRedirect     Class = "redirect"       // the bucket is in another region or endpoint
	ClassThrottle     Class = "throttle"       // 429, 503, SlowDown and kin
	ClassTimeout      Class = "timeout"        // an attempt exceeded the request timeout
	ClassServer       Class = "server"         // another 5xx
	ClassTransport    Class = "transport"      // the connection failed or was cut
	ClassParse        Class = "parse"          // a 200 whose body did not parse
	ClassInvalidPage  Class = "invalid_page"   // a page that breaks the listing contract (provider-detected)
	ClassCanceled     Class = "canceled"       // the caller's context ended
	ClassOther        Class = "other"
)

// Transient reports whether a request that failed this way may succeed
// later: the caller retries it after a backoff and never fails the run on
// it (liveness is the command's watchdog's decision).
func (c Class) Transient() bool {
	switch c {
	case ClassThrottle, ClassTimeout, ClassServer, ClassTransport, ClassParse:
		return true
	}
	return false
}

// ExhaustedError is a transient failure that outlasted the client's own
// retries. The caller decides what happens next.
type ExhaustedError struct {
	Attempts int
	Err      error
}

func (e *ExhaustedError) Error() string {
	return fmt.Sprintf("after %d attempts: %v", e.Attempts, e.Err)
}
func (e *ExhaustedError) Unwrap() error { return e.Err }

// ErrInvalidPage marks a page that breaks the listing contract.
var ErrInvalidPage = errors.New("invalid page")

// Classify returns the class of err.
func Classify(err error) Class {
	var se *Error
	var timeout bool
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return ClassCanceled
	case errors.Is(err, ErrInvalidPage):
		return ClassInvalidPage
	case errors.Is(err, errCredentials):
		return ClassAuth
	case errors.As(err, &se):
		switch {
		case se.Throttle():
			return ClassThrottle
		case se.Code == "RequestTimeout":
			return ClassTimeout
		case se.Status >= 500 || se.Retryable():
			return ClassServer
		case se.Code == "NoSuchBucket":
			return ClassNoSuchBucket
		case se.Status == 301 || se.Code == "PermanentRedirect" || se.Code == "AuthorizationHeaderMalformed":
			return ClassRedirect
		case se.Code == "InvalidAccessKeyId" || se.Code == "SignatureDoesNotMatch" || se.Code == "ExpiredToken" ||
			se.Code == "InvalidToken" || se.Code == "TokenRefreshRequired":
			return ClassAuth
		case se.Status == 403 || se.Code == "AccessDenied" || se.Code == "AllAccessDisabled":
			return ClassAccessDenied
		case se.Status == 404:
			return ClassNoSuchBucket
		}
		return ClassOther
	case errors.Is(err, ErrSyntax):
		return ClassParse
	}
	if retry, _ := retryable(err, &timeout); retry {
		if timeout {
			return ClassTimeout
		}
		return ClassTransport
	}
	return ClassOther
}
