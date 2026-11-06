// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"errors"
)

// Class is what kind of failure an error is, for the caller's decision:
// retry the request later or stop the run. The vocabulary is S3's.
type Class string

const (
	ClassAuth         Class = "auth"           // credentials unusable, refused or not issued
	ClassAccessDenied Class = "access_denied"  // the principal may not list this bucket
	ClassNoSuchBucket Class = "no_such_bucket" // the bucket does not exist
	ClassThrottle     Class = "throttle"       // 429 and rate-limit reasons
	ClassTimeout      Class = "timeout"        // an attempt exceeded the request timeout (or 408)
	ClassServer       Class = "server"         // another 5xx
	ClassTransport    Class = "transport"      // the connection failed or was cut
	ClassParse        Class = "parse"          // a 200 whose body did not parse
	ClassInvalidPage  Class = "invalid_page"   // a page that breaks the listing contract (provider-detected)
	ClassCanceled     Class = "canceled"       // the caller's context ended
	ClassOther        Class = "other"
)

// ErrInvalidPage marks a page that parsed but breaks the listing contract
// (out of order, truncated without a token); the range engine wraps it.
var ErrInvalidPage = errors.New("invalid page")

// Classify returns the class of err.
func Classify(err error) Class {
	var se *Error
	var ce *CredentialError
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		if errors.As(err, &ce) && ce.Timeout {
			return ClassAuth // the token did not arrive within an attempt's own deadline
		}
		return ClassCanceled
	case errors.Is(err, ErrInvalidPage):
		return ClassInvalidPage
	case errors.As(err, &ce):
		return ClassAuth
	case errors.As(err, &se):
		switch {
		case se.Throttle():
			return ClassThrottle
		case se.Status == 408:
			return ClassTimeout
		case se.Status >= 500:
			return ClassServer
		case se.Status == 401:
			return ClassAuth
		case se.Status == 403:
			return ClassAccessDenied
		case se.Status == 404:
			return ClassNoSuchBucket
		}
		return ClassOther
	case errors.Is(err, ErrSyntax):
		return ClassParse
	case errors.Is(err, errTimeout):
		return ClassTimeout
	case transport(err):
		return ClassTransport
	}
	return ClassOther
}
