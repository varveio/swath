// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"errors"

	"github.com/varveio/swath/go/internal/source/objhttp"
)

// ExhaustedError is a transient failure that outlasted the client's own
// retries (objhttp.ExhaustedError).
type ExhaustedError = objhttp.ExhaustedError

// ErrInvalidPage marks a page that breaks the listing contract.
var ErrInvalidPage = objhttp.ErrInvalidPage

// Classify returns the class of err (objhttp's vocabulary, with S3's
// answers mapped by their status and code).
func Classify(err error) objhttp.Class { return objhttp.Classify(err, answer) }

func answer(err error) (objhttp.Class, bool) {
	var se *Error
	if !errors.As(err, &se) {
		return "", false
	}
	switch {
	case se.Throttle():
		return objhttp.ClassThrottle, true
	case se.Code == "RequestTimeout":
		return objhttp.ClassTimeout, true
	case se.Status >= 500 || se.Retryable():
		return objhttp.ClassServer, true
	case se.Code == "NoSuchBucket":
		return objhttp.ClassNoSuchBucket, true
	case se.Status == 301 || se.Code == "PermanentRedirect" || se.Code == "AuthorizationHeaderMalformed":
		return objhttp.ClassRedirect, true
	case se.Code == "InvalidAccessKeyId" || se.Code == "SignatureDoesNotMatch" || se.Code == "ExpiredToken" ||
		se.Code == "InvalidToken" || se.Code == "TokenRefreshRequired":
		return objhttp.ClassAuth, true
	case se.Status == 403 || se.Code == "AccessDenied" || se.Code == "AllAccessDisabled":
		return objhttp.ClassAccessDenied, true
	case se.Status == 404:
		return objhttp.ClassNoSuchBucket, true
	}
	return objhttp.ClassOther, true
}
