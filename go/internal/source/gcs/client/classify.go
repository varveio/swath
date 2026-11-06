// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"errors"

	"github.com/varveio/swath/go/internal/source/objhttp"
)

// ErrInvalidPage marks a page that parsed but breaks the listing contract
// (out of order, truncated without a token); the range engine wraps it.
var ErrInvalidPage = objhttp.ErrInvalidPage

// Classify returns the class of err (objhttp's vocabulary, with the JSON
// API's answers mapped by status and reason).
func Classify(err error) objhttp.Class { return objhttp.Classify(err, answer) }

func answer(err error) (objhttp.Class, bool) {
	var se *Error
	if !errors.As(err, &se) {
		return "", false
	}
	switch {
	case se.Throttle():
		return objhttp.ClassThrottle, true
	case se.Status == 408:
		return objhttp.ClassTimeout, true
	case se.Status >= 500:
		return objhttp.ClassServer, true
	case se.Status == 401:
		return objhttp.ClassAuth, true
	case se.Status == 403:
		return objhttp.ClassAccessDenied, true
	case se.Status == 404:
		return objhttp.ClassNoSuchBucket, true
	}
	return objhttp.ClassOther, true
}
