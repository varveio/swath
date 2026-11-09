// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import "github.com/varveio/swath/go/internal/source/objhttp"

// ExhaustedError is a transient failure that outlasted the client's own
// retries (objhttp.ExhaustedError).
type ExhaustedError = objhttp.ExhaustedError

// ErrInvalidPage marks a page that breaks the listing contract.
var ErrInvalidPage = objhttp.ErrInvalidPage

// Classify returns the stored report label (or normalizes a construction error).
func Classify(err error) objhttp.Class { return objhttp.Classify(err) }

// Failure maps the service answer once; labels do not drive retry policy.
func (e *Error) Failure() objhttp.Failure {
	f := objhttp.Failure{Status: e.Status, Code: e.Code}
	switch e.Code {
	case "SlowDown", "Throttling", "ThrottlingException", "RequestLimitExceeded", "TooManyRequestsException", "RequestThrottled":
		f.Throttle = true
	}
	f.Throttle = f.Throttle || e.Status == 429 || e.Status == 503
	f.Timeout = !f.Throttle && e.Code == "RequestTimeout"
	f.Transient = f.Throttle || e.Status >= 500 || e.Code == "RequestTimeout" || e.Code == "InternalError" || e.Code == "ServiceUnavailable"
	if !f.Transient {
		switch e.Code {
		case "InvalidAccessKeyId", "SignatureDoesNotMatch", "ExpiredToken", "InvalidToken", "TokenRefreshRequired":
			f.Class = objhttp.ClassAuth
		default:
			if e.Status == 403 || e.Code == "AccessDenied" || e.Code == "AllAccessDisabled" {
				f.Class = objhttp.ClassAccessDenied
			}
		}
	}
	return f
}
