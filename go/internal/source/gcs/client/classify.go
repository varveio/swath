// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import "github.com/varveio/swath/go/internal/source/objhttp"

// Failure preserves reason-sensitive throttling, including a 403.
func (e *Error) Failure() objhttp.Failure {
	f := objhttp.Failure{Status: e.Status, Code: e.Reason}
	f.Throttle = e.Status == 429 || e.Reason == "rateLimitExceeded" || e.Reason == "userRateLimitExceeded"
	f.Timeout = !f.Throttle && e.Status == 408
	f.Transient = f.Throttle || e.Status >= 500 || e.Status == 408
	if !f.Transient {
		switch e.Status {
		case 401:
			f.Class = objhttp.ClassAuth
		case 403:
			f.Class = objhttp.ClassAccessDenied
		}
	}
	return f
}
