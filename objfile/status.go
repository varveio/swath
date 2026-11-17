// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objfile

// WithHTTPStatus returns err with the HTTP status of the service answer it
// carries, which a store adapter has read from its SDK's error type: a
// caller then classifies the failure (a report's "http_503") by asking for
// HTTPStatusCode, without importing the SDK. The chain under err is kept for
// errors.Is and errors.As. A status of 0 or less (no answer was received) or
// a nil err is returned unchanged.
func WithHTTPStatus(err error, status int) error {
	if err == nil || status <= 0 {
		return err
	}
	return &statusError{err: err, status: status}
}

type statusError struct {
	err    error
	status int
}

func (e *statusError) Error() string { return e.err.Error() }

func (e *statusError) Unwrap() error { return e.err }

// HTTPStatusCode is the status of the answer the error carries.
func (e *statusError) HTTPStatusCode() int { return e.status }
