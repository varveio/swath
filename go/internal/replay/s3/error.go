// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

// Error is an S3 protocol error: an HTTP status, an S3 error code and a
// message, rendered by RenderError. Reason, when set, is also sent in the
// x-swath-replay-error header, so a client can tell a replay refusal (an
// overload, a response cap) from an S3 answer.
type Error struct {
	Status  int
	Code    string
	Message string
	Reason  string
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

// The S3 answers for a request the route cannot serve.
var (
	errMethodNotAllowed = &Error{Status: 405, Code: "MethodNotAllowed", Message: "method not allowed"}
	errNoSuchBucket     = &Error{Status: 404, Code: "NoSuchBucket", Message: "The specified bucket does not exist"}
	errListType         = &Error{Status: 400, Code: "InvalidArgument", Message: "only ListObjectsV2 is supported"}
	errMaxKeysNegative  = &Error{Status: 400, Code: "InvalidArgument", Message: "max-keys must be non-negative"}
	errMaxKeysInteger   = &Error{Status: 400, Code: "InvalidArgument", Message: "max-keys must be an integer"}
	// The answers below are live S3's code and message.
	errMarker       = &Error{Status: 400, Code: "InvalidArgument", Message: "Marker unsupported with REST.GET.BUCKET in list-type=2"}
	errNUL          = &Error{Status: 400, Code: "InvalidArgument", Message: "Value must be a sequence of Unicode characters and cannot include Null."}
	errEncodingType = &Error{Status: 400, Code: "InvalidArgument", Message: "Invalid Encoding Method specified in Request"}
)
