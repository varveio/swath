// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

// Error is an S3 protocol error: an HTTP status, an S3 error code, a
// message and the elements S3 writes after the message, rendered by
// AppendError. Reason, when set, is also sent in the x-swath-replay-error
// header, so a client can tell a replay refusal (an overload, a response
// cap) from an S3 answer.
type Error struct {
	Status  int
	Code    string
	Message string
	Reason  string
	// Details are the elements between Message and RequestId, in order:
	// ArgumentName and ArgumentValue for InvalidArgument, BucketName for
	// NoSuchBucket, Method and ResourceType for MethodNotAllowed
	// (TestLiveErrorDocuments).
	Details []Detail
}

// Detail is one element of an error document.
type Detail struct{ Name, Value string }

func (e *Error) Error() string { return e.Code + ": " + e.Message }

// with is e with the given details.
func (e *Error) with(details ...Detail) *Error {
	c := *e
	c.Details = details
	return &c
}

// argument is e naming the query parameter it refuses, and its value
// when S3 echoes one.
func (e *Error) argument(name string, value ...string) *Error {
	d := []Detail{{"ArgumentName", name}}
	for _, v := range value {
		d = append(d, Detail{"ArgumentValue", v})
	}
	return e.with(d...)
}

// The S3 answers for a request the route cannot serve. The codes and
// messages are live S3's, probed on 2026-11-07, except errListType: S3
// serves ListObjects V1 where the replay refuses it, so that refusal is
// the replay's own message in S3's InvalidArgument form.
var (
	errMethodNotAllowed = &Error{Status: 405, Code: "MethodNotAllowed", Message: "The specified method is not allowed against this resource."}
	errNoSuchBucket     = &Error{Status: 404, Code: "NoSuchBucket", Message: "The specified bucket does not exist"}
	errListType         = &Error{Status: 400, Code: "InvalidArgument", Message: "only ListObjectsV2 is supported"}
	errMaxKeysNegative  = &Error{Status: 400, Code: "InvalidArgument", Message: "Argument maxKeys must be an integer between 0 and 2147483647"}
	errMaxKeysInteger   = &Error{Status: 400, Code: "InvalidArgument", Message: "Provided max-keys not an integer or within integer range"}
	errMarker           = &Error{Status: 400, Code: "InvalidArgument", Message: "Marker unsupported with REST.GET.BUCKET in list-type=2"}
	errNUL              = &Error{Status: 400, Code: "InvalidArgument", Message: "Value must be a sequence of Unicode characters and cannot include Null."}
	errEncodingType     = &Error{Status: 400, Code: "InvalidArgument", Message: "Invalid Encoding Method specified in Request"}
)
