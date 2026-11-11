// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objfile

import "github.com/varveio/swath/go/objfile/internal/redact"

// RedactError returns err with every URL in its text reduced to its scheme,
// host and path: user information, the query and the fragment are dropped.
// A store's request URLs can carry credentials (a GCS resumable session URI
// is a bearer capability for its upload; a presigned URL is a signature),
// and an SDK's transport error quotes the request URL in full. The result
// wraps err, so errors.Is and errors.As still see the original chain; only
// its text changes. A nil err is nil. The store packages redact their own
// errors (objfile/internal/redact); a caller redacts what it adds.
func RedactError(err error) error { return redact.URLs(err) }

// RedactText reduces every URL in s as RedactError does; a URL it cannot
// parse is replaced whole.
func RedactText(s string) string { return redact.URLText(s) }
