// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"

	"github.com/varveio/swath/objfile"
)

// observed is one call's request as each of its attempts reports it to the
// objfile.Observer its context carries, if any. The command puts no
// Observer on its contexts; a library caller may.
type observed struct {
	request objfile.Request
}

// begin reports an attempt to ctx's observer, when it carries one, and
// returns what reports its end, with the bytes of the answer read and the
// attempt's failure. Every attempt after the first is a retry.
func (o *observed) begin(ctx context.Context) func(bytes int64, err error) {
	obs := objfile.ObserverFromContext(ctx)
	if obs == nil {
		return func(int64, error) {}
	}

	end := obs.Begin(o.request)
	o.request.Retry = true
	return func(bytes int64, err error) {
		if end != nil {
			end(objfile.RequestResult{Bytes: bytes, Err: objfile.RedactError(err)})
		}
	}
}
