// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package redact keeps a value out of an error's text: an upload's session
// URI or upload id, which a transport error quotes in the URL it names.
package redact

import "strings"

// Error returns err with every occurrence of secret in its text replaced
// by placeholder; errors.Is and errors.As still see err (for its class),
// but the returned error's text never holds secret. A nil err, or an
// empty secret, is returned as is.
func Error(err error, secret, placeholder string) error {
	if err == nil || secret == "" || !strings.Contains(err.Error(), secret) {
		return err
	}
	return &redacted{err: err, msg: strings.ReplaceAll(err.Error(), secret, placeholder)}
}

type redacted struct {
	err error
	msg string
}

func (r *redacted) Error() string { return r.msg }
func (r *redacted) Unwrap() error { return r.err }
