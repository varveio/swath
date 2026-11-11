// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package redact

import (
	"net/url"
	"regexp"
	"strings"
)

// URLs returns err with every URL in its text reduced to its scheme, host
// and path: user information, the query and the fragment are dropped (a
// GCS resumable session URI, which the SDK's transport errors quote, is a
// bearer capability for its upload). errors.Is and errors.As still see
// err. A nil err is nil; an err whose text holds no such URL is returned
// as is.
func URLs(err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	if clean := URLText(msg); clean != msg {
		return &redacted{err: err, msg: clean}
	}
	return err
}

var urlPattern = regexp.MustCompile(`[A-Za-z][A-Za-z0-9+.-]*://[^\s"'<>]+`)

// URLText reduces every URL in s as URLs does; a URL it cannot parse is
// replaced whole.
func URLText(s string) string {
	return urlPattern.ReplaceAllStringFunc(s, func(raw string) string {
		u, err := url.Parse(raw)
		if err != nil || u.Host == "" && u.Opaque != "" {
			return "[redacted URL]"
		}
		clean := u.Scheme + "://" + u.Host + u.EscapedPath()
		if u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.ContainsAny(u.EscapedPath(), "?#") {
			clean += "?[redacted]"
		}
		return clean
	})
}
