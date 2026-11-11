// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package redact

import (
	"net/url"
	"regexp"
	"slices"
	"strings"
)

// SAS returns a function that removes the shared access signature sas (a
// query string) from text: the SAS as given, each signature value as sent
// or decoded, and the value of every parameter the SAS names (sig always),
// wherever it stands. An SDK sorts the SAS's parameters in among a
// request's and re-encodes them, and a service may quote the query back
// decoded, escaped in another case or escaped once more, so each parameter
// is found by name, its value replaced by [redacted] and the rest of the
// text kept. It is the source's scrubber (internal/source/azblob/client),
// for the objfile module, which cannot import it.
func SAS(sas string) func(string) string {
	sas = strings.TrimPrefix(sas, "?")
	literal := []string{sas}
	names := map[string]bool{"sig": true}
	if q, err := url.ParseQuery(sas); err == nil {
		for k, vs := range q {
			if k != "" {
				names[k], names[url.QueryEscape(k)] = true, true
			}
			if k == "sig" {
				for _, v := range vs {
					literal = append(literal, v, url.QueryEscape(v))
				}
			}
		}
	}
	alts := make([]string, 0, len(names))
	for k := range names {
		alts = append(alts, regexp.QuoteMeta(k))
	}
	slices.Sort(alts)
	param := regexp.MustCompile(`(?i)((?:^|[?&;\s"'(>]|%26|%3F)(?:` + strings.Join(alts, "|") + `)(?:=|%3D))` + sasValue)
	return func(s string) string {
		for _, x := range literal {
			if x != "" {
				s = strings.ReplaceAll(s, x, "[redacted]")
			}
		}
		return param.ReplaceAllString(s, "${1}[redacted]")
	}
}

// sasValue is a parameter's value in text: anything up to '&', an escaped
// '&' (%26), a space, a quote or a bracket.
const sasValue = `(?:[^&%\s"'<>)]|%(?:[013-9A-Fa-f][0-9A-Fa-f]|2[0-57-9A-Fa-f]))*`
