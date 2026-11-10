// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objfile

import (
	"fmt"
	"strings"
)

// Location is a dataset root in an object store, parsed from a URI:
// s3://BUCKET[/PREFIX], gs://BUCKET[/PREFIX] or az://ACCOUNT/CONTAINER[/PREFIX].
type Location struct {
	Scheme    string // "s3", "gs" or "az"
	Account   string // the Azure storage account; "" for s3 and gs
	Container string // the bucket, or the Azure container
	Prefix    string // the literal key prefix: "" or ending in "/"
}

// String returns the location's canonical URI, which ParseLocation returns
// unchanged.
func (l Location) String() string {
	if l.Account != "" {
		return l.Scheme + "://" + l.Account + "/" + l.Container + "/" + l.Prefix
	}
	return l.Scheme + "://" + l.Container + "/" + l.Prefix
}

// ParseLocation parses a dataset root URI. The prefix is literal (nothing is
// decoded) and a dataset root: a "/" is added when it lacks one, and its path
// must be one CheckPath accepts. Bucket, account and container names are
// lower-case letters, digits, '.', '-' and '_', other than "." and "..". A URI with a query, a
// fragment, user information or a port is refused.
func ParseLocation(raw string) (Location, error) {
	scheme, rest, ok := strings.Cut(raw, "://")
	shown := raw
	authority, _, _ := strings.Cut(rest, "/")
	if strings.ContainsAny(raw, "?#") || strings.ContainsAny(authority, "@:") {
		shown = "[redacted]"
	}
	fail := func(why string) (Location, error) {
		return Location{}, fmt.Errorf("objfile: location %q: %s", shown, why)
	}
	if !ok || scheme != "s3" && scheme != "gs" && scheme != "az" {
		return fail("want s3://BUCKET[/PREFIX], gs://BUCKET[/PREFIX] or az://ACCOUNT/CONTAINER[/PREFIX]")
	}
	if strings.ContainsAny(rest, "?#") {
		return fail("has a query or a fragment")
	}
	l := Location{Scheme: scheme}
	if scheme == "az" {
		l.Account, rest, _ = strings.Cut(rest, "/")
		if !name(l.Account) {
			return fail("no valid account")
		}
	}
	l.Container, l.Prefix, _ = strings.Cut(rest, "/")
	if !name(l.Container) {
		return fail("no valid bucket or container")
	}
	if l.Prefix != "" {
		if err := CheckPath(strings.TrimSuffix(l.Prefix, "/")); err != nil {
			return fail("prefix is not a path: " + err.Error())
		}
		l.Prefix = strings.TrimSuffix(l.Prefix, "/") + "/"
	}
	return l, nil
}

// name reports whether s is a bucket, account or container name of the
// characters the stores allow, and not "." or "..".
func name(s string) bool {
	for _, r := range s {
		if !('a' <= r && r <= 'z' || '0' <= r && r <= '9' || r == '.' || r == '-' || r == '_') {
			return false
		}
	}
	return s != "" && s != "." && s != ".."
}
