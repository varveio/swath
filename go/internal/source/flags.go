// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package source

import (
	"flag"
	"fmt"
	"time"
)

// DefaultRequestTimeout bounds one object-store request attempt.
const DefaultRequestTimeout = 30 * time.Second

// A FlagGroup is a flag group several providers share (the store's, the
// range engine's): the command registers it once and passes its value to
// every provider that names it (registry.Factory.Shared), so the flags are
// spelled the same for every provider and owned by each of them. A group
// is identified by its address: providers name the one declared value
// (StoreGroup, keyrange.Group), so two groups never alias by name.
type FlagGroup struct {
	// Name names the group in messages.
	Name string
	// Flags registers the group's flags on fs and returns the value they
	// fill when fs is parsed; diag adds its diagnostic flags (swath diag
	// list only).
	Flags func(fs *flag.FlagSet, diag bool) any
	// Operational are the group's flags a resume may change: how requests
	// are authenticated, timed and admitted, which no identity holds.
	Operational []string
	// Hints are every flag the group registers, in help order.
	Hints []Hint
	// URLs are the group's flags whose value is a URL: a recorded command
	// line redacts one that carries userinfo, or does not parse
	// (registry.RedactArgv).
	URLs []string
}

// SharedValues are the values of the shared groups a provider names, by
// group, as its flags receive them (registry.Factory.Flags).
type SharedValues map[*FlagGroup]any

// Shared is group g's value in values, the *T its Flags returned. A
// provider asking for a group it does not name, or for another type than
// the group's, is a programming error, so Shared panics rather than
// dropping the group's flags silently.
func Shared[T any](values SharedValues, g *FlagGroup) *T {
	v, ok := values[g]
	if !ok {
		panic(fmt.Sprintf("source: flag group %q is not among the groups the factory names (registry.Factory.Shared)", g.Name))
	}
	t, ok := v.(*T)
	if !ok {
		panic(fmt.Sprintf("source: flag group %q holds %T, not %T", g.Name, v, t))
	}
	return t
}

// A Hint places a flag in the command's help. When, if set, says when to
// change it: the flag is then a limit an operator tunes (the help's Limits
// group); a flag without one describes the source or how it is reached. A
// diagnostic flag is shown with the diagnostic ones either way.
type Hint struct{ Flag, When string }

// Store is the store group's value: what every object store takes alike.
// Each provider reads it into its own options.
type Store struct {
	// Endpoint replaces the provider's own service (an emulator, a
	// compatible store); "" is the service itself.
	Endpoint string
	// Timeout bounds each request; one that exceeds it is retried.
	Timeout time.Duration
	// Anonymous sends no credentials: the one way an object store is
	// listed unauthenticated. Without it a source with no usable
	// credentials fails (class auth) before any listing request.
	Anonymous bool
}

// The store group's flag names.
const (
	endpointFlag       = "endpoint"
	requestTimeoutFlag = "request-timeout"
	anonymousFlag      = "anonymous"
)

// StoreGroup is the flag group every object store provider names: one
// --endpoint, --request-timeout and --anonymous, whichever provider owns
// the root. --endpoint is part of each provider's identity (a resume
// refuses another); the timeout and anonymity are operational.
var StoreGroup = &FlagGroup{
	Name:        "store",
	Flags:       func(fs *flag.FlagSet, _ bool) any { return StoreFlags(fs) },
	Operational: []string{anonymousFlag, requestTimeoutFlag},
	Hints: []Hint{
		{Flag: anonymousFlag},
		{Flag: endpointFlag},
		{Flag: requestTimeoutFlag, When: "raise it for a slow endpoint or emulator (an S3-compatible store, fake-gcs-server, Azurite) whose pages take longer to answer"},
	},
	URLs: []string{endpointFlag},
}

// StoreFlags registers the store group on fs and returns the Store value
// it fills when fs is parsed.
func StoreFlags(fs *flag.FlagSet) *Store {
	s := &Store{}
	fs.BoolVar(&s.Anonymous, anonymousFlag, false, "credentials: send no credentials, ignoring any configured (a public S3 bucket, GCS bucket or Azure container); without it a source with no usable credentials fails (auth) before listing")
	fs.StringVar(&s.Endpoint, endpointFlag, "", "store: endpoint `URL` in place of the provider's own service: an S3-compatible store (MinIO, LocalStack; path-style addressing unless --path-style=false), GCS's JSON API base URL (fake-gcs-server), or Azure's blob service URL, the account then its first path segment (Azurite); user information, a query and a fragment are refused; part of the resume identity")
	fs.DurationVar(&s.Timeout, requestTimeoutFlag, DefaultRequestTimeout, "requests: per-request timeout; a request that exceeds it is retried")
	return s
}
