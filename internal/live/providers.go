// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package live

import (
	"context"
	"maps"
	"slices"
	"sync"
)

// Options are source.Options as an Opener receives them. Package source
// converts its Options to this type, so the two must keep the same fields;
// source.Options documents them.
type Options struct {
	Region          string
	Endpoint        string
	PathStyle       bool
	Profile         string
	CredentialsFile string
	Anonymous       bool
	UserProject     string
	MaxRequests     int
	RequestRate     float64
}

// Opener opens the location an address names after its scheme's "://":
// a bucket, or ACCOUNT/CONTAINER, with no trailing slash.
type Opener func(ctx context.Context, location string, opts Options) (*Source, error)

// openers are the providers linked into the program, by address scheme.
// Each provider package registers its own in init.
var openers = struct {
	sync.RWMutex
	m map[string]Opener
}{m: map[string]Opener{}}

// Register makes open the opener of addresses of scheme. Registering a
// scheme twice panics, as sql.Register does.
func Register(scheme string, open Opener) {
	openers.Lock()
	defer openers.Unlock()

	if _, ok := openers.m[scheme]; ok {
		panic("live: Register called twice for scheme " + scheme)
	}
	openers.m[scheme] = open
}

// Lookup returns the opener registered for scheme.
func Lookup(scheme string) (Opener, bool) {
	openers.RLock()
	defer openers.RUnlock()

	open, ok := openers.m[scheme]
	return open, ok
}

// Schemes returns the registered schemes, sorted.
func Schemes() []string {
	openers.RLock()
	defer openers.RUnlock()

	return slices.Sorted(maps.Keys(openers.m))
}
