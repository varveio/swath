// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package azblob opens an Azure Blob Storage container as a live
// swath.Source.
//
// A page is in byte order, as every swath.Source's is, which is not the
// order Azure lists a level in. A container of an account with a flat
// namespace lists a level in UTF-16 code-unit order, which is byte order
// except between names whose first differing characters are one in
// U+E000..U+FFFF and one at U+10000 or above. A container of an account
// with a hierarchical namespace (Data Lake Storage Gen2) lists a level in
// that namespace's order, in which a directory "a/" comes before "a-b"
// and "a.txt", and lists each directory twice: as its prefix and as a
// blob whose metadata marks it a directory (hdi_isfolder=true). A level
// that one answer of Azure holds whole (at most 5,000 entries) is sorted
// into byte order, each directory once, as a prefix, and each later page
// of it reads it whole again unless it was in byte order already. A level
// longer than one answer is paged in Azure's order and checked: once it
// shows a key out of byte order, or a directory's blob, the page is
// listing.ErrUnsupported, since sorting each answer cannot make byte order
// across answers.
//
// User metadata can mark a flat namespace's blob hdi_isfolder=true too, so
// the first marked blob a source meets makes it ask the account's
// namespace (Get Account Information, then the account's Data Lake host),
// once. A flat namespace's marked blob is an object like any other. Where
// the namespace cannot be read, a page holding a marked blob is
// listing.ErrUnsupported and a lookup of one is unknown.
//
// The API is experimental: it may change in any release.
package azblob

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/varveio/swath"
	"github.com/varveio/swath/internal/live"
	azsource "github.com/varveio/swath/internal/source/azblob"
	"github.com/varveio/swath/internal/source/objhttp"
	"github.com/varveio/swath/objfile"
	"github.com/varveio/swath/reader/listing"
)

// Options configure an Azure Blob Storage source. The zero value reaches
// Azure's own service at https://ACCOUNT.blob.core.windows.net with the
// credentials the environment holds, as the swath command finds them: a
// SAS from AZURE_STORAGE_SAS_TOKEN, else an account key from
// AZURE_STORAGE_KEY, else Entra ID through the Azure SDK's default chain.
type Options struct {
	// Endpoint replaces https://ACCOUNT.blob.core.windows.net, as
	// http(s)://host[:port][/path] without user information, a query or a
	// fragment; the account is then the first segment of each request's
	// path, as an emulator such as Azurite has it.
	Endpoint string
	// Anonymous sends every request without credentials, whatever the
	// environment holds: a public container's.
	Anonymous bool
	// MaxRequests bounds the requests in flight at once across every call
	// on the source, through an adaptive limiter that lowers the bound
	// while the service throttles and raises it back. Zero sets no bound.
	MaxRequests int
	// RequestRate caps the requests the source sends per second, retries
	// included. Zero sets no cap.
	RequestRate float64
}

// accountName is a storage account's name: 3 to 24 lower-case letters and
// digits.
var accountName = regexp.MustCompile(`^[a-z0-9]{3,24}$`)

// Open opens the container of account as a live source. It builds
// the source's client, which may load credentials, and sends nothing: a
// container that is not there, or that the credentials may not read,
// fails the first call, not Open. Credentials that do not load are
// listing.ErrAuth; an endpoint of the wrong form is listing.ErrUnsupported.
// The caller closes the source.
//
// Page needs permission to list the container's blobs. Lookup needs
// permission to read the blob's properties, and to list the container to
// tell an absent blob: a key is absent only when Get Blob Properties
// finds no blob and a listing then shows the container is there.
func Open(ctx context.Context, account, container string, opts Options) (*swath.Source, error) {
	switch {
	case !accountName.MatchString(account) || container == "" || strings.ContainsAny(container, "/\x00"):
		return nil, objfile.RedactError(fmt.Errorf("%w: account %q, container %q", listing.ErrInvalidRequest, account, container))
	case opts.MaxRequests < 0 || opts.RequestRate < 0:
		return nil, fmt.Errorf("%w: a negative MaxRequests or RequestRate", listing.ErrInvalidRequest)
	}

	st, err := azsource.OpenLive(ctx, azsource.Options{
		Account:   account,
		Container: container,
		Endpoint:  opts.Endpoint,
		Anonymous: opts.Anonymous,
		Workers:   opts.MaxRequests,
		Admission: objhttp.Admission{Limiter: opts.MaxRequests > 0, RequestRate: opts.RequestRate},
	})
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, objfile.RedactError(err)
	}
	return live.NewSource(st).(*swath.Source), nil
}
