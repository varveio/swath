// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/keyrange"
)

// r2Host is the host suffix of Cloudflare R2's S3 endpoints:
// ACCOUNT.r2.cloudflarestorage.com and jurisdiction hosts such as
// ACCOUNT.eu.r2.cloudflarestorage.com.
const r2Host = ".r2.cloudflarestorage.com"

// R2Endpoint reports whether endpoint is a Cloudflare R2 S3 endpoint: its
// parsed host name ends in .r2.cloudflarestorage.com, in any case. R2's
// ListObjectsV2 does not list keys in byte order (a key that is a proper
// prefix of others sorts among them, and keys sort by their Unicode NFC
// form), and its start-after seeks in that order, so the range engine can
// miss keys there without an error. No request is made.
func R2Endpoint(endpoint string) bool {
	if endpoint == "" {
		return false
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return false
	}
	host := strings.TrimSuffix(strings.ToLower(u.Hostname()), ".")
	return strings.HasSuffix(host, r2Host)
}

// The listing's engagement reasons (Stats.ListingReason).
const (
	ReasonDefault       = "default_range"  // ListingAuto on a store that is not R2: the range engine
	ReasonR2Endpoint    = "r2_endpoint"    // ListingAuto on an R2 endpoint: the folder walk
	ReasonExplicitRange = "explicit_range" // ListingRange
	ReasonExplicitTree  = "explicit_tree"  // ListingTree
)

// Notices of the listing's choice (Options.Warn).
const (
	r2Notice       = "Cloudflare R2 does not list keys in byte order: listing folder by folder (--listing tree), each folder read whole and sorted; --listing range lists by key ranges, which can miss keys there without an error"
	r2RangeWarning = "--listing range on Cloudflare R2, which does not list keys in byte order: the range engine can miss keys there without an error (--listing tree lists folder by folder)"
)

// listing is how opt lists: the folder walk (tree) or the range engine,
// and why. ListingAuto reads the endpoint: Options.Endpoint, else the
// route a supplied client names (Routed). A supplied client that names
// none, with no Endpoint, gives nothing to read, and lists by key ranges:
// ask for ListingTree to walk folders through it.
func (o *Options) listing() (tree bool, reason string, err error) {
	switch l := o.Listing.OrAuto(); l {
	case source.ListingAuto:
		if R2Endpoint(o.endpoint()) {
			return true, ReasonR2Endpoint, nil
		}
		return false, ReasonDefault, nil
	case source.ListingRange:
		return false, ReasonExplicitRange, nil
	case source.ListingTree:
		return true, ReasonExplicitTree, nil
	default:
		return false, "", fmt.Errorf("s3: unknown listing %q (auto, range or tree)", l)
	}
}

// warnListing tells the user of the listing's choice where it matters: the
// folder walk chosen for R2, or the range engine forced on it.
func (o *Options) warnListing(tree bool, reason string) {
	switch {
	case o.Warn == nil:
	case tree && reason == ReasonR2Endpoint:
		o.Warn(source.WarnTreeListing, r2Notice)
	case !tree && R2Endpoint(o.endpoint()):
		o.Warn(source.WarnUnorderedRange, r2RangeWarning)
	}
}

// endpoint is where requests go, for choosing the listing: the endpoint
// given, else a supplied Routed client's route for the bucket.
func (o *Options) endpoint() string {
	if o.Endpoint != "" {
		return o.Endpoint
	}
	if r, ok := o.Client.(Routed); ok {
		return r.Route(o.Bucket)
	}
	return ""
}

// errTreeOpen: a folder-walk listing asked of the range engine's
// constructors (New, NewVersions), which a library caller opens with Open.
var errTreeOpen = errors.New("s3: this listing is a folder walk (Options.Listing, or an R2 endpoint): open it with Open")

// treeRefusal is why the folder walk cannot serve opt: a range-engine
// control or a versions listing given with it, or nil.
func treeRefusal(opt *Options, reason string) error {
	why := "--listing tree walks folders"
	if reason == ReasonR2Endpoint {
		why = "on Cloudflare R2 the listing walks folders (--listing tree), since R2 does not list keys in byte order"
	}
	control := ""
	switch p := opt.Policy; {
	case opt.AllVersions:
		return source.Unsupported(fmt.Errorf("--all-versions lists with ListObjectVersions on the range engine, and %s, which lists current objects only (--listing range lists versions, at the risk of missing keys where the store does not list in byte order)", why))
	case p.Sequential:
		control = "--sequential"
	case p.MaxKeys != 0:
		control = "--max-keys"
	case p.SlowSplit:
		control = "--slow-split"
	case p.Structure == keyrange.StructureOn:
		control = "--structure=on"
	case p.TilePages != 0:
		control = "--seed-from-tile-pages"
	case p.WindowRows != 0:
		control = "--seed-from-window-rows"
	default:
		return nil
	}
	return source.Unsupported(fmt.Errorf("%s is a control of the range engine, and %s: give --listing range to list by key ranges", control, why))
}
