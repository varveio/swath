// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"encoding/json"
	"slices"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/apache/arrow-go/v18/arrow"
)

// Include selects the datasets a List Blobs request adds (include=). The
// zero value sends none, the request every other listing sends.
type Include struct {
	// Versions, Snapshots and Deleted list a name's versions, snapshots
	// and soft-deleted entries beside its current blob: a name then
	// repeats, its entries consecutive (Grouped).
	Versions, Snapshots, Deleted bool
	// Metadata adds the user metadata (Props.Metadata).
	Metadata bool
	// Tags adds the blob index tags (Props.Tags). The service refuses the
	// whole request without the right to read tags (a SAS without the t
	// permission: 403 AuthorizationPermissionMismatch; anonymous: 401).
	Tags bool
	// Immutability adds the immutability policy and the legal hold
	// (include=immutabilitypolicy,legalhold).
	Immutability bool
}

// Grouped reports a listing whose names may repeat: one entry per version,
// snapshot or soft-deleted blob, a name's entries consecutive.
func (i Include) Grouped() bool { return i.Versions || i.Snapshots || i.Deleted }

// sdk is the include as the SDK's listing options take it.
func (i Include) sdk() container.ListBlobsInclude {
	return container.ListBlobsInclude{Versions: i.Versions, Snapshots: i.Snapshots, Deleted: i.Deleted,
		Metadata: i.Metadata, Tags: i.Tags, ImmutabilityPolicy: i.Immutability, LegalHold: i.Immutability}
}

// Entry is what tells a name's entries apart (Include.Grouped). A
// versioned account lists the current blob's VersionID and
// IsCurrentVersion in every listing.
type Entry struct {
	Snapshot         string // the snapshot's timestamp, as listed; "" for a blob or a version
	VersionID        string // the version's identifier (a timestamp), as listed; "" when none
	IsCurrentVersion bool
	Deleted          bool  // soft-deleted
	DeletedTime      int64 // Unix microseconds; 0 when absent
	// RetentionDays is RemainingRetentionDays when HasRetention.
	RetentionDays int64
	HasRetention  bool
}

// Tri is an optional boolean: Unset when the listing omits it.
type Tri uint8

const (
	Unset Tri = iota
	False
	True
)

// tri is b as a Tri.
func tri(b *bool) Tri {
	switch {
	case b == nil:
		return Unset
	case *b:
		return True
	}
	return False
}

// Get is the value and whether it was listed.
func (t Tri) Get() (bool, bool) { return t == True, t != Unset }

// Props are the blob properties every listing carries beyond Item's first
// fields, and those an Include asks for. Strings are as listed, "" when
// absent; times are Unix microseconds, 0 when absent.
type Props struct {
	AccessTierInferred        Tri
	AccessTierChangeTime      int64
	ArchiveStatus             string
	RehydratePriority         string
	LastAccessTime            int64
	ContentEncoding           string
	ServerEncrypted           Tri
	EncryptionScope           string
	CustomerProvidedKeySHA256 string
	LeaseState                string
	TagCount                  int64 // 0 when absent (a blob with tags has 1 to 10)

	// Include.Metadata and Include.Tags: a JSON object of the names and
	// values, keys sorted; "" when the listing carried none.
	Metadata string
	Tags     string
	// Include.Immutability.
	ImmutabilityUntil int64
	ImmutabilityMode  string
	LegalHold         Tri
}

// entry reads b's entry fields.
func entry(b *container.BlobItem) Entry {
	e := Entry{Snapshot: deref(b.Snapshot), VersionID: deref(b.VersionID), IsCurrentVersion: b.IsCurrentVersion != nil && *b.IsCurrentVersion,
		Deleted: b.Deleted != nil && *b.Deleted}
	if p := b.Properties; p != nil {
		e.DeletedTime = micros(p.DeletedTime)
		if p.RemainingRetentionDays != nil {
			e.RetentionDays, e.HasRetention = int64(*p.RemainingRetentionDays), true
		}
	}
	return e
}

// props reads b's properties.
func props(b *container.BlobItem) Props {
	var o Props
	if p := b.Properties; p != nil {
		o.AccessTierInferred, o.AccessTierChangeTime = tri(p.AccessTierInferred), micros(p.AccessTierChangeTime)
		if p.ArchiveStatus != nil {
			o.ArchiveStatus = string(*p.ArchiveStatus)
		}
		if p.RehydratePriority != nil {
			o.RehydratePriority = string(*p.RehydratePriority)
		}
		o.LastAccessTime, o.ContentEncoding = micros(p.LastAccessedOn), deref(p.ContentEncoding)
		o.ServerEncrypted, o.EncryptionScope = tri(p.ServerEncrypted), deref(p.EncryptionScope)
		o.CustomerProvidedKeySHA256 = deref(p.CustomerProvidedKeySHA256)
		if p.LeaseState != nil {
			o.LeaseState = intern(string(*p.LeaseState))
		}
		if p.TagCount != nil {
			o.TagCount = int64(*p.TagCount)
		}
		o.ImmutabilityUntil = micros(p.ImmutabilityPolicyExpiresOn)
		if p.ImmutabilityPolicyMode != nil {
			o.ImmutabilityMode = string(*p.ImmutabilityPolicyMode)
		}
		o.LegalHold = tri(p.LegalHold)
	}
	o.Metadata = object(b.Metadata)
	if b.BlobTags != nil {
		tags := make(map[string]*string, len(b.BlobTags.BlobTagSet))
		for _, t := range b.BlobTags.BlobTagSet {
			if t != nil && t.Key != nil {
				tags[*t.Key] = t.Value
			}
		}
		o.Tags = object(tags)
	}
	return o
}

// leaseStates are the lease states Azure lists, interned: nearly every blob
// lists one, and a page's copies would each hold their own bytes.
var leaseStates = []string{"available", "leased", "expired", "breaking", "broken"}

func intern(s string) string {
	if i := slices.Index(leaseStates, s); i >= 0 {
		return leaseStates[i]
	}
	return s
}

func micros(t *time.Time) int64 {
	if t == nil {
		return 0
	}
	return t.UnixMicro()
}

// object is m as a JSON object with its keys sorted (encoding/json sorts
// map keys), a null value as JSON null; "" for an empty or nil map. The
// XML and Arrow decoders both give a map, so the text is the same for
// either format.
func object(m map[string]*string) string {
	if len(m) == 0 {
		return ""
	}
	// Not json.Marshal: it escapes &, < and > (\u0026...), which a query
	// comparing the stored text (URLs are common in metadata) would miss.
	var b strings.Builder
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if enc.Encode(m) != nil {
		return "" // a map of strings always encodes
	}
	return strings.TrimSuffix(b.String(), "\n")
}

// The Arrow types the SDK's resolvers read for the entry and property
// columns item reads (see arrowColumns): a boolean, a uint64 or int32
// (resolveInt32), and a map of utf8 to utf8 (resolveMapCol).
var (
	arrowBool  = func(t arrow.DataType) bool { return t.ID() == arrow.BOOL }
	arrowInt32 = func(t arrow.DataType) bool { return t.ID() == arrow.UINT64 || t.ID() == arrow.INT32 }
	arrowMap   = func(t arrow.DataType) bool {
		m, ok := t.(*arrow.MapType)
		return ok && m.KeyType().ID() == arrow.STRING && m.ItemType().ID() == arrow.STRING
	}
)
