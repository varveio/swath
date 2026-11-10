// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package journal

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"
)

// Identity is what must not change between a run and its resume: anything
// that changes which rows are listed or how they are written. The pipeline
// freezes the schema, the writer, the part size and the key cap; the
// source names the rest (Provider and Source, from source.Identified).
type Identity struct {
	Tool     string `json:"tool"`      // buildinfo.Tool
	Format   int    `json:"format"`    // dataset format version (the journal's own is in the header)
	SchemaID string `json:"schema_id"` // output schema
	KeyCap   int    `json:"key_cap"`   // the schema's key length limit (rows it refuses)
	Writer   string `json:"writer"`    // the writer recipe (parquetout.Recipe)
	PartSize int64  `json:"part_size"` // part boundaries
	// PartMaxRows is the row bound per part (0: none), a part boundary too.
	PartMaxRows int64   `json:"part_max_rows,omitempty"`
	Provider    string  `json:"provider"` // the source's provider ("s3")
	Source      []Field `json:"source"`   // the provider's frozen settings, in report order
	// Filters are the row filters' descriptions (package filter), in
	// order; none for an unfiltered run.
	Filters []string `json:"filters,omitempty"`
}

// Redacted returns a diagnostic copy, including for journals written before
// credential-bearing endpoints were refused. Identity comparison uses the original.
func (id Identity) Redacted() Identity {
	id.Source = slices.Clone(id.Source)
	for i := range id.Source {
		f := &id.Source[i]
		switch f.Name {
		case "endpoint":
			f.Value, f.Canonical = redactEndpoint(f.Value), redactEndpoint(f.Canonical)
		case "source":
			f.Value, f.Canonical = redactSource(f.Value), redactSource(f.Canonical)
		case "account", "container":
			f.Value, f.Canonical = redactName(f.Value), redactName(f.Canonical)
		}
	}
	return id
}

// Field is one frozen source setting. Two values are the same setting when
// their Canonical forms are equal (Value when Canonical is empty).
type Field struct {
	Name      string `json:"name"`
	Value     string `json:"value"`
	Canonical string `json:"canonical,omitempty"`
}

func (f Field) canon() string {
	if f.Canonical != "" {
		return f.Canonical
	}
	return f.Value
}

// MismatchError says which part of the identity differs.
type MismatchError struct {
	Field             string
	Stored, Requested any
}

func (e *MismatchError) Error() string {
	// Older journals may have recorded credentials in endpoint or root URLs.
	stored, requested := e.Stored, e.Requested
	switch e.Field {
	case "endpoint":
		stored, requested = redactEndpoint(fmt.Sprint(stored)), redactEndpoint(fmt.Sprint(requested))
	case "source":
		stored, requested = redactSource(fmt.Sprint(stored)), redactSource(fmt.Sprint(requested))
	case "account", "container":
		stored, requested = redactName(fmt.Sprint(stored)), redactName(fmt.Sprint(requested))
	}
	return fmt.Sprintf("cannot resume: %s was %v, now %v", e.Field, stored, requested)
}

// Check compares a stored identity with a requested one. It is a pure
// function. Workers, encoders, memory and whatever the source leaves out of
// its identity (for S3: seeding, probing, credentials) may change across a
// resume.
func Check(stored, requested Identity) error {
	switch {
	case stored.Tool != requested.Tool:
		return &MismatchError{"tool", stored.Tool, requested.Tool}
	case stored.Format != requested.Format:
		return &MismatchError{"format", stored.Format, requested.Format}
	case stored.SchemaID != requested.SchemaID:
		return &MismatchError{"schema", stored.SchemaID, requested.SchemaID}
	case stored.Provider != requested.Provider:
		return &MismatchError{"provider", stored.Provider, requested.Provider}
	}
	byName := map[string]Field{}
	for _, f := range stored.Source {
		byName[f.Name] = f
	}
	for _, r := range requested.Source {
		s, ok := byName[r.Name]
		if !ok {
			return &MismatchError{r.Name, "(not recorded)", r.Value}
		}
		if s.canon() != r.canon() {
			return &MismatchError{r.Name, s.Value, r.Value}
		}
		delete(byName, r.Name)
	}
	for _, s := range stored.Source {
		if _, left := byName[s.Name]; left {
			return &MismatchError{s.Name, s.Value, "(not set)"}
		}
	}
	switch {
	case stored.KeyCap != requested.KeyCap:
		return &MismatchError{"key cap", stored.KeyCap, requested.KeyCap}
	case stored.Writer != requested.Writer:
		return &MismatchError{"writer", stored.Writer, requested.Writer}
	case stored.PartSize != requested.PartSize:
		return &MismatchError{"part size", stored.PartSize, requested.PartSize}
	case stored.PartMaxRows != requested.PartMaxRows:
		return &MismatchError{"part rotation max rows", stored.PartMaxRows, requested.PartMaxRows}
	case !slices.Equal(stored.Filters, requested.Filters):
		return &MismatchError{"filters", fmt.Sprintf("%q", stored.Filters), fmt.Sprintf("%q", requested.Filters)}
	}
	return nil
}

// Hash is the identity's digest, the run report's args_hash: SHA-256 over
// a versioned, label-tagged encoding of every frozen input (the source's
// fields in their canonical form), each followed by a zero byte. Two runs
// with the same Hash list the same rows into the same parts; a resume keeps
// it. It is not Java swath's args_hash (that one covers the listing scope
// only), and holds no credential.
func (id Identity) Hash() string {
	h := sha256.New()
	put := func(label, v string) {
		h.Write([]byte(label))
		h.Write([]byte(v))
		h.Write([]byte{0})
	}
	put("v=", "swath-go/args/1")
	put("tool=", id.Tool)
	put("format=", strconv.Itoa(id.Format))
	put("schema=", id.SchemaID)
	put("key_cap=", strconv.Itoa(id.KeyCap))
	put("writer=", id.Writer)
	put("part_size=", strconv.FormatInt(id.PartSize, 10))
	put("provider=", id.Provider)
	for _, f := range id.Source {
		put("source."+f.Name+"=", f.canon())
	}
	if id.PartMaxRows > 0 {
		put("part_max_rows=", strconv.FormatInt(id.PartMaxRows, 10))
	}
	for _, f := range id.Filters {
		put("filter=", f)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// redactEndpoint and redactSource protect diagnostics from credentials
// recorded by older builds, with the standard library only (this package
// imports no provider). An endpoint is hidden whole when it has user
// information, a query or a fragment, literal or escaped.
func redactEndpoint(value string) string {
	u, err := url.Parse(value)
	if err != nil || u.User != nil || strings.ContainsAny(value, "?#") ||
		strings.ContainsFunc(u.Path, func(r rune) bool { return r == '?' || r == '#' || r == '%' || r < 0x20 || r == 0x7f }) ||
		strings.ContainsFunc(u.Host, func(r rune) bool { return r <= 0x20 || r == 0x7f }) {
		return "[redacted]"
	}
	return value
}

// A source root keeps its prefix, which is a literal key where ? and # are
// data: only a credential-bearing authority is hidden, and an Azure root's
// whole query, which is where its shared access signature goes. Deciding
// which Azure queries are signatures is the provider's job; a display can
// afford to hide a literal '?' in an Azure prefix instead.
func redactSource(value string) string {
	scheme, rest, ok := strings.Cut(value, "://")
	authority, _, _ := strings.Cut(rest, "/")
	if ok && strings.ContainsAny(authority, "@?#") {
		return scheme + "://[redacted]"
	}
	if i := strings.IndexByte(value, '?'); i >= 0 && scheme == "az" {
		return value[:i] + "?[redacted]"
	}
	return value
}

// redactName hides an account or container that an older build took from a
// root with a credential-bearing authority.
func redactName(value string) string {
	if strings.ContainsAny(value, "@?#") {
		return "[redacted]"
	}
	return value
}
