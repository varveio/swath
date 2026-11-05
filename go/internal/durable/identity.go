// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package durable

import "fmt"

// Identity is what must not change between a run and its resume: anything
// that changes which rows are listed or how they are written. The pipeline
// freezes the schema, the writer, the part size and the key cap; the
// source names the rest (Provider and Source, from source.Identified).
type Identity struct {
	Tool     string  `json:"tool"`      // "swath-go"
	Format   int     `json:"format"`    // dataset format version (the journal's own is in the header)
	SchemaID string  `json:"schema_id"` // output schema
	KeyCap   int     `json:"key_cap"`   // the schema's key length limit (rows it refuses)
	Writer   string  `json:"writer"`    // the writer recipe (parquet.Recipe)
	PartSize int64   `json:"part_size"` // part boundaries
	Provider string  `json:"provider"`  // the source's provider ("s3")
	Source   []Field `json:"source"`    // the provider's frozen settings, in report order
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
	return fmt.Sprintf("cannot resume: %s was %v, now %v (start over with -overwrite, or resume with the original settings)", e.Field, e.Stored, e.Requested)
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
	}
	return nil
}
