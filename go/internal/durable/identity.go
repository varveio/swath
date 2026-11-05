// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package durable

import "fmt"

// Identity is what must not change between a run and its resume: anything
// that changes which rows are listed or how they are written.
type Identity struct {
	Tool       string `json:"tool"`        // "swath-go"
	Format     int    `json:"format"`      // journal and dataset format version
	SchemaID   string `json:"schema_id"`   // output schema
	Provider   string `json:"provider"`    // "s3"
	Bucket     string `json:"bucket"`      // the source, not its URL: the endpoint may change
	Prefix     string `json:"prefix"`      //
	Writer     string `json:"writer"`      // encoding, geometry and part-size rule (parquet.WriterID)
	PartSize   int64  `json:"part_size"`   // part boundaries
	FetchOwner bool   `json:"fetch_owner"` // owner columns
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
// function; workers, encoders, memory, seeding and the endpoint (an
// equivalent route to the same bucket) may change across a resume because
// they do not change the dataset.
func Check(stored, requested Identity) error {
	switch {
	case stored.Tool != requested.Tool:
		return &MismatchError{"tool", stored.Tool, requested.Tool}
	case stored.Format != requested.Format:
		return &MismatchError{"format", stored.Format, requested.Format}
	case stored.SchemaID != requested.SchemaID:
		return &MismatchError{"schema", stored.SchemaID, requested.SchemaID}
	case stored.Provider != requested.Provider || stored.Bucket != requested.Bucket || stored.Prefix != requested.Prefix:
		return &MismatchError{"source", stored.Provider + "://" + stored.Bucket + "/" + stored.Prefix, requested.Provider + "://" + requested.Bucket + "/" + requested.Prefix}
	case stored.Writer != requested.Writer:
		return &MismatchError{"writer", stored.Writer, requested.Writer}
	case stored.PartSize != requested.PartSize:
		return &MismatchError{"part size", stored.PartSize, requested.PartSize}
	case stored.FetchOwner != requested.FetchOwner:
		return &MismatchError{"fetch owner", stored.FetchOwner, requested.FetchOwner}
	}
	return nil
}
