// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
)

// UnmarshalJSON refuses incomplete or contradictory boundary records.
// Absence of the optional enclosing member remains unknown coverage.
func (b *FilesystemBoundaries) UnmarshalJSON(data []byte) error {
	var wire struct {
		Keys     []string `json:"keys_base64"`
		Count    *int64   `json:"total_count"`
		Complete *bool    `json:"complete"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	if wire.Keys == nil || wire.Count == nil || wire.Complete == nil ||
		*wire.Count < int64(len(wire.Keys)) || *wire.Complete != (*wire.Count == int64(len(wire.Keys))) {
		return errors.New("filesystem_boundaries: missing members or inconsistent count/completeness")
	}
	for _, encoded := range wire.Keys {
		key, err := base64.StdEncoding.Strict().DecodeString(encoded)
		if err != nil || len(key) == 0 || key[0] == '/' || key[len(key)-1] != '/' || bytes.IndexByte(key, 0) >= 0 ||
			base64.StdEncoding.EncodeToString(key) != encoded {
			return errors.New("filesystem_boundaries: key is not a base64 root-relative directory key")
		}
	}
	*b = FilesystemBoundaries{KeysBase64: wire.Keys, TotalCount: *wire.Count, Complete: *wire.Complete}
	return nil
}
