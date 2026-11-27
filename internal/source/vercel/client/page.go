// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/objhttp"
)

type ListOutput struct {
	Blobs      []Blob
	Folders    []string
	NextCursor string
}

type Blob struct {
	Pathname   string
	Size       int64
	UploadedAt time.Time
	ETag       string
}

// members reads exact field names and tracks only the fixed set accepted
// by read. Unknown fields can repeat without growing the seen map.
func members(d *json.Decoder, read func(string) (known bool, err error)) error {
	if t, err := d.Token(); err != nil || t != json.Delim('{') {
		return errors.New("expected a JSON object")
	}
	seen := make(map[string]bool)
	for d.More() {
		t, err := d.Token()
		if err != nil {
			return err
		}
		name := t.(string) // the decoder returns a string for an object member name
		if seen[name] {
			return errors.New("duplicate listing field")
		}
		known, err := read(name)
		if err != nil {
			return err
		}
		if known {
			seen[name] = true
		} else if err := d.Decode(&ignoredValue{}); err != nil {
			return err
		}
	}
	_, err := d.Token()
	return err
}

// ignoredValue lets the decoder consume extension values without making
// an additional RawMessage copy of its buffer.
type ignoredValue struct{}

func (*ignoredValue) UnmarshalJSON([]byte) error { return nil }

// entries checks the remaining page capacity before decoding each entry.
func entries[T any](d *json.Decoder, remaining int, decode func(*json.Decoder) (T, error)) ([]T, error) {
	if t, err := d.Token(); err != nil || t != json.Delim('[') {
		return nil, invalidPage("expected a folded listing array")
	}
	var items []T
	for d.More() {
		if len(items) == remaining {
			return nil, invalidPage("page exceeds the requested limit")
		}
		item, err := decode(d)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	_, err := d.Token()
	return items, err
}

func decodeBlob(d *json.Decoder) (Blob, error) {
	var name, etag, uploaded wireString
	var size *int64
	err := members(d, func(field string) (bool, error) {
		switch field {
		case "pathname":
			return true, d.Decode(&name)
		case "size":
			return true, d.Decode(&size)
		case "uploadedAt":
			return true, d.Decode(&uploaded)
		case "etag":
			return true, d.Decode(&etag)
		default:
			return false, nil
		}
	})
	if err != nil || name == "" || size == nil || *size < 0 {
		return Blob{}, invalidPage("missing or invalid blob metadata")
	}
	uploadedAt, err := time.Parse(time.RFC3339Nano, string(uploaded))
	if err != nil {
		return Blob{}, invalidPage("missing or invalid blob upload time")
	}
	return Blob{Pathname: string(name), Size: *size, UploadedAt: uploadedAt, ETag: string(etag)}, nil
}

func decodeFolder(d *json.Decoder) (string, error) {
	var folder wireString
	if d.Decode(&folder) != nil || folder == "" {
		return "", invalidPage("invalid folded listing folder")
	}
	return string(folder), nil
}

type wireString string

func (s *wireString) UnmarshalJSON(raw []byte) error {
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return err
	}
	// The JSON decoder converts a lone UTF-16 surrogate to U+FFFD.
	// A real U+FFFD pathname is valid. Check the escapes only when the
	// decoded string contains that rune.
	if strings.ContainsRune(value, utf8.RuneError) && hasLoneSurrogate(raw) {
		return errors.New("unpaired UTF-16 surrogate in listing string")
	}
	*s = wireString(value)
	return nil
}

func hasLoneSurrogate(raw []byte) bool {
	for i := 0; i+1 < len(raw); i++ {
		if raw[i] != '\\' {
			continue
		}
		i++ // consume an escape, including an escaped backslash
		if raw[i] != 'u' {
			continue
		}
		n := escapeCodeUnit(raw[i+1:])
		i += 4
		switch {
		case n >= 0xDC00 && n <= 0xDFFF:
			return true
		case n >= 0xD800 && n <= 0xDBFF:
			if i+6 >= len(raw) || raw[i+1] != '\\' || raw[i+2] != 'u' {
				return true
			}
			if low := escapeCodeUnit(raw[i+3:]); low < 0xDC00 || low > 0xDFFF {
				return true
			}
			i += 6
		}
	}
	return false
}

func escapeCodeUnit(raw []byte) uint64 {
	// Unmarshal checked the escape syntax and length before this scan.
	n, _ := strconv.ParseUint(string(raw[:4]), 16, 16)
	return n
}

func decodePage(body []byte, limit int) (ListOutput, error) {
	if !utf8.Valid(body) {
		return ListOutput{}, fmt.Errorf("%w: invalid UTF-8", objhttp.ErrSyntax)
	}
	if !json.Valid(body) {
		return ListOutput{}, fmt.Errorf("%w: invalid JSON", objhttp.ErrSyntax)
	}
	d := json.NewDecoder(bytes.NewReader(body))
	var out ListOutput
	var blobs, folders bool
	var hasMore *bool
	var cursor wireString
	err := members(d, func(field string) (bool, error) {
		var err error
		switch field {
		case "blobs":
			blobs = true
			out.Blobs, err = entries(d, limit-len(out.Folders), decodeBlob)
		case "folders":
			folders = true
			out.Folders, err = entries(d, limit-len(out.Blobs), decodeFolder)
		case "hasMore":
			err = d.Decode(&hasMore)
		case "cursor":
			err = d.Decode(&cursor)
		case "error":
			err = errors.New("unexpected listing error member")
		default:
			return false, nil
		}
		return true, err
	})
	if errors.Is(err, objhttp.ErrInvalidPage) {
		return ListOutput{}, err
	}
	// Decoder errors may quote field values, including the opaque cursor.
	if err != nil || !blobs || !folders || hasMore == nil {
		return ListOutput{}, invalidPage("missing or invalid folded listing fields or unexpected error member")
	}
	if *hasMore {
		if cursor == "" {
			return ListOutput{}, invalidPage("missing continuation cursor")
		}
		out.NextCursor = string(cursor)
	} else if cursor != "" {
		return ListOutput{}, invalidPage("continuation cursor on a final page")
	}
	return out, nil
}

func invalidPage(reason string) error {
	return source.Violation(fmt.Errorf("%w: %s", objhttp.ErrInvalidPage, reason))
}
