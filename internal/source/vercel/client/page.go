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

// objectFields fills the versioned field names exactly. encoding/json's
// matching of struct fields is case insensitive and duplicate members
// overwrite earlier values, which could turn a listing into an empty page.
// The raw values alias body, which must already be valid JSON.
func objectFields(body []byte, fields map[string][]byte) error {
	if len(body) == 0 || body[0] != '{' {
		return errors.New("expected a JSON object")
	}
	body = bytes.TrimSpace(body[1:])
	for body[0] != '}' {
		rawName, rest := splitJSONValue(body)
		var name string
		if err := json.Unmarshal(rawName, &name); err != nil {
			return err
		}
		value, rest := splitJSONValue(bytes.TrimSpace(bytes.TrimSpace(rest)[1:]))
		if prior, known := fields[name]; known {
			if prior != nil {
				return errors.New("duplicate listing field")
			}
			fields[name] = value
		}
		body = bytes.TrimSpace(rest)
		if body[0] == ',' {
			body = bytes.TrimSpace(body[1:])
		}
	}
	return nil
}

// splitJSONValue returns one raw value and the rest of the input without
// copying or decoding extension fields. json.Valid has already checked
// matching delimiters, escapes and whitespace.
func splitJSONValue(body []byte) (value, rest []byte) {
	switch body[0] {
	case '"':
		i := quotedEnd(body)
		return body[:i], body[i:]
	case '{', '[':
		depth := 1
		for i := 1; ; i++ {
			switch body[i] {
			case '"':
				i += quotedEnd(body[i:]) - 1
			case '{', '[':
				depth++
			case '}', ']':
				depth--
				if depth == 0 {
					return body[:i+1], body[i+1:]
				}
			}
		}
	default:
		i := bytes.IndexAny(body, ",]} \r\n\t")
		if i < 0 {
			i = len(body)
		}
		return body[:i], body[i:]
	}
}

func quotedEnd(body []byte) int {
	for i := 1; ; i++ {
		switch body[i] {
		case '"':
			return i + 1
		case '\\':
			i++
		}
	}
}

// entries checks the limit before growing the result, even when a bad
// response squeezes excessive entries into an otherwise bounded body.
func entries[T any](body []byte, limit int, decode func([]byte) (T, error)) ([]T, error) {
	if len(body) == 0 || body[0] != '[' {
		return nil, invalidPage("expected a folded listing array")
	}
	var items []T
	body = bytes.TrimSpace(body[1:])
	for body[0] != ']' {
		if len(items) == limit {
			return nil, invalidPage("page exceeds the requested limit")
		}
		raw, rest := splitJSONValue(body)
		item, err := decode(raw)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
		body = bytes.TrimSpace(rest)
		if body[0] == ',' {
			body = bytes.TrimSpace(body[1:])
		}
	}
	return items, nil
}

func decodeBlob(raw []byte) (Blob, error) {
	fields := map[string][]byte{"pathname": nil, "size": nil, "uploadedAt": nil, "etag": nil}
	var name, etag, uploaded wireString
	var size *int64
	if objectFields(raw, fields) != nil ||
		json.Unmarshal(fields["pathname"], &name) != nil ||
		json.Unmarshal(fields["size"], &size) != nil ||
		json.Unmarshal(fields["uploadedAt"], &uploaded) != nil ||
		name == "" || size == nil || *size < 0 {
		return Blob{}, invalidPage("missing or invalid blob metadata")
	}
	if raw := fields["etag"]; raw != nil && json.Unmarshal(raw, &etag) != nil {
		return Blob{}, invalidPage("invalid blob ETag")
	}
	uploadedAt, err := time.Parse(time.RFC3339Nano, string(uploaded))
	if err != nil {
		return Blob{}, invalidPage("missing or invalid blob upload time")
	}
	return Blob{Pathname: string(name), Size: *size, UploadedAt: uploadedAt, ETag: string(etag)}, nil
}

func decodeFolder(raw []byte) (string, error) {
	var folder wireString
	if json.Unmarshal(raw, &folder) != nil || folder == "" {
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
	fields := map[string][]byte{"blobs": nil, "folders": nil, "hasMore": nil, "cursor": nil, "error": nil}
	var hasMore *bool
	if objectFields(bytes.TrimSpace(body), fields) != nil || fields["error"] != nil ||
		json.Unmarshal(fields["hasMore"], &hasMore) != nil || hasMore == nil {
		return ListOutput{}, invalidPage("missing or invalid folded listing fields or unexpected error member")
	}
	var cursor wireString
	if raw := fields["cursor"]; raw != nil && json.Unmarshal(raw, &cursor) != nil {
		return ListOutput{}, invalidPage("invalid continuation cursor")
	}
	var out ListOutput
	if *hasMore {
		if cursor == "" {
			return ListOutput{}, invalidPage("missing continuation cursor")
		}
		out.NextCursor = string(cursor)
	} else if cursor != "" {
		return ListOutput{}, invalidPage("continuation cursor on a final page")
	}
	if limit == 0 {
		limit = MaxPageSize
	}
	var err error
	out.Blobs, err = entries(fields["blobs"], limit, decodeBlob)
	if err != nil {
		return ListOutput{}, err
	}
	out.Folders, err = entries(fields["folders"], limit-len(out.Blobs), decodeFolder)
	if err != nil {
		return ListOutput{}, err
	}
	return out, nil
}

func invalidPage(reason string) error {
	return source.Violation(fmt.Errorf("%w: %s", objhttp.ErrInvalidPage, reason))
}
