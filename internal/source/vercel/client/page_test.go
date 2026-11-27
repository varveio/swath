// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/objhttp"
)

func TestDecodePage(t *testing.T) {
	body := []byte(`{
		"folders": ["virtual/😀/", "virtual/\uD83D\uDE00/x/", "�/"],
		"extra": {"nested":[{"blobs":[{}],"closer":"} ] \" \\"}, ["",42,null]]},
		"blobs": [
			{"pathname":"a/\uFFFD\uD800\uDC00\\uD800/","size":9223372036854775807,
			"uploadedAt":"\u0032026-11-27T12:35:00.123456Z","etag":"\"tag\"","url":"https://example.com/"},
			{"pathname":"a/a","size":0,"uploadedAt":"2026-11-27T12:35:01Z","other":null}
		],
		"hasMore": true,
		"cursor": "next+cursor/="
	}`)
	got, err := decodePage(body, 5)
	want := []Blob{
		{Pathname: "a/�𐀀\\uD800/", Size: 9223372036854775807, UploadedAt: time.Date(2026, 11, 27, 12, 35, 0, 123_456_000, time.UTC), ETag: `"tag"`},
		{Pathname: "a/a", Size: 0, UploadedAt: time.Date(2026, 11, 27, 12, 35, 1, 0, time.UTC)},
	}
	if err != nil || !slices.Equal(got.Blobs, want) ||
		!slices.Equal(got.Folders, []string{"virtual/😀/", "virtual/😀/x/", "�/"}) || got.NextCursor != "next+cursor/=" {
		t.Fatalf("decodePage = %+v, %v", got, err)
	}
}

func TestMalformedPage(t *testing.T) {
	const blob = `{"pathname":"a","size":0,"uploadedAt":"2026-11-27T12:35:00Z","etag":"\"tag\""}`
	for _, tt := range []struct {
		name string
		body string
		want error
	}{
		{"empty body", "", objhttp.ErrSyntax},
		{"truncated JSON", `{"blobs":[]`, objhttp.ErrSyntax},
		{"trailing JSON", emptyPage + `{}`, objhttp.ErrSyntax},
		{"raw UTF8", strings.Replace(blob, `"a"`, "\"a\xff\"", 1), objhttp.ErrSyntax},
		{"null root", "null", objhttp.ErrInvalidPage},
		{"empty object", "{}", objhttp.ErrInvalidPage},
		{"array root", "[]", objhttp.ErrInvalidPage},
		{"absent blobs", `{"folders":[],"hasMore":false}`, objhttp.ErrInvalidPage},
		{"null blobs", `{"blobs":null,"folders":[],"hasMore":false}`, objhttp.ErrInvalidPage},
		{"mis-cased blobs", `{"Blobs":[],"folders":[],"hasMore":false}`, objhttp.ErrInvalidPage},
		{"absent folders", `{"blobs":[],"hasMore":false}`, objhttp.ErrInvalidPage},
		{"null folders", `{"blobs":[],"folders":null,"hasMore":false}`, objhttp.ErrInvalidPage},
		{"bad folder", `{"blobs":[],"folders":[null],"hasMore":false}`, objhttp.ErrInvalidPage},
		{"folder surrogate", `{"blobs":[],"folders":["\uD800/"],"hasMore":false}`, objhttp.ErrInvalidPage},
		{"absent hasMore", `{"blobs":[],"folders":[]}`, objhttp.ErrInvalidPage},
		{"null hasMore", `{"blobs":[],"folders":[],"hasMore":null}`, objhttp.ErrInvalidPage},
		{"string hasMore", `{"blobs":[],"folders":[],"hasMore":"false"}`, objhttp.ErrInvalidPage},
		{"missing cursor", `{"blobs":[],"folders":[],"hasMore":true}`, objhttp.ErrInvalidPage},
		{"cursor surrogate", `{"blobs":[],"folders":[],"hasMore":true,"cursor":"\uD800"}`, objhttp.ErrInvalidPage},
		{"terminal cursor", `{"blobs":[],"folders":[],"hasMore":false,"cursor":"new"}`, objhttp.ErrInvalidPage},
		{"null blob", withBlob("null"), objhttp.ErrInvalidPage},
		{"missing name", withBlob(strings.Replace(blob, `"pathname":"a",`, "", 1)), objhttp.ErrInvalidPage},
		{"empty name", withBlob(strings.Replace(blob, `"pathname":"a"`, `"pathname":""`, 1)), objhttp.ErrInvalidPage},
		{"unpaired high surrogate", withBlob(strings.Replace(blob, `"a"`, `"a\uD800"`, 1)), objhttp.ErrInvalidPage},
		{"unpaired low surrogate", withBlob(strings.Replace(blob, `"a"`, `"a\uDC00"`, 1)), objhttp.ErrInvalidPage},
		{"high surrogate then other character", withBlob(strings.Replace(blob, `"a"`, `"a\uD800\u0062"`, 1)), objhttp.ErrInvalidPage},
		{"missing size", withBlob(strings.Replace(blob, `"size":0,`, "", 1)), objhttp.ErrInvalidPage},
		{"null size", withBlob(strings.Replace(blob, `"size":0`, `"size":null`, 1)), objhttp.ErrInvalidPage},
		{"negative size", withBlob(strings.Replace(blob, `"size":0`, `"size":-1`, 1)), objhttp.ErrInvalidPage},
		{"fractional size", withBlob(strings.Replace(blob, `"size":0`, `"size":0.1`, 1)), objhttp.ErrInvalidPage},
		{"overflowing size", withBlob(strings.Replace(blob, `"size":0`, `"size":9223372036854775808`, 1)), objhttp.ErrInvalidPage},
		{"bad upload time", withBlob(strings.Replace(blob, `"2026-11-27T12:35:00Z"`, `"bad date"`, 1)), objhttp.ErrInvalidPage},
		{"null upload time", withBlob(strings.Replace(blob, `"2026-11-27T12:35:00Z"`, "null", 1)), objhttp.ErrInvalidPage},
		{"bad etag", withBlob(strings.Replace(blob, `"etag":"\"tag\""`, `"etag":73`, 1)), objhttp.ErrInvalidPage},
		{"duplicate blob metadata", withBlob(strings.Replace(blob, `"size":0`, `"size":5,"size":0`, 1)), objhttp.ErrInvalidPage},
		{"duplicate envelope array", `{"blobs":[` + blob + `],"b\u006cobs":[],"folders":[],"hasMore":false}`, objhttp.ErrInvalidPage},
		{"duplicate hasMore", `{"blobs":[],"folders":[],"hasMore":true,"cursor":"new","hasMore":false,"cursor":null}`, objhttp.ErrInvalidPage},
		{"error envelope", `{"blobs":[],"folders":[],"hasMore":false,"error":null}`, objhttp.ErrInvalidPage},
		{"combined page limit", `{"blobs":[` + blob + `],"folders":["a/","b/"],"hasMore":false}`, objhttp.ErrInvalidPage},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := decodePage([]byte(tt.body), 2)
			if !errors.Is(err, tt.want) || got.Blobs != nil || got.Folders != nil || got.NextCursor != "" {
				t.Fatalf("decodePage = %+v, %v, want no page and %v", got, err, tt.want)
			}
			if tt.want == objhttp.ErrInvalidPage && !errors.Is(err, source.ErrProtocol) {
				t.Fatalf("error %v does not match source.ErrProtocol", err)
			}
		})
	}
}

func withBlob(blob string) string {
	return `{"blobs":[` + blob + `],"folders":[],"hasMore":false}`
}

func TestPageLimitBeforeDecodingExtraEntry(t *testing.T) {
	_, err := decodePage([]byte(`{"blobs":[],"folders":["a/",{}],"hasMore":false}`), 1)
	if !errors.Is(err, objhttp.ErrInvalidPage) || !strings.Contains(err.Error(), "page exceeds the requested limit") {
		t.Fatalf("decodePage error = %v, want a page limit error before decoding the extra entry", err)
	}
}
