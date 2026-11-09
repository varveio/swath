// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package sdkclient

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/varveio/swath/go/internal/source/objhttp"
	"github.com/varveio/swath/go/internal/source/s3/client"
)

// The walk decodes a captured ListVersionsResult body with encoding/xml's
// tokenizer, the root's children in document order: the SDK splits
// <Version> and <DeleteMarker> into two lists, which cannot be merged back
// into the wire order when a key's entries share a timestamp
// (TestStockSDKLosesVersionsOrder). Values a page repeats (storage class,
// owner, checksum fields, a "null" version ID) share the previous entry's
// string. The key fields are URL-decoded once when the answer says
// encoding-type=url; version IDs stay verbatim. A body that does not
// decode is ErrSyntax.

// decodeVersions decodes a ListVersionsResult body: its <Version> and
// <DeleteMarker> entries in one slice in wire order.
func decodeVersions(body []byte) (*client.VersionPage, error) {
	w := &walk{d: xml.NewDecoder(bytes.NewReader(body))}
	pg, err := w.versions()
	if err == nil {
		err = unescapeVersions(pg)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: ListVersionsResult response (%d bytes): %v", objhttp.ErrSyntax, len(body), err)
	}
	return pg, nil
}

// walk is one body's decoder.
type walk struct {
	d    *xml.Decoder
	text []byte // chars' scratch
}

func (w *walk) versions() (*client.VersionPage, error) {
	el, ok, err := w.next()
	if err != nil {
		return nil, err
	}
	if !ok || el.Name.Local != "ListVersionsResult" {
		return nil, fmt.Errorf("root element <%s>, want <ListVersionsResult>", el.Name.Local)
	}
	pg := &client.VersionPage{Versions: []client.Version{}} // as package client's
	return pg, w.children(func(name string) (err error) {
		switch name {
		case "Version", "DeleteMarker":
			var prev client.Version
			if n := len(pg.Versions); n > 0 {
				prev = pg.Versions[n-1]
			}
			pg.Versions = append(pg.Versions, client.Version{DeleteMarker: name == "DeleteMarker"})
			return w.entry(&pg.Versions[len(pg.Versions)-1], &prev)
		case "CommonPrefixes":
			var p string
			err = w.children(func(name string) (err error) {
				if name == "Prefix" {
					p, err = w.str("")
					return err
				}
				return w.d.Skip()
			})
			pg.CommonPrefixes = append(pg.CommonPrefixes, p)
		case "IsTruncated":
			pg.IsTruncated, err = parse(w, strconv.ParseBool)
		case "EncodingType":
			pg.EncodingType, err = w.str("")
		case "NextKeyMarker":
			pg.NextKeyMarker, err = w.str("")
		case "NextVersionIdMarker":
			pg.NextVersionIDMarker, err = w.str("")
		case "Prefix":
			pg.Prefix, err = w.str("")
		case "KeyMarker":
			pg.KeyMarker, err = w.str("")
		default:
			err = w.d.Skip()
		}
		return err
	})
}

// entry decodes a <Version> or <DeleteMarker> into v; prev is the previous
// entry.
func (w *walk) entry(v, prev *client.Version) error {
	o, alg := &v.Object, false
	return w.children(func(name string) (err error) {
		switch name {
		case "Key":
			o.Key, err = w.str("")
		case "VersionId":
			v.VersionID, err = w.str(prev.VersionID)
		case "IsLatest":
			v.IsLatest, err = parse(w, strconv.ParseBool)
		case "ETag":
			o.ETag, err = w.str("")
		case "Size":
			o.Size, err = parse(w, func(s string) (int64, error) { return strconv.ParseInt(s, 10, 64) })
		case "LastModified":
			o.Mtime, err = parse(w, func(s string) (int64, error) {
				var t time.Time
				err := t.UnmarshalText([]byte(s)) // as encoding/xml and package client
				return t.UnixMicro(), err
			})
		case "StorageClass":
			o.StorageClass, err = w.str(prev.StorageClass)
		case "ChecksumType":
			o.ChecksumType, err = w.str(prev.ChecksumType)
		case "ChecksumAlgorithm": // the first listed
			var s string
			if s, err = w.str(prev.ChecksumAlgorithm); !alg {
				o.ChecksumAlgorithm, alg = s, true
			}
		case "Owner":
			err = w.children(func(name string) (err error) {
				switch name {
				case "ID":
					o.OwnerID, err = w.str(prev.OwnerID)
				case "DisplayName":
					o.OwnerDisplayName, err = w.str(prev.OwnerDisplayName)
				default:
					err = w.d.Skip()
				}
				return err
			})
		default:
			err = w.d.Skip()
		}
		return err
	})
}

// children calls child with each child element's name until the current
// element ends; child consumes the element through its end.
func (w *walk) children(child func(name string) error) error {
	for {
		el, ok, err := w.next()
		if err != nil || !ok {
			return err
		}
		if err := child(el.Name.Local); err != nil {
			return err
		}
	}
}

// next returns the current element's next child element, or ok false at
// its end.
func (w *walk) next() (el xml.StartElement, ok bool, err error) {
	for {
		tok, err := w.d.Token()
		if err != nil {
			return el, false, err
		}
		switch t := tok.(type) {
		case xml.StartElement:
			return t, true, nil
		case xml.EndElement:
			return el, false, nil
		}
	}
}

// str is the element's character data through its end, child elements
// skipped (as encoding/xml's Unmarshal into a string); prev itself when
// equal (no new string).
func (w *walk) str(prev string) (string, error) {
	w.text = w.text[:0]
	for {
		tok, err := w.d.Token()
		if err != nil {
			return "", err
		}
		switch t := tok.(type) {
		case xml.CharData:
			w.text = append(w.text, t...)
		case xml.StartElement:
			if err := w.d.Skip(); err != nil {
				return "", err
			}
		case xml.EndElement:
			if string(w.text) == prev {
				return prev, nil
			}
			return string(w.text), nil
		}
	}
}

// parse is the element's text through p, spaces trimmed (as encoding/xml);
// empty text is the zero value.
func parse[T any](w *walk, p func(string) (T, error)) (v T, err error) {
	s, err := w.str("")
	if s = strings.TrimSpace(s); err != nil || s == "" {
		return v, err
	}
	return p(s)
}

// unescapeVersions URL-decodes a page's key fields once when the answer
// says encoding-type=url.
func unescapeVersions(pg *client.VersionPage) (err error) {
	keys := []*string{&pg.NextKeyMarker, &pg.Prefix, &pg.KeyMarker}
	for i := range pg.Versions {
		keys = append(keys, &pg.Versions[i].Key)
	}
	for i := range pg.CommonPrefixes {
		keys = append(keys, &pg.CommonPrefixes[i])
	}
	for _, s := range keys {
		if *s, err = unescape(pg.EncodingType, *s); err != nil {
			return err
		}
	}
	return nil
}
