// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package sdkclient

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"strconv"
	"time"

	"github.com/varveio/swath/go/internal/source/objhttp"
	"github.com/varveio/swath/go/internal/source/s3/client"
)

// The walk decodes a captured ListBucketResult or ListVersionsResult body
// with encoding/xml's tokenizer, the root's children in document order.
// Versions need it whatever the decoder: the SDK splits <Version> and
// <DeleteMarker> into two lists, which cannot be merged back into the wire
// order when a key's entries share a timestamp (versions_order_test.go).
// Contents, Version and DeleteMarker share one element decoder; values a
// page repeats (storage class, owner, checksum fields, a "null" version ID)
// share the previous entry's string. Key fields are URL-decoded once when
// the answer says encoding-type=url; tokens and version IDs stay verbatim.
// A body that does not decode is ErrSyntax.

// envelope is either root's fields.
type envelope struct {
	entries                                    []client.Version
	prefixes                                   []string
	truncated, hasToken                        bool
	keyCount                                   int64
	token, enc, nextKey, nextVID, prefix, mark string
}

// decodeList decodes a ListBucketResult body.
func decodeList(body []byte) (*client.Page, error) {
	e, err := decodeRoot(body, "ListBucketResult")
	if err != nil {
		return nil, err
	}
	pg := &client.Page{CommonPrefixes: e.prefixes, IsTruncated: e.truncated, NextContinuationToken: e.token,
		HasContinuation: e.hasToken, KeyCount: int(e.keyCount), EncodingType: e.enc, Contents: make([]client.Object, len(e.entries))}
	for i := range e.entries {
		pg.Contents[i] = e.entries[i].Object
	}
	return pg, nil
}

// decodeVersions decodes a ListVersionsResult body: its <Version> and
// <DeleteMarker> entries in one slice in wire order.
func decodeVersions(body []byte) (*client.VersionPage, error) {
	e, err := decodeRoot(body, "ListVersionsResult")
	if err != nil {
		return nil, err
	}
	if e.entries == nil {
		e.entries = []client.Version{} // as package client's
	}
	return &client.VersionPage{Versions: e.entries, CommonPrefixes: e.prefixes, IsTruncated: e.truncated,
		NextKeyMarker: e.nextKey, NextVersionIDMarker: e.nextVID, Prefix: e.prefix, KeyMarker: e.mark, EncodingType: e.enc}, nil
}

// decodeRoot decodes body, whose root element must be root.
func decodeRoot(body []byte, root string) (*envelope, error) {
	w := &walk{d: xml.NewDecoder(bytes.NewReader(body))}
	e, err := w.root(root)
	if err == nil {
		err = e.unescape()
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %s response (%d bytes): %v", objhttp.ErrSyntax, root, len(body), err)
	}
	return e, nil
}

// walk is one body's decoder.
type walk struct {
	d    *xml.Decoder
	text []byte // chars' scratch
}

func (w *walk) root(root string) (*envelope, error) {
	el, ok, err := w.next()
	if err != nil {
		return nil, err
	}
	if !ok || el.Name.Local != root {
		return nil, fmt.Errorf("root element <%s>, want <%s>", el.Name.Local, root)
	}
	versions := root == "ListVersionsResult"
	e := &envelope{}
	for {
		el, ok, err := w.next()
		if err != nil || !ok {
			return e, err
		}
		switch name := el.Name.Local; {
		case name == "Contents" && !versions, (name == "Version" || name == "DeleteMarker") && versions:
			prev := client.Version{}
			if n := len(e.entries); n > 0 {
				prev = e.entries[n-1]
			}
			e.entries = append(e.entries, client.Version{DeleteMarker: name == "DeleteMarker"})
			err = w.entry(&e.entries[len(e.entries)-1], &prev, versions)
		case name == "CommonPrefixes":
			var p string
			err = w.children(func(name string) (err error) {
				if name == "Prefix" {
					p, err = w.str("")
					return err
				}
				return w.d.Skip()
			})
			e.prefixes = append(e.prefixes, p)
		case name == "IsTruncated":
			e.truncated, err = parse(w, strconv.ParseBool)
		case name == "KeyCount" && !versions:
			e.keyCount, err = parse(w, parseInt)
		case name == "NextContinuationToken" && !versions:
			e.token, err = w.str("")
			e.hasToken = true
		case name == "EncodingType":
			e.enc, err = w.str("")
		case name == "NextKeyMarker" && versions:
			e.nextKey, err = w.str("")
		case name == "NextVersionIdMarker" && versions:
			e.nextVID, err = w.str("")
		case name == "Prefix" && versions:
			e.prefix, err = w.str("")
		case name == "KeyMarker" && versions:
			e.mark, err = w.str("")
		default:
			err = w.d.Skip()
		}
		if err != nil {
			return nil, err
		}
	}
}

// entry decodes a <Contents>, <Version> or <DeleteMarker> into v (VersionId
// and IsLatest in a versions listing only); prev is the previous entry.
func (w *walk) entry(v, prev *client.Version, versions bool) error {
	o, alg := &v.Object, false
	return w.children(func(name string) (err error) {
		switch name {
		case "Key":
			o.Key, err = w.str("")
		case "ETag":
			o.ETag, err = w.str("")
		case "Size":
			o.Size, err = parse(w, parseInt)
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
		case "VersionId":
			if !versions {
				return w.d.Skip()
			}
			v.VersionID, err = w.str(prev.VersionID)
		case "IsLatest":
			if !versions {
				return w.d.Skip()
			}
			v.IsLatest, err = parse(w, strconv.ParseBool)
		default:
			err = w.d.Skip()
		}
		return err
	})
}

// children calls child with each child element's name; child consumes the
// element through its end.
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

// chars returns the current element's character data through its end,
// child elements skipped (as encoding/xml's Unmarshal into a string),
// valid until the next call.
func (w *walk) chars() ([]byte, error) {
	w.text = w.text[:0]
	for {
		tok, err := w.d.Token()
		if err != nil {
			return nil, err
		}
		switch t := tok.(type) {
		case xml.CharData:
			w.text = append(w.text, t...)
		case xml.StartElement:
			if err := w.d.Skip(); err != nil {
				return nil, err
			}
		case xml.EndElement:
			return w.text, nil
		}
	}
}

// str is the element's text; prev itself when equal (no new string).
func (w *walk) str(prev string) (string, error) {
	b, err := w.chars()
	if err != nil || string(b) == prev {
		return prev, err
	}
	return string(b), nil
}

// parse is the element's text through p, spaces trimmed (as encoding/xml);
// empty text is the zero value.
func parse[T any](w *walk, p func(string) (T, error)) (v T, err error) {
	b, err := w.chars()
	if b = bytes.TrimSpace(b); err != nil || len(b) == 0 {
		return v, err
	}
	return p(string(b))
}

func parseInt(s string) (int64, error) { return strconv.ParseInt(s, 10, 64) }

// unescape URL-decodes the key fields once when the answer says
// encoding-type=url.
func (e *envelope) unescape() (err error) {
	for i := range e.entries {
		if e.entries[i].Key, err = unescape(e.enc, e.entries[i].Key); err != nil {
			return err
		}
	}
	for i := range e.prefixes {
		if e.prefixes[i], err = unescape(e.enc, e.prefixes[i]); err != nil {
			return err
		}
	}
	for _, s := range []*string{&e.nextKey, &e.prefix, &e.mark} {
		if *s, err = unescape(e.enc, *s); err != nil {
			return err
		}
	}
	return nil
}
