// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"bytes"
	"cmp"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
	"unicode/utf8"
)

// bom starts every body Azure sends, success or error (both service
// versions; every capture of the October 2026 campaign).
var bom = []byte("\xef\xbb\xbf")

// MaxPageSize is the most entries Azure returns on one page: a larger
// maxresults is answered with 5,000 (capture azure-08e) and an absent one
// means 5,000.
const MaxPageSize = 5000

// The body cap is derived from the page size, not fixed. A name is at most
// 1,024 UTF-16 code units, so at most 3,072 bytes of UTF-8 (a scalar of
// three UTF-8 bytes is one unit; one of four bytes is two units). Its
// widest spelling on the wire is the Encoded form, which percent-encodes
// every byte but the 66 unreserved ones: 3 x 3,072 = 9,216 bytes (XML
// escaping is wider per byte only for ASCII metacharacters, "&quot;" at 6
// bytes, and 6 x 1,024 = 6,144 is less).
//
// The properties an uploader sets (Content-Type, Content-Encoding,
// Content-Language, Cache-Control, Content-Disposition) have no documented
// length of their own: they arrive as request headers of one Put Blob or
// Set Blob Properties call (which sets all five together), and Azure's
// front end (HTTP.sys, "Microsoft-HTTPAPI/2.0" in every capture) refuses a
// request whose request line and headers pass MaxRequestBytes, 16,384
// bytes by default. So the five together hold at most 16,384 bytes, at
// most 6 x 16,384 = 98,304 once XML-escaped. The fixed properties
// (Creation-Time, Last-Modified, Etag, Content-Length, Content-MD5,
// Content-CRC64, BlobType, AccessTier and the lease and encryption flags)
// and the entry's tags take about 660 bytes (azure-08d: 3,505,280 bytes for
// 5,000 entries of 45-byte names): fixedEntryBytes allows 2,048. An entry
// is thus at most 9,216 + 98,304 + 2,048 = 109,568 bytes. The envelope
// holds the declaration, ServiceEndpoint, the Prefix and Delimiter echoes
// (each at most a name, 18 KiB escaped) and two opaque markers (under 200
// bytes in every capture): envelopeBytes. A 5,000-entry page is capped at
// 64 KiB + 5,000 x 109,568 bytes, about 522 MiB; real pages are far smaller
// (3.5 MB in the captures) and the read grows only with the body. A body
// past the cap cannot be a valid page: it is a parse failure, retried and
// then requeued, never a terminal failure.
const (
	maxNameBytes    = 3 * 1024
	maxNameWire     = 3 * maxNameBytes
	maxUserProps    = 16 << 10 // HTTP.sys MaxRequestBytes default
	xmlEscapeFactor = 6        // "&quot;" for '"'
	fixedEntryBytes = 2 << 10
	envelopeBytes   = 64 << 10
	maxEntryOnWire  = maxNameWire + xmlEscapeFactor*maxUserProps + fixedEntryBytes
)

// bodyCap is the most bytes a page of maxResults entries can take (0 or
// more than MaxPageSize: MaxPageSize).
func bodyCap(maxResults int) int64 {
	n := maxResults
	if n <= 0 || n > MaxPageSize {
		n = MaxPageSize
	}
	return envelopeBytes + int64(n)*maxEntryOnWire
}

// wireName is a <Name>: XML text, or with Encoded="true" percent-encoded
// UTF-8 (Azure encodes a name XML cannot carry, one with U+FFFE or
// U+FFFF).
type wireName struct {
	Encoded bool   `xml:"Encoded,attr"`
	Text    string `xml:",chardata"`
}

// wireBlob holds only the fields a listing keeps; encoding/xml skips the
// others.
type wireBlob struct {
	Name  wireName `xml:"Name"`
	Props struct {
		CreationTime  string `xml:"Creation-Time"`
		LastModified  string `xml:"Last-Modified"`
		ETag          string `xml:"Etag"`
		ContentLength string `xml:"Content-Length"`
		ContentType   string `xml:"Content-Type"`
		ContentMD5    string `xml:"Content-MD5"`
		BlobType      string `xml:"BlobType"`
		AccessTier    string `xml:"AccessTier"`
	} `xml:"Properties"`
}

type wirePrefix struct {
	Name wireName `xml:"Name"`
}

// parse reads an EnumerationResults body: the BOM stripped, then
// encoding/xml's streaming decoder, one entry at a time. A body that is
// not a listing envelope (an EnumerationResults root holding a Blobs
// element and no Error element, with nothing but whitespace after its
// close), or ends before its close, is ErrSyntax (retried, never an empty
// final page); a name that does not decode is ErrInvalidPage. The caller's
// cancellation is checked between entries.
func parse(ctx context.Context, body []byte) (*ListOutput, error) {
	size := len(body)
	d := xml.NewDecoder(bytes.NewReader(bytes.TrimPrefix(body, bom)))
	root, err := firstElement(d)
	if err != nil {
		return nil, fmt.Errorf("%w (%d bytes): %v", ErrSyntax, size, err)
	}
	if root.Name.Local != "EnumerationResults" {
		return nil, fmt.Errorf("%w: a 200 whose body is <%s>, not <EnumerationResults> (%d bytes)", ErrSyntax, root.Name.Local, size)
	}
	out := &ListOutput{}
	inBlobs, sawBlobs := false, false
	for n := 0; ; n++ {
		if n%256 == 0 && ctx.Err() != nil {
			return nil, ctx.Err()
		}
		tok, err := d.Token()
		if err != nil {
			return nil, fmt.Errorf("%w (%d bytes): %v", ErrSyntax, size, err)
		}
		switch t := tok.(type) {
		case xml.StartElement:
			switch {
			case t.Name.Local == "Error":
				return nil, fmt.Errorf("%w: a 200 listing holding an <Error> element (%d bytes)", ErrSyntax, size)
			case !inBlobs && t.Name.Local == "Blobs":
				if sawBlobs {
					return nil, fmt.Errorf("%w: two <Blobs> elements (%d bytes)", ErrSyntax, size)
				}
				inBlobs, sawBlobs = true, true
			case inBlobs && t.Name.Local == "Blob":
				var w wireBlob
				if err := d.DecodeElement(&w, &t); err != nil {
					return nil, fmt.Errorf("%w (%d bytes): %v", ErrSyntax, size, err)
				}
				it, err := blobItem(&w)
				if err != nil {
					return nil, err
				}
				out.Items = append(out.Items, it)
			case inBlobs && t.Name.Local == "BlobPrefix":
				var w wirePrefix
				if err := d.DecodeElement(&w, &t); err != nil {
					return nil, fmt.Errorf("%w (%d bytes): %v", ErrSyntax, size, err)
				}
				name, err := decodeName(w.Name)
				if err != nil {
					return nil, err
				}
				out.Items = append(out.Items, Item{Name: name, Prefix: true})
			case !inBlobs && t.Name.Local == "NextMarker":
				if err := d.DecodeElement(&out.NextMarker, &t); err != nil {
					return nil, fmt.Errorf("%w (%d bytes): %v", ErrSyntax, size, err)
				}
			default:
				if err := d.Skip(); err != nil {
					return nil, fmt.Errorf("%w (%d bytes): %v", ErrSyntax, size, err)
				}
			}
		case xml.EndElement:
			switch t.Name.Local {
			case "Blobs":
				inBlobs = false
			case "EnumerationResults":
				if !sawBlobs {
					return nil, fmt.Errorf("%w: <EnumerationResults> without <Blobs> (%d bytes)", ErrSyntax, size)
				}
				if err := trailing(d); err != nil {
					return nil, fmt.Errorf("%w: after </EnumerationResults> (%d bytes): %v", ErrSyntax, size, err)
				}
				return out, nil
			}
		}
	}
}

// trailing checks that nothing but whitespace, comments and processing
// instructions follows the root element.
func trailing(d *xml.Decoder) error {
	for {
		tok, err := d.Token()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		switch t := tok.(type) {
		case xml.CharData:
			if len(bytes.TrimSpace(t)) != 0 {
				return fmt.Errorf("text %.40q", t)
			}
		case xml.Comment, xml.ProcInst:
		default:
			return fmt.Errorf("unexpected %T", tok)
		}
	}
}

// firstElement is the document's root element.
func firstElement(d *xml.Decoder) (xml.StartElement, error) {
	for {
		tok, err := d.Token()
		if err == io.EOF {
			return xml.StartElement{}, errors.New("no root element")
		}
		if err != nil {
			return xml.StartElement{}, err
		}
		if s, ok := tok.(xml.StartElement); ok {
			return s, nil
		}
	}
}

func blobItem(w *wireBlob) (Item, error) {
	name, err := decodeName(w.Name)
	if err != nil {
		return Item{}, err
	}
	p := &w.Props
	it := Item{Name: name, ETag: p.ETag, ContentMD5: p.ContentMD5, ContentType: p.ContentType, BlobType: p.BlobType, AccessTier: p.AccessTier}
	if p.ContentLength != "" {
		if it.Size, err = strconv.ParseInt(p.ContentLength, 10, 64); err != nil {
			return Item{}, fmt.Errorf("%w: Content-Length %q of %q", ErrSyntax, p.ContentLength, name)
		}
	}
	if it.LastModified, err = httpTime(p.LastModified); err != nil {
		return Item{}, fmt.Errorf("%w: Last-Modified %q of %q", ErrSyntax, p.LastModified, name)
	}
	if it.CreationTime, err = httpTime(p.CreationTime); err != nil {
		return Item{}, fmt.Errorf("%w: Creation-Time %q of %q", ErrSyntax, p.CreationTime, name)
	}
	return it, nil
}

// httpTime reads an RFC 1123 time (Azure's spelling) as Unix
// microseconds; 0 when absent.
func httpTime(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	t, err := time.Parse(http.TimeFormat, s)
	if err != nil {
		return 0, err
	}
	return t.UnixMicro(), nil
}

// decodeName is a name as listed: XML text as it is, an Encoded name
// percent-decoded once with path semantics (a literal '+' stays '+').
// A malformed escape, or bytes that are not UTF-8 once decoded, is an
// invalid page.
func decodeName(n wireName) (string, error) {
	if !n.Encoded {
		return n.Text, nil
	}
	s, err := url.PathUnescape(n.Text)
	if err != nil {
		return "", fmt.Errorf("%w: Encoded name %q: %v", ErrInvalidPage, n.Text, err)
	}
	if !utf8.ValidString(s) {
		return "", fmt.Errorf("%w: Encoded name %q is not UTF-8 once decoded", ErrInvalidPage, n.Text)
	}
	return s, nil
}

// validate holds a parsed page to the listing contract: names (blobs and
// prefixes together, in wire order) ascend strictly in UTF-16 code-unit
// order, and the next marker is not the one sent (the listing would loop).
// An empty NextMarker is the end.
func (p *ListOutput) validate(sent string) error {
	for i := 1; i < len(p.Items); i++ {
		if a, b := p.Items[i-1].Name, p.Items[i].Name; compareNames(a, b) >= 0 {
			return fmt.Errorf("%w: %q listed after %q (UTF-16 code-unit order)", ErrInvalidPage, b, a)
		}
	}
	if p.NextMarker != "" && p.NextMarker == sent {
		return fmt.Errorf("%w: NextMarker repeats the marker sent", ErrInvalidPage)
	}
	return nil
}

// compareNames is Azure's listing order, UTF-16 code units. For valid
// UTF-8 it is byte order except at the first differing byte, where lead
// bytes F0..F4 (U+10000 and up, surrogate pairs D800..DFFF in UTF-16)
// rank before EE..EF (U+E000..U+FFFF): at a first difference both bytes
// are lead bytes or both continuation bytes, so ranking the byte values
// 00..ED, F0..F4, EE..EF, F5..FF compares the code units.
func compareNames(a, b string) int {
	for i := range min(len(a), len(b)) {
		if a[i] != b[i] {
			return cmp.Compare(unitRank(a[i]), unitRank(b[i]))
		}
	}
	return cmp.Compare(len(a), len(b))
}

func unitRank(v byte) int {
	switch {
	case v >= 0xf0 && v <= 0xf4:
		return int(v) - 2
	case v == 0xee || v == 0xef:
		return int(v) + 5
	}
	return int(v)
}

// errorBody reads an <Error> document's Code and Message (the BOM
// stripped); both empty when the body is not one.
func errorBody(body []byte) (code, message string) {
	var w struct {
		Code    string `xml:"Code"`
		Message string `xml:"Message"`
	}
	if xml.Unmarshal(bytes.TrimPrefix(body, bom), &w) != nil {
		return "", ""
	}
	return w.Code, w.Message
}
