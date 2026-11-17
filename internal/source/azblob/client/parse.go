// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"fmt"

	"github.com/varveio/swath/internal/source/objhttp"
	"github.com/varveio/swath/internal/utf16order"
)

// bom starts every body Azure sends, success or error (both service
// versions; every capture of the October 2026 campaign).
var bom = []byte("\xef\xbb\xbf")

// MaxPageSize is the most entries Azure returns on one page: a larger
// maxresults is answered with 5,000 (capture azure-08e) and an absent one
// means 5,000.
const MaxPageSize = 5000

// Validate holds a page to the listing contract: names (blobs and
// prefixes together, in wire order) ascend strictly in UTF-16 code-unit
// order, and the next marker is not the one sent (the listing would loop).
// An empty NextMarker is the end. A grouped listing (Include.Grouped)
// lists a name once per entry: its blobs' names ascend or repeat, a
// prefix still never repeats.
func (p *ListOutput) Validate(sent string, grouped bool) error {
	for i := 1; i < len(p.Items); i++ {
		a, b := &p.Items[i-1], &p.Items[i]
		c := utf16order.Compare(a.Name, b.Name)
		if c > 0 || c == 0 && (!grouped || a.Prefix || b.Prefix) {
			return fmt.Errorf("%w: %q listed after %q (UTF-16 code-unit order)", objhttp.ErrInvalidPage, b.Name, a.Name)
		}
	}
	return p.continues(sent)
}

// continues holds the page's next marker to not being the one sent: the
// listing would loop. It is all a page of ListInput.AnyOrder is held to.
func (p *ListOutput) continues(sent string) error {
	if p.NextMarker != "" && p.NextMarker == sent {
		return fmt.Errorf("%w: NextMarker repeats the marker sent", objhttp.ErrInvalidPage)
	}
	return nil
}

// errorDocument reads code and message from the bounded body, stripping
// its BOM: an XML error document, else a JSON one's code, found where
// azcore finds it ({"error": {...}}, {"odata.error": {...}} or the top
// level). XML character decoding repairs azcore's headerless code
// parsing, which does not read CDATA or entity references.
func errorDocument(body []byte) (code, message string) {
	body = bytes.TrimPrefix(body, bom)
	var w struct {
		Code    string `xml:"Code"`
		Message string `xml:"Message"`
	}
	if xml.Unmarshal(body, &w) == nil {
		return w.Code, w.Message
	}

	var doc map[string]any
	if json.Unmarshal(body, &doc) != nil {
		return "", ""
	}
	for _, k := range []string{"error", "odata.error"} {
		if inner, ok := doc[k]; ok {
			doc, _ = inner.(map[string]any)
			break
		}
	}
	code, _ = doc["code"].(string)
	return code, ""
}
