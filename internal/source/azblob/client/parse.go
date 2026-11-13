// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"bytes"
	"encoding/xml"
	"fmt"

	"github.com/varveio/swath/go/internal/source/objhttp"
	"github.com/varveio/swath/go/internal/utf16order"
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
	if p.NextMarker != "" && p.NextMarker == sent {
		return fmt.Errorf("%w: NextMarker repeats the marker sent", objhttp.ErrInvalidPage)
	}
	return nil
}

// errorDocument reads code and message from the bounded XML body, stripping
// its BOM. XML character decoding repairs azcore's headerless code parsing,
// which does not read CDATA or entity references.
func errorDocument(body []byte) (code, message string) {
	var w struct {
		Code    string `xml:"Code"`
		Message string `xml:"Message"`
	}
	if xml.Unmarshal(bytes.TrimPrefix(body, bom), &w) != nil {
		return "", ""
	}
	return w.Code, w.Message
}
