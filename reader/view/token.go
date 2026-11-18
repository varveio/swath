// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package view

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"strings"

	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/reader/listing"
)

// tokenVersion begins every page token: "inv1.", then the base64url of
// the binding, then the SeekParquet cursor token (Cursor.Encode) after the
// page's last entry. A token of another version is refused.
const tokenVersion = "inv1"

// binding is the part of a token that ties it to one inventory, by its
// manifest digest, to one prefix, and to what the request's child totals
// count: none, or the kinds totals selects. Limit and MaxDuration bound
// one page's work, not what the level holds, and may change from page to
// page. Tokens are not signed: the
// binding detects a token used with another inventory, prefix or count,
// not a forged one.
func (inv *Inventory) binding(prefix []byte, totals *selection) string {
	h := sha256.New()
	h.Write([]byte("swath view page token\x00"))
	h.Write([]byte(inv.base.Manifest))
	h.Write([]byte{0})
	h.Write(prefix)
	h.Write([]byte{0})
	if totals != nil {
		// One byte per kind, 1 where totals counts it, so that the same
		// count spelt in two ways binds alike.
		for k := listing.KindObject; k <= lastKind; k++ {
			var counted byte
			if totals.counts(k) {
				counted = 1
			}
			h.Write([]byte{counted})
		}
	}
	return base64.RawURLEncoding.EncodeToString(h.Sum(nil)[:16])
}

// encodeToken is the token that continues the level bind names after c.
func encodeToken(bind string, c *seekparquet.Cursor) (string, error) {
	cursor, err := c.Encode()
	if err != nil {
		return "", err
	}
	return tokenVersion + "." + bind + "." + cursor, nil
}

// decodeToken returns the cursor a token of prefix's level, bound as bind,
// carries: nil for the empty token, which starts the level. A token of
// another version or binding, or whose cursor is not after an entry of
// the level, is ErrInvalidToken.
func decodeToken(prefix []byte, bind, token string) (*seekparquet.Cursor, error) {
	if token == "" {
		return nil, nil
	}
	parts := strings.SplitN(token, ".", 3)
	if len(parts) != 3 || parts[0] != tokenVersion {
		return nil, badToken("not a token of this version")
	}
	if parts[1] != bind {
		return nil, badToken("issued for another inventory, prefix or count of child totals")
	}
	c, err := seekparquet.ParseCursor(parts[2])
	if err != nil {
		return nil, badToken("its cursor does not parse")
	}
	switch {
	case c.Key != nil && (c.Offset != 0 || !bytes.HasPrefix(c.Key, prefix)):
		return nil, badToken("its cursor is after no entry of the level")
	case c.Prefix != nil && (len(c.Prefix) <= len(prefix) || !bytes.HasPrefix(c.Prefix, prefix) || c.Prefix[len(c.Prefix)-1] != '/'):
		return nil, badToken("its cursor is after no entry of the level")
	}
	return c, nil
}

func badToken(reason string) error {
	return wrap(listing.ErrInvalidToken, reason)
}
