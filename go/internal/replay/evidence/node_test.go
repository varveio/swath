// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package evidence

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// node is a parsed JSON value that keeps object member order, so a page
// can be edited and re-rendered with its fields where they were, and that
// is parsed strictly: a repeated member name or anything after the value
// is an error, the policy the conformance comparator reads pages with.
type node struct {
	kind   byte // 'o' object, 'a' array, 's' string, 'n' number, 'b' bool, 'z' null; 0 missing
	keys   []string
	fields map[string]*node
	elems  []*node
	str    string // string text, number literal
	b      bool
}

var missing = &node{}

func parseNode(b []byte) (*node, error) {
	if len(bytes.TrimSpace(b)) == 0 {
		return missing, nil
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	n, err := decodeNode(dec)
	if err != nil {
		return nil, err
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, errors.New("trailing token after JSON value")
	}
	return n, nil
}

func decodeNode(dec *json.Decoder) (*node, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	switch t := tok.(type) {
	case json.Delim:
		switch t {
		case '{':
			n := &node{kind: 'o', fields: map[string]*node{}}
			for dec.More() {
				kt, err := dec.Token()
				if err != nil {
					return nil, err
				}
				k := kt.(string)
				if _, dup := n.fields[k]; dup {
					return nil, fmt.Errorf("duplicate field %q", k)
				}
				v, err := decodeNode(dec)
				if err != nil {
					return nil, err
				}
				n.keys = append(n.keys, k)
				n.fields[k] = v
			}
			_, err := dec.Token()
			return n, err
		case '[':
			n := &node{kind: 'a'}
			for dec.More() {
				v, err := decodeNode(dec)
				if err != nil {
					return nil, err
				}
				n.elems = append(n.elems, v)
			}
			_, err := dec.Token()
			return n, err
		}
	case string:
		return &node{kind: 's', str: t}, nil
	case json.Number:
		return &node{kind: 'n', str: string(t)}, nil
	case bool:
		return &node{kind: 'b', b: t}, nil
	case nil:
		return &node{kind: 'z'}, nil
	}
	return nil, fmt.Errorf("unexpected token %v", tok)
}

// get is the member name, or missing.
func (n *node) get(name string) *node {
	if n.kind == 'o' {
		if v, ok := n.fields[name]; ok {
			return v
		}
	}
	return missing
}

func (n *node) has(name string) bool { return n.get(name) != missing }

// integral reports whether a number has no fraction or exponent.
func (n *node) integral() (int64, bool) {
	if n.kind != 'n' || strings.ContainsAny(n.str, ".eE") {
		return 0, false
	}
	v, err := strconv.ParseInt(n.str, 10, 64)
	return v, err == nil
}

// set replaces or appends a member.
func (n *node) set(name string, v *node) {
	if _, ok := n.fields[name]; !ok {
		n.keys = append(n.keys, name)
	}
	n.fields[name] = v
}

// encode renders the value compactly with members in order.
func (n *node) encode(dst []byte) []byte {
	switch n.kind {
	case 'o':
		dst = append(dst, '{')
		for i, k := range n.keys {
			if i > 0 {
				dst = append(dst, ',')
			}
			kb, _ := json.Marshal(k)
			dst = append(append(dst, kb...), ':')
			dst = n.fields[k].encode(dst)
		}
		return append(dst, '}')
	case 'a':
		dst = append(dst, '[')
		for i, e := range n.elems {
			if i > 0 {
				dst = append(dst, ',')
			}
			dst = e.encode(dst)
		}
		return append(dst, ']')
	case 's':
		sb, _ := json.Marshal(n.str)
		return append(dst, sb...)
	case 'n':
		return append(dst, n.str...)
	case 'b':
		return strconv.AppendBool(dst, n.b)
	}
	return append(dst, "null"...)
}
