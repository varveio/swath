// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package memstore

import (
	"bufio"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/varveio/swath/internal/replay/store"
)

// The TSV fixture format: one object per line, tab-separated columns
//
//	key  size  last_modified_us  etag  storage_class  owner_id  owner_display_name  checksum_algorithm  checksum_type
//
// Every field is percent-escaped (%XX; a key is bytes, and a tab or newline
// in it must not split the line), `\N` is null, and missing trailing
// columns are null. Lines that are empty or start with '#' are skipped.
// Keys need not be sorted.

// Columns is the TSV header, for writers that want one (as a comment).
const Columns = "key\tsize\tlast_modified_us\tetag\tstorage_class\towner_id\towner_display_name\tchecksum_algorithm\tchecksum_type"

const null = `\N`

// ReadTSV parses a TSV fixture.
func ReadTSV(r io.Reader) ([]store.Object, error) {
	var objs []store.Object
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), 16<<20)
	line := 0
	for sc.Scan() {
		line++
		text := sc.Text()
		if text == "" || text[0] == '#' {
			continue
		}
		o, err := parseLine(strings.Split(text, "\t"))
		if err != nil {
			return nil, fmt.Errorf("memstore: line %d: %w", line, err)
		}
		objs = append(objs, o)
	}
	return objs, sc.Err()
}

func parseLine(f []string) (store.Object, error) {
	var o store.Object
	field := func(i int) (*string, error) {
		if i >= len(f) || f[i] == null {
			return nil, nil
		}
		v, err := url.PathUnescape(f[i])
		if err != nil {
			return nil, fmt.Errorf("column %d: %w", i+1, err)
		}
		return &v, nil
	}
	key, err := field(0)
	if err != nil || key == nil {
		return o, fmt.Errorf("a key is required: %v", err)
	}
	o.Key = []byte(*key)
	for i, dst := range []**string{nil, nil, nil, &o.ETag, &o.StorageClass, &o.OwnerID, &o.OwnerDisplayName, &o.ChecksumAlgorithm, &o.ChecksumType} {
		if dst == nil {
			continue
		}
		if *dst, err = field(i); err != nil {
			return o, err
		}
	}
	for i, set := range []func(int64){
		1: func(v int64) { o.Size = &v },
		2: func(v int64) { t := time.UnixMicro(v).UTC(); o.LastModified = &t },
	} {
		if set == nil {
			continue
		}
		s, err := field(i)
		if err != nil {
			return o, err
		}
		if s == nil {
			continue
		}
		v, err := strconv.ParseInt(*s, 10, 64)
		if err != nil {
			return o, fmt.Errorf("column %d: %w", i+1, err)
		}
		set(v)
	}
	return o, nil
}

// AppendTSV appends o as one TSV line.
func AppendTSV(dst []byte, o *store.Object) []byte {
	dst = escape(dst, string(o.Key))
	num := func(set bool, v int64) {
		dst = append(dst, '\t')
		if set {
			dst = strconv.AppendInt(dst, v, 10)
		} else {
			dst = append(dst, null...)
		}
	}
	var size, mtime int64
	if o.Size != nil {
		size = *o.Size
	}
	if o.LastModified != nil {
		mtime = o.LastModified.UnixMicro()
	}
	num(o.Size != nil, size)
	num(o.LastModified != nil, mtime)
	for _, s := range []*string{o.ETag, o.StorageClass, o.OwnerID, o.OwnerDisplayName, o.ChecksumAlgorithm, o.ChecksumType} {
		dst = append(dst, '\t')
		if s == nil {
			dst = append(dst, null...)
		} else {
			dst = escape(dst, *s)
		}
	}
	return append(dst, '\n')
}

// escape percent-escapes everything but unreserved ASCII and '/', so the
// field holds no tab, newline, '%' or backslash.
func escape(dst []byte, s string) []byte {
	const hex = "0123456789ABCDEF"
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.IndexByte("-._~/", c) >= 0 {
			dst = append(dst, c)
		} else {
			dst = append(dst, '%', hex[c>>4], hex[c&15])
		}
	}
	return dst
}
