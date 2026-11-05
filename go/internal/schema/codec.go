// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"encoding/binary"
	"errors"
	"unsafe"
)

// AppendRows encodes rows for a spill frame: every field, integers as
// varints, strings length-prefixed. The format is private to one run.
func AppendRows(dst []byte, rows []Row) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(rows)))
	for i := range rows {
		r := &rows[i]
		dst = appendString(dst, r.Key)
		dst = appendString(dst, r.LinkTarget)
		for _, v := range [...]int64{r.Size, r.Mtime, r.Ctime, r.Atime, r.Btime, r.Inode, r.Dev, r.Blocks} {
			dst = binary.AppendVarint(dst, v)
		}
		for _, v := range [...]uint32{r.Mode, r.UID, r.GID, r.Nlink} {
			dst = binary.AppendUvarint(dst, uint64(v))
		}
		dst = binary.AppendUvarint(dst, uint64(r.Present))
		dst = append(dst, byte(r.Kind))
		if o := r.Object; o != nil {
			dst = append(dst, 1)
			for _, s := range [...]string{o.ETag, o.StorageClass, o.OwnerID, o.OwnerDisplayName, o.ChecksumAlgorithm, o.ChecksumType} {
				dst = appendString(dst, s)
			}
		} else {
			dst = append(dst, 0)
		}
	}
	return dst
}

func appendString(dst []byte, s string) []byte {
	return append(binary.AppendUvarint(dst, uint64(len(s))), s...)
}

var errCodec = errors.New("spill frame: malformed rows")

// DecodeRows decodes a frame written by AppendRows. The rows' strings view
// src, which the caller hands over (it must not be modified afterwards).
func DecodeRows(src []byte) ([]Row, error) {
	d := decoder{b: src}
	n := d.uvarint()
	if d.err != nil || n > uint64(len(src)) {
		return nil, errCodec
	}
	rows := make([]Row, n)
	var metas []ObjectMeta
	for i := range rows {
		r := &rows[i]
		r.Key, r.LinkTarget = d.str(), d.str()
		for _, p := range [...]*int64{&r.Size, &r.Mtime, &r.Ctime, &r.Atime, &r.Btime, &r.Inode, &r.Dev, &r.Blocks} {
			*p = d.varint()
		}
		for _, p := range [...]*uint32{&r.Mode, &r.UID, &r.GID, &r.Nlink} {
			*p = uint32(d.uvarint())
		}
		r.Present = Presence(d.uvarint())
		r.Kind = Kind(d.byte())
		if d.byte() == 1 {
			if metas == nil {
				metas = make([]ObjectMeta, len(rows))
			}
			o := &metas[i]
			o.ETag, o.StorageClass, o.OwnerID = d.str(), d.str(), d.str()
			o.OwnerDisplayName, o.ChecksumAlgorithm, o.ChecksumType = d.str(), d.str(), d.str()
			r.Object = o
		}
		if d.err != nil {
			return nil, d.err
		}
	}
	if len(d.b) != 0 {
		return nil, errCodec
	}
	return rows, nil
}

type decoder struct {
	b   []byte
	err error
}

func (d *decoder) uvarint() uint64 {
	v, n := binary.Uvarint(d.b)
	if n <= 0 {
		d.err, d.b = errCodec, nil
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *decoder) varint() int64 {
	v, n := binary.Varint(d.b)
	if n <= 0 {
		d.err, d.b = errCodec, nil
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *decoder) byte() byte {
	if len(d.b) == 0 {
		d.err = errCodec
		return 0
	}
	c := d.b[0]
	d.b = d.b[1:]
	return c
}

func (d *decoder) str() string {
	n := d.uvarint()
	if d.err != nil || n > uint64(len(d.b)) {
		d.err, d.b = errCodec, nil
		return ""
	}
	if n == 0 {
		return ""
	}
	s := unsafe.String(&d.b[0], int(n))
	d.b = d.b[n:]
	return s
}
