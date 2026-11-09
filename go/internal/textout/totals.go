// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package textout

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/varveio/swath/go/internal/totals"
)

// WriteTotals writes per-prefix totals (swath count --depth), in the order
// given, as the listing's text rows are written: TSV under a header line,
// JSONL, or the table, with the same escaping (appendEscaped), so a
// prefix's bytes, raw ones included, round-trip and never send a terminal a
// control byte. The fields are Prefix, Objects, Size (AllocatedSize when
// allocated) and LastModified (the newest; absent when no object under the
// prefix has a time). JSON carries text only, so a caller with byte keys
// (schema.Binary) must not ask for JSONL.
func WriteTotals(w io.Writer, format string, allocated bool, rows []totals.Prefix) error {
	size := "Size"
	if allocated {
		size = "AllocatedSize"
	}
	bw := bufio.NewWriterSize(w, bufferBytes)
	var b []byte
	switch format {
	case TSV:
		b = append(b, "Prefix\tObjects\t"+size+"\tLastModified\n"...)
	case JSONL, Table:
	default:
		return fmt.Errorf("text output: per-prefix totals have no %q format", format)
	}
	for i := range rows {
		r := &rows[i]
		switch format {
		case TSV:
			b = appendEscaped(b, r.Prefix, false)
			b = strconv.AppendInt(append(b, '\t'), r.Objects, 10)
			b = strconv.AppendInt(append(b, '\t'), r.Bytes, 10)
			b = append(b, '\t')
			if r.HasNewest {
				b = appendISO(b, r.Newest)
			}
		case JSONL:
			b = appendJSON(append(b, `{"Prefix":`...), r.Prefix)
			b = strconv.AppendInt(append(b, `,"Objects":`...), r.Objects, 10)
			b = strconv.AppendInt(append(b, `,"`+size+`":`...), r.Bytes, 10)
			if r.HasNewest {
				b = append(appendISO(append(b, `,"LastModified":"`...), r.Newest), '"')
			}
			b = append(b, '}')
		case Table:
			// The aws s3 ls line, widened: the newest time to the second
			// (UTC), the objects right-aligned in 10, the bytes in 15,
			// the prefix; the total's prefix is empty.
			if r.HasNewest {
				b = time.UnixMicro(r.Newest).UTC().AppendFormat(b, "2006-01-02 15:04:05")
			} else {
				b = append(b, "                   "...)
			}
			b = appendRight(append(b, ' '), r.Objects, 10)
			b = appendRight(append(b, ' '), r.Bytes, 15)
			b = appendEscaped(append(b, ' '), r.Prefix, false)
		}
		b = append(b, '\n')
		if _, err := bw.Write(b); err != nil {
			return err
		}
		b = b[:0]
	}
	if _, err := bw.Write(b); err != nil { // the TSV header of an empty list
		return err
	}
	return bw.Flush()
}

// appendRight appends v right-aligned in width columns.
func appendRight(b []byte, v int64, width int) []byte {
	var num [20]byte
	n := strconv.AppendInt(num[:0], v, 10)
	for i := len(n); i < width; i++ {
		b = append(b, ' ')
	}
	return append(b, n...)
}
