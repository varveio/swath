// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package parquetout_test

import (
	"fmt"
	"strings"
	"testing"

	pq "github.com/parquet-go/parquet-go"

	"github.com/varveio/swath/go/internal/parquetout"
	"github.com/varveio/swath/go/internal/source/s3"
)

// s3Page is a 1,024-row page of S3 current objects shaped like
// aws-public-blockchain's: ledger keys, hex ETags, STANDARD, a full-object
// CRC64NVME checksum and no owner (no --s3-fetch-owner), so two of the
// nine columns null. Each row holds its own strings, as decoded rows do.
// With distinct, every string column alternates between two values of
// different lengths, so no string repeats the row before's (a synthetic
// case: no value is reused, and each comparison ends at the length).
func s3Page(distinct bool) []s3.Row {
	rows := make([]s3.Row, 1024)
	for i := range rows {
		class, alg, typ := "STANDARD", "CRC64NVME", "FULL_OBJECT"
		if distinct && i%2 == 1 {
			class, alg, typ = "STANDARD_IA", "CRC32C", "COMPOSITE"
		}
		rows[i] = s3.Row{
			Key: fmt.Sprintf("v1.1/stellar/ledgers/pubnet/FE3EC7FF--29440000-29503999/%08X--%d.xdr.zst", 0xFE3DFCDF-i, 29492000+i),
			Data: s3.Object{Size: int64(100000 + i*7919), Mtime: int64(1700000000000000 + i*1000),
				ETag: fmt.Sprintf("%032x", uint64(i)*0x9E3779B97F4A7C15), StorageClass: strings.Clone(class),
				ChecksumAlgorithm: strings.Clone(alg), ChecksumType: strings.Clone(typ)},
		}
	}
	return rows
}

// BenchmarkFill: every column of an S3 page, rows to parquet values (the
// page's fill, before encoding), per row of the page.
func BenchmarkFill(b *testing.B) {
	for _, distinct := range []bool{false, true} {
		b.Run(fmt.Sprintf("distinct=%v", distinct), func(b *testing.B) {
			page, columns := s3Page(distinct), 1+len(s3.Schema.Columns)
			values := make([]pq.Value, len(page))
			b.ReportAllocs()
			for b.Loop() {
				for c := range columns {
					parquetout.FillPage(&s3.Schema, page, c, values)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(page)), "ns/row")
			b.ReportMetric(0, "ns/op")
		})
	}
}
