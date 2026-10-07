// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// prefixFailure writes a real prefix before failing. Its cut is a byte offset
// in the independently reopened reference file, rather than a write-call count.
type prefixFailure struct {
	output io.Writer
	cut    int64
	wrote  int64
	err    error
}

func (writer *prefixFailure) Write(data []byte) (int, error) {
	remaining := writer.cut - writer.wrote
	if remaining <= 0 {
		return 0, writer.err
	}
	length := min(int64(len(data)), remaining)
	n, err := writer.output.Write(data[:length])
	writer.wrote += int64(n)
	if err != nil {
		return n, err
	}
	if int64(len(data)) > length {
		return n, writer.err
	}
	return n, nil
}

func faultConfig() writerConfig {
	return writerConfig{
		Layout: "served", KeyEncoding: "dict", IntegerEncoding: "dict", PageStatistics: true,
		PageRows: 16, GroupRows: 64,
	}
}

// faultRows crosses pages and row groups, including null, zero and empty values
// of every optional primitive type. Random suffixes prevent compression from
// collapsing the body into the writer's final buffered write.
func faultRows() []Row {
	random := rand.New(rand.NewSource(941))
	rows := make([]Row, 193)
	for index := range rows {
		suffix := make([]byte, 80)
		_, _ = random.Read(suffix)
		row := Row{Key: fmt.Sprintf("%06d/%s", index, hex.EncodeToString(suffix)), RowType: "OBJECT"}
		if index%3 != 0 {
			size := int64(index)
			modified := int64(1729000000000000 + index)
			latest := index%2 == 0
			value := fmt.Sprintf("value-%d", index)
			if index%3 == 1 {
				size, modified, latest, value = 0, 0, false, ""
			}
			row.Size, row.LastModified, row.IsLatest = &size, &modified, &latest
			row.ETag, row.StorageClass, row.VersionID = &value, &value, &value
			row.OwnerID, row.OwnerDisplayName = &value, &value
			row.ChecksumAlgorithm, row.ChecksumType = &value, &value
		}
		if index%7 == 0 {
			row.RowType, row.IsDeleteMarker = "DELETE_MARKER", true
		}
		rows[index] = row
	}
	return rows
}

type failureCut struct {
	name   string
	offset int64
}

func referenceCuts(t *testing.T, rows []Row, config writerConfig) ([]byte, []failureCut) {
	t.Helper()
	var output bytes.Buffer
	if _, err := encodeRows(&output, rows, config); err != nil {
		t.Fatalf("reference encoding: %v", err)
	}
	data := output.Bytes()
	if len(data) < 12 || string(data[:4]) != "PAR1" || string(data[len(data)-4:]) != "PAR1" {
		t.Fatal("reference is missing Parquet magic")
	}
	footerLength := int64(binary.LittleEndian.Uint32(data[len(data)-8 : len(data)-4]))
	footerStart := int64(len(data)) - 8 - footerLength
	if footerLength <= 0 || footerStart <= 4 {
		t.Fatal("reference has an invalid footer extent")
	}
	file, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("reopen reference: %v", err)
	}
	if len(file.RowGroups()) < 3 {
		t.Fatalf("fixture has %d row groups; needs multiple flushed groups", len(file.RowGroups()))
	}
	firstPage := file.Metadata().RowGroups[0].Columns[0].MetaData.DataPageOffset
	if firstPage < 4 || firstPage >= footerStart {
		t.Fatal("reference has no usable first data page")
	}
	laterPage := file.Metadata().RowGroups[1].Columns[0].MetaData.DataPageOffset
	if laterPage <= firstPage || laterPage >= footerStart {
		t.Fatal("reference has no usable later row-group page")
	}
	return data, []failureCut{
		{name: "header", offset: 0},
		{name: "encoded page", offset: firstPage + 1},
		{name: "later row group", offset: laterPage + 1},
		{name: "footer metadata", offset: footerStart + footerLength/2},
		{name: "footer trailer", offset: int64(len(data)) - 2},
	}
}

func TestEncodeRowsRejectsPartialBodyAndFooter(t *testing.T) {
	rows, config := faultRows(), faultConfig()
	_, cuts := referenceCuts(t, rows, config)
	for _, cut := range cuts {
		t.Run(cut.name, func(t *testing.T) {
			failure := errors.New("injected output failure")
			var partial bytes.Buffer
			output := &prefixFailure{output: &partial, cut: cut.offset, err: failure}
			result, err := encodeRows(output, rows, config)
			if !errors.Is(err, failure) {
				t.Fatalf("encode error = %v, want injected failure", err)
			}
			if result != (encodeResult{}) {
				t.Fatalf("failed encoding returned successful measurements: %+v", result)
			}
			if int64(partial.Len()) != cut.offset {
				t.Fatalf("accepted %d bytes, want cut at %d", partial.Len(), cut.offset)
			}
			if _, err := parquet.OpenFile(bytes.NewReader(partial.Bytes()), int64(partial.Len())); err == nil {
				t.Fatal("partial body/footer was accepted as a completed Parquet file")
			}
		})
	}
}

// observedFile closes a real descriptor even when reporting an injected close
// error. Tests can then check both cleanup and the returned metadata boundary.
type observedFile struct {
	file       *os.File
	output     io.Writer
	syncErr    error
	closeErr   error
	closeCalls int
	events     *[]string
}

func (file *observedFile) Write(data []byte) (int, error) {
	return file.output.Write(data)
}

func (file *observedFile) Sync() error {
	*file.events = append(*file.events, "file sync")
	if file.syncErr != nil {
		return file.syncErr
	}
	return file.file.Sync()
}

func (file *observedFile) Close() error {
	file.closeCalls++
	*file.events = append(*file.events, "file close")
	return errors.Join(file.file.Close(), file.closeErr)
}

func TestWritePartRejectsFailuresBeforeDurableMetadata(t *testing.T) {
	rows, config := faultRows(), faultConfig()
	_, cuts := referenceCuts(t, rows, config)
	failure := errors.New("injected durability failure")
	cleanupFailure := errors.New("injected cleanup failure")
	cases := []struct {
		name       string
		cut        *failureCut
		syncErr    error
		closeErr   error
		parentErr  error
		wantEvents []string
	}{
		{name: "page write", cut: &cuts[1], wantEvents: []string{"file close"}},
		{name: "footer write", cut: &cuts[3], wantEvents: []string{"file close"}},
		{name: "footer trailer", cut: &cuts[4], wantEvents: []string{"file close"}},
		{name: "file sync", syncErr: failure, wantEvents: []string{"file sync", "file close"}},
		{name: "file close", closeErr: failure, wantEvents: []string{"file sync", "file close"}},
		{name: "parent sync", parentErr: failure, wantEvents: []string{"file sync", "file close", "parent sync"}},
		{name: "write and cleanup", cut: &cuts[1], closeErr: cleanupFailure, wantEvents: []string{"file close"}},
		{name: "sync and cleanup", syncErr: failure, closeErr: cleanupFailure, wantEvents: []string{"file sync", "file close"}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "part.parquet")
			var events []string
			var acquired *observedFile
			ops := fileOps{
				create: func(path string) (durableFile, error) {
					file, err := os.Create(path)
					if err != nil {
						return nil, err
					}
					var output io.Writer = file
					if test.cut != nil {
						output = &prefixFailure{output: file, cut: test.cut.offset, err: failure}
					}
					acquired = &observedFile{file: file, output: output, syncErr: test.syncErr, closeErr: test.closeErr, events: &events}
					return acquired, nil
				},
				syncParent: func(string) error {
					events = append(events, "parent sync")
					return test.parentErr
				},
			}
			result, err := writePart(path, rows, config, ops)
			if !errors.Is(err, failure) {
				t.Fatalf("write error = %v, want original failure", err)
			}
			if test.closeErr == cleanupFailure && !errors.Is(err, cleanupFailure) {
				t.Fatalf("cleanup error was lost: %v", err)
			}
			if !reflect.DeepEqual(result, partResult{}) {
				t.Fatalf("failed write returned durable metadata: %+v", result)
			}
			if acquired == nil || acquired.closeCalls != 1 {
				t.Fatalf("acquired file = %+v; want exactly one Close", acquired)
			}
			if _, err := acquired.file.Stat(); !errors.Is(err, os.ErrClosed) {
				t.Fatalf("descriptor survived cleanup: %v", err)
			}
			if !reflect.DeepEqual(events, test.wantEvents) {
				t.Fatalf("durability operations = %v, want %v", events, test.wantEvents)
			}
		})
	}
}

func TestWritePartCreateFailureHasNoPublication(t *testing.T) {
	failure := errors.New("injected create failure")
	parentCalled := false
	ops := fileOps{
		create: func(string) (durableFile, error) { return nil, failure },
		syncParent: func(string) error {
			parentCalled = true
			return nil
		},
	}
	result, err := writePart(filepath.Join(t.TempDir(), "absent.parquet"), faultRows(), faultConfig(), ops)
	if !errors.Is(err, failure) || !reflect.DeepEqual(result, partResult{}) || parentCalled {
		t.Fatalf("create failure = %+v, %v, parent called %t", result, err, parentCalled)
	}
}

func TestWritePartDurableOutputReopensWithAllNullableRows(t *testing.T) {
	rows, config := faultRows(), faultConfig()
	path := filepath.Join(t.TempDir(), "part.parquet")
	result, err := writePart(path, rows, config, diskOps())
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	digest := md5.Sum(data)
	if result.Path != path || result.Bytes != int64(len(data)) || result.Rows != int64(len(rows)) || result.MD5 != hex.EncodeToString(digest[:]) {
		t.Fatalf("published metadata disagrees with reopened file: %+v", result)
	}
	if result.MinKey == nil || *result.MinKey != rows[0].Key || result.MaxKey == nil || *result.MaxKey != rows[len(rows)-1].Key {
		t.Fatalf("published key bounds disagree with rows: %+v", result)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	reader := parquet.NewGenericReader[Row](file)
	defer reader.Close()
	var decoded []Row
	buffer := make([]Row, 11)
	for {
		n, err := reader.Read(buffer)
		decoded = append(decoded, buffer[:n]...)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			t.Fatal("reader made no progress before EOF")
		}
	}
	if !reflect.DeepEqual(decoded, rows) {
		for index := range min(len(decoded), len(rows)) {
			if !reflect.DeepEqual(decoded[index], rows[index]) {
				t.Fatalf("reopened row %d differs: got %+v, want %+v", index, decoded[index], rows[index])
			}
		}
		t.Fatalf("reopened row count = %d, want %d", len(decoded), len(rows))
	}
}
