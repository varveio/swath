// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

//go:build windows

package localfs

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"unsafe"

	"golang.org/x/sys/windows"
)

// The measurement of #568 (docs/internals/measurements.md, "Space on disk
// in a Windows directory listing"): what the directory enumeration readDir
// makes says of each file's space on disk, against what a call per file
// says. It asserts nothing; it logs one row per case. It runs only with
// SWATH_WINDOWS_MEASURE=1.

// standardInfo is FILE_STANDARD_INFO.
type standardInfo struct {
	AllocationSize int64
	EndOfFile      int64
	NumberOfLinks  uint32
	DeletePending  uint8
	Directory      uint8
}

type enumerated struct {
	eof, alloc, fullAlloc, id int64
	attrs                     uint32
}

var (
	kernel32                  = windows.NewLazySystemDLL("kernel32.dll")
	procGetCompressedFileSize = kernel32.NewProc("GetCompressedFileSizeW")
	procGetDiskFreeSpace      = kernel32.NewProc("GetDiskFreeSpaceW")
)

func compressedSize(path string) (int64, error) {
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}
	var high uint32
	low, _, e := procGetCompressedFileSize.Call(uintptr(unsafe.Pointer(p)), uintptr(unsafe.Pointer(&high)))
	if uint32(low) == 0xFFFFFFFF && e != windows.ERROR_SUCCESS {
		return 0, e
	}
	return int64(high)<<32 | int64(uint32(low)), nil
}

func clusterSize(root string) (int64, error) {
	p, err := windows.UTF16PtrFromString(root)
	if err != nil {
		return 0, err
	}
	var spc, bps, free, total uint32
	r, _, e := procGetDiskFreeSpace.Call(uintptr(unsafe.Pointer(p)), uintptr(unsafe.Pointer(&spc)), uintptr(unsafe.Pointer(&bps)), uintptr(unsafe.Pointer(&free)), uintptr(unsafe.Pointer(&total)))
	if r == 0 {
		return 0, e
	}
	return int64(spc) * int64(bps), nil
}

func readEnumeration(t *testing.T, dir string) map[string]*enumerated {
	t.Helper()
	read := func() []fs.DirEntry {
		f, err := os.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		ents, err := readDir(f)
		if err != nil {
			t.Fatalf("readDir %s: %v", dir, err)
		}
		return ents
	}
	m := map[string]*enumerated{}
	for _, d := range read() {
		w := d.(*winEntry).info
		m[d.Name()] = &enumerated{eof: d.(*winEntry).Size(), alloc: w.AllocationSize, id: w.FileID, attrs: w.FileAttributes}
	}
	readDirFileIDs = false
	defer func() { readDirFileIDs = true }()
	for _, d := range read() {
		if e := m[d.Name()]; e != nil {
			e.fullAlloc = d.(*winEntry).info.AllocationSize
		}
	}
	return m
}

// handleInfo opens path for its attributes only and reads its standard
// information and file index.
func handleInfo(path string) (standardInfo, int64, error) {
	var s standardInfo
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return s, 0, err
	}
	h, err := windows.CreateFile(p, windows.FILE_READ_ATTRIBUTES, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil,
		windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT, 0)
	if err != nil {
		return s, 0, err
	}
	defer windows.CloseHandle(h)
	if err := windows.GetFileInformationByHandleEx(h, windows.FileStandardInfo, (*byte)(unsafe.Pointer(&s)), uint32(unsafe.Sizeof(s))); err != nil {
		return s, 0, err
	}
	var bh windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(h, &bh); err != nil {
		return s, 0, err
	}
	return s, int64(bh.FileIndexHigh)<<32 | int64(bh.FileIndexLow), nil
}

func ioctl(f *os.File, code uint32, in []byte) error {
	var n uint32
	var inp *byte
	if len(in) > 0 {
		inp = &in[0]
	}
	return windows.DeviceIoControl(windows.Handle(f.Fd()), code, inp, uint32(len(in)), nil, 0, &n, nil)
}

func compressible(n int) []byte {
	return []byte(strings.Repeat("swath measures the space on disk. ", n/34+1)[:n])
}

func writeFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestMeasureAllocationSize logs, for each case, readDir's length and
// AllocationSize (FileIdBothDirectoryInfo, and the AllocationSize of
// FileFullDirectoryInfo), against GetCompressedFileSize
// and FILE_STANDARD_INFO through a handle to the file, and whether the
// enumeration's FileId is the handle's file index.
func TestMeasureAllocationSize(t *testing.T) {
	if os.Getenv("SWATH_WINDOWS_MEASURE") != "1" {
		t.Skip("a measurement, logged: SWATH_WINDOWS_MEASURE=1")
	}
	dir := t.TempDir()
	vol := filepath.VolumeName(dir) + `\`
	var fsName [64]uint16
	var flags uint32
	vp, _ := windows.UTF16PtrFromString(vol)
	if err := windows.GetVolumeInformation(vp, nil, 0, nil, nil, &flags, &fsName[0], uint32(len(fsName))); err != nil {
		t.Fatal(err)
	}
	cluster, err := clusterSize(vol)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("MEASURE volume %s filesystem %s cluster %d flags %#x dir %s", vol, windows.UTF16ToString(fsName[:]), cluster, flags, dir)

	sub := filepath.Join(dir, "sub")
	if err := os.Mkdir(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	var rows []string
	row := func(label, path string, m map[string]*enumerated) {
		e := m[filepath.Base(path)]
		if e == nil {
			t.Fatalf("%s: %s not enumerated", label, path)
		}
		cs, err := compressedSize(path)
		cse := ""
		if err != nil {
			cse = err.Error()
		}
		s, idx, err := handleInfo(path)
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		var a []string
		if e.attrs&windows.FILE_ATTRIBUTE_COMPRESSED != 0 {
			a = append(a, "compressed")
		}
		if e.attrs&windows.FILE_ATTRIBUTE_SPARSE_FILE != 0 {
			a = append(a, "sparse")
		}
		rows = append(rows, fmt.Sprintf("MEASURE\t%s\t%s\tenum_eof=%d\tenum_alloc=%d\tenum_full_alloc=%d\tcompressed_size=%d%s\thandle_alloc=%d\thandle_eof=%d\tlinks=%d\tfile_id_matches=%v",
			label, strings.Join(a, ","), e.eof, e.alloc, e.fullAlloc, cs, cse, s.AllocationSize, s.EndOfFile, s.NumberOfLinks, e.id == idx))
	}

	// Ordinary files: one in the MFT (100 bytes), one not.
	writeFile(t, filepath.Join(dir, "tiny"), compressible(100))
	writeFile(t, filepath.Join(dir, "ordinary"), compressible(10000))
	writeFile(t, filepath.Join(dir, "ordinary-1m"), make([]byte, 1<<20))

	// Compressed: marked before writing, and after.
	for _, name := range []string{"compressed-before", "compressed-after", "compressed-before-flushed"} {
		f, err := os.Create(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		format := []byte{1, 0} // COMPRESSION_FORMAT_DEFAULT
		if name != "compressed-after" {
			if err := ioctl(f, windows.FSCTL_SET_COMPRESSION, format); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := f.Write(compressible(4 << 20)); err != nil {
			t.Fatal(err)
		}
		if name == "compressed-after" {
			if err := ioctl(f, windows.FSCTL_SET_COMPRESSION, format); err != nil {
				t.Fatal(err)
			}
		}
		if strings.HasSuffix(name, "-flushed") {
			if err := f.Sync(); err != nil {
				t.Fatal(err)
			}
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}

	// Sparse: 64 MiB with 64 KiB written in the middle, and 64 MiB unwritten.
	for _, name := range []string{"sparse", "sparse-flushed", "sparse-empty"} {
		f, err := os.Create(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		if err := ioctl(f, windows.FSCTL_SET_SPARSE, nil); err != nil {
			t.Fatal(err)
		}
		if err := f.Truncate(64 << 20); err != nil {
			t.Fatal(err)
		}
		if name != "sparse-empty" {
			if _, err := f.WriteAt(compressible(64<<10), 32<<20); err != nil {
				t.Fatal(err)
			}
		}
		if name == "sparse-flushed" {
			if err := f.Sync(); err != nil {
				t.Fatal(err)
			}
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}

	// Extended without writing, not sparse.
	if f, err := os.Create(filepath.Join(dir, "extended")); err != nil {
		t.Fatal(err)
	} else {
		if err := f.Truncate(8 << 20); err != nil {
			t.Fatal(err)
		}
		f.Close()
	}

	// Hard links: hl-a written, linked as hl-b (same directory) and
	// sub/hl-c, then grown by 1 MiB through hl-a.
	a := filepath.Join(dir, "hl-a")
	writeFile(t, a, compressible(10000))
	if err := os.Link(a, filepath.Join(dir, "hl-b")); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(a, filepath.Join(sub, "hl-c")); err != nil {
		t.Fatal(err)
	}
	if f, err := os.OpenFile(a, os.O_WRONLY|os.O_APPEND, 0); err != nil {
		t.Fatal(err)
	} else {
		if _, err := f.Write(make([]byte, 1<<20)); err != nil {
			t.Fatal(err)
		}
		f.Close()
	}

	m := readEnumeration(t, dir)
	ms := readEnumeration(t, sub)
	names := make([]string, 0, len(m))
	for n := range m {
		if n != "sub" && n != "." && n != ".." {
			names = append(names, n)
		}
	}
	sort.Strings(names)
	for _, n := range names {
		row(n, filepath.Join(dir, n), m)
	}
	row("sub/hl-c", filepath.Join(sub, "hl-c"), ms)

	// A file being written: 1 MiB, still open; 2 MiB, still open; closed.
	w := filepath.Join(dir, "writing")
	f, err := os.Create(w)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(make([]byte, 1<<20)); err != nil {
		t.Fatal(err)
	}
	row("writing (1 MiB, open)", w, readEnumeration(t, dir))
	if _, err := f.Write(make([]byte, 1<<20)); err != nil {
		t.Fatal(err)
	}
	row("writing (2 MiB, open)", w, readEnumeration(t, dir))
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	row("writing (2 MiB, open, flushed)", w, readEnumeration(t, dir))
	f.Close()
	row("writing (2 MiB, closed)", w, readEnumeration(t, dir))

	// The hard links again after hl-b is opened (and closed) for reading.
	if f, err := os.Open(filepath.Join(dir, "hl-b")); err == nil {
		f.Close()
	}
	m = readEnumeration(t, dir)
	row("hl-a (after hl-b opened)", a, m)
	row("hl-b (after hl-b opened)", filepath.Join(dir, "hl-b"), m)
	row("sub/hl-c (after hl-b opened)", filepath.Join(sub, "hl-c"), readEnumeration(t, sub))

	// Everything again once the cache is flushed to disk: compression and
	// sparse allocation happen as the data is written back.
	for _, n := range names {
		if f, err := os.OpenFile(filepath.Join(dir, n), os.O_RDWR, 0); err == nil {
			f.Sync()
			f.Close()
		}
	}
	m = readEnumeration(t, dir)
	for _, n := range names {
		row(n+" (all flushed)", filepath.Join(dir, n), m)
	}

	for _, r := range rows {
		t.Log(r)
	}
}
