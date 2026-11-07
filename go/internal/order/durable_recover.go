// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package order

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/klauspost/compress/zstd"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/schema"
)

// ErrDurableUnavailable marks a durable spill that could not be set up
// (its directories or ledger): the run goes on with durable spill off.
var ErrDurableUnavailable = errors.New("durable spill unavailable")

// CreateDurable sets up a fresh run's durable spill under root: the
// directories (each one created synced, with its parent, before any seal
// record can follow) and an empty ledger, synced. An error other than the
// output's replacement (verify's) wraps ErrDurableUnavailable: the run
// goes on without durable spill.
func CreateDurable(root *os.Root, verify func() error, uuid, ident string) (*Cache, error) {
	c, err := newCache(root, verify, uuid, ident)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDurableUnavailable, err)
	}
	if err := c.fresh(); err != nil {
		return nil, err
	}
	return c, nil
}

// fresh makes the directories and a new ledger (mutations verified first).
func (c *Cache) fresh() error {
	if err := c.ensureDirs(); err != nil {
		return err
	}
	return c.newLedger()
}

// unavailable wraps err as ErrDurableUnavailable unless it is the output's
// replacement (Lock.Verify's), which fails the run.
func (c *Cache) unavailable(err error) error {
	if verr := c.verify(); verr != nil {
		return verr
	}
	return fmt.Errorf("%w: %w", ErrDurableUnavailable, err)
}

// ensureDirs creates spill/ and spill/durable/ where missing; each one
// created is synced with its parent's entry for it.
func (c *Cache) ensureDirs() error {
	parent := filepath.Dir(DurableDir)
	var made []string
	for _, rel := range []string{parent, DurableDir} {
		fi, err := dataset.NeedIn(c.root, rel, true, true)
		if err != nil {
			return c.unavailable(err)
		}
		if fi != nil {
			continue
		}
		if err := c.verify(); err != nil {
			return err
		}
		if err := c.root.Mkdir(rel, 0o755); err != nil && !errors.Is(err, fs.ErrExist) {
			return c.unavailable(err)
		}
		made = append(made, rel)
	}
	for _, rel := range made {
		err := faults.At("spill-dir-sync")
		if err == nil {
			err = dataset.SyncDirIn(c.root, rel)
		}
		if err == nil {
			err = dataset.SyncDirIn(c.root, filepath.Dir(rel))
		}
		if err != nil {
			return c.unavailable(err)
		}
		c.st.DirsCreated.Add(1)
	}
	return nil
}

// newLedger replaces the ledger with one holding only its header, synced,
// and its name synced in the directory.
func (c *Cache) newLedger() error {
	if err := c.verify(); err != nil {
		return err
	}
	if fi, err := dataset.NeedIn(c.root, c.rel(ledgerName), false, true); err != nil {
		return c.unavailable(err)
	} else if fi != nil {
		if err := c.root.Remove(c.rel(ledgerName)); err != nil {
			return c.unavailable(err)
		}
	}
	f, err := c.root.OpenFile(c.rel(ledgerName), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return c.unavailable(err)
	}
	hdr := frame2(nil, ledgerHeader, c.ledgerHeaderBody())
	if _, err := faults.Write(f, hdr); err != nil {
		f.Close()
		return c.unavailable(err)
	}
	if err := faults.Sync(f); err != nil {
		f.Close()
		return c.unavailable(err)
	}
	if err := dataset.SyncDirIn(c.root, DurableDir); err != nil {
		f.Close()
		return c.unavailable(err)
	}
	c.ledger, c.lend, c.durable = f, int64(len(hdr)), int64(len(hdr))
	c.hist = 0
	return nil
}

// entry is a sealed-not-retired ledger record.
type entry struct {
	id          [16]byte
	cs, ce      string
	ceOpen      bool
	rows        int64
	last        string
	bytes       int64
	crc         uint32
	order       int // the record's position (ties)
	overflowing bool
}

// RecoverDurable is a resume's recovery of its durable spill, single
// threaded under the output lock, through root, with verify before each
// mutation. K is the dataset's published prefix from part records (nil:
// ⊥). It reads the ledger's durable prefix (truncating after it), unlinks
// the files no record in it names, validates every recorded file and
// classifies it: accepted (a replay range), or retired (covered by K,
// missing, corrupt, foreign, overlapping), writing its retirements and
// unlinks before it returns; Start applies the new caps. A missing
// directory or ledger, or a foreign ledger header, starts afresh (every
// spill file is unlinked). An error is the output's replacement or an
// unsafe layout (a run failure), or ErrDurableUnavailable (the run goes on
// without durable spill and relists).
func RecoverDurable(root *os.Root, verify func() error, uuid, ident string, k *string) (*Cache, error) {
	c, err := newCache(root, verify, uuid, ident)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDurableUnavailable, err)
	}
	c.resumed = true
	if k != nil {
		c.k, c.hasK = *k, true
	}
	fi, err := dataset.NeedIn(root, DurableDir, true, true)
	if err != nil {
		return nil, err
	}
	if fi == nil {
		return c, c.fresh()
	}
	entries, err := c.readLedger()
	if err != nil {
		return nil, err
	}
	if err := c.sweep(entries); err != nil {
		return nil, err
	}
	return c, c.classify(entries)
}

// readLedger opens the ledger and reads its durable prefix: frames from
// the start to the first invalid one or the end; the rest is dropped (the
// ledger is truncated to the prefix and synced before anything is
// appended). A missing, foreign or unreadable header starts a new ledger,
// and every file becomes an orphan (nil entries).
func (c *Cache) readLedger() (map[[16]byte]*entry, error) {
	fi, err := dataset.NeedIn(c.root, c.rel(ledgerName), false, true)
	if err != nil {
		return nil, err
	}
	if fi == nil {
		return nil, c.newLedger()
	}
	f, err := dataset.OpenRegularIn(c.root, c.rel(ledgerName), os.O_RDWR)
	if err != nil {
		return nil, err
	}
	size := fi.Size()
	r := bufio.NewReader(f)
	var off int64
	next := func() (byte, []byte, bool) {
		var hdr [8]byte
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return 0, nil, false
		}
		n := binary.LittleEndian.Uint32(hdr[:4])
		if n == 0 || n > maxLedgerFrame || int64(n) > size-off-8 { // checked before allocating
			return 0, nil, false
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(r, payload); err != nil || !check2(hdr, payload) {
			return 0, nil, false
		}
		off += 8 + int64(n)
		return payload[0], payload[1:], true
	}
	typ, body, ok := next()
	if !ok || typ != ledgerHeader || !bytes.Equal(body, c.ledgerHeaderBody()) {
		f.Close()
		c.st.LedgerDroppedBytes.Add(size)
		return nil, c.newLedger()
	}
	entries := map[[16]byte]*entry{}
	live, records := 0, 0
	for {
		at := off
		typ, body, ok := next()
		if !ok {
			off = at
			break
		}
		switch e, ok := parseRecord(typ, body); {
		case !ok:
			off = at
		case typ == ledgerRetired:
			records++
			if _, ok := entries[e.id]; ok {
				delete(entries, e.id)
				live--
			}
			continue
		default:
			records++
			if _, dup := entries[e.id]; dup {
				continue // a second seal of one file: the first stands
			}
			e.order = records
			if live >= maxCertificates {
				e.overflowing = true
			}
			entries[e.id] = e
			live++
			continue
		}
		break
	}
	if dropped := size - off; dropped > 0 {
		c.st.LedgerDroppedBytes.Add(dropped)
		c.st.LedgerTruncatedFrom.Store(off)
		if err := c.verify(); err != nil {
			f.Close()
			return nil, err
		}
		if err := faults.At("spill-ledger-truncate"); err != nil {
			f.Close()
			return nil, err
		}
		if err := f.Truncate(off); err != nil {
			f.Close()
			return nil, c.unavailable(err)
		}
		if err := faults.Sync(f); err != nil {
			f.Close()
			return nil, c.unavailable(err)
		}
	}
	if _, err := f.Seek(off, io.SeekStart); err != nil {
		f.Close()
		return nil, c.unavailable(err)
	}
	c.ledger, c.lend, c.durable, c.hist = f, off, off, records
	return entries, nil
}

// parseRecord parses a ledger record frame.
func parseRecord(typ byte, body []byte) (*entry, bool) {
	e := &entry{}
	if len(body) < 16 {
		return nil, false
	}
	copy(e.id[:], body[:16])
	body = body[16:]
	switch typ {
	case ledgerRetired:
		return e, len(body) == 1 && body[0] >= 1 && body[0] <= 3
	case ledgerSealed:
	default:
		return nil, false
	}
	d := schema.NewDecoder(body)
	e.cs, e.ce = strings.Clone(d.Str()), strings.Clone(d.Str())
	e.ceOpen = d.Byte() == 1
	e.rows = int64(d.Uvarint())
	e.last = strings.Clone(d.Str())
	e.bytes = int64(d.Uvarint())
	sum := d.Fixed(4)
	if d.Err() != nil || d.Len() != 0 || len(sum) != 4 || e.rows < 0 || e.bytes <= 0 || (!e.ceOpen && e.ce <= e.cs) {
		return nil, false
	}
	e.crc = binary.LittleEndian.Uint32(sum)
	return e, true
}

// sweep streams the directory and unlinks every entry no record in the
// prefix names (an orphan: a file sealed after the prefix, never sealed,
// or abandoned), and with nil entries every spill file; the ledger stays.
func (c *Cache) sweep(entries map[[16]byte]*entry) error {
	d, err := c.root.Open(DurableDir)
	if err != nil {
		return c.unavailable(err)
	}
	defer d.Close()
	for {
		ents, err := d.ReadDir(256)
		for _, de := range ents {
			name := de.Name()
			if name == ledgerName {
				continue
			}
			if id, ok := parseName(name); ok {
				if e := entries[id]; e != nil && !e.overflowing {
					continue
				}
			}
			rel := c.rel(name)
			if _, err := dataset.NeedIn(c.root, rel, false, false); err != nil {
				return err // a directory or a link here is not swath's
			}
			if err := c.verify(); err != nil {
				return err
			}
			if err := c.root.Remove(rel); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return c.unavailable(err)
			}
			c.st.Orphans.Add(1)
		}
		if err == io.EOF || len(ents) == 0 {
			break
		}
		if err != nil {
			return c.unavailable(err)
		}
	}
	return nil
}

// parseName is the file ID a durable file's name holds.
func parseName(name string) ([16]byte, bool) {
	var id [16]byte
	hexID, ok := strings.CutSuffix(name, ".spill")
	if !ok || len(hexID) != 32 {
		return id, false
	}
	b, err := hex.DecodeString(hexID)
	if err != nil {
		return id, false
	}
	copy(id[:], b)
	return id, true
}

// Rejection kinds.
type rejection uint8

const (
	accepted rejection = iota
	rejectMissing
	rejectCorrupt
	rejectForeign
)

// classify validates every entry's file and settles it: accepted, or
// retired with its file unlinked; the retirements are appended and synced
// before the unlinks, then the directory is synced.
func (c *Cache) classify(entries map[[16]byte]*entry) error {
	var keep []*Cert
	var retire []*Cert
	for _, e := range entries {
		if e.overflowing {
			c.st.RejectedBudget.Add(1)
			continue // unlinked by sweep; its record retired below
		}
		d := &Cert{id: e.id, name: fileName(e.id), cs: e.cs, ce: e.ce, ceOpen: e.ceOpen, captured: e.rows, last: e.last,
			bytes: e.bytes, crc: e.crc, certified: true, reused: true}
		why, err := c.validate(d)
		if err != nil {
			return err
		}
		switch why {
		case rejectMissing:
			c.st.RejectedMissing.Add(1)
		case rejectCorrupt:
			c.st.RejectedCorrupt.Add(1)
		case rejectForeign:
			c.st.RejectedForeign.Add(1)
		}
		switch {
		case why != accepted:
			retire = append(retire, d)
		case c.coveredLocked(d):
			retire = append(retire, d)
		default:
			keep = append(keep, d)
		}
	}
	for _, e := range entries {
		if e.overflowing {
			retire = append(retire, &Cert{id: e.id, name: fileName(e.id)})
		}
	}
	slices.SortFunc(keep, func(a, b *Cert) int { return strings.Compare(a.cs, b.cs) })
	// Disjointness: certificates of one run cannot overlap (I2); any that
	// do are all rejected (tests assert none is).
	overlap := make([]bool, len(keep))
	for i := 1; i < len(keep); i++ {
		if prev := keep[i-1]; prev.ceOpen || keep[i].cs < prev.ce {
			overlap[i-1], overlap[i] = true, true
		}
	}
	kept := keep[:0]
	for i, d := range keep {
		if overlap[i] {
			c.st.RejectedOverlap.Add(1)
			retire = append(retire, d)
			continue
		}
		kept = append(kept, d)
	}
	c.reused = kept
	return c.retireNow(retire)
}

// retireNow is recovery's (and Start's) retirement, before the flusher
// owns the ledger: the records appended and synced, then the unlinks, then
// the directory synced.
func (c *Cache) retireNow(ds []*Cert) error {
	if len(ds) == 0 {
		return nil
	}
	var buf []byte
	for _, d := range ds {
		buf = appendLedgerFrame(buf, ledgerRec{cert: d, retire: retireRecovery})
		c.st.RetiredRecovery.Add(1)
	}
	c.lockLedger()
	defer c.lmu.Unlock()
	if err := c.verify(); err != nil {
		return err
	}
	if err := faults.At("spill-recovery-retire"); err != nil {
		return err
	}
	if _, err := faults.Write(c.ledger, buf); err != nil {
		return c.unavailable(err)
	}
	if err := faults.Sync(c.ledger); err != nil {
		return c.unavailable(err)
	}
	c.lend += int64(len(buf))
	c.durable = c.lend
	c.hist += len(ds)
	for _, d := range ds {
		if err := c.verify(); err != nil {
			return err
		}
		if err := faults.At("spill-recovery-unlink"); err != nil {
			return err
		}
		if err := c.root.Remove(c.rel(d.name)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return c.unavailable(err)
		}
	}
	if err := dataset.SyncDirIn(c.root, DurableDir); err != nil {
		return c.unavailable(err)
	}
	return nil
}

// Start applies this attempt's caps to the accepted certificates (above
// cap slots, or past max retained bytes (0: none): the highest-keyed are
// retired first, rejected_budget), charges the rest, and starts the
// flusher, which owns the ledger from here; cancel is the attempt's, for a
// fatal flusher error. cap 0 rejects every accepted certificate and opens
// no slot (durable spill off for this attempt).
func (c *Cache) Start(cap int, max int64, cancel func(error)) error {
	if c == nil {
		return nil
	}
	var drop []*Cert
	var total int64
	for _, d := range c.reused {
		total += d.bytes
	}
	for len(c.reused) > 0 && (len(c.reused) > cap || (max > 0 && total > max)) {
		d := c.reused[len(c.reused)-1]
		c.reused = c.reused[:len(c.reused)-1]
		total -= d.bytes
		drop = append(drop, d)
		c.st.RejectedBudget.Add(1)
	}
	if err := c.retireNow(drop); err != nil {
		return err
	}
	c.mu.Lock()
	c.cap, c.cancel = cap, cancel
	c.slots = len(c.reused)
	c.hist += len(c.reused) // each one's retirement
	for _, d := range c.reused {
		d.slot = true
		c.charge(d, d.bytes)
		c.st.Accepted.Add(1)
		c.st.AcceptedBytes.Add(d.bytes)
	}
	c.started = true
	c.mu.Unlock()
	go c.flusher()
	return nil
}

// Gaps records the live gaps a resume lists (the complement of the
// accepted certificates above K, for the report).
func (c *Cache) Gaps(n int) {
	if c != nil {
		c.st.Gaps.Store(int64(n))
	}
}

// Reused is the number of accepted certificates.
func (c *Cache) Reused() int {
	if c == nil {
		return 0
	}
	return len(c.reused)
}

// RetainedBytes is the bytes of retained durable files (charged to the
// live spill budget).
func (c *Cache) RetainedBytes() int64 {
	if c == nil {
		return 0
	}
	return c.retained.Load()
}

// validate checks d's file against its record, without the row codec:
// a regular file of the recorded length and CRC-32C, every frame's CRC,
// the header (magic, version, run UUID, file ID, identity digest; its
// frame limits within the hard ones), every rows frame's claimed rows and
// decoded bytes within the limits (checked before allocation, zstd frames
// decoded under the bound), one capture start equal to the record's cs, and
// a certified trailer equal to the record, then the tail. It records the
// largest frame's rows and storage, replay's decode bounds. Rows' order and
// membership are rechecked during recovery replay.
func (c *Cache) validate(d *Cert) (rejection, error) {
	rel := c.rel(d.name)
	fi, err := dataset.NeedIn(c.root, rel, false, true)
	switch {
	case err != nil && errors.Is(err, dataset.ErrUnsafeLayout):
		return rejectCorrupt, nil
	case err != nil:
		return rejectMissing, nil
	case fi == nil:
		return rejectMissing, nil
	case fi.Size() != d.bytes:
		return rejectCorrupt, nil
	}
	f, err := dataset.OpenRegularIn(c.root, rel, os.O_RDONLY)
	if err != nil {
		return rejectMissing, nil
	}
	defer f.Close()
	h := crc32.New(spillCRC)
	r := bufio.NewReaderSize(io.TeeReader(f, h), 1<<16)
	v := validator{c: c, d: d, r: r, size: d.bytes}
	why := v.run()
	if why == accepted && h.Sum32() != d.crc {
		why = rejectCorrupt
	}
	return why, nil
}

// validator walks one file's frames.
type validator struct {
	c                  *Cache
	d                  *Cert
	r                  *bufio.Reader
	size, off          int64
	prior              uint32 // CRC of the bytes before the current frame
	running            uint32
	dec                *zstd.Decoder
	maxRows, maxDecode uint64
}

func (v *validator) frame() (byte, []byte, bool) {
	var hdr [8]byte
	if _, err := io.ReadFull(v.r, hdr[:]); err != nil {
		return 0, nil, false
	}
	n := binary.LittleEndian.Uint32(hdr[:4])
	if n == 0 || n > maxSpillFrame || int64(n) > v.size-v.off-8 { // before allocating
		return 0, nil, false
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(v.r, payload); err != nil || !check2(hdr, payload) {
		return 0, nil, false
	}
	v.prior = v.running
	v.running = crc32.Update(crc32.Update(v.running, spillCRC, hdr[:]), spillCRC, payload)
	v.off += 8 + int64(n)
	return payload[0], payload[1:], true
}

func (v *validator) run() rejection {
	defer func() {
		if v.dec != nil {
			v.dec.Close()
		}
	}()
	typ, body, ok := v.frame()
	if !ok || typ != frameHeader {
		return rejectCorrupt
	}
	if why := v.header(body); why != accepted {
		return why
	}
	var rows, captured int64
	capture := false
	for {
		start := v.off
		typ, body, ok := v.frame()
		if !ok {
			return rejectCorrupt
		}
		switch typ {
		case frameCapture:
			dd := schema.NewDecoder(body)
			cs := dd.Str()
			if capture || dd.Err() != nil || dd.Len() != 0 || cs != v.d.cs {
				return rejectCorrupt
			}
			capture = true
		case frameRows, frameRowsZstd, frameDelta, frameDeltaZstd:
			n, owned, ok := v.claims(typ, body)
			if !ok {
				return rejectCorrupt
			}
			rows += n
			if capture {
				captured += n
			}
			v.d.most, v.d.owned = max(v.d.most, n), max(v.d.owned, owned)
		case frameTrailer:
			t, ok := parseTrailer(body)
			d := v.d
			if !ok || t.off != start || t.prior != v.prior || !t.certified || !t.capture || !capture ||
				t.rows != rows || t.captured != captured || t.captured != d.captured || t.cs != d.cs || t.ce != d.ce || t.open != d.ceOpen || t.last != d.last {
				return rejectCorrupt
			}
			d.rows, d.first = rows, t.first
			var tl [tailSize]byte
			if _, err := io.ReadFull(v.r, tl[:]); err != nil || int64(binary.LittleEndian.Uint64(tl[:8])) != start || string(tl[8:]) != durableMagic {
				return rejectCorrupt
			}
			if _, err := v.r.ReadByte(); err != io.EOF {
				return rejectCorrupt
			}
			return accepted
		default:
			return rejectCorrupt
		}
	}
}

// header checks the H body.
func (v *validator) header(body []byte) rejection {
	if len(body) < len(durableMagic)+1+32 || string(body[:len(durableMagic)]) != durableMagic || body[len(durableMagic)] != durableVersion {
		return rejectCorrupt
	}
	b := body[len(durableMagic)+1:]
	if !bytes.Equal(b[:16], v.c.uuid[:]) || !bytes.Equal(b[16:32], v.d.id[:]) {
		return rejectForeign
	}
	dd := schema.NewDecoder(b[32:])
	ident, codec := dd.Str(), strings.Clone(dd.Str())
	maxRows, maxDecode := dd.Uvarint(), dd.Uvarint()
	lo, hi := strings.Clone(dd.Str()), strings.Clone(dd.Str())
	open := dd.Byte()
	if dd.Err() != nil || dd.Len() != 0 || open > 1 {
		return rejectCorrupt
	}
	if ident != v.c.ident {
		return rejectForeign
	}
	if maxRows == 0 || maxRows > publishRows || maxDecode == 0 || maxDecode > maxSpillFrame {
		return rejectCorrupt // a limit above the hard one rejects the file
	}
	v.maxRows, v.maxDecode = maxRows, min(maxDecode, maxSpillDecoded)
	v.d.lo, v.d.hi, v.d.open = lo, hi, open == 1
	v.d.codec = codec
	return accepted
}

// claims are a rows frame's claimed rows and declared storage, checked
// against the decode bounds before anything sized by them is allocated
// (a zstd frame is decoded under the bound to read them).
func (v *validator) claims(typ byte, body []byte) (int64, int64, bool) {
	if typ == frameRowsZstd || typ == frameDeltaZstd {
		if v.dec == nil {
			var err error
			if v.dec, err = zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(v.maxDecode)); err != nil {
				return 0, 0, false
			}
		}
		out, err := v.dec.DecodeAll(body, nil)
		if err != nil {
			return 0, 0, false
		}
		body = out
	}
	if uint64(len(body)) > v.maxDecode {
		return 0, 0, false
	}
	n, k := binary.Uvarint(body)
	if k <= 0 || n > v.maxRows || n > uint64(len(body)) {
		return 0, 0, false
	}
	owned := uint64(len(body))
	if typ == frameDelta || typ == frameDeltaZstd {
		o, k2 := binary.Uvarint(body[k:])
		if k2 <= 0 || o > v.maxDecode {
			return 0, 0, false
		}
		owned = o
	}
	return int64(n), int64(owned), true
}
