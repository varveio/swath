// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package objfile

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"strconv"
	"sync"
	"sync/atomic"
)

// ErrInvalidSnapshot reports a snapshot that Restore cannot take: malformed,
// of another version, or of another scope.
var ErrInvalidSnapshot = errors.New("objfile: invalid pin snapshot")

// snapshotVersion is the version of the snapshot format Snapshot writes and
// Restore reads.
const snapshotVersion = 1

// Pinned is an Opener's objects pinned across opens: the first Open of a name
// (or first read of an OpenSized Object, or ReadObject) records the object's
// size and Identity, and every later one of that name must find the same, or
// fails with ErrObjectChanged. A recorded pin is never
// replaced. Pinned re-proves the current object; it never reads an older
// version by its recorded identity. It is safe for concurrent use.
type Pinned struct {
	o      Opener
	scope  string
	sealed bool

	mu   sync.Mutex
	pins map[string]pin
}

type pin struct {
	Size     int64  `json:"size"`
	Identity string `json:"identity"`
}

// Pin pins the objects o opens. Scope names o's storage namespace (provider,
// bucket or account and container, custom endpoint, root prefix); a snapshot
// is restored only for the same scope.
func Pin(o Opener, scope string) *Pinned {
	return &Pinned{o: o, scope: scope, pins: map[string]pin{}}
}

// Restore returns the Pinned that snapshot records, sealed: it opens only the
// names the snapshot holds, and any other name fails with an error wrapping
// fs.ErrNotExist without a request. A snapshot of another scope, of another
// version or malformed fails with ErrInvalidSnapshot.
func Restore(o Opener, scope string, snapshot []byte) (*Pinned, error) {
	pins, err := decodeSnapshot(snapshot, scope)
	if err != nil {
		return nil, err
	}
	return &Pinned{o: o, scope: scope, sealed: true, pins: pins}, nil
}

// Open opens name, a path CheckPath accepts, and checks it against its pin:
// the first successful Open records the object's size and Identity; a later
// one, or one that loses a race to record, must find the same, or it closes
// the object and fails with ErrObjectChanged. A recorded object that is gone
// also fails with ErrObjectChanged.
func (p *Pinned) Open(ctx context.Context, name string) (Object, error) {
	if err := CheckPath(name); err != nil {
		return nil, err
	}
	p.mu.Lock()
	_, recorded := p.pins[name]
	p.mu.Unlock()
	if p.sealed && !recorded {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fmt.Errorf("%w: not in the pin snapshot", fs.ErrNotExist)}
	}
	f, err := p.o.Open(ctx, name)
	if err != nil {
		if recorded && errors.Is(err, fs.ErrNotExist) {
			err = fmt.Errorf("%w: %w", ErrObjectChanged, err)
		}
		return nil, err
	}
	if err := p.check(name, "open", pin{Size: f.Size(), Identity: f.Identity()}); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

// OpenSized opens name, a path CheckPath accepts, of size bytes, without a
// request (Opener.OpenSized), and checks it against its pin at its first
// read: a recorded pin of another size fails at once with ErrObjectChanged;
// otherwise the first read that pins the Object records its size and
// Identity, or must find the recorded ones, else it fails with
// ErrObjectChanged and returns none of the bytes it read. Until then the
// Object's reads go to the store, so the current object is re-proved as
// Open re-proves it. A size other than the object's, found before any pin
// is recorded, is a *SizeMismatchError; after, ErrObjectChanged. Size 0,
// which no read reaches the store for, is Open, and checks that the object
// is empty; a negative size is invalid.
func (p *Pinned) OpenSized(ctx context.Context, name string, size int64) (Object, error) {
	if err := CheckPath(name); err != nil {
		return nil, err
	}
	if size < 0 {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fmt.Errorf("%w: negative size %d", fs.ErrInvalid, size)}
	}
	p.mu.Lock()
	want, recorded := p.pins[name]
	p.mu.Unlock()
	if p.sealed && !recorded {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fmt.Errorf("%w: not in the pin snapshot", fs.ErrNotExist)}
	}
	if recorded && want.Size != size {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fmt.Errorf("%w: opened as %d bytes, pinned %d bytes at %q",
			ErrObjectChanged, size, want.Size, want.Identity)}
	}
	if size == 0 {
		f, err := p.Open(ctx, name) // checked against a recorded pin
		if err == nil && f.Size() != 0 {
			f.Close()
			return nil, &fs.PathError{Op: "open", Path: name, Err: &SizeMismatchError{Name: name, Expected: 0, Actual: f.Size()}}
		}
		return f, err
	}
	f, err := p.o.OpenSized(ctx, name, size)
	if err != nil {
		return nil, err
	}
	return &pinnedObject{Object: f, p: p, name: name, recorded: recorded}, nil
}

// ReadObject reads name, a path CheckPath accepts, whole (ObjectReader)
// and checks it against its pin as Open does: the first read records the
// object's size and Identity, and a later one must find the same. An
// object over limit is ErrTooLarge, or ErrObjectChanged when its Identity
// is not the recorded one; it records nothing.
func (p *Pinned) ReadObject(ctx context.Context, name string, limit int64) ([]byte, string, error) {
	if err := CheckPath(name); err != nil {
		return nil, "", err
	}
	p.mu.Lock()
	_, recorded := p.pins[name]
	p.mu.Unlock()
	if p.sealed && !recorded {
		return nil, "", &fs.PathError{Op: "read", Path: name, Err: fmt.Errorf("%w: not in the pin snapshot", fs.ErrNotExist)}
	}
	b, id, err := ReadObject(ctx, p.o, name, limit)
	switch {
	case errors.Is(err, ErrTooLarge):
		p.mu.Lock()
		want, recorded := p.pins[name]
		p.mu.Unlock()
		if recorded && (id != want.Identity || want.Size <= limit) {
			return nil, "", &fs.PathError{Op: "read", Path: name, Err: fmt.Errorf("%w: more than %d bytes at %q, pinned %d bytes at %q",
				ErrObjectChanged, limit, id, want.Size, want.Identity)}
		}
		return nil, id, err
	case err != nil:
		if recorded && errors.Is(err, fs.ErrNotExist) {
			err = fmt.Errorf("%w: %w", ErrObjectChanged, err)
		}
		return nil, "", err
	}
	if err := p.check(name, "read", pin{Size: int64(len(b)), Identity: id}); err != nil {
		return nil, "", err
	}
	return b, id, nil
}

// check records got as name's pin when none is, and otherwise fails with
// ErrObjectChanged unless got is the recorded pin.
func (p *Pinned) check(name, op string, got pin) error {
	if got.Identity == "" {
		return &fs.PathError{Op: op, Path: name, Err: errors.New("the object has no identity to pin")}
	}
	p.mu.Lock()
	want, recorded := p.pins[name]
	if !recorded {
		p.pins[name] = got
		want = got
	}
	p.mu.Unlock()
	if got != want {
		return &fs.PathError{Op: op, Path: name, Err: fmt.Errorf("%w: %d bytes at %q, pinned %d bytes at %q",
			ErrObjectChanged, got.Size, got.Identity, want.Size, want.Identity)}
	}
	return nil
}

// pinnedObject is an Object of Pinned.OpenSized: its reads are checked
// against the pin until one has pinned the Object and matched it.
type pinnedObject struct {
	Object
	p        *Pinned
	name     string
	recorded bool // the name had a pin when opened
	checked  atomic.Bool
}

// confirm checks the Object against its pin once a read, which failed with
// err (or nil), has pinned it: it returns the error the read is to fail
// with instead, after whether there is one. A read that failed before pinning
// (Identity still "") is not checked: it returned no bytes. A missing object
// of a recorded pin is ErrObjectChanged, as is a size other than a recorded
// pin's (a *SizeMismatchError of the read, which is not kept).
func (f *pinnedObject) confirm(err error) (bool, error) {
	if f.checked.Load() {
		return false, nil
	}
	if err != nil && f.recorded && errors.Is(err, fs.ErrNotExist) {
		return true, fmt.Errorf("%w: %w", ErrObjectChanged, err)
	}
	var sm *SizeMismatchError
	if err != nil && errors.As(err, &sm) {
		f.p.mu.Lock()
		want, recorded := f.p.pins[f.name]
		f.p.mu.Unlock()
		if recorded {
			return true, &fs.PathError{Op: "read", Path: f.name, Err: fmt.Errorf("%w: %d bytes, pinned %d bytes at %q",
				ErrObjectChanged, sm.Actual, want.Size, want.Identity)}
		}
	}
	if f.Identity() == "" {
		return false, nil
	}
	if cerr := f.p.check(f.name, "read", pin{Size: f.Size(), Identity: f.Identity()}); cerr != nil {
		return true, cerr
	}
	f.checked.Store(true)
	return false, nil
}

func (f *pinnedObject) ReadAt(p []byte, off int64) (int, error) {
	return f.readAt(context.Background(), p, off)
}

func (f *pinnedObject) WithContext(ctx context.Context) io.ReaderAt {
	return readerFunc(func(p []byte, off int64) (int, error) { return f.readAt(ctx, p, off) })
}

func (f *pinnedObject) readAt(ctx context.Context, p []byte, off int64) (int, error) {
	n, err := f.Object.WithContext(ctx).ReadAt(p, off)
	if replaced, cerr := f.confirm(err); replaced {
		return 0, cerr
	}
	return n, err
}

func (f *pinnedObject) ReadRanges(ctx context.Context, ranges []Range) error {
	err := ReadRanges(ctx, f.Object, ranges)
	if replaced, cerr := f.confirm(err); replaced {
		return cerr
	}
	return err
}

func (f *pinnedObject) batchPolicy() BatchPolicy { return policyOf(f.Object) }

// Snapshot returns the pins recorded so far as versioned JSON,
// {"version":1,"scope":...,"objects":{name:{"size":n,"identity":s}}}, with
// members in a fixed order and names sorted, so that equal pins give equal
// bytes. It records the objects opened, not which objects a dataset needs:
// that a snapshot holds every object a reader requires is established by
// the reader that opened them.
func (p *Pinned) Snapshot() ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return json.Marshal(struct {
		Version int            `json:"version"`
		Scope   string         `json:"scope"`
		Objects map[string]pin `json:"objects"`
	}{snapshotVersion, p.scope, p.pins})
}

// decodeSnapshot reads a snapshot strictly: exactly the members Snapshot
// writes, each once, names CheckPath accepts, sizes of at least 0 and
// identities non-empty without control characters.
func decodeSnapshot(b []byte, scope string) (map[string]pin, error) {
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	var (
		version, gotScope bool
		pins              map[string]pin
	)
	err := members(d, func(key string) error {
		switch key {
		case "version":
			n, err := integer(d)
			if err != nil {
				return err
			}
			if n != snapshotVersion {
				return fmt.Errorf("version %d, want %d", n, snapshotVersion)
			}
			version = true
		case "scope":
			s, err := text(d)
			if err != nil {
				return err
			}
			if s != scope {
				return fmt.Errorf("scope %q, want %q", s, scope)
			}
			gotScope = true
		case "objects":
			pins = map[string]pin{}
			return members(d, func(name string) error {
				if err := CheckPath(name); err != nil {
					return err
				}
				pn, err := decodePin(d)
				if err != nil {
					return fmt.Errorf("%q: %w", name, err)
				}
				pins[name] = pn
				return nil
			})
		default:
			return fmt.Errorf("unknown member %q", key)
		}
		return nil
	})
	if err == nil {
		if _, end := d.Token(); end != io.EOF {
			err = errors.New("data after the snapshot")
		}
	}
	if err == nil && (!version || !gotScope || pins == nil) {
		err = errors.New("missing version, scope or objects")
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidSnapshot, err)
	}
	return pins, nil
}

func decodePin(d *json.Decoder) (pin, error) {
	var (
		pn             pin
		size, identity bool
	)
	err := members(d, func(key string) error {
		var err error
		switch key {
		case "size":
			pn.Size, err = integer(d)
			size = err == nil
		case "identity":
			pn.Identity, err = text(d)
			identity = err == nil
		default:
			return fmt.Errorf("unknown member %q", key)
		}
		return err
	})
	switch {
	case err != nil:
		return pin{}, err
	case !size || !identity:
		return pin{}, errors.New("missing size or identity")
	case pn.Size < 0:
		return pin{}, fmt.Errorf("negative size %d", pn.Size)
	case pn.Identity == "":
		return pin{}, errors.New("empty identity")
	}
	for _, r := range pn.Identity {
		if r < 0x20 || r == 0x7f {
			return pin{}, errors.New("identity contains a control character")
		}
	}
	return pn, nil
}

// members reads a JSON object from d, calling member with each key, once
// per key, to read its value.
func members(d *json.Decoder, member func(key string) error) error {
	if t, err := d.Token(); err != nil {
		return err
	} else if t != json.Delim('{') {
		return fmt.Errorf("%v where an object was expected", t)
	}
	seen := map[string]bool{}
	for d.More() {
		t, err := d.Token()
		if err != nil {
			return err
		}
		key := t.(string) // the decoder reads only string keys in an object
		if seen[key] {
			return fmt.Errorf("duplicate member %q", key)
		}
		seen[key] = true
		if err := member(key); err != nil {
			return err
		}
	}
	_, err := d.Token() // the closing brace
	return err
}

func integer(d *json.Decoder) (int64, error) {
	t, err := d.Token()
	if err != nil {
		return 0, err
	}
	n, ok := t.(json.Number)
	if !ok {
		return 0, fmt.Errorf("%v where an integer was expected", t)
	}
	return strconv.ParseInt(string(n), 10, 64)
}

func text(d *json.Decoder) (string, error) {
	t, err := d.Token()
	if err != nil {
		return "", err
	}
	s, ok := t.(string)
	if !ok {
		return "", fmt.Errorf("%v where a string was expected", t)
	}
	return s, nil
}
