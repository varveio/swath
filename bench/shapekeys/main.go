// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Command shapekeys writes one bucket shape's keys (package internal/shapes)
// at a scale, under a prefix, one per line in byte order: the key file
// bench/s3fill fills a bucket from, and the oracle a listing of that prefix
// is compared with. The prefix is added the same way to every key, so the
// file stays sorted. A key longer than S3's 1,024 bytes is refused; with
// -overlay the prefix replaces the keys' first bytes instead of preceding
// them, which keeps a shape's key lengths (maxlen's 1,024 bytes) and is
// allowed only when every key shares the bytes it replaces.
//
//	go run ./bench/shapekeys -shape baseline -scale 10000 -prefix r2test/baseline/ -out baseline.keys
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/varveio/swath/internal/shapes"
)

// maxKey is S3's key limit in bytes.
const maxKey = 1024

func main() {
	shape := flag.String("shape", "", "the shape's name (internal/shapes)")
	scale := flag.Int("scale", 10_000, "the shape's scale (about this many keys)")
	prefix := flag.String("prefix", "", "the prefix of every key")
	overlay := flag.Bool("overlay", false, "the prefix replaces the keys' first bytes, which every key must share")
	out := flag.String("out", "", "the key file to write")
	flag.Parse()
	s, ok := shapes.Lookup(*shape)
	if !ok || *out == "" {
		log.Fatalf("shapekeys: -shape (one of internal/shapes) and -out are required")
	}
	keys, err := prefixed(s.Keys(*scale), *prefix, *overlay)
	if err != nil {
		log.Fatalf("shapekeys: %s: %v", *shape, err)
	}
	f, err := os.Create(*out)
	if err != nil {
		log.Fatal(err)
	}
	if err := write(f, keys); err != nil {
		log.Fatal(err)
	}
	if err := f.Close(); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s: %d keys at scale %d under %q in %s\n", *shape, len(keys), *scale, *prefix, *out)
}

// prefixed puts prefix on each of the sorted keys: before it, or with
// overlay over its first len(prefix) bytes, which every key must share so
// that the order holds. A key it makes longer than maxKey is an error.
func prefixed(keys []string, prefix string, overlay bool) ([]string, error) {
	out := make([]string, len(keys))
	for i, k := range keys {
		if overlay {
			if len(keys[0]) < len(prefix) || !strings.HasPrefix(k, keys[0][:len(prefix)]) {
				return nil, fmt.Errorf("-overlay: %q does not share the first %d bytes of %q", k, len(prefix), keys[0])
			}
			k = k[len(prefix):]
		}
		out[i] = prefix + k
		if len(out[i]) > maxKey {
			return nil, fmt.Errorf("%q is %d bytes under the prefix, more than %d", k, len(out[i]), maxKey)
		}
	}
	return out, nil
}

// write puts each key on its own line.
func write(w io.Writer, keys []string) error {
	b := bufio.NewWriter(w)
	for _, k := range keys {
		if strings.ContainsAny(k, "\r\n") {
			return errors.New("shapekeys: a key holds a line break")
		}
		b.WriteString(k)
		b.WriteByte('\n')
	}
	return b.Flush()
}
