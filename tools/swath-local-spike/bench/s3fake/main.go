// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Command s3fake serves an in-memory S3 bucket (gofakes3) loaded from a TSV
// of key<TAB>mtime-microseconds, for benchmarks without credentials. Each
// object's body is its key, so sizes are the key length and ETags differ.
//
//	go run ./bench/s3fake -keys keys.tsv -bucket bench -addr 127.0.0.1:9000
package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
)

// clock returns the modification time set for the next put.
type clock struct{ t time.Time }

func (c *clock) Now() time.Time                  { return c.t }
func (c *clock) Since(t time.Time) time.Duration { return c.t.Sub(t) }

func main() {
	keys := flag.String("keys", "", "TSV of key and mtime (microseconds)")
	bucket := flag.String("bucket", "bench", "bucket name")
	addr := flag.String("addr", "127.0.0.1:9000", "listen address")
	flag.Parse()
	c := &clock{}
	b := s3mem.New(s3mem.WithTimeSource(c))
	if err := b.CreateBucket(*bucket); err != nil {
		log.Fatal(err)
	}
	f, err := os.Open(*keys)
	if err != nil {
		log.Fatal(err)
	}
	start, n := time.Now(), 0
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		key, mt, _ := strings.Cut(sc.Text(), "\t")
		us, _ := strconv.ParseInt(mt, 10, 64)
		c.t = time.UnixMicro(us).UTC()
		if _, err := b.PutObject(*bucket, key, nil, bytes.NewReader([]byte(key)), int64(len(key)), nil); err != nil {
			log.Fatal(err)
		}
		n++
	}
	if err := sc.Err(); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("loaded %d objects in %s; serving s3://%s on http://%s\n", n, time.Since(start).Round(time.Millisecond), *bucket, *addr)
	log.Fatal(http.ListenAndServe(*addr, gofakes3.New(b).Server()))
}
