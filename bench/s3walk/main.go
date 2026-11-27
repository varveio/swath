// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Command s3walk lists a prefix of an S3-compatible bucket the plain way:
// one ListObjectsV2 request after another from the first key (or
// -start-after), following continuation tokens, with encoding-type=url. It
// is the independent listing a store's answers to swath are checked against
// (docs/sources/s3.md, "S3-compatible stores"), and uses none of swath's
// code. It writes the keys one per line, in the order the store sent them,
// and prints the pages, the keys, the smallest and largest page that was
// not the last, request latencies and every HTTP request sent.
//
//	go run ./bench/s3walk -endpoint https://ACCOUNT.r2.cloudflarestorage.com -region auto \
//	  -bucket swath-test -prefix r2test/baseline/ -out walk.keys
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func main() {
	endpoint := flag.String("endpoint", "", "the S3 endpoint")
	region := flag.String("region", "auto", "the region requests are signed for")
	bucket := flag.String("bucket", "", "the bucket")
	prefix := flag.String("prefix", "", "the prefix to list")
	maxKeys := flag.Int("max-keys", 1000, "max-keys of each request")
	startAfter := flag.String("start-after", "", "list the keys after this one")
	out := flag.String("out", "", "the file the keys are written to")
	flag.Parse()
	if *endpoint == "" || *bucket == "" || *out == "" {
		log.Fatal("s3walk: -endpoint, -bucket and -out are required")
	}
	ctx := context.Background()
	c, sent, err := client(ctx, *endpoint, *region)
	if err != nil {
		log.Fatal(err)
	}
	f, err := os.Create(*out)
	if err != nil {
		log.Fatal(err)
	}
	w := walker{c: c, bucket: *bucket, prefix: *prefix, startAfter: *startAfter, maxKeys: *maxKeys}
	start := time.Now()
	st, err := w.walk(ctx, f)
	if err != nil {
		log.Fatal(err)
	}
	if err := f.Close(); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s: %s in %s; requests sent %d\n", *prefix, st, time.Since(start).Round(time.Millisecond), sent.Load())
}

// client is an S3 client for endpoint, path-style, signing for region,
// and the count of the HTTP requests it sends.
func client(ctx context.Context, endpoint, region string) (*s3.Client, *atomic.Int64, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, nil, err
	}
	sent := new(atomic.Int64)
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint, o.UsePathStyle = aws.String(endpoint), true
		o.HTTPClient = &http.Client{Transport: counting{http.DefaultTransport, sent}}
	}), sent, nil
}

// walker is one walk's settings.
type walker struct {
	c                          *s3.Client
	bucket, prefix, startAfter string
	maxKeys                    int
}

// stats is what one walk saw.
type stats struct {
	keys, pages int
	raw         int   // pages not URL-encoded although asked to be
	sizes       []int // the sizes of the pages that were not the last
	lat         []time.Duration
}

func (s stats) String() string {
	full := "none"
	if len(s.sizes) > 0 {
		full = fmt.Sprintf("%d..%d", slices.Min(s.sizes), slices.Max(s.sizes))
	}
	lat := slices.Sorted(slices.Values(s.lat))
	return fmt.Sprintf("%d keys in %d pages (non-final page sizes %s; %d not URL-encoded); latency p50 %s p90 %s max %s",
		s.keys, s.pages, full, s.raw, lat[len(lat)/2].Round(time.Millisecond),
		lat[len(lat)*9/10].Round(time.Millisecond), lat[len(lat)-1].Round(time.Millisecond))
}

// walk lists the prefix from its first key (or after startAfter) to its
// last and writes each key on its own line, as the store sent them.
func (w walker) walk(ctx context.Context, out io.Writer) (stats, error) {
	b := bufio.NewWriter(out)
	in := &s3.ListObjectsV2Input{Bucket: aws.String(w.bucket), Prefix: aws.String(w.prefix),
		MaxKeys: aws.Int32(int32(w.maxKeys)), EncodingType: types.EncodingTypeUrl}
	if w.startAfter != "" {
		in.StartAfter = aws.String(w.startAfter)
	}
	var st stats
	for {
		t := time.Now()
		p, err := w.c.ListObjectsV2(ctx, in)
		if err != nil {
			return st, fmt.Errorf("page %d: %w", st.pages+1, err)
		}
		st.lat = append(st.lat, time.Since(t))
		st.pages++
		if p.EncodingType != types.EncodingTypeUrl {
			st.raw++
		}
		for _, o := range p.Contents {
			k := aws.ToString(o.Key)
			if p.EncodingType == types.EncodingTypeUrl {
				if k, err = url.QueryUnescape(k); err != nil {
					return st, fmt.Errorf("page %d: key %q: %w", st.pages, aws.ToString(o.Key), err)
				}
			}
			if strings.ContainsAny(k, "\r\n") {
				return st, fmt.Errorf("key %q holds a line break", k)
			}
			b.WriteString(k)
			b.WriteByte('\n')
		}
		st.keys += len(p.Contents)
		if !aws.ToBool(p.IsTruncated) {
			return st, b.Flush()
		}
		if p.NextContinuationToken == nil {
			return st, errors.New("a truncated page without a continuation token")
		}
		st.sizes = append(st.sizes, len(p.Contents))
		in.ContinuationToken = p.NextContinuationToken
	}
}

// counting counts the requests it sends.
type counting struct {
	http.RoundTripper
	n *atomic.Int64
}

func (c counting) RoundTrip(r *http.Request) (*http.Response, error) {
	c.n.Add(1)
	return c.RoundTripper.RoundTrip(r)
}
