// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Command s3fill fills an S3-compatible bucket (MinIO, for the durable
// spill campaign; Cloudflare R2) from a sorted key file: one empty object
// per line, written by parallel PUTs. It is idempotent: a fill that
// completed records the key file's SHA-256 in a marker, so a second run
// with the same file returns at once. By default the bucket is created if
// absent and the marker is an object named by the SHA-256 in the marker
// bucket BUCKET-fill. On a store whose keys may not create buckets (an R2
// token scoped to one bucket), -create=false creates none and
// -marker-prefix P makes the marker the key P<sha256> in the bucket itself.
// Keys the service refuses are counted and printed, never fatal: the
// campaign's oracle is a listing of the bucket, not the file. The last line
// counts every HTTP request sent, retries included.
// Credentials come from AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.
//
//	go run ./bench/s3fill -endpoint http://127.0.0.1:9000 -bucket swath-durable -keys keys.txt
//	go run ./bench/s3fill -endpoint https://ACCOUNT.r2.cloudflarestorage.com -region auto \
//	  -create=false -marker-prefix _fill/ -bucket swath-test -keys keys.txt
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// fill is one run's settings.
type fill struct {
	endpoint, region, bucket, keys string
	workers                        int
	create                         bool   // create the bucket (and the marker bucket) if absent
	markerPrefix                   string // "": the marker is in BUCKET-fill; else the key markerPrefix+sha256 in the bucket
}

func main() {
	var f fill
	flag.StringVar(&f.endpoint, "endpoint", "http://127.0.0.1:9000", "the S3 endpoint")
	flag.StringVar(&f.region, "region", "us-east-1", "the region requests are signed for (R2: auto)")
	flag.StringVar(&f.bucket, "bucket", "swath-durable", "the bucket to fill")
	flag.StringVar(&f.keys, "keys", "", "the sorted key file, one key per line")
	flag.IntVar(&f.workers, "workers", 64, "parallel PUTs")
	flag.BoolVar(&f.create, "create", true, "create the bucket, and the marker bucket, if absent")
	flag.StringVar(&f.markerPrefix, "marker-prefix", "", "record the completed fill as the key PREFIX<sha256> in the bucket, not in the bucket BUCKET-fill")
	flag.Parse()
	if f.keys == "" {
		log.Fatal("s3fill: -keys is required")
	}
	if err := f.run(context.Background(), os.Stdout); err != nil {
		log.Fatal(err)
	}
}

// marker is the bucket and key that record a completed fill of the key
// file whose SHA-256 is sum.
func (f fill) marker(sum string) (bucket, key string) {
	if f.markerPrefix != "" {
		return f.bucket, f.markerPrefix + sum
	}
	return f.bucket + "-fill", sum
}

func (f fill) run(ctx context.Context, out io.Writer) error {
	sum, err := fileSHA(f.keys)
	if err != nil {
		return err
	}
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(f.region))
	if err != nil {
		return err
	}
	var sent atomic.Int64
	defer func() { fmt.Fprintf(out, "requests sent: %d\n", sent.Load()) }()
	c := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint, o.UsePathStyle = aws.String(f.endpoint), true
		o.HTTPClient = &http.Client{Transport: counting{http.DefaultTransport, &sent}}
	})
	mb, mk := f.marker(sum)
	if f.create {
		for _, b := range []string{f.bucket, mb} {
			if _, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(b)}); err != nil && !owned(err) {
				return fmt.Errorf("create %s: %w", b, err)
			}
		}
	}
	if _, err := c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(mb), Key: aws.String(mk)}); err == nil {
		fmt.Fprintf(out, "%s already filled from %s (sha256 %s)\n", f.bucket, f.keys, sum)
		return nil
	}
	kf, err := os.Open(f.keys)
	if err != nil {
		return err
	}
	defer kf.Close()
	start := time.Now()
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	work := make(chan string, 4*f.workers)
	var put, refused atomic.Int64
	var wg sync.WaitGroup
	for range f.workers {
		wg.Go(func() {
			for k := range work {
				_, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(f.bucket), Key: aws.String(k), Body: bytes.NewReader(nil)})
				var api smithy.APIError
				switch {
				case err == nil:
					put.Add(1)
				case ctx.Err() != nil:
				case errors.As(err, &api):
					if refused.Add(1) <= 10 {
						fmt.Fprintf(out, "refused %q: %s\n", k, api.ErrorCode())
					}
				default:
					cancel(fmt.Errorf("put %q: %w", k, err))
				}
			}
		})
	}
	sc := bufio.NewScanner(kf)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() && ctx.Err() == nil {
		if k := sc.Text(); k != "" {
			work <- k
		}
	}
	close(work)
	wg.Wait()
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if err := sc.Err(); err != nil {
		return err
	}
	if _, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(mb), Key: aws.String(mk), Body: bytes.NewReader(nil)}); err != nil {
		return err
	}
	fmt.Fprintf(out, "%s: %d objects put, %d refused, in %s\n", f.bucket, put.Load(), refused.Load(), time.Since(start).Round(time.Second))
	return nil
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

// owned: the bucket exists already and is ours.
func owned(err error) bool {
	var api smithy.APIError
	return errors.As(err, &api) && (api.ErrorCode() == "BucketAlreadyOwnedByYou" || api.ErrorCode() == "BucketAlreadyExists")
}

func fileSHA(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
