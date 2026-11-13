// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Command s3fill fills an S3-compatible bucket (MinIO, for the durable
// spill campaign) from a sorted key file: one empty object per line,
// written by parallel PUTs. It is idempotent: the bucket is created if
// absent, and a fill that completed records the key file's SHA-256 in the
// marker bucket (BUCKET-fill), so a second run with the same file returns
// at once. Keys the service refuses are counted and printed, never fatal:
// the campaign's oracle is a listing of the bucket, not the file.
// Credentials come from AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.
//
//	go run ./bench/s3fill -endpoint http://127.0.0.1:9000 -bucket swath-durable -keys keys.txt
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
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

func main() {
	endpoint := flag.String("endpoint", "http://127.0.0.1:9000", "the S3 endpoint")
	bucket := flag.String("bucket", "swath-durable", "the bucket to fill")
	keys := flag.String("keys", "", "the sorted key file, one key per line")
	workers := flag.Int("workers", 64, "parallel PUTs")
	flag.Parse()
	if *keys == "" {
		log.Fatal("s3fill: -keys is required")
	}
	ctx := context.Background()
	sum, err := fileSHA(*keys)
	if err != nil {
		log.Fatal(err)
	}
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("us-east-1"))
	if err != nil {
		log.Fatal(err)
	}
	c := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint, o.UsePathStyle = aws.String(*endpoint), true
	})
	marker := *bucket + "-fill"
	for _, b := range []string{*bucket, marker} {
		if _, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(b)}); err != nil && !owned(err) {
			log.Fatalf("create %s: %v", b, err)
		}
	}
	if _, err := c.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(marker), Key: aws.String(sum)}); err == nil {
		fmt.Printf("%s already filled from %s (sha256 %s)\n", *bucket, *keys, sum)
		return
	}
	f, err := os.Open(*keys)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	start := time.Now()
	work := make(chan string, 4**workers)
	var put, refused atomic.Int64
	var wg sync.WaitGroup
	for range *workers {
		wg.Go(func() {
			for k := range work {
				_, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(*bucket), Key: aws.String(k), Body: bytes.NewReader(nil)})
				var api smithy.APIError
				switch {
				case err == nil:
					put.Add(1)
				case errors.As(err, &api):
					if refused.Add(1) <= 10 {
						fmt.Printf("refused %q: %s\n", k, api.ErrorCode())
					}
				default:
					log.Fatalf("put %q: %v", k, err)
				}
			}
		})
	}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		if k := sc.Text(); k != "" {
			work <- k
		}
	}
	close(work)
	wg.Wait()
	if err := sc.Err(); err != nil {
		log.Fatal(err)
	}
	if _, err := c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(marker), Key: aws.String(sum), Body: bytes.NewReader(nil)}); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s: %d objects put, %d refused, in %s\n", *bucket, put.Load(), refused.Load(), time.Since(start).Round(time.Second))
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
