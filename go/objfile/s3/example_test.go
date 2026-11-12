// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http/httptest"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/varveio/swath/go/objfile"
	"github.com/varveio/swath/go/objfile/s3"
	"github.com/varveio/swath/go/objfile/s3/s3fake"
)

// fakeClient is an SDK client for the in-process fake bucket "bkt". A
// program passes its own client, made with config.LoadDefaultConfig.
func fakeClient(srv *s3fake.Server) (*awss3.Client, func()) {
	hs := httptest.NewServer(srv)
	return awss3.New(awss3.Options{
		BaseEndpoint: aws.String(hs.URL),
		UsePathStyle: true,
		Region:       "us-east-1",
		Credentials:  aws.AnonymousCredentials{},
	}), hs.Close
}

// Open an object, read two byte ranges of it in one batch, and see a
// replacement refused: every read returns bytes of the version Open
// pinned, or fails with objfile.ErrObjectChanged.
func Example() {
	srv := s3fake.New("bkt")
	srv.Put("data/part-00000.parquet", []byte("PAR1 header ... footer PAR1"))
	client, done := fakeClient(srv)
	defer done()
	ctx := context.Background()

	// The dataset root: names are relative to it.
	store := objfile.Root(s3.New(client, "bkt"), "data")
	obj, err := store.Open(ctx, "part-00000.parquet")
	if err != nil {
		log.Fatal(err)
	}
	defer obj.Close()
	fmt.Println("size", obj.Size())

	head, tail := make([]byte, 4), make([]byte, 4)
	err = objfile.ReadRanges(ctx, obj, []objfile.Range{{Off: 0, Buf: head}, {Off: obj.Size() - 4, Buf: tail}})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s %s\n", head, tail)

	srv.Put("data/part-00000.parquet", []byte("PAR1 another object PAR1"))
	_, err = obj.ReadAt(head, 0)
	fmt.Println(errors.Is(err, objfile.ErrObjectChanged))
	// Output:
	// size 27
	// PAR1 PAR1
	// true
}

// Create an object only if the name is free: a second creation of the
// same name fails with objfile.ErrExists and leaves the first in place.
func ExampleNewWriter() {
	srv := s3fake.New("bkt")
	client, done := fakeClient(srv)
	defer done()
	ctx := context.Background()

	w, err := s3.NewWriter(client, "bkt", s3.WriteOptions{})
	if err != nil {
		log.Fatal(err)
	}
	put := func(body string) error {
		_, err := w.PutIfAbsent(ctx, "manifest.json", []byte(body))
		return err
	}
	fmt.Println(put(`{"v":1}`))
	fmt.Println(errors.Is(put(`{"v":2}`), objfile.ErrExists))
	o, _ := srv.Get("manifest.json")
	fmt.Println(string(o.Data))
	// Output:
	// <nil>
	// true
	// {"v":1}
}
