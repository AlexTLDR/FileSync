package main

import (
	"context"
	"log"
	"time"

	gcs "cloud.google.com/go/storage"
	"github.com/AlexTLDR/FileSync/filesync"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/gcsblob"
	"google.golang.org/api/option"
)

func main() {
	ctx := context.Background()

	// Replace with your actual local directory, credentials file path, and bucket name
	credsFilePath := "key/filesync-415212-ecb8c3396d06.json"
	bucketName := "gs://file-sync"

	// Create a Google Cloud client with the credentials file
	_, err := gcs.NewClient(ctx, option.WithCredentialsFile(credsFilePath))
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	dir1, err := blob.OpenBucket(ctx, "file:///home/alex/git/FileSync/test-data/dir1")
	if err != nil {
		panic(err)
	}
	defer func(dir1 *blob.Bucket) {
		err := dir1.Close()
		if err != nil {
			panic(err)
		}
	}(dir1)

	// Open the bucket
	bucket, err := blob.OpenBucket(ctx, bucketName)
	if err != nil {
		panic(err)
	}
	defer func(bucket *blob.Bucket) {
		err := bucket.Close()
		if err != nil {
			panic(err)
		}
	}(bucket)

	// Start periodic bucket scan in a separate goroutine
	go func() {
		filesync.PeriodicBucketScan(ctx, bucket, 5*time.Second)
	}()

	// Run SyncDirToBucket in an infinite loop
	for {
		filesync.SyncDirToBucket(ctx, dir1, bucket)
		time.Sleep(5 * time.Second) // Adjust the sleep duration as needed
	}
}
