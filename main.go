package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"cloud.google.com/go/storage"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/gcsblob"
	"google.golang.org/api/option"
)

func syncDirToBucket(dir string, bucket *blob.Bucket) error {
	ctx := context.Background()

	// Walk through the local directory
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories and hidden files
		if info.IsDir() || info.Name()[0] == '.' {
			return nil
		}

		// Get the relative path within the local directory
		relPath, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}

		// Check if the file exists in the bucket
		r, err := bucket.NewReader(ctx, relPath, nil)
		if err != nil {
			// If the file does not exist in the bucket, upload it
			// Open local file
			f, err := os.Open(path)
			if err != nil {
				return err
			}
			defer f.Close()

			// Write the local file to the bucket
			w, err := bucket.NewWriter(ctx, relPath, nil)
			if err != nil {
				return err
			}

			_, err = io.Copy(w, f)
			if err != nil {
				w.Close()
				return err
			}

			return w.Close()
		}

		// If the file exists in the bucket, check its modification time
		defer r.Close()

		var attrs storage.ObjectAttrs
		if r.As(&attrs) {
			// If the file in the bucket is newer, skip this file
			if attrs.Updated.After(info.ModTime()) {
				return nil
			}
		}

		// If the file in the bucket is older, upload the local file
		// Open local file
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()

		// Write the local file to the bucket
		w, err := bucket.NewWriter(ctx, relPath, nil)
		if err != nil {
			return err
		}

		_, err = io.Copy(w, f)
		if err != nil {
			w.Close()
			return err
		}

		return w.Close()
	})

	return err
}

func main() {
	ctx := context.Background()

	// Replace with your actual local directory, credentials file path, and bucket name
	dir := "test-data/dir1"
	credsFilePath := "key/filesync-415212-60d3073be092.json"
	bucketName := "file-sync"

	// Create a Google Cloud client with the credentials file
	_, err := storage.NewClient(ctx, option.WithCredentialsFile(credsFilePath))
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	// Get a reference to the bucket
	// bucket := client.Bucket(bucketName)

	// Open the bucket
	bucketHandle, err := blob.OpenBucket(ctx, bucketName)
	if err != nil {
		panic(err)
	}
	defer bucketHandle.Close()

	// Sync the local directory to the bucket
	err = syncDirToBucket(dir, bucketHandle)
	if err != nil {
		fmt.Println("Error syncing directory to bucket:", err)
	}
}
