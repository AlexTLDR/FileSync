package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"cloud.google.com/go/storage"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/gcsblob"
	"google.golang.org/api/option"
)

func syncDirToBucket(dir *blob.Bucket, bucket *blob.Bucket) error {
	ctx := context.Background()

	// Create a map to store the local files
	localFiles := make(map[string]blob.ListObject)

	// Iterate over the local directory
	it := dir.List(&blob.ListOptions{})
	for {
		obj, err := it.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		// Skip directories and hidden files
		if obj.IsDir || strings.HasPrefix(obj.Key, ".") {
			continue
		}

		localFiles[obj.Key] = *obj
	}

	// Iterate over the bucket
	it = bucket.List(&blob.ListOptions{})
	for {
		obj, err := it.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		localObj, exists := localFiles[obj.Key]

		// If the file exists in both the local directory and the bucket
		if exists {
			// If the local file is newer, upload it to the bucket
			if localObj.ModTime.After(obj.ModTime) {
				if err := uploadFile(ctx, dir, bucket, obj.Key); err != nil {
					return err
				}
				// If the bucket file is newer, download it to the local directory
			} else if obj.ModTime.After(localObj.ModTime) {
				if err := downloadFile(ctx, dir, bucket, obj.Key); err != nil {
					return err
				}
			}

			// Remove the file from the localFiles map
			delete(localFiles, obj.Key)
		} else {
			// If the file only exists in the bucket, download it to the local directory
			if err := downloadFile(ctx, dir, bucket, obj.Key); err != nil {
				return err
			}
		}
	}

	// If there are any files left in the localFiles map, they only exist in the local directory
	// Upload these files to the bucket
	for key := range localFiles {
		if err := uploadFile(ctx, dir, bucket, key); err != nil {
			return err
		}
	}

	return nil
}

func uploadFile(ctx context.Context, dir *blob.Bucket, bucket *blob.Bucket, key string) error {
	// Open the local file
	r, err := dir.NewReader(ctx, key, nil)
	if err != nil {
		return err
	}
	defer r.Close()

	// Create a writer for the bucket file
	w, err := bucket.NewWriter(ctx, key, nil)
	if err != nil {
		return err
	}

	// Copy the local file to the bucket
	if _, err := io.Copy(w, r); err != nil {
		w.Close()
		return err
	}

	// Close the writer to commit the upload
	return w.Close()
}

func downloadFile(ctx context.Context, dir *blob.Bucket, bucket *blob.Bucket, key string) error {
	// Open the bucket file
	r, err := bucket.NewReader(ctx, key, nil)
	if err != nil {
		return err
	}
	defer r.Close()

	// Create a writer for the local file
	w, err := dir.NewWriter(ctx, key, nil)
	if err != nil {
		return err
	}

	// Copy the bucket file to the local directory
	if _, err := io.Copy(w, r); err != nil {
		w.Close()
		return err
	}

	// Close the writer to commit the download
	return w.Close()
}
func main() {
	ctx := context.Background()

	// Set the GO_BLOB_DEFAULT_BUFFER_DIR environment variable
	os.Setenv("GO_BLOB_DEFAULT_BUFFER_DIR", "/home/alex/git/FileSync/test-data")

	// Replace with your actual local directory, credentials file path, and bucket name
	credsFilePath := "key/filesync-415212-ecb8c3396d06.json"
	bucketName := "gs://file-sync"

	// Create a Google Cloud client with the credentials file
	_, err := storage.NewClient(ctx, option.WithCredentialsFile(credsFilePath))
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	dir1, err := blob.OpenBucket(ctx, "file:///home/alex/git/FileSync/test-data/dir1")
	if err != nil {
		panic(err)
	}
	defer dir1.Close()

	// Open the bucket
	bucketHandle, err := blob.OpenBucket(ctx, bucketName)
	if err != nil {
		panic(err)
	}
	defer bucketHandle.Close()

	// Sync the local directory to the bucket
	err = syncDirToBucket(dir1, bucketHandle)
	if err != nil {
		fmt.Println("Error syncing directory to bucket:", err)
	}

	// You can now use the `files` map for further processing
}
