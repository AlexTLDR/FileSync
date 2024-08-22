package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/gcsblob"
	"google.golang.org/api/option"
)

type foo struct {
	isDeleted bool
	deletedOn *time.Time
	obj       blob.ListObject
}

func syncDirToBucket(ctx context.Context, dir *blob.Bucket, bucket *blob.Bucket) error {
	for {
		localFiles := make(map[string]*blob.ListObject)

		// List local files
		it := dir.List(&blob.ListOptions{})
		for {
			obj, err := it.Next(ctx)
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			if !obj.IsDir && !strings.HasPrefix(obj.Key, ".") {
				localFiles[obj.Key] = obj
			}
		}

		// Sync with bucket
		it = bucket.List(&blob.ListOptions{})
		for {
			bucketObj, err := it.Next(ctx)
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}

			localObj, exists := localFiles[bucketObj.Key]

			if exists {
				if localObj.ModTime.After(bucketObj.ModTime) {
					if err := uploadFile(ctx, dir, bucket, bucketObj.Key); err != nil {
						return err
					}
				} else if bucketObj.ModTime.After(localObj.ModTime) {
					if err := downloadFile(ctx, bucket, bucketObj.Key, filepath.Join("/home/alex/git/FileSync/test-data/dir1", bucketObj.Key)); err != nil {
						return err
					}
				}
				delete(localFiles, bucketObj.Key)
			} else {
				if err := downloadFile(ctx, bucket, bucketObj.Key, filepath.Join("/home/alex/git/FileSync/test-data/dir1", bucketObj.Key)); err != nil {
					return err
				}
			}
		}

		// Upload remaining local files
		for key := range localFiles {
			if err := uploadFile(ctx, dir, bucket, key); err != nil {
				return err
			}
		}

		time.Sleep(5 * time.Second)
	}
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

func downloadFile(ctx context.Context, bucket *blob.Bucket, key string, destPath string) error {
	// Open the bucket file
	r, err := bucket.NewReader(ctx, key, nil)
	if err != nil {
		return err
	}
	defer r.Close()

	// Create the destination file
	destFile, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer destFile.Close()

	// Copy the bucket file to the destination file
	if _, err := io.Copy(destFile, r); err != nil {
		return err
	}

	return nil
}
func main() {
	ctx := context.Background()

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
	err = syncDirToBucket(ctx, dir1, bucketHandle)
	if err != nil {
		fmt.Println("Error syncing directory to bucket:", err)
	}
}
