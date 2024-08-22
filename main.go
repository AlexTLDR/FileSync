package main

import (
	"context"
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

type fileStatus struct {
	isDeleted bool
	deletedOn *time.Time
	obj       *blob.ListObject
}

const timeThreshold = 10 * time.Second

func syncDirToBucket(ctx context.Context, dir *blob.Bucket, bucket *blob.Bucket) {
	localFiles := make(map[string]*fileStatus)
	bucketFiles := make(map[string]*fileStatus)

	for {
		log.Println("Starting synchronization cycle")

		log.Println("Updating local file statuses")
		updateFileStatuses(ctx, dir, localFiles)
		log.Println("Updating bucket file statuses")
		updateFileStatuses(ctx, bucket, bucketFiles)

		for key, localStatus := range localFiles {
			bucketStatus, existsInBucket := bucketFiles[key]

			log.Printf("Processing file: %s", key)
			localUTCTime := localStatus.obj.ModTime.UTC()
			log.Printf("Local status: isDeleted=%v, modTime=%v (UTC)", localStatus.isDeleted, localUTCTime)
			if existsInBucket {
				log.Printf("Bucket status: isDeleted=%v, modTime=%v", bucketStatus.isDeleted, bucketStatus.obj.ModTime)
			} else {
				log.Println("File does not exist in bucket")
			}

			if localStatus.isDeleted && !existsInBucket {
				log.Printf("File %s deleted locally and doesn't exist in bucket, removing from tracking", key)
				delete(localFiles, key)
			} else if localStatus.isDeleted && existsInBucket && !bucketStatus.isDeleted {
				log.Printf("Deleting file %s from bucket", key)
				if err := bucket.Delete(ctx, key); err != nil {
					log.Printf("Error deleting file %s from bucket: %v", key, err)
				}
			} else if !existsInBucket {
				log.Printf("Uploading new local file %s to bucket", key)
				if err := uploadFile(ctx, dir, bucket, key); err != nil {
					log.Printf("Error uploading file %s: %v", key, err)
				}
			} else if bucketStatus.isDeleted && !localStatus.isDeleted {
				log.Printf("Deleting local file %s", key)
				localPath := filepath.Join("/home/alex/git/FileSync/test-data/dir1", key)
				if err := os.Remove(localPath); err != nil && !os.IsNotExist(err) {
					log.Printf("Error deleting local file %s: %v", key, err)
				}
				localStatus.isDeleted = true
				localStatus.deletedOn = bucketStatus.deletedOn
			} else if !localStatus.isDeleted && !bucketStatus.isDeleted {
				timeDiff := localUTCTime.Sub(bucketStatus.obj.ModTime)
				if timeDiff > timeThreshold {
					log.Printf("Uploading newer local file %s to bucket", key)
					if err := uploadFile(ctx, dir, bucket, key); err != nil {
						log.Printf("Error uploading file %s: %v", key, err)
					}
				} else if timeDiff < -timeThreshold {
					log.Printf("Downloading newer bucket file %s", key)
					if err := downloadFile(ctx, bucket, key, filepath.Join("/home/alex/git/FileSync/test-data/dir1", key)); err != nil {
						log.Printf("Error downloading file %s: %v", key, err)
					}
				} else {
					log.Printf("File %s is up to date (time difference: %v)", key, timeDiff)
				}
			}
		}
		for key, bucketStatus := range bucketFiles {
			if _, existsLocally := localFiles[key]; !existsLocally && !bucketStatus.isDeleted {
				log.Printf("Downloading new bucket file %s", key)
				if err := downloadFile(ctx, bucket, key, filepath.Join("/home/alex/git/FileSync/test-data/dir1", key)); err != nil {
					log.Printf("Error downloading new file %s: %v", key, err)
				}
			}
		}

		log.Println("Synchronization cycle completed")
		time.Sleep(timeThreshold)
	}
}
func updateFileStatuses(ctx context.Context, b *blob.Bucket, files map[string]*fileStatus) {
	currentFiles := make(map[string]bool)

	it := b.List(&blob.ListOptions{})
	for {
		obj, err := it.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error listing files: %v", err)
			return
		}
		if !obj.IsDir && !strings.HasPrefix(obj.Key, ".") {
			currentFiles[obj.Key] = true
			if status, exists := files[obj.Key]; exists {
				if status.isDeleted {
					// File was previously deleted but now exists again
					status.isDeleted = false
					status.deletedOn = nil
				}
				status.obj = obj
			} else {
				files[obj.Key] = &fileStatus{obj: obj, isDeleted: false}
			}
		}
	}

	// Mark files as deleted if they no longer exist
	for key, status := range files {
		if !currentFiles[key] && !status.isDeleted {
			status.isDeleted = true
			now := time.Now()
			status.deletedOn = &now
		}
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
	syncDirToBucket(ctx, dir1, bucketHandle)
}
