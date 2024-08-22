package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/gcsblob"
	"google.golang.org/api/option"
)

const timeThreshold = 10 * time.Second

func syncDirToBucket(ctx context.Context, dir *blob.Bucket, bucket *blob.Bucket) {
	for {
		metadata, err := getBucketMetadata(ctx, bucket)
		if err != nil {
			log.Printf("Error getting bucket metadata: %v", err)
			continue
		}

		localFiles := make(map[string]*blob.ListObject)
		if err := listFiles(ctx, dir, localFiles); err != nil {
			log.Printf("Error listing local files: %v", err)
			continue
		}

		bucketFiles := make(map[string]*blob.ListObject)
		if err := listFiles(ctx, bucket, bucketFiles); err != nil {
			log.Printf("Error listing bucket files: %v", err)
			continue
		}

		// Update metadata with bucket files
		for key, bucketObj := range bucketFiles {
			if _, exists := metadata[key]; !exists {
				metadata[key] = FileMetadata{
					ModTime:     bucketObj.ModTime,
					IsDeleted:   false,
					DeletedTime: time.Time{},
				}
			}
		}

		for key, localObj := range localFiles {
			metaData, exists := metadata[key]
			bucketExists, _ := bucket.Exists(ctx, key)

			if !exists || localObj.ModTime.After(metaData.ModTime) || (exists && metaData.IsDeleted) {
				log.Printf("Uploading file %s to bucket", key)
				if err := uploadFile(ctx, dir, bucket, key); err != nil {
					log.Printf("Error uploading file %s: %v", key, err)
					continue
				}
				metadata[key] = FileMetadata{
					ModTime:     localObj.ModTime,
					IsDeleted:   false,
					DeletedTime: time.Time{},
				}
			} else if !bucketExists && !metaData.IsDeleted {
				log.Printf("Deleting local file %s", key)
				if err := dir.Delete(ctx, key); err != nil {
					log.Printf("Error deleting local file %s: %v", key, err)
				}
				metadata[key] = FileMetadata{
					ModTime:     time.Now(),
					IsDeleted:   true,
					DeletedTime: time.Now(),
				}
			}
		}

		if err := updateBucketMetadata(ctx, bucket, metadata); err != nil {
			log.Printf("Error updating bucket metadata: %v", err)
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
	syncDirToBucket(ctx, dir1, bucketHandle)
}

type FileMetadata struct {
	ModTime     time.Time `json:"mod_time"`
	IsDeleted   bool      `json:"is_deleted"`
	DeletedTime time.Time `json:"deleted_time"`
}

type BucketMetadata map[string]FileMetadata

const MetadataFileName = ".filesync_metadata.json"

func updateBucketMetadata(ctx context.Context, bucket *blob.Bucket, metadata BucketMetadata) error {
	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %v", err)
	}

	w, err := bucket.NewWriter(ctx, MetadataFileName, nil)
	if err != nil {
		return fmt.Errorf("failed to create writer for metadata: %v", err)
	}
	defer w.Close()

	_, err = w.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write metadata: %v", err)
	}

	return nil
}

func getBucketMetadata(ctx context.Context, bucket *blob.Bucket) (BucketMetadata, error) {
	exists, err := bucket.Exists(ctx, MetadataFileName)
	if err != nil {
		log.Printf("Error checking metadata file existence: %v", err)
		return nil, fmt.Errorf("failed to check metadata existence: %v", err)
	}

	if !exists {
		log.Println("Metadata file does not exist. Creating new file.")
		initialMetadata := BucketMetadata{}
		if err := updateBucketMetadata(ctx, bucket, initialMetadata); err != nil {
			log.Printf("Failed to create initial metadata: %v", err)
			return nil, fmt.Errorf("failed to create initial metadata: %v", err)
		}
		log.Println("New metadata file created successfully.")
		return initialMetadata, nil
	}

	r, err := bucket.NewReader(ctx, MetadataFileName, nil)
	if err != nil {
		log.Printf("Error opening metadata file: %v", err)
		return nil, fmt.Errorf("failed to open metadata: %v", err)
	}
	defer r.Close()

	data, err := io.ReadAll(r)
	if err != nil {
		log.Printf("Error reading metadata content: %v", err)
		return nil, fmt.Errorf("failed to read metadata content: %v", err)
	}

	var metadata BucketMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		log.Printf("Error unmarshaling metadata: %v", err)
		return nil, fmt.Errorf("failed to unmarshal metadata: %v", err)
	}
	log.Println("Metadata retrieved successfully.")
	return metadata, nil
}

func listFiles(ctx context.Context, b *blob.Bucket, files map[string]*blob.ListObject) error {
	it := b.List(&blob.ListOptions{})
	for {
		obj, err := it.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if !obj.IsDir && !strings.HasPrefix(obj.Key, ".") {
			files[obj.Key] = obj
		}
	}
	return nil
}

func ensureMetadataFileExists(ctx context.Context, bucket *blob.Bucket) error {
	exists, err := bucket.Exists(ctx, MetadataFileName)
	if err != nil {
		return fmt.Errorf("failed to check if metadata file exists: %v", err)
	}
	if !exists {
		emptyMetadata := BucketMetadata{}
		if err := updateBucketMetadata(ctx, bucket, emptyMetadata); err != nil {
			return fmt.Errorf("failed to create initial metadata file: %v", err)
		}
	}
	return nil
}
