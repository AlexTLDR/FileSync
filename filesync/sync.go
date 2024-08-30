package filesync

import (
	"bytes"
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/AlexTLDR/FileSync/storage"
	"gocloud.dev/blob"
)

func SyncDirToBucket(ctx context.Context, dir *blob.Bucket, bucket *blob.Bucket) {
	for {
		metadata, err := storage.GetBucketMetadata(ctx, bucket)
		if err != nil {
			log.Printf("Error getting bucket metadata: %v", err)
			continue
		}

		localFiles := make(map[string]*blob.ListObject)
		if err := ListFiles(ctx, dir, localFiles); err != nil {
			log.Printf("Error listing local files: %v", err)
			continue
		}

		// Handle file downloads from the bucket
		for key, metaData := range metadata {
			_, localExists := localFiles[key]
			bucketExists, _ := bucket.Exists(ctx, key)

			if !localExists && bucketExists && !metaData.IsDeleted {
				log.Printf("File %s exists in the bucket but not locally. Downloading...", key)
				if err := DownloadFile(ctx, bucket, key, key); err != nil {
					log.Printf("Error downloading file %s: %v", key, err)
				}
			}
		}

		// Handle file deletions from the bucket
		for key, metaData := range metadata {
			_, localExists := localFiles[key]
			bucketExists, _ := bucket.Exists(ctx, key)

			if !localExists && !bucketExists && !metaData.IsDeleted {
				log.Printf("File %s exists in metadata but not in the bucket or locally. Marking as deleted...", key)
				metadata[key] = storage.FileMetadata{
					ModTime:     metaData.ModTime,
					IsDeleted:   true,
					DeletedTime: time.Now(),
				}
				if err := dir.Delete(ctx, key); err != nil {
					log.Printf("Error deleting file %s locally: %v", key, err)
				}
			}
		}

		// Handle file uploads and downloads
		for key, localObj := range localFiles {
			metaData, exists := metadata[key]
			bucketExists, _ := bucket.Exists(ctx, key)

			log.Printf("Processing local file: %s", key)
			log.Printf("File exists in metadata: %v", exists)
			log.Printf("File exists in bucket: %v", bucketExists)

			if !exists || localObj.ModTime.After(metaData.ModTime) || (exists && metaData.IsDeleted) {
				log.Printf("Uploading file %s to bucket", key)
				if err := UploadFile(ctx, dir, bucket, key); err != nil {
					log.Printf("Error uploading file %s: %v", key, err)
					continue
				}
				metadata[key] = storage.FileMetadata{
					ModTime:     localObj.ModTime,
					IsDeleted:   false,
					DeletedTime: time.Time{},
				}
			} else if !bucketExists && !metaData.IsDeleted {
				log.Printf("File %s exists in metadata but not in the bucket. Deleting locally...", key)
				if err := dir.Delete(ctx, key); err != nil {
					log.Printf("Error deleting file %s locally: %v", key, err)
				}
				delete(metadata, key)
			}
		}

		if err := storage.UpdateBucketMetadata(ctx, bucket, metadata); err != nil {
			log.Printf("Error updating bucket metadata: %v", err)
		}

		time.Sleep(5 * time.Second)
	}
}

func PeriodicBucketScan(ctx context.Context, bucket *blob.Bucket, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Periodic bucket scan stopped")
			return
		case <-ticker.C:
			log.Println("Starting periodic bucket scan...")
			metadata, err := storage.GetBucketMetadata(ctx, bucket)
			if err != nil {
				log.Printf("Error getting bucket metadata: %v", err)
				continue
			}
			log.Printf("Current metadata entries: %d", len(metadata))

			log.Println("Listing bucket files...")
			iter := bucket.List(&blob.ListOptions{})
			updated := false
			newFiles := 0
			for {
				obj, err := iter.Next(ctx)
				if err == io.EOF {
					break
				}
				if err != nil {
					log.Printf("Error listing bucket contents: %v", err)
					break
				}
				log.Printf("Found file in bucket: %s", obj.Key)

				if obj.Key != storage.MetadataFileName {
					metaData, exists := metadata[obj.Key]
					if !exists || obj.ModTime.After(metaData.ModTime) {
						log.Printf("New or modified file detected: %s", obj.Key)
						metadata[obj.Key] = storage.FileMetadata{
							ModTime:     obj.ModTime,
							IsDeleted:   false,
							DeletedTime: time.Time{},
						}
						updated = true
						newFiles++
					}
				}
			}

			if updated {
				log.Printf("Found %d new or modified files in the bucket", newFiles)
				log.Printf("Updating metadata file with %d entries", len(metadata))
				if err := storage.UpdateBucketMetadata(ctx, bucket, metadata); err != nil {
					log.Printf("Error updating bucket metadata: %v", err)
				} else {
					log.Println("Metadata file successfully updated with new changes")
				}
			} else {
				log.Println("No changes detected in bucket")
			}

			log.Println("Periodic bucket scan completed")
		}
	}
}

func ListFiles(ctx context.Context, b *blob.Bucket, files map[string]*blob.ListObject) error {
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

func UploadFile(ctx context.Context, dir *blob.Bucket, bucket *blob.Bucket, key string) error {
	// Check if the file already exists in the bucket
	_, err := bucket.Attributes(ctx, key)
	if err == nil {
		// Compare the file contents
		localReader, err := dir.NewReader(ctx, key, nil)
		if err != nil {
			log.Printf("Error opening local file %s: %v", key, err)
			return err
		}
		defer localReader.Close()

		bucketReader, err := bucket.NewReader(ctx, key, nil)
		if err != nil {
			log.Printf("Error opening bucket file %s: %v", key, err)
			return err
		}
		defer bucketReader.Close()

		localContent, err := io.ReadAll(localReader)
		if err != nil {
			log.Printf("Error reading local file %s: %v", key, err)
			return err
		}

		bucketContent, err := io.ReadAll(bucketReader)
		if err != nil {
			log.Printf("Error reading bucket file %s: %v", key, err)
			return err
		}

		if bytes.Equal(localContent, bucketContent) {
			log.Printf("File %s already exists in the bucket with the same content. Skipping upload.", key)
			return nil
		}
	}

	// File doesn't exist in the bucket or has different content, proceed with upload
	log.Printf("Uploading file %s to bucket", key)
	r, err := dir.NewReader(ctx, key, nil)
	if err != nil {
		log.Printf("Error opening local file %s: %v", key, err)
		return err
	}
	defer r.Close()

	writerOptions := &blob.WriterOptions{
		ContentType: "application/octet-stream", // Set a default content type
	}

	// Check if the file exists in the bucket
	if _, err := bucket.Attributes(ctx, key); err == nil {
		// File exists in the bucket, retrieve the content type
		attrs, err := bucket.Attributes(ctx, key)
		if err == nil {
			writerOptions.ContentType = attrs.ContentType
		}
	}

	w, err := bucket.NewWriter(ctx, key, writerOptions)
	if err != nil {
		log.Printf("Error creating bucket writer for file %s: %v", key, err)
		return err
	}
	defer w.Close()

	written, err := io.Copy(w, r)
	if err != nil {
		log.Printf("Error uploading file %s: %v", key, err)
		return err
	}

	log.Printf("Successfully uploaded %s to bucket (%d bytes written)", key, written)
	return nil
}

func DownloadFile(ctx context.Context, bucket *blob.Bucket, key string, destPath string) error {
	log.Printf("Starting download of %s", key)

	r, err := bucket.NewReader(ctx, key, nil)
	if err != nil {
		log.Printf("Error opening bucket file %s: %v", key, err)
		return err
	}
	defer r.Close()

	fullPath := filepath.Join("/home/alex/git/FileSync/test-data/dir1", destPath)
	destFile, err := os.Create(fullPath)
	if err != nil {
		log.Printf("Error creating destination file %s: %v", fullPath, err)
		return err
	}
	defer destFile.Close()

	written, err := io.Copy(destFile, r)
	if err != nil {
		log.Printf("Error copying file %s: %v", key, err)
		return err
	}

	log.Printf("Successfully downloaded %s to %s (%d bytes written)", key, fullPath, written)
	return nil
}
