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
			time.Sleep(5 * time.Second)
			continue
		}

		localFiles := make(map[string]*blob.ListObject)
		if err := ListFiles(ctx, dir, localFiles); err != nil {
			log.Printf("Error listing local files: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		// Handle deletions
		for key, metaData := range metadata {
			if key == storage.MetadataFileName {
				continue
			}

			localExists := localFiles[key] != nil
			bucketExists, _ := bucket.Exists(ctx, key)

			if metaData.IsDeletedLocal && bucketExists {
				// File was marked for deletion locally and still exists in the bucket
				log.Printf("Deleting file %s from bucket (local deletion)", key)
				if err := bucket.Delete(ctx, key); err != nil {
					log.Printf("Error deleting file %s from bucket: %v", key, err)
				} else {
					// Only mark as deleted in the bucket after successful deletion
					metaData.IsDeletedBucket = true
					metaData.BucketDeleteTime = time.Now()
				}
			} else if metaData.IsDeletedBucket && localExists {
				// File was marked for deletion in the bucket and still exists locally
				log.Printf("Deleting local file %s (bucket deletion)", key)
				if err := dir.Delete(ctx, key); err != nil && !os.IsNotExist(err) {
					log.Printf("Error deleting local file %s: %v", key, err)
				} else {
					// Only mark as deleted locally after successful deletion
					metaData.IsDeletedLocal = true
					metaData.LocalDeleteTime = time.Now()
					delete(localFiles, key) // Remove from localFiles to avoid re-processing
				}
			}

			// Remove from metadata only if deleted from both locations
			if metaData.IsDeletedLocal && metaData.IsDeletedBucket {
				delete(metadata, key)
			} else {
				metadata[key] = metaData
			}
		}

		// Handle remaining files
		for key, localObj := range localFiles {
			metaData, exists := metadata[key]
			if !exists {
				metaData = storage.FileMetadata{}
			}

			bucketExists, _ := bucket.Exists(ctx, key)

			if !bucketExists {
				// Upload new or modified local files
				log.Printf("Uploading file %s to bucket", key)
				if err := UploadFile(ctx, dir, bucket, key); err != nil {
					log.Printf("Error uploading file %s: %v", key, err)
				} else {
					metaData.BucketModTime = localObj.ModTime
					metaData.IsDeletedBucket = false
					metaData.BucketDeleteTime = time.Time{}
				}
			} else {
				// Check for modifications
				bucketAttrs, _ := bucket.Attributes(ctx, key)
				if bucketAttrs.ModTime.After(metaData.LocalModTime) {
					// Bucket version is newer
					log.Printf("Updating local file %s from bucket", key)
					if err := DownloadFile(ctx, bucket, key, key); err != nil {
						log.Printf("Error updating local file %s: %v", key, err)
					} else {
						metaData.LocalModTime = bucketAttrs.ModTime
					}
				} else if localObj.ModTime.After(metaData.BucketModTime) {
					// Local version is newer
					log.Printf("Updating bucket file %s from local", key)
					if err := UploadFile(ctx, dir, bucket, key); err != nil {
						log.Printf("Error updating bucket file %s: %v", key, err)
					} else {
						metaData.BucketModTime = localObj.ModTime
					}
				}
			}

			metaData.IsDeletedLocal = false
			metaData.LocalDeleteTime = time.Time{}
			metaData.LocalModTime = localObj.ModTime
			metadata[key] = metaData
		}

		// Check for files in metadata that don't exist locally
		for key, metaData := range metadata {
			if key == storage.MetadataFileName {
				continue
			}

			localExists := localFiles[key] != nil
			bucketExists, _ := bucket.Exists(ctx, key)

			if !localExists && bucketExists && !metaData.IsDeletedBucket {
				log.Printf("Downloading new file %s from bucket", key)
				if err := DownloadFile(ctx, bucket, key, key); err != nil {
					log.Printf("Error downloading file %s: %v", key, err)
				} else {
					metaData.LocalModTime = time.Now()
					metaData.IsDeletedLocal = false
					metaData.LocalDeleteTime = time.Time{}
					metadata[key] = metaData
				}
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
			return
		case <-ticker.C:
			log.Println("Starting periodic bucket scan...")

			metadata, err := storage.GetBucketMetadata(ctx, bucket)
			if err != nil {
				log.Printf("Error retrieving metadata: %v", err)
				continue
			}
			log.Printf("Current metadata entries: %d", len(metadata))

			log.Println("Listing bucket files...")
			iter := bucket.List(nil)
			for {
				obj, err := iter.Next(ctx)
				if err == io.EOF {
					break
				}
				if err != nil {
					log.Printf("Error listing bucket files: %v", err)
					break
				}

				log.Printf("Found file in bucket: %s", obj.Key)

				if _, exists := metadata[obj.Key]; !exists {
					metadata[obj.Key] = storage.FileMetadata{
						BucketModTime:    obj.ModTime,
						LocalModTime:     time.Time{},
						IsDeletedBucket:  false,
						IsDeletedLocal:   false,
						BucketDeleteTime: time.Time{},
						LocalDeleteTime:  time.Time{},
						LastSyncTime:     time.Now(),
					}
				} else {
					metaData := metadata[obj.Key]
					if obj.ModTime.After(metaData.BucketModTime) {
						metaData.BucketModTime = obj.ModTime
						metaData.IsDeletedBucket = false
						metaData.BucketDeleteTime = time.Time{}
						metaData.LastSyncTime = time.Now()
						metadata[obj.Key] = metaData
					}
				}
			}

			// Check for deleted files
			for key, metaData := range metadata {
				exists, _ := bucket.Exists(ctx, key)
				if !exists {
					metaData.IsDeletedBucket = true
					metaData.BucketDeleteTime = time.Now()
					metaData.LastSyncTime = time.Now()
					metadata[key] = metaData
					log.Printf("File %s no longer exists in bucket. Marked as deleted.", key)
				}
			}

			if err := storage.UpdateBucketMetadata(ctx, bucket, metadata); err != nil {
				log.Printf("Error updating metadata: %v", err)
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
