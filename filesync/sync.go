package filesync

import (
	"context"
	"io"
	"log"
	"time"

	"github.com/AlexTLDR/FileSync/storage"
	"gocloud.dev/blob"
)

func SyncDirToBucket(ctx context.Context, dir *blob.Bucket, bucket *blob.Bucket) {
	for {
		// Ensure local metadata file exists and is up-to-date
		localMetadata, err := storage.GetMetadata(ctx, dir, true)
		if err != nil {
			log.Printf("Error getting local metadata: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		// Update local metadata file
		if err := storage.UpdateMetadata(ctx, dir, localMetadata, true); err != nil {
			log.Printf("Error updating local metadata: %v", err)
		}

		bucketMetadata, err := storage.GetMetadata(ctx, bucket, false)
		if err != nil {
			log.Printf("Error getting bucket metadata: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		// Compare and sync files
		for key, localData := range localMetadata {
			if key == storage.LocalMetadataFileName || key == storage.BucketMetadataFileName {
				continue // Skip metadata files
			}
			bucketData, exists := bucketMetadata[key]
			if !exists {
				// File exists locally but not in bucket, upload it
				uploadFile(ctx, dir, bucket, key, localData)
			} else if localData.ModTimeUnix > bucketData.ModTimeUnix {
				// Local file is newer, upload it
				uploadFile(ctx, dir, bucket, key, localData)
			} else if localData.ModTimeUnix < bucketData.ModTimeUnix {
				// Bucket file is newer, download it
				downloadFile(ctx, dir, bucket, key, bucketData)
			}
			// If mod times are equal, no action needed
		}

		// Check for files in bucket that don't exist locally
		for key, bucketData := range bucketMetadata {
			if key == storage.LocalMetadataFileName || key == storage.BucketMetadataFileName {
				continue // Skip metadata files
			}
			if _, exists := localMetadata[key]; !exists {
				downloadFile(ctx, dir, bucket, key, bucketData)
			}
		}

		// Handle deletions
		handleDeletions(ctx, dir, bucket, localMetadata, bucketMetadata)

		// Update metadata files
		if err := storage.UpdateMetadata(ctx, dir, localMetadata, true); err != nil {
			log.Printf("Error updating local metadata: %v", err)
		}
		if err := storage.UpdateMetadata(ctx, bucket, bucketMetadata, false); err != nil {
			log.Printf("Error updating bucket metadata: %v", err)
		}

		time.Sleep(5 * time.Second) // Adjust this interval as needed
	}
}

func uploadFile(ctx context.Context, dir, bucket *blob.Bucket, key string, metadata storage.FileMetadata) {
	reader, err := dir.NewReader(ctx, key, nil)
	if err != nil {
		log.Printf("Error creating reader for local file %s: %v", key, err)
		return
	}
	defer reader.Close()

	writer, err := bucket.NewWriter(ctx, key, nil)
	if err != nil {
		log.Printf("Error creating writer for bucket file %s: %v", key, err)
		return
	}
	defer writer.Close()

	_, err = io.Copy(writer, reader)
	if err != nil {
		log.Printf("Error uploading file %s: %v", key, err)
		return
	}

	log.Printf("Successfully uploaded file: %s", key)
}

func downloadFile(ctx context.Context, dir, bucket *blob.Bucket, key string, metadata storage.FileMetadata) {
	reader, err := bucket.NewReader(ctx, key, nil)
	if err != nil {
		log.Printf("Error creating reader for bucket file %s: %v", key, err)
		return
	}
	defer reader.Close()

	writer, err := dir.NewWriter(ctx, key, nil)
	if err != nil {
		log.Printf("Error creating writer for local file %s: %v", key, err)
		return
	}
	defer writer.Close()

	_, err = io.Copy(writer, reader)
	if err != nil {
		log.Printf("Error downloading file %s: %v", key, err)
		return
	}

	log.Printf("Successfully downloaded file: %s", key)
}

func handleDeletions(ctx context.Context, dir, bucket *blob.Bucket, localMetadata, bucketMetadata storage.Metadata) {
	for key, localData := range localMetadata {
		bucketData, exists := bucketMetadata[key]
		if localData.IsDeleted && (!exists || !bucketData.IsDeleted) {
			// File was deleted locally, delete from bucket
			if err := bucket.Delete(ctx, key); err != nil {
				log.Printf("Error deleting file %s from bucket: %v", key, err)
			} else {
				log.Printf("Deleted file %s from bucket", key)
				bucketMetadata[key] = storage.FileMetadata{IsDeleted: true, DeleteTimeUnix: time.Now().Unix()}
			}
		}
	}

	for key, bucketData := range bucketMetadata {
		localData, exists := localMetadata[key]
		if bucketData.IsDeleted && (!exists || !localData.IsDeleted) {
			// File was deleted in bucket, delete locally
			if err := dir.Delete(ctx, key); err != nil {
				log.Printf("Error deleting local file %s: %v", key, err)
			} else {
				log.Printf("Deleted local file %s", key)
				localMetadata[key] = storage.FileMetadata{IsDeleted: true, DeleteTimeUnix: time.Now().Unix()}
			}
		}
	}
}

func ListFiles(ctx context.Context, bucket *blob.Bucket, files map[string]*blob.ListObject) error {
	iter := bucket.List(nil)
	for {
		obj, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		files[obj.Key] = obj
	}
	return nil
}

func PeriodicBucketScan(ctx context.Context, bucket *blob.Bucket, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			log.Println("Performing periodic bucket scan")

			bucketMetadata, err := storage.GetMetadata(ctx, bucket, false)
			if err != nil {
				log.Printf("Error getting bucket metadata: %v", err)
				continue
			}

			bucketFiles := make(map[string]*blob.ListObject)
			if err := ListFiles(ctx, bucket, bucketFiles); err != nil {
				log.Printf("Error listing bucket files: %v", err)
				continue
			}

			for key := range bucketFiles {
				if key == storage.LocalMetadataFileName || key == storage.BucketMetadataFileName {
					continue // Skip metadata files
				}
				if _, exists := bucketMetadata[key]; !exists {
					attrs, err := bucket.Attributes(ctx, key)
					if err != nil {
						log.Printf("Error get attributes for file %s: %v", key, err)
						continue
					}
					bucketMetadata[key] = storage.FileMetadata{
						ModTimeUnix: attrs.ModTime.Unix(),
						IsDeleted:   false,
					}
				}
			}

			for key := range bucketMetadata {
				if key == storage.LocalMetadataFileName || key == storage.BucketMetadataFileName {
					continue // Skip metadata files
				}
				if _, exists := bucketFiles[key]; !exists {
					delete(bucketMetadata, key)
				}
			}

			if err := storage.UpdateMetadata(ctx, bucket, bucketMetadata, false); err != nil {
				log.Printf("Error updating bucket metadata: %v", err)
			}
		}
	}
}
