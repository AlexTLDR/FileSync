package filesync

import (
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/AlexTLDR/FileSync/storage"
	"gocloud.dev/blob"
)

func SyncDirToBucket(ctx context.Context, dir *blob.Bucket, bucket *blob.Bucket) {
	for {
		// Update local metadata
		localMetadata := updateLocalMetadata(ctx, dir)

		// Update bucket metadata
		bucketMetadata := updateBucketMetadata(ctx, bucket)

		// Compare and reconcile metadata
		reconcileMetadata(ctx, dir, bucket, localMetadata, bucketMetadata)

		// Apply changes based on reconciled metadata
		applyChanges(ctx, dir, bucket, localMetadata, bucketMetadata)

		time.Sleep(5 * time.Second)
	}
}

func updateBucketMetadata(ctx context.Context, bucket *blob.Bucket) storage.Metadata {
	bucketMetadata, err := storage.GetMetadata(ctx, bucket, false)
	if err != nil {
		log.Printf("Error getting bucket metadata: %v", err)
		bucketMetadata = make(storage.Metadata)
	}

	bucketFiles := make(map[string]*blob.ListObject)
	if err := ListFiles(ctx, bucket, bucketFiles); err != nil {
		log.Printf("Error listing bucket files: %v", err)
		return bucketMetadata
	}

	for key, obj := range bucketFiles {
		if key == storage.LocalMetadataFileName || key == storage.BucketMetadataFileName {
			continue
		}
		bucketMetadata[key] = storage.FileMetadata{
			ModTimeUnix: obj.ModTime.Unix(),
			IsDeleted:   false,
		}
	}

	if err := storage.UpdateMetadata(ctx, bucket, bucketMetadata, false); err != nil {
		log.Printf("Error updating bucket metadata: %v", err)
	}

	return bucketMetadata
}

func reconcileMetadata(ctx context.Context, dir *blob.Bucket, bucket *blob.Bucket, localMetadata, bucketMetadata storage.Metadata) {
	for key, localData := range localMetadata {
		bucketData, exists := bucketMetadata[key]
		if !exists {
			if !localData.IsDeleted {
				// File exists locally but not in bucket, mark for upload
				localMetadata[key] = storage.FileMetadata{
					ModTimeUnix: localData.ModTimeUnix,
					IsDeleted:   false,
				}
			}
		} else {
			if localData.IsDeleted && !bucketData.IsDeleted {
				// File deleted locally, mark for deletion in bucket
				bucketMetadata[key] = storage.FileMetadata{
					IsDeleted:      true,
					DeleteTimeUnix: localData.DeleteTimeUnix,
				}
			} else if !localData.IsDeleted && bucketData.IsDeleted {
				// File deleted in bucket, mark for deletion locally
				localMetadata[key] = storage.FileMetadata{
					IsDeleted:      true,
					DeleteTimeUnix: bucketData.DeleteTimeUnix,
				}
			} else if !localData.IsDeleted && !bucketData.IsDeleted {
				// Both exist, keep the most recent version
				if localData.ModTimeUnix > bucketData.ModTimeUnix {
					bucketMetadata[key] = localData
				} else {
					localMetadata[key] = bucketData
				}
			}
		}
	}

	for key, bucketData := range bucketMetadata {
		if _, exists := localMetadata[key]; !exists && !bucketData.IsDeleted {
			// File exists in bucket but not locally, mark for download
			localMetadata[key] = bucketData
		}
	}
}

func applyChanges(ctx context.Context, dir *blob.Bucket, bucket *blob.Bucket, localMetadata, bucketMetadata storage.Metadata) {
	for key, localData := range localMetadata {
		bucketData, exists := bucketMetadata[key]
		if !exists {
			if !localData.IsDeleted {
				// Upload file to bucket
				uploadFile(ctx, dir, bucket, key, localData)
			}
		} else {
			if localData.IsDeleted && !bucketData.IsDeleted {
				// Delete file from bucket
				if err := bucket.Delete(ctx, key); err != nil {
					log.Printf("Error deleting file %s from bucket: %v", key, err)
				}
			} else if !localData.IsDeleted && bucketData.IsDeleted {
				// Delete file locally
				if err := dir.Delete(ctx, key); err != nil {
					log.Printf("Error deleting local file %s: %v", key, err)
				}
			} else if !localData.IsDeleted && !bucketData.IsDeleted && localData.ModTimeUnix != bucketData.ModTimeUnix {
				// Update file in the appropriate direction
				if localData.ModTimeUnix > bucketData.ModTimeUnix {
					uploadFile(ctx, dir, bucket, key, localData)
				} else {
					downloadFile(ctx, dir, bucket, key, bucketData)
				}
			}
		}
	}

	for key, bucketData := range bucketMetadata {
		if _, exists := localMetadata[key]; !exists && !bucketData.IsDeleted {
			// Download file from bucket
			downloadFile(ctx, dir, bucket, key, bucketData)
		}
	}
}

func updateLocalMetadata(ctx context.Context, dir *blob.Bucket) storage.Metadata {
	localMetadata, err := storage.GetMetadata(ctx, dir, true)
	if err != nil {
		log.Printf("Error getting local metadata: %v", err)
		localMetadata = make(storage.Metadata)
	}

	localFiles := make(map[string]*blob.ListObject)
	if err := ListFiles(ctx, dir, localFiles); err != nil {
		log.Printf("Error listing local files: %v", err)
		return localMetadata
	}

	for key, obj := range localFiles {
		if key == storage.LocalMetadataFileName || key == storage.BucketMetadataFileName {
			continue
		}
		localMetadata[key] = storage.FileMetadata{
			ModTimeUnix: obj.ModTime.Unix(),
			IsDeleted:   false,
		}
	}

	// Mark files as deleted if they're in metadata but not in the directory
	for key := range localMetadata {
		if key == storage.LocalMetadataFileName || key == storage.BucketMetadataFileName {
			continue
		}
		if _, exists := localFiles[key]; !exists {
			localMetadata[key] = storage.FileMetadata{
				IsDeleted:      true,
				DeleteTimeUnix: time.Now().Unix(),
			}
		}
	}

	if err := storage.UpdateMetadata(ctx, dir, localMetadata, true); err != nil {
		log.Printf("Error updating local metadata: %v", err)
	}

	return localMetadata
}

func calculateChecksum(ctx context.Context, bucket *blob.Bucket, key string) (string, error) {
	reader, err := bucket.NewReader(ctx, key, nil)
	if err != nil {
		return "", err
	}
	defer reader.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, reader); err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", hash.Sum(nil)), nil
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
	localPath := filepath.Join("/home/alex/git/FileSync/test-data/dir1", key)

	// Check if file already exists locally with the same modification time
	info, err := os.Stat(localPath)
	if err == nil && info.ModTime().Unix() == metadata.ModTimeUnix {
		log.Printf("File %s is already up to date locally", key)
		return
	}

	data, err := bucket.ReadAll(ctx, key)
	if err != nil {
		log.Printf("Error reading bucket file %s: %v", key, err)
		return
	}

	if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
		log.Printf("Error creating directory for file %s: %v", key, err)
		return
	}

	if err := os.WriteFile(localPath, data, 0644); err != nil {
		log.Printf("Error writing local file %s: %v", key, err)
		return
	}

	log.Printf("Successfully downloaded file: %s (%d bytes)", key, len(data))
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
