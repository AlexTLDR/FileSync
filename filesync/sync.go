package filesync

import (
	"context"
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
		bucketData, existsInBucket := bucketMetadata[key]
		if !existsInBucket {
			if localData.IsDeleted {
				delete(localMetadata, key)
			}
		} else {
			if localData.IsDeleted && !bucketData.IsDeleted {
				bucketMetadata[key] = localData
			} else if !localData.IsDeleted && bucketData.IsDeleted {
				localMetadata[key] = bucketData
			} else if !localData.IsDeleted && !bucketData.IsDeleted {
				if localData.ModTimeUnix > bucketData.ModTimeUnix {
					bucketMetadata[key] = localData
				} else {
					localMetadata[key] = bucketData
				}
			}
		}
	}

	for key, bucketData := range bucketMetadata {
		if _, existsLocally := localMetadata[key]; !existsLocally {
			if !bucketData.IsDeleted {
				localMetadata[key] = bucketData
			}
		}
	}
}

func applyChanges(ctx context.Context, dir *blob.Bucket, bucket *blob.Bucket, localMetadata, bucketMetadata storage.Metadata) {
	for key, localData := range localMetadata {
		if key == storage.LocalMetadataFileName {
			continue // Skip local metadata file
		}

		bucketData, existsInBucket := bucketMetadata[key]
		if !existsInBucket {
			if !localData.IsDeleted {
				uploadFile(ctx, dir, bucket, key, localData)
			}
		} else {
			if !localData.IsDeleted {
				// Check if the file exists locally
				_, err := dir.NewReader(ctx, key, nil)
				if err != nil {
					// File doesn't exist locally, download it
					downloadFile(ctx, dir, bucket, key, bucketData)
				} else if localData.ModTimeUnix != bucketData.ModTimeUnix {
					// File exists but mod times differ, update accordingly
					if localData.ModTimeUnix > bucketData.ModTimeUnix {
						uploadFile(ctx, dir, bucket, key, localData)
					} else {
						downloadFile(ctx, dir, bucket, key, bucketData)
					}
				}
			} else if !bucketData.IsDeleted {
				// Local file is marked as deleted but exists in bucket, delete from bucket
				if err := bucket.Delete(ctx, key); err != nil {
					log.Printf("Error deleting file %s from bucket: %v", key, err)
				} else {
					log.Printf("Deleted file %s from bucket", key)
					delete(bucketMetadata, key)
				}
			}
		}
	}

	// Download files that exist in bucket but not in local metadata
	for key, bucketData := range bucketMetadata {
		if key == storage.BucketMetadataFileName {
			continue // Skip bucket metadata file
		}
		if _, existsLocally := localMetadata[key]; !existsLocally && !bucketData.IsDeleted {
			downloadFile(ctx, dir, bucket, key, bucketData)
		}
	}

	// Update only bucket metadata after applying changes
	if err := storage.UpdateMetadata(ctx, bucket, bucketMetadata, false); err != nil {
		log.Printf("Error updating bucket metadata: %v", err)
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

	// Update metadata for existing files and mark non-existent files as deleted
	for key := range localMetadata {
		if obj, exists := localFiles[key]; exists {
			localMetadata[key] = storage.FileMetadata{
				ModTimeUnix: obj.ModTime.Unix(),
				IsDeleted:   false,
			}
		} else {
			localMetadata[key] = storage.FileMetadata{
				IsDeleted:      true,
				DeleteTimeUnix: time.Now().Unix(),
			}
		}
	}

	// Add new files to metadata
	for key, obj := range localFiles {
		if _, exists := localMetadata[key]; !exists {
			localMetadata[key] = storage.FileMetadata{
				ModTimeUnix: obj.ModTime.Unix(),
				IsDeleted:   false,
			}
		}
	}

	if err := storage.UpdateMetadata(ctx, dir, localMetadata, true); err != nil {
		log.Printf("Error updating local metadata: %v", err)
	}

	return localMetadata
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
		// No need to create a reader here, just add the object to the map
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
