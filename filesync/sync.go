package filesync

import (
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

		for key, metaData := range metadata {
			_, localExists := localFiles[key]
			if !localExists && !metaData.IsDeleted {
				log.Printf("File %s exists in metadata but not locally. Starting download...", key)
				if err := DownloadFile(ctx, bucket, key, key); err != nil {
					log.Printf("Error downloading file %s: %v", key, err)
					continue
				}
				// Immediately update localFiles map after successful download
				if attr, err := dir.Attributes(ctx, key); err == nil {
					localFiles[key] = &blob.ListObject{
						Key:     key,
						ModTime: attr.ModTime,
						Size:    attr.Size,
					}
					log.Printf("Updated local files map with %s", key)
				} else {
					log.Printf("Error getting attributes for %s: %v", key, err)
				}
			}
		}

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
				log.Printf("Deleting local file %s", key)
				if err := dir.Delete(ctx, key); err != nil {
					log.Printf("Error deleting local file %s: %v", key, err)
				}
				metadata[key] = storage.FileMetadata{
					ModTime:     time.Now(),
					IsDeleted:   true,
					DeletedTime: time.Now(),
				}
			} else {
				log.Printf("File %s is up to date, skipping upload", key)
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
