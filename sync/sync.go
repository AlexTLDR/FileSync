package sync

import (
	"context"
	"crypto/md5"
	"fmt"
	"github.com/AlexTLDR/FileSync/metadata"
	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"
)

const metadataFileName = "__metadata.json"

func SyncFiles(ctx context.Context, localDir string, bucket *blob.Bucket) error {
	log.Println("Starting file synchronization")
	bucketMetadata, err := metadata.LoadMetadataFromBucket(ctx, bucket)
	if err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			log.Println("No metadata file found in bucket, creating a new one")
			bucketMetadata = metadata.BucketMetadata{Files: make(map[string]metadata.FileMetadata)}
		} else {
			log.Printf("Error loading bucket metadata: %v", err)
			return err
		}
	}

	err = syncLocalToBucket(ctx, localDir, bucket, &bucketMetadata)
	if err != nil {
		return err
	}

	err = syncBucketToLocal(ctx, localDir, bucket, &bucketMetadata)
	if err != nil {
		return err
	}

	log.Println("Saving updated metadata to bucket")
	return metadata.SaveMetadataToBucket(ctx, bucket, bucketMetadata)
}

func syncLocalToBucket(ctx context.Context, localDir string, bucket *blob.Bucket, bucketMetadata *metadata.BucketMetadata) error {
	localFiles := make(map[string]bool)

	err := filepath.Walk(localDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Printf("Error accessing path %s: %v", path, err)
			return err
		}

		if info.IsDir() {
			return nil
		}

		relPath, err := filepath.Rel(localDir, path)
		if err != nil {
			log.Printf("Error getting relative path for %s: %v", path, err)
			return err
		}

		if relPath == metadataFileName {
			return nil
		}

		localFiles[relPath] = true

		hash, err := calculateFileHash(path)
		if err != nil {
			log.Printf("Error calculating hash for %s: %v", path, err)
			return err
		}

		fileMetadata, exists := bucketMetadata.Files[relPath]
		if !exists || fileMetadata.Bucket.Hash != hash {
			log.Printf("Uploading file: %s", relPath)
			err = uploadFile(ctx, bucket, path, relPath)
			if err != nil {
				log.Printf("Error uploading file %s: %v", relPath, err)
				return err
			}
			metadata.UpdateFileMetadata(bucketMetadata, relPath, false, info.ModTime(), hash, false)
		}

		metadata.UpdateFileMetadata(bucketMetadata, relPath, true, info.ModTime(), hash, false)
		return nil
	})

	if err != nil {
		return err
	}

	// Handle local deletions
	for filename, fileMetadata := range bucketMetadata.Files {
		if !localFiles[filename] && !fileMetadata.Local.Deleted {
			log.Printf("Deleting file from bucket: %s", filename)
			err := bucket.Delete(ctx, filename)
			if err != nil {
				log.Printf("Error deleting file from bucket %s: %v", filename, err)
				return err
			}
			metadata.UpdateFileMetadata(bucketMetadata, filename, true, time.Now(), "", true)
			metadata.UpdateFileMetadata(bucketMetadata, filename, false, time.Now(), "", true)
		}
	}

	return nil
}

func syncBucketToLocal(ctx context.Context, localDir string, bucket *blob.Bucket, bucketMetadata *metadata.BucketMetadata) error {
	log.Println("Syncing bucket to local")
	bucketFiles := make(map[string]bool)

	iter := bucket.List(&blob.ListOptions{Prefix: ""})
	for {
		obj, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error listing bucket objects: %v", err)
			return err
		}

		if obj.Key == metadata.MetadataFileName {
			continue
		}

		bucketFiles[obj.Key] = true

		localPath := filepath.Join(localDir, obj.Key)
		localInfo, err := os.Stat(localPath)
		if err != nil && !os.IsNotExist(err) {
			log.Printf("Error checking local file %s: %v", localPath, err)
			return err
		}

		if os.IsNotExist(err) {
			log.Printf("Downloading new file: %s", obj.Key)
			err = downloadFile(ctx, bucket, localPath, obj.Key)
			if err != nil {
				log.Printf("Error downloading file %s: %v", obj.Key, err)
				return err
			}
		} else if !localInfo.IsDir() {
			localHash, err := calculateFileHash(localPath)
			if err != nil {
				log.Printf("Error calculating hash for %s: %v", localPath, err)
				return err
			}

			bucketHash, err := calculateBucketFileHash(ctx, bucket, obj.Key)
			if err != nil {
				log.Printf("Error calculating hash for bucket file %s: %v", obj.Key, err)
				return err
			}

			if bucketHash != localHash {
				log.Printf("Downloading updated file: %s", obj.Key)
				err = downloadFile(ctx, bucket, localPath, obj.Key)
				if err != nil {
					log.Printf("Error downloading file %s: %v", obj.Key, err)
					return err
				}
				metadata.UpdateFileMetadata(bucketMetadata, obj.Key, true, time.Now(), bucketHash, false)
			}
		}
	}

	err := filepath.Walk(localDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		relPath, err := filepath.Rel(localDir, path)
		if err != nil {
			return err
		}
		if !bucketFiles[relPath] {
			log.Printf("Deleting local file not in bucket: %s", relPath)
			err = os.Remove(path)
			if err != nil {
				log.Printf("Error deleting local file %s: %v", relPath, err)
				return err
			}
			delete(bucketMetadata.Files, relPath)
		}
		return nil
	})

	if err != nil {
		return err
	}

	for relPath := range bucketMetadata.Files {
		if !bucketFiles[relPath] {
			delete(bucketMetadata.Files, relPath)
		}
	}

	return nil
}

func calculateFileHash(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}

func calculateBucketFileHash(ctx context.Context, bucket *blob.Bucket, key string) (string, error) {
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

func uploadFile(ctx context.Context, bucket *blob.Bucket, localPath, remotePath string) error {
	data, err := os.ReadFile(localPath)
	if err != nil {
		return err
	}

	return bucket.WriteAll(ctx, remotePath, data, nil)
}

func downloadFile(ctx context.Context, bucket *blob.Bucket, localPath, remotePath string) error {
	data, err := bucket.ReadAll(ctx, remotePath)
	if err != nil {
		return err
	}

	return os.WriteFile(localPath, data, 0644)
}
