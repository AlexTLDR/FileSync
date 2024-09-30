package sync

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"github.com/AlexTLDR/FileSync/metadata"
	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"
)

func SyncFiles(ctx context.Context, localDir string, bucket *blob.Bucket) error {
	log.Println("Starting file synchronization")
	bucketMetadata, err := metadata.LoadMetadataFromBucket(ctx, bucket)
	if err != nil {
		log.Printf("Error loading bucket metadata: %v", err)
		return err
	}

	// Sync local files to bucket
	err = syncLocalToBucket(ctx, localDir, bucket, &bucketMetadata)
	if err != nil {
		return err
	}

	// Sync bucket files to local
	err = syncBucketToLocal(ctx, localDir, bucket, &bucketMetadata)
	if err != nil {
		return err
	}

	log.Println("Saving updated metadata to bucket")
	return metadata.SaveMetadataToBucket(ctx, bucket, bucketMetadata)
}

func syncLocalToBucket(ctx context.Context, localDir string, bucket *blob.Bucket, bucketMetadata *metadata.BucketMetadata) error {
	return filepath.Walk(localDir, func(path string, info os.FileInfo, err error) error {
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

		hash, err := calculateFileHash(path)
		if err != nil {
			log.Printf("Error calculating hash for %s: %v", path, err)
			return err
		}

		metadata.UpdateFileMetadata(bucketMetadata, relPath, true, info.ModTime(), hash, false)

		shouldSync, uploadToBucket := metadata.ShouldSyncFile(bucketMetadata.Files[relPath])
		if shouldSync {
			if uploadToBucket {
				log.Printf("Uploading file: %s", relPath)
				err = uploadFile(ctx, bucket, path, relPath)
			} else {
				log.Printf("Downloading file: %s", relPath)
				err = downloadFile(ctx, bucket, path, relPath)
			}
			if err != nil {
				log.Printf("Error syncing file %s: %v", relPath, err)
				return err
			}
		}

		return nil
	})
}

func syncBucketToLocal(ctx context.Context, localDir string, bucket *blob.Bucket, bucketMetadata *metadata.BucketMetadata) error {
	iter := bucket.List(nil)
	for {
		obj, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error listing bucket objects: %v", err)
			return err
		}

		localPath := filepath.Join(localDir, obj.Key)
		if _, err := os.Stat(localPath); os.IsNotExist(err) {
			log.Printf("Downloading missing file: %s", obj.Key)
			err = downloadFile(ctx, bucket, localPath, obj.Key)
			if err != nil {
				log.Printf("Error downloading file %s: %v", obj.Key, err)
				return err
			}
			fileInfo, err := os.Stat(localPath)
			if err != nil {
				log.Printf("Error getting file info for %s: %v", localPath, err)
				return err
			}
			hash, err := calculateFileHash(localPath)
			if err != nil {
				log.Printf("Error calculating hash for %s: %v", localPath, err)
				return err
			}
			metadata.UpdateFileMetadata(bucketMetadata, obj.Key, true, fileInfo.ModTime(), hash, false)
		}
	}

	// Handle deletions
	for filename := range bucketMetadata.Files {
		localPath := filepath.Join(localDir, filename)
		_, err := bucket.Attributes(ctx, filename)
		if err != nil {
			if gcerrors.Code(err) == gcerrors.NotFound {
				log.Printf("File deleted from bucket, deleting locally: %s", filename)
				os.Remove(localPath)
				metadata.UpdateFileMetadata(bucketMetadata, filename, true, time.Now(), "", true)
				metadata.UpdateFileMetadata(bucketMetadata, filename, false, time.Now(), "", true)
			} else {
				log.Printf("Error checking bucket file %s: %v", filename, err)
				return err
			}
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

	return hex.EncodeToString(hash.Sum(nil)), nil
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