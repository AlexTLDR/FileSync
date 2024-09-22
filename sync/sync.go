package sync

import (
	"context"
	"crypto/sha256"
	"fmt"
	"github.com/AlexTLDR/FileSync/frontend"
	"github.com/AlexTLDR/FileSync/metadata"
	"github.com/AlexTLDR/FileSync/storage"
	"gocloud.dev/blob"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"
)

func SyncFiles(ctx context.Context) {
	// Open the local directory as a bucket

	dirBucket, err := blob.OpenBucket(ctx, "file://"+frontend.Dir)
	if err != nil {
		log.Printf("Failed to open directory bucket: %v", err)
		return
	}
	defer dirBucket.Close()

	// Open the remote bucket
	bucketName := "gs://file-sync"
	bucket, err := blob.OpenBucket(ctx, bucketName)
	if err != nil {
		log.Printf("Failed to open remote bucket: %v", err)
		return
	}
	defer bucket.Close()

	// Walk through the local directory and upload files
	err = filepath.Walk(frontend.Dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		relPath, err := filepath.Rel(frontend.Dir, path)
		if err != nil {
			return fmt.Errorf("failed to get relative path: %v", err)
		}

		file, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("failed to open file %s: %v", path, err)
		}
		defer file.Close()

		writer, err := bucket.NewWriter(ctx, relPath, nil)
		if err != nil {
			return fmt.Errorf("failed to create writer for %s: %v", relPath, err)
		}
		defer writer.Close()

		if info.Size() > frontend.LargeFileSizeThreshold {
			err = storage.UploadLargeFile(file, writer, relPath)
		} else {
			_, err = io.Copy(writer, file)
		}

		if err != nil {
			return fmt.Errorf("failed to copy file %s: %v", relPath, err)
		}

		hash, err := calculateFileHash(path)
		if err != nil {
			log.Printf("Failed to calculate hash for %s: %v", relPath, err)
		} else {
			metadata.UploadedFiles[relPath] = metadata.FileMetadata{
				Hash:       hash,
				UploadTime: time.Now(),
			}
			log.Printf("Uploaded: %s (Hash: %s)", relPath, hash)
		}

		return nil
	})

	if err != nil {
		log.Printf("Error walking through directory: %v", err)
	} else {
		log.Println("All files uploaded successfully")
	}
}

func calculateFileHash(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}
