package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"gocloud.dev/blob"
	"io"
	"log"
	"os"
	"path/filepath"
)

type FileMetadata struct {
	ModTimeUnix    int64 `json:"mod_time_unix"`
	IsDeleted      bool  `json:"is_deleted"`
	DeleteTimeUnix int64 `json:"delete_time_unix"`
	LastSyncUnix   int64 `json:"last_sync_unix"`
}

type Metadata map[string]FileMetadata

const (
	BucketMetadataFileName = ".filesync_bucket_metadata.json"
	LocalMetadataFileName  = ".filesync_local_metadata.json"
)

func GetMetadata(ctx context.Context, bucket *blob.Bucket, isLocal bool) (Metadata, error) {
	metadataFileName := BucketMetadataFileName
	if isLocal {
		metadataFileName = filepath.Join("test-data", "dir1", LocalMetadataFileName)
	}

	metadata := make(Metadata)
	exists, err := bucket.Exists(ctx, metadataFileName)
	if err != nil {
		return nil, fmt.Errorf("error checking metadata file existence: %v", err)
	}
	if !exists {
		return metadata, nil
	}

	reader, err := bucket.NewReader(ctx, metadataFileName, nil)
	if err != nil {
		return nil, fmt.Errorf("error opening metadata file: %v", err)
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("error reading metadata file: %v", err)
	}

	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("error unmarshaling metadata: %v", err)
	}

	return metadata, nil
}

func UpdateMetadata(ctx context.Context, bucket *blob.Bucket, metadata Metadata, isLocal bool) error {
	metadataFileName := BucketMetadataFileName
	if isLocal {
		metadataFileName = filepath.Join("test-data", "dir1", LocalMetadataFileName)
	}

	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("error marshaling metadata: %v", err)
	}

	if isLocal {
		// For local operations, write directly to the file system
		dir := filepath.Dir(metadataFileName)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("error creating directory: %v", err)
		}
		if err := os.WriteFile(metadataFileName, data, 0644); err != nil {
			return fmt.Errorf("error writing local metadata file: %v", err)
		}
	} else {
		// For bucket operations, use the blob.Bucket interface
		if err := bucket.WriteAll(ctx, metadataFileName, data, nil); err != nil {
			return fmt.Errorf("error writing bucket metadata file: %v", err)
		}
	}

	log.Printf("Updated %s metadata file (%d bytes written)", metadataFileName, len(data))
	return nil
}

func CleanMetadata(ctx context.Context, bucket *blob.Bucket, metadata Metadata, isLocal bool) (Metadata, error) {
	cleanedMetadata := make(Metadata)
	for key, data := range metadata {
		exists, err := bucket.Exists(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("error checking existence of %s: %v", key, err)
		}
		if exists {
			cleanedMetadata[key] = data
		} else {
			log.Printf("Removing %s from metadata as it no longer exists in the %s", key, map[bool]string{true: "local directory", false: "bucket"}[isLocal])
		}
	}
	return cleanedMetadata, nil
}

func MetadataExists(ctx context.Context, bucket *blob.Bucket, isLocal bool) bool {
	metadataFileName := BucketMetadataFileName
	if isLocal {
		metadataFileName = LocalMetadataFileName
	}

	exists, _ := bucket.Exists(ctx, metadataFileName)
	return exists
}
