package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"time"

	"gocloud.dev/blob"
)

type FileMetadata struct {
	BucketModTime    time.Time `json:"bucket_mod_time"`
	LocalModTime     time.Time `json:"local_mod_time"`
	IsDeletedBucket  bool      `json:"is_deleted_bucket"`
	IsDeletedLocal   bool      `json:"is_deleted_local"`
	BucketDeleteTime time.Time `json:"bucket_delete_time"`
	LocalDeleteTime  time.Time `json:"local_delete_time"`
	LastSyncTime     time.Time `json:"last_sync_time"`
}

type BucketMetadata map[string]FileMetadata

const MetadataFileName = ".filesync_metadata.json"

func GetBucketMetadata(ctx context.Context, bucket *blob.Bucket) (BucketMetadata, error) {
	metadata := make(BucketMetadata)
	exists, err := bucket.Exists(ctx, MetadataFileName)
	if err != nil {
		return nil, fmt.Errorf("error checking metadata file existence: %v", err)
	}
	if !exists {
		return metadata, nil
	}

	reader, err := bucket.NewReader(ctx, MetadataFileName, nil)
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

func UpdateBucketMetadata(ctx context.Context, bucket *blob.Bucket, metadata BucketMetadata) error {
	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("error marshaling metadata: %v", err)
	}

	writer, err := bucket.NewWriter(ctx, MetadataFileName, nil)
	if err != nil {
		return fmt.Errorf("error creating metadata file writer: %v", err)
	}
	defer writer.Close()

	if _, err := writer.Write(data); err != nil {
		return fmt.Errorf("error writing metadata file: %v", err)
	}

	return nil
}

func CleanMetadata(ctx context.Context, bucket *blob.Bucket, metadata BucketMetadata) (BucketMetadata, error) {
	cleanedMetadata := make(BucketMetadata)
	for key, data := range metadata {
		exists, err := bucket.Exists(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("error checking existence of %s: %v", key, err)
		}
		if exists {
			cleanedMetadata[key] = data
		} else {
			log.Printf("Removing %s from metadata as it no longer exists in the bucket", key)
		}
	}
	return cleanedMetadata, nil
}
