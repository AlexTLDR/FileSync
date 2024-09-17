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
	ModTime     time.Time `json:"mod_time"`
	IsDeleted   bool      `json:"is_deleted"`
	DeletedTime time.Time `json:"deleted_time"`
	HasChanged  bool      `json:"-"`
}

type BucketMetadata map[string]FileMetadata

const MetadataFileName = ".filesync_metadata.json"

func UpdateBucketMetadata(ctx context.Context, bucket *blob.Bucket, metadata BucketMetadata) error {
	data, err := json.MarshalIndent(metadata, "", " ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %v", err)
	}

	w, err := bucket.NewWriter(ctx, MetadataFileName, nil)
	if err != nil {
		return fmt.Errorf("failed to create writer for metadata: %v", err)
	}
	defer w.Close()

	_, err = w.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write metadata: %v", err)
	}

	return nil
}

func GetBucketMetadata(ctx context.Context, bucket *blob.Bucket) (BucketMetadata, error) {
	exists, err := bucket.Exists(ctx, MetadataFileName)
	if err != nil {
		log.Printf("Error checking metadata file existence: %v", err)
		return nil, fmt.Errorf("failed to check metadata existence: %v", err)
	}

	if !exists {
		log.Println("Metadata file does not exist. Creating new file.")
		initialMetadata := BucketMetadata{}
		if err := UpdateBucketMetadata(ctx, bucket, initialMetadata); err != nil {
			log.Printf("Failed to create initial metadata: %v", err)
			return nil, fmt.Errorf("failed to create initial metadata: %v", err)
		}
		log.Println("New metadata file created successfully.")
		return initialMetadata, nil
	}

	r, err := bucket.NewReader(ctx, MetadataFileName, nil)
	if err != nil {
		log.Printf("Error opening metadata file: %v", err)
		return nil, fmt.Errorf("failed to open metadata: %v", err)
	}
	defer r.Close()

	data, err := io.ReadAll(r)
	if err != nil {
		log.Printf("Error reading metadata content: %v", err)
		return nil, fmt.Errorf("failed to read metadata content: %v", err)
	}

	var Metadata BucketMetadata
	if err := json.Unmarshal(data, &Metadata); err != nil {
		log.Printf("Error unmarshaling metadata: %v", err)
		return nil, fmt.Errorf("failed to unmarshal metadata: %v", err)
	}
	log.Println("Metadata retrieved successfully.")
	return Metadata, nil
}
