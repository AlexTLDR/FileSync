package main

import (
	"context"
	"fmt"
	"github.com/AlexTLDR/FileSync/frontend"
	"github.com/AlexTLDR/FileSync/metadata"
	"github.com/AlexTLDR/FileSync/sync"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/gcsblob"
	"log"
	"time"
)

func main() {
	ctx := context.Background()
	bucket, err := openBucket(ctx)
	if err != nil {
		log.Fatalf("Failed to open bucket: %v", err)
	}
	defer bucket.Close()

	for {
		bucketMetadata, err := metadata.LoadMetadataFromBucket(ctx, bucket)
		if err != nil {
			log.Printf("Failed to load metadata from bucket: %v", err)
			continue
		}

		err = sync.SyncFiles(ctx, frontend.Dir, bucket)
		if err != nil {
			log.Printf("Failed to sync files: %v", err)
		}

		err = metadata.SaveMetadataToBucket(ctx, bucket, bucketMetadata)
		if err != nil {
			log.Printf("Failed to save metadata to bucket: %v", err)
		}

		// Sleep for a specified interval before the next sync
		time.Sleep(frontend.SyncTime)
	}
}
func openBucket(ctx context.Context) (*blob.Bucket, error) {

	bucket, err := blob.OpenBucket(ctx, frontend.BucketName)
	if err != nil {
		return nil, fmt.Errorf("failed to open bucket: %v", err)
	}
	return bucket, nil
}
