package main

import (
	"context"
	"fmt"
	"github.com/AlexTLDR/FileSync/frontend"
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
		err := sync.SyncFiles(ctx, frontend.Dir, bucket)
		if err != nil {
			log.Printf("Error during sync: %v", err)
		}

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
