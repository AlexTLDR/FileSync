package main

import (
	"context"
	"github.com/AlexTLDR/FileSync/metadata"
	"github.com/AlexTLDR/FileSync/sync"
	"log"
	"time"

	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/gcsblob"
)

func main() {
	ctx := context.Background()

	for {
		sync.SyncFiles(ctx)
		if err := metadata.SaveMetadata(); err != nil {
			log.Printf("Failed to save metadata: %v", err)
		}
		time.Sleep(5 * time.Minute)
	}
}
