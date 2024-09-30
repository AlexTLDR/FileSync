package frontend

import "time"

const (
	BucketName             = "gs://file-sync"
	Dir                    = "/home/alex/git/FileSync/test-data/dir1"
	LargeFileSizeThreshold = 100 * 1024 * 1024 // 100 MB
	ProgressUpdateInterval = 10 * time.Second
	SyncTime               = 15 * time.Second
)
