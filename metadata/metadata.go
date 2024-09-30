package metadata

import (
	"context"
	"encoding/json"
	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"
	"time"
)

type FileStatus struct {
	CreationTime time.Time `json:"creationTime"`
	Hash         string    `json:"hash"`
	Deleted      bool      `json:"deleted"`
}

type FileMetadata struct {
	Local  FileStatus `json:"local"`
	Bucket FileStatus `json:"bucket"`
}

type BucketMetadata struct {
	Files map[string]FileMetadata `json:"files"`
}

func SaveMetadataToBucket(ctx context.Context, bucket *blob.Bucket, metadata BucketMetadata) error {
	data, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	return bucket.WriteAll(ctx, "metadata.json", data, nil)
}

func LoadMetadataFromBucket(ctx context.Context, bucket *blob.Bucket) (BucketMetadata, error) {
	data, err := bucket.ReadAll(ctx, "metadata.json")
	if err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			return BucketMetadata{Files: make(map[string]FileMetadata)}, nil
		}
		return BucketMetadata{}, err
	}

	var metadata BucketMetadata
	err = json.Unmarshal(data, &metadata)
	if err != nil {
		return BucketMetadata{}, err
	}

	return metadata, nil
}

func UpdateFileMetadata(metadata *BucketMetadata, filename string, isLocal bool, creationTime time.Time, hash string, deleted bool) {
	fileMetadata, exists := metadata.Files[filename]
	if !exists {
		fileMetadata = FileMetadata{}
	}

	status := FileStatus{
		CreationTime: creationTime,
		Hash:         hash,
		Deleted:      deleted,
	}

	if isLocal {
		fileMetadata.Local = status
	} else {
		fileMetadata.Bucket = status
	}

	metadata.Files[filename] = fileMetadata
}

func ShouldSyncFile(metadata FileMetadata) (bool, bool) {
	if metadata.Local.Deleted && !metadata.Bucket.Deleted {
		return true, true // Delete from bucket
	}
	if !metadata.Local.Deleted && metadata.Bucket.Deleted {
		return true, false // Restore locally
	}
	if metadata.Local.Hash != metadata.Bucket.Hash {
		return true, metadata.Local.CreationTime.After(metadata.Bucket.CreationTime)
	}
	return false, false
}
