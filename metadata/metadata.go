package metadata

import (
	"context"
	"encoding/json"
	"gocloud.dev/blob"
	"time"
)

const MetadataFileName = "__metadata.json"

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
	return bucket.WriteAll(ctx, MetadataFileName, data, nil)
}

func LoadMetadataFromBucket(ctx context.Context, bucket *blob.Bucket) (BucketMetadata, error) {
	data, err := bucket.ReadAll(ctx, MetadataFileName)
	if err != nil {
		return BucketMetadata{Files: make(map[string]FileMetadata)}, nil
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
