package storage

import (
	"context"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

func NewClient(ctx context.Context, credsFilePath string) (*storage.Client, error) {
	return storage.NewClient(ctx, option.WithCredentialsFile(credsFilePath))
}
