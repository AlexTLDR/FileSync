package main

import (
	"context"
	"fmt"
	"io"
	"log"

	"cloud.google.com/go/storage"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/gcsblob"
	"google.golang.org/api/option"
)

func syncDirToBucket(dir, bucket *blob.Bucket) error {
	bucketMap := make(map[string]*blob.ListObject)
	dirMap := make(map[string]*blob.ListObject)
	iter2 := bucket.List(nil)
	for {
		obj, err := iter2.Next(context.Background())
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		bucketMap[obj.Key] = obj
	}
	iter1 := dir.List(nil)
	for {
		obj, err := iter1.Next(context.Background())
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		dirMap[obj.Key] = obj
	}
	for _, obj := range dirMap {
		fmt.Println(obj.Key, obj.Size, obj.ModTime, obj.MD5)
	}
	for _, obj := range bucketMap {
		fmt.Println(obj.Key, obj.Size, obj.ModTime, obj.MD5)
	}
	// ctx := context.Background()
	// for {
	// 	// Walk through the local directory
	// 	if err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
	// 		if err != nil {
	// 			return err
	// 		}

	// 		// Skip directories and hidden files
	// 		if info.IsDir() || info.Name()[0] == '.' {
	// 			return nil
	// 		}

	// 		// Get the relative path within the local directory
	// 		relPath, err := filepath.Rel(dir, path)
	// 		if err != nil {
	// 			return err
	// 		}

	// 		// Check if the file exists in the bucket
	// 		r, err := bucket.NewReader(ctx, relPath, nil)
	// 		if err != nil {
	// 			// If the file does not exist in the bucket, upload it
	// 			// Open local file
	// 			f, err := os.Open(path)
	// 			if err != nil {
	// 				return err
	// 			}
	// 			defer f.Close()

	// 			// Write the local file to the bucket
	// 			w, err := bucket.NewWriter(ctx, relPath, nil)
	// 			if err != nil {
	// 				return err
	// 			}

	// 			_, err = io.Copy(w, f)
	// 			if err != nil {
	// 				w.Close()
	// 				return err
	// 			}

	// 			return w.Close()
	// 		}

	// 		// If the file exists in the bucket, check its modification time
	// 		defer r.Close()

	// 		var attrs storage.ObjectAttrs
	// 		if r.As(&attrs) {
	// 			// If the file in the bucket is newer, skip this file
	// 			if attrs.Updated.After(info.ModTime()) {
	// 				return nil
	// 			}
	// 		}

	// 		// If the file in the bucket is older, upload the local file
	// 		// Open local file
	// 		f, err := os.Open(path)

	// 		if err != nil {
	// 			fmt.Println("Error syncing directory to bucket:", err)
	// 		}
	// 		if err != nil {
	// 			return err
	// 		}
	// 		defer f.Close()

	// 		// Write the local file to the bucket
	// 		w, err := bucket.NewWriter(ctx, relPath, nil)
	// 		if err != nil {
	// 			return err
	// 		}

	// 		_, err = io.Copy(w, f)
	// 		if err != nil {
	// 			w.Close()
	// 			return err
	// 		}

	// 		return w.Close()
	// 	}); err != nil {

	// 		return err
	// 	}
	// 	time.Sleep(1 * time.Second)
	// }
	return nil
}

func main() {
	ctx := context.Background()

	// Replace with your actual local directory, credentials file path, and bucket name
	// dir := "test-data/dir1"
	credsFilePath := "key/filesync-415212-ecb8c3396d06.json"
	// credsFilePath := "/home/alex/.config/gcloud/application_default_credentials.json"
	bucketName := "gs://file-sync"

	// Create a Google Cloud client with the credentials file
	_, err := storage.NewClient(ctx, option.WithCredentialsFile(credsFilePath))
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	// Get a reference to the bucket
	// bucket := client.Bucket(bucketName)
	dir1, err := blob.OpenBucket(ctx, "file:///home/alex/git/FileSync/test-data/dir1")
	if err != nil {
		panic(err) //
	}
	defer dir1.Close()
	// Open the bucket
	bucketHandle, err := blob.OpenBucket(ctx, bucketName)
	if err != nil {
		panic(err)
	}
	defer bucketHandle.Close()

	// Sync the local directory to the bucket
	err = syncDirToBucket(dir1, bucketHandle)
	if err != nil {
		fmt.Println("Error syncing directory to bucket:", err)
	}
}
