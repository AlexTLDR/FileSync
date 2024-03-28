package main

import (
	"context"
	"fmt"
	"io"
	"time"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/memblob"
)

func syncDirs(srcDir, destDir *blob.Bucket) error {

	srcDirIt := srcDir.List(&blob.ListOptions{})
	for {
		obj, err := srcDirIt.Next(context.Background())
		if err != nil && err != io.EOF {
			return err
		}
		if obj == nil {
			break
		}
		fmt.Println(obj.Key)
	}
	/*
		// Keep track of all files in the destination directory
		destFiles := make(map[string]bool)
		err := filepath.Walk(destDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if info.IsDir() {
				return nil
			}

			relPath, err := filepath.Rel(destDir, path)
			if err != nil {
				return err
			}

			destFiles[relPath] = false
			return nil
		})

		if err != nil {
			return err
		}

		// Walk through the source directory
		err = filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			// Skip hidden files and directories
			if info.Name()[0] == '.' {
				return nil
			}

			// Get the relative path within the source directory
			relPath, err := filepath.Rel(srcDir, path)
			if err != nil {
				return err
			}

			// Construct the destination path
			destPath := filepath.Join(destDir, relPath)

			// Check if the destination file exists
			destInfo, err := os.Stat(destPath)
			if os.IsNotExist(err) || info.ModTime().After(destInfo.ModTime()) {
				// If the destination file doesn't exist or the source file is newer,
				// create the destination file/directory
				if info.IsDir() {
					err = os.MkdirAll(destPath, 0755)
				} else {
					// Read the content
					data, err := os.ReadFile(path)
					if err != nil {
						return err
					}

					// Write the content to the destination file
					err = os.WriteFile(destPath, data, 0644)
					if err != nil {
						return err
					}
				}
			}

			// Mark the file as updated
			destFiles[relPath] = true
			return nil
		})

		if err != nil {
			return err	srcDirIt := srcDir.List(&blob.ListOptions{})
			for {
				obj, err := srcDirIt.Next(context.Background())
				if err != nil && err != io.EOF {
					return err
				}
				if obj == nil {
					break
				}
				fmt.Println(obj.Key)
			}
		}

		// Delete files in the destination directory that weren't updated
		for relPath, updated := range destFiles {
			if !updated {
				err = os.Remove(filepath.Join(destDir, relPath))
				if err != nil {
					return err
				}
			}
		}
	*/
	return nil

}

func main() {
	// Replace with your actual source and destination folder paths
	// dir1 := "test-data/dir1"
	//dir2 := "test-data/dir2"
	ctx := context.Background()
	dir1, err := blob.OpenBucket(ctx, "file:///home/alex/git/FileSync/test-data/dir1")
	if err != nil {
		panic(err) //
	}
	defer dir1.Close()

	dir2, err := blob.OpenBucket(ctx, "file:///home/alex/git/FileSync/test-data/dir2")
	if err != nil {
		panic(err) //
	}
	defer dir2.Close()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			err := syncDirs(dir1, dir2)
			if err != nil {
				fmt.Println("Error syncing dir1 to dir2:", err)
			}

			err = syncDirs(dir2, dir1)
			if err != nil {
				fmt.Println("Error syncing dir2 to dir1:", err)
			}
		}
	}
}
