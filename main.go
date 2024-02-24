package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

func syncDirs(srcDir, destDir string) error {
	// Walk through the source directory
	return filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
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
		if _, err := os.Stat(destPath); os.IsNotExist(err) {
			// Create the destination file/directory if it doesn't exist
			if info.IsDir() {
				err = os.MkdirAll(destPath, 0755)
			} else {
				// Read the file content
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

		return nil
	})
}

func main() {
	// Replace with your actual source and destination folder paths
	dir1 := "test-data/dir1"
	dir2 := "test-data/dir2"

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
