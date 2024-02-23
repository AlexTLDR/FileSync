package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

func main() {
	// Replace with your actual source and destination folder paths
	srcDir := "test-data/dir1"
	destDir := "test-data/dir2"

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Walk through the source directory
			err := filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
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
						// Read the source file content
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

			if err != nil {
				fmt.Println("Error walking the source directory:", err)
			}
		}
	}
}
