package main

import (
	"fmt"
	"os"
	"path/filepath"
)

func visitFile(path string, info os.FileInfo, err error) error {
	if err != nil {
		fmt.Println("Error:", err)
		return nil
	}

	if !info.IsDir() {
		fmt.Println("File:", path)
	}

	return nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run scanner.go <directory>")
		os.Exit(1)
	}

	root := os.Args[1]

	fmt.Println("Scanning directory:", root)

	err := filepath.Walk(root, visitFile)

	if err != nil {
		fmt.Println("Error:", err)
	}
}
