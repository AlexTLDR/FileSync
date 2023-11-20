package main

import (
	"bytes"
	"io"
	"log"
	"os"
)

func main() {
	file1, err := os.Open("/home/alex/Git/FileSync/go.mod")
	if err != nil {
		log.Fatal(err)
	}
	defer file1.Close()

	file2, err := os.Open("/home/alex/Git/FileSync/test")
	if err != nil {
		log.Fatal(err)
	}
	defer file2.Close()

	content1, err := io.ReadAll(file1)
	if err != nil {
		log.Fatal(err)
	}

	content2, err := io.ReadAll(file2)
	if err != nil {
		log.Fatal(err)
	}

	if bytes.Equal(content1, content2) {
		log.Println("Files are the same")
	} else {
		log.Println("Files are different")
	}
}
