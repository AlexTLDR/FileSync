package storage

import (
	"github.com/AlexTLDR/FileSync/frontend"
	"io"
	"log"
	"time"
)

func UploadLargeFile(src io.Reader, dst io.Writer, filename string) error {
	buf := make([]byte, 32*1024)
	ticker := time.NewTicker(frontend.ProgressUpdateInterval)
	defer ticker.Stop()

	go func() {
		for range ticker.C {
			log.Printf("Upload of file %s in progress...", filename)
		}
	}()

	_, err := io.CopyBuffer(dst, src, buf)
	ticker.Stop()
	return err
}
