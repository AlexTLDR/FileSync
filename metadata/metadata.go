package metadata

import (
	"encoding/json"
	"github.com/AlexTLDR/FileSync/frontend"
	"os"
	"path/filepath"
	"time"
)

type FileMetadata struct {
	Hash       string    `json:"hash"`
	UploadTime time.Time `json:"upload_time"`
}

var UploadedFiles = make(map[string]FileMetadata)

func SaveMetadata() error {
	data, err := json.MarshalIndent(UploadedFiles, "", "  ")
	if err != nil {
		return err
	}
	metadataPath := filepath.Join(frontend.Dir, "uploaded_files_metadata.json")
	return os.WriteFile(metadataPath, data, 0644)
}
