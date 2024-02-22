package main

import (
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load(".env")
	if err != nil {
		fmt.Println("Failed to load env file", err)
		return
	}

	filePath := "test.txt"
	bucketName := os.Getenv("BUCKET_NAME")
	fileKey := os.Getenv("FILE_KEY")

	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println("Failed to open file", err)
		return
	}
	defer file.Close()

	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String("us-west-2")},
	)

	uploader := s3manager.NewUploader(sess)

	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(fileKey),
		Body:   file,
	})
	if err != nil {
		fmt.Println("Failed to upload", err)
		return
	}
	fmt.Println("Successfully uploaded to", bucketName)
}
