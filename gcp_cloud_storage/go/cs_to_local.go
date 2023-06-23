package main

/*

The program takes the bucket name, bucket folder path, and local folder path as command-line arguments.

This program uses the flag package to parse command-line arguments. The downloadBlobToLocalFile function

*/


import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"cloud.google.com/go/storage"
)

func main() {
	// Parse command-line arguments
	bucketName := flag.String("bucket-name", "", "Name of the Google Cloud Storage bucket")
	bucketFolder := flag.String("bucket-folder", "", "Path of the Google Cloud Storage bucket folder")
	localPath := flag.String("local-path", "", "Path of the local directory to copy files to")
	flag.Parse()

	// Validate command-line arguments
	if *bucketName == "" {
		log.Fatal("Bucket name argument is required")
	}
	if *bucketFolder == "" {
		log.Fatal("Bucket folder argument is required")
	}
	if *localPath == "" {
		log.Fatal("Local path argument is required")
	}

	// Initialize Google Cloud Storage client
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// List blobs in bucket folder
	bucket := client.Bucket(*bucketName)
	query := &storage.Query{Prefix: *bucketFolder}
	objIter := bucket.Objects(ctx, query)
	for {
		objAttrs, err := objIter.Next()
		if err == storage.ErrObjectIteratorDone {
			break
		}
		if err != nil {
			log.Printf("Failed to iterate objects in bucket %q: %v", *bucketName, err)
			continue
		}
		if objAttrs.Size == 0 {
			// Skip zero-sized blobs
			continue
		}

		// Download blob to local file
		localFilePath := filepath.Join(*localPath, objAttrs.Name)
		err = downloadBlobToLocalFile(ctx, bucket, objAttrs.Name, localFilePath)
		if err != nil {
			log.Printf("Failed to download blob %q to local file %q: %v", objAttrs.Name, localFilePath, err)
			continue
		}
	}
}

func downloadBlobToLocalFile(ctx context.Context, bucket *storage.BucketHandle, blobName string, localFilePath string) error {
	// Create local file
	localFile, err := os.Create(localFilePath)
	if err != nil {
		return fmt.Errorf("Failed to create local file %q: %v", localFilePath, err)
	}
	defer localFile.Close()

	// Get blob handle
	blob := bucket.Object(blobName)

	// Download blob to local file
	reader, err := blob.NewReader(ctx)
	if err != nil {
		return fmt.Errorf("Failed to create blob reader for blob %q: %v", blobName, err)
	}
	defer reader.Close()

	_, err = ioutil.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("Failed to read blob %q: %v", blobName, err)
	}

	_, err = reader.Seek(0, 0)
	if err != nil {
		return fmt.Errorf("Failed to seek to start of blob %q: %v", blobName, err)
	}

	_, err = io.Copy(localFile, reader)
	if err != nil {
		return fmt.Errorf("Failed to copy blob %q to local file %q: %v", blobName, localFilePath, err)
	}

	return nil
}
