package main

/*
This program uses the flag package to parse command-line arguments. 
The copyFileToBucket function is called for each file in the local directory, which opens the file, 
creates a handle for the Cloud Storage bucket and object, and uploads the file to the object.

To use this program, save it in a file named `copy

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
	localPath := flag.String("local-path", "", "Path of the local directory to copy")
	bucketName := flag.String("bucket-name", "", "Name of the Google Cloud Storage bucket to copy to")
	flag.Parse()

	// Validate command-line arguments
	if *localPath == "" {
		log.Fatal("Local path argument is required")
	}
	if *bucketName == "" {
		log.Fatal("Bucket name argument is required")
	}

	// Initialize Google Cloud Storage client
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Copy files from local directory to Cloud Storage bucket
	err = filepath.Walk(*localPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Printf("Failed to access path %q: %v", path, err)
			return err
		}
		if !info.Mode().IsRegular() {
			// Skip non-regular files (e.g., directories)
			return nil
		}
		objectName := filepath.Base(path)
		err = copyFileToBucket(ctx, client, *bucketName, objectName, path)
		if err != nil {
			log.Printf("Failed to copy file %q to bucket %q: %v", path, *bucketName, err)
			return err
		}
		return nil
	})
	if err != nil {
		log.Fatalf("Failed to copy files to bucket %q: %v", *bucketName, err)
	}
}

func copyFileToBucket(ctx context.Context, client *storage.Client, bucketName string, objectName string, filePath string) error {
	// Open local file
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("Failed to open file %q: %v", filePath, err)
	}
	defer file.Close()

	// Get bucket handle
	bucket := client.Bucket(bucketName)

	// Create object handle
	object := bucket.Object(objectName)

	// Upload file to object
	writer := object.NewWriter(ctx)
	_, err = ioutil.ReadAll(file)
	if err != nil {
		return fmt.Errorf("Failed to read file %q: %v", filePath, err)
	}
	_, err = file.Seek(0, 0)
	if err != nil {
		return fmt.Errorf("Failed to seek file %q: %v", filePath, err)
	}
	_, err = io.Copy(writer, file)
	if err != nil {
		return fmt.Errorf("Failed to upload file %q to object %q: %v", filePath, objectName, err)
	}

	// Close object
	err = writer.Close()
	if err != nil {
		return fmt.Errorf("Failed to close object %q: %v", objectName, err)
	}

	return nil
}
