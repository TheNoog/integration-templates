
/*

deletes all blobs in a specified folder:

Replace "your-bucket-name" and "your-folder-path" with the name of your Google Cloud Storage bucket 
and the path of the folder that contains the blobs you want to delete.

Note: The program requires the Google Cloud Storage Go Client library to be installed. You can 
install it using the following command: go get cloud.google.com/go/storage.

*/


package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"cloud.google.com/go/storage"
)

func main() {
	// Set the Google Cloud Storage bucket and folder path
	bucketName := "your-bucket-name"
	folderPath := "your-folder-path/"

	// Create a Google Cloud Storage client
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	// Get the specified bucket
	bucket := client.Bucket(bucketName)

	// Get all objects in the specified folder
	query := &storage.Query{Prefix: folderPath}
	objects := bucket.Objects(ctx, query)

	// Delete each object in the folder
	for {
		objAttrs, err := objects.Next()
		if err == storage.ErrObjectNotExist {
			break // No more objects in the folder
		}
		if err != nil {
			log.Fatalf("Failed to get object: %v", err)
		}

		// Delete the object
		err = bucket.Object(objAttrs.Name).Delete(ctx)
		if err != nil {
			log.Fatalf("Failed to delete object: %v", err)
		}
		fmt.Printf("Deleted object: %v\n", objAttrs.Name)
	}

	fmt.Println("All objects deleted successfully")
}
