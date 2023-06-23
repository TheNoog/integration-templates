
/*
deletes a dataset:

Replace "your-project-id" and "your-dataset-id" with your own values.

Note: The program requires the BigQuery Go Client library to be installed. 
You can install it using the following command: go get cloud.google.com/go/bigquery.

*/

package main

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/bigquery"
)

func main() {
	// Set the Google Cloud project ID and dataset ID
	projectID := "your-project-id"
	datasetID := "your-dataset-id"

	// Create a Google Cloud BigQuery client
	ctx := context.Background()
	bqClient, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("Failed to create BigQuery client: %v", err)
	}

	// Check if the dataset exists
	ds := bqClient.Dataset(datasetID)
	if _, err := ds.Metadata(ctx); err != nil {
		log.Fatalf("Failed to get metadata for dataset %s: %v", datasetID, err)
	}

	// Delete the dataset
	if err := ds.Delete(ctx); err != nil {
		log.Fatalf("Failed to delete dataset %s: %v", datasetID, err)
	}

	// Print success message
	fmt.Printf("Dataset %s deleted\n", datasetID)
}
