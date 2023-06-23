
/*

creates a new dataset:

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
	"google.golang.org/api/iterator"
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

	// Check if the dataset already exists
	_, err = bqClient.Dataset(datasetID).Metadata(ctx)
	if err == nil {
		log.Fatalf("Dataset %s already exists", datasetID)
	}

	// Create the dataset
	if err := bqClient.Dataset(datasetID).Create(ctx, &bigquery.DatasetMetadata{}); err != nil {
		log.Fatalf("Failed to create dataset %s: %v", datasetID, err)
	}

	// Print success message
	fmt.Printf("Dataset %s created\n", datasetID)
}
