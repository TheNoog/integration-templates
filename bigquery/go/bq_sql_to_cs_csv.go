
/*

exports a SQL statement result to a CSV file stored in a Google Cloud Storage bucket:

Replace "your-project-id", "your_dataset.your_table", "your-bucket-name" and "your-output-file-path.csv" with your own values.

Note: The program requires the BigQuery Go Client and Google Cloud Storage Go Client libraries to be installed. 
You can install them using the following commands: go get cloud.google.com/go/bigquery and go get cloud.google.com/go/storage.

*/

package main

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

func main() {
	// Set the Google Cloud project ID, SQL statement and GCS output file path
	projectID := "your-project-id"
	sql := "SELECT column1, column2 FROM your_dataset.your_table"
	gcsBucket := "your-bucket-name"
	gcsObject := "your-output-file-path.csv"

	// Create a Google Cloud Storage client
	ctx := context.Background()
	storageClient, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create storage client: %v", err)
	}

	// Create a Google Cloud BigQuery client
	bqClient, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("Failed to create BigQuery client: %v", err)
	}

	// Create a Google Cloud Storage writer to the CSV file
	gcsWriter := storageClient.Bucket(gcsBucket).Object(gcsObject).NewWriter(ctx)
	defer gcsWriter.Close()

	// Create a BigQuery query job to export the SQL statement result to the GCS file
	queryJob := bqClient.Query(sql)
	queryJob.Location = "US"
	queryJob.WriteTo(gcsWriter)
	queryJob.Format = bigquery.CSV

	// Wait for the query job to complete
	jobStatus, err := queryJob.Run(ctx)
	if err != nil {
		log.Fatalf("Failed to run query job: %v", err)
	}
	status := jobStatus.Status()
	for !status.Done() {
		status, err = status.Wait(ctx)
		if err != nil {
			log.Fatalf("Failed to wait for job to complete: %v", err)
		}
	}

	// Check for errors in the query job result
	if err := status.Err(); err != nil {
		log.Fatalf("Failed to export result to GCS: %v", err)
	}

	// Print success message
	fmt.Printf("SQL result exported to gs://%s/%s\n", gcsBucket, gcsObject)
}
