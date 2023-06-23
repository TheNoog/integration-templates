/*

runs a SQL query:

Replace "your-project-id", "your-dataset-id", "your-table-id", and 
"SELECT COUNT(*) as count FROM your-project-id.your-dataset-id.your-table-id" with your own values.

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
	// Set the Google Cloud project ID, dataset ID, and SQL query
	projectID := "your-project-id"
	datasetID := "your-dataset-id"
	sqlQuery := "SELECT COUNT(*) as count FROM `your-project-id.your-dataset-id.your-table-id`"

	// Create a Google Cloud BigQuery client
	ctx := context.Background()
	bqClient, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("Failed to create BigQuery client: %v", err)
	}

	// Run the SQL query
	query := bqClient.Query(sqlQuery)
	job, err := query.Run(ctx)
	if err != nil {
		log.Fatalf("Failed to run query %s: %v", sqlQuery, err)
	}

	// Wait for the query to complete and get the results
	status, err := job.Wait(ctx)
	if err != nil {
		log.Fatalf("Failed to wait for query %s: %v", sqlQuery, err)
	}
	if err := status.Err(); err != nil {
		log.Fatalf("Query %s failed: %v", sqlQuery, err)
	}
	it, err := job.Read(ctx)
	if err != nil {
		log.Fatalf("Failed to read results for query %s: %v", sqlQuery, err)
	}

	// Parse the results
	var row struct {
		Count int64 `bigquery:"count"`
	}
	if err := it.Next(&row); err != nil {
		log.Fatalf("Failed to read results for query %s: %v", sqlQuery, err)
	}

	// Print the results
	fmt.Printf("Count: %d\n", row.Count)
}
