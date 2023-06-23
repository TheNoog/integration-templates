/*

creates a table from a JSON schema file:

Replace "your-project-id", "your-dataset-id", "your-table-id", and "your-schema-file.json" with your own values.

Note: The program requires the BigQuery Go Client library to be installed. 
You can install it using the following command: go get cloud.google.com/go/bigquery.

*/


package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"

	"cloud.google.com/go/bigquery"
)

func main() {
	// Set the Google Cloud project ID, dataset ID, and table ID
	projectID := "your-project-id"
	datasetID := "your-dataset-id"
	tableID := "your-table-id"

	// Read the JSON schema file
	schemaFile := "your-schema-file.json"
	schemaBytes, err := ioutil.ReadFile(schemaFile)
	if err != nil {
		log.Fatalf("Failed to read schema file %s: %v", schemaFile, err)
	}

	// Parse the JSON schema
	var schema bigquery.Schema
	if err := json.Unmarshal(schemaBytes, &schema); err != nil {
		log.Fatalf("Failed to parse schema file %s: %v", schemaFile, err)
	}

	// Create a Google Cloud BigQuery client
	ctx := context.Background()
	bqClient, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("Failed to create BigQuery client: %v", err)
	}

	// Create the table
	tableRef := bqClient.Dataset(datasetID).Table(tableID)
	if err := tableRef.Create(ctx, &bigquery.TableMetadata{Schema: schema}); err != nil {
		log.Fatalf("Failed to create table %s.%s.%s: %v", projectID, datasetID, tableID, err)
	}

	// Print success message
	fmt.Printf("Table %s.%s.%s created\n", projectID, datasetID, tableID)
}
