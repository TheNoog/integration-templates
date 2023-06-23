/*

The program takes the following command-line arguments:

    -server: The name of the MSSQL server
    -database: The name of the MSSQL database
    -table: The name of the MSSQL table
    -file: The path of the file to bulk insert
    -delimiter: The delimiter used in the file (default is ~)

*/

package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os/exec"
	"strings"

	_ "github.com/denisenkom/go-mssqldb"
)

func main() {
	// Parse command-line arguments
	server := flag.String("server", "", "MSSQL server name")
	database := flag.String("database", "", "MSSQL database name")
	table := flag.String("table", "", "MSSQL table name")
	filePath := flag.String("file", "", "File path for bulk insert")
	delimiter := flag.String("delimiter", "~", "Delimiter for bulk insert file")
	flag.Parse()

	// Validate command-line arguments
	if *server == "" || *database == "" || *table == "" || *filePath == "" {
		log.Fatal("Server, database, table, and file arguments are required")
	}

	// Get BCP command and arguments
	bcpCmd := "bcp"
	bcpArgs := []string{
		*database + "." + *table,
		"in",
		*filePath,
		"-c",
		"-t" + *delimiter,
		"-S" + *server,
	}

	// Execute BCP command
	cmd := exec.Command(bcpCmd,
		bcpArgs...)
		output, err := cmd.CombinedOutput()
		if err != nil {
		log.Fatalf("Error executing BCP command: %s\n%s", err, string(output))
		}

	// Parse BCP output
	outputLines := strings.Split(string(output), "")
	for _, line := range outputLines {
		if strings.HasPrefix(line, "Rows read:") {
			fmt.Println(line)
		}
	}

	fmt.Println("Bulk insert completed successfully")
}