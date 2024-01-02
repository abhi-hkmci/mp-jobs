// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// [START cloudrun_jobs_quickstart]
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"

	"cloud.google.com/go/bigquery"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/objx"
	"google.golang.org/api/iterator"
)

func PgSqlRowsToJson(rows pgx.Rows) []byte {

	fieldDescriptions := rows.FieldDescriptions()
	var columns []string

	for _, col := range fieldDescriptions {
		columns = append(columns,
			string(col.Name))
	}

	count := len(columns)
	tableData := make([]map[string]interface{}, 0)
	valuePtrs := make([]interface{}, count)

	for rows.Next() {

		values, _ := rows.Values()
		for i, v := range values {
			// Handle null values gracefully
			if v == nil {
				valuePtrs[i] = new(interface{}) // Allocate a pointer to a nil interface
			} else {
				valuePtrs[i] = reflect.New(reflect.TypeOf(v)).Interface() // Allocate pointer to type
			}
		}

		rows.Scan(valuePtrs...)
		entry := make(map[string]interface{}, count)
		for i, col := range columns {
			var v interface{}
			val := reflect.ValueOf(valuePtrs[i]).Elem().Interface()

			// Dereference pointer
			if val == nil { // If the value is still nil, set it to an empty string
				v = ""
			} else if b, ok := val.([]byte); ok {
				v = string(b)
			} else {
				v = val
			}
			entry[col] = v
		}
		tableData = append(tableData, entry)
	}

	jsonData, _ := json.Marshal(tableData)
	return jsonData
}

func main() {

	projectID := "masterplanner"

	conn, err := pgx.Connect(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(context.Background())

	sql := "SELECT * FROM public.shipments limit 2 "
	rows, err := conn.Query(context.Background(), sql)

	rows_json := PgSqlRowsToJson(rows)

	// fmt.Println("JSON Result : ", string(rows_json))

	// Declared an empty interface of type Array
	var results []map[string]interface{}

	// Unmarshal or Decode the JSON to the interface.
	json.Unmarshal([]byte(rows_json), &results)

	//  BIGQUERY
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)

	if err != nil {
		fmt.Println("Error: cannot connect BigQuery")
		panic(err.Error())
	}
	defer client.Close()

	it := client.Datasets(ctx)
	for {
		dataset, err := it.Next()
		if err == iterator.Done {
			break
		}
		fmt.Println(dataset.DatasetID)
	}
	println("Client connection verified ")

	m := objx.MustFromJSON(string(rows_json))
	for key, value := range m {
		fmt.Println(key, value)
	}

	// insert query  here ------------------------------------------------------------------------------------------

	// for key, result := range results {

	// 	fmt.Println("Reading Value for Key :", key)

	// 	fmt.Println("uid :", result["uid"])
	// 	fmt.Println("org_uid :", result["org_uid"])
	// 	fmt.Println("properties :", result["properties"])

	// 	bigquery_sql := fmt.Sprintf(`INSERT INTO logs.shipments (uid,org_uid,properties) VALUES (
	// 		'` + result["uid"].(string) + `',
	// 		'` + result["org_uid"].(string) + `'
	// 		JSON"""` + result["properties"].(map[string]interface{}) + `"""

	// 	)`)

	// 	q := client.Query(bigquery_sql)

	// 	// Execute the query
	// 	result, err := q.Run(ctx)
	// 	if err != nil {
	// 		log.Fatalf("Failed to execute query: %v", err)
	// 	}

	// 	// Wait for the job to complete
	// 	status, err := result.Wait(ctx)
	// 	if err != nil {
	// 		log.Fatalf("Failed to wait for job completion: %v", err)
	// 	}

	// 	// Check the job status for errors
	// 	if err := status.Err(); err != nil {
	// 		log.Fatalf("Query execution error: %v", err)

	// 	}

	// 	println("Success")

	// } // loop end here

}
