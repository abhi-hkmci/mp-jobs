package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"reflect"

	"cloud.google.com/go/bigquery"
	"github.com/jackc/pgx/v5"
	"google.golang.org/api/iterator"
)

var projectID = "masterplanner"
var trasnfered_shipment []string
var pgConn *pgx.Conn

func PgSqlRowsToJson(rows pgx.Rows) ([]byte, error) {

	fieldDescriptions := rows.FieldDescriptions()
	var columns []string

	for _, col := range fieldDescriptions {
		columns = append(columns, string(col.Name))
	}

	count := len(columns)
	tableData := make([]map[string]interface{}, 0)

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, err
		}

		entry := make(map[string]interface{}, count)
		for i, col := range columns {
			val := values[i]

			// Handle null values gracefully
			if val == nil {
				entry[col] = ""
			} else if b, ok := val.([]byte); ok {
				entry[col] = string(b)
			} else {
				entry[col] = val
			}
		}
		tableData = append(tableData, entry)
	}
	jsonData, err := json.Marshal(tableData)
	if err != nil {
		return nil, err
	}
	return jsonData, nil
}
func checkBigqueryConnection() (*bigquery.Client, context.Context, error) {
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

	return client, ctx, nil
}

// ... (Previous code)

func deleteRows(idsToDelete []string, pgConn *pgx.Conn) ([]string, error) {
	var deletedIDs []string

	tx, err := pgConn.Begin(context.Background())
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			tx.Rollback(context.Background())
		}
	}()

	for _, id := range idsToDelete {
		result, err := tx.Exec(context.Background(), "DELETE FROM public.shipments WHERE uid = $1", id)
		if err != nil {
			return nil, err
		}

		// Check if the row was actually deleted
		rowsAffected := result.RowsAffected()
		if rowsAffected > 0 {
			deletedIDs = append(deletedIDs, id)
		}
	}

	err = tx.Commit(context.Background())
	if err != nil {
		return nil, err
	}

	return deletedIDs, nil
}

func serviceDB() {

	// REINDEX DATABASE  get ;
	// REINDEX  SYSTEM get ;
	// ANALYZE
	// VACUUM  FULL ANALYZE ;

	_, err := pgConn.Exec(context.Background(), "REINDEX DATABASE get")
	if err != nil {
		// Handle errors appropriately
		fmt.Println("Error executing maintanance query:", err)
		return
	}
	_, err = pgConn.Exec(context.Background(), "REINDEX  SYSTEM get")
	if err != nil {
		// Handle errors appropriately
		fmt.Println("Error executing maintanance query:", err)
		return
	}
	_, err = pgConn.Exec(context.Background(), "ANALYZE")
	if err != nil {
		// Handle errors appropriately
		fmt.Println("Error executing maintanance query:", err)
		return
	}
	_, err = pgConn.Exec(context.Background(), "VACUUM FULL ANALYZE")
	if err != nil {
		// Handle errors appropriately
		fmt.Println("Error executing maintanance query:", err)
		return
	}
	fmt.Println("completed running maintanance query")

}

func main() {

	var err error
	pgConn, err = pgx.Connect(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer pgConn.Close(context.Background())

	sql := "SELECT * FROM public.shipments WHERE (properties ->> 'status' = '6') AND updated_at < CURRENT_TIMESTAMP - INTERVAL '3 months'"
	//sql := "SELECT * FROM public.shipments limit 1"
	rows, err := pgConn.Query(context.Background(), sql)

	rowsJSON, err := PgSqlRowsToJson(rows)
	if err != nil {
		fmt.Printf("Error converting rows to JSON: %v\n", err)
		return
	}

	client, ctx, err := checkBigqueryConnection()

	var results []map[string]interface{}
	json.Unmarshal([]byte(rowsJSON), &results)
	for _, result := range results {

		prop := result["properties"].(map[string]interface{})
		propString, err := json.Marshal(prop)

		var latlng = false
		var lat = 0.0
		var lng = 0.0
		var ok = false
		if lat, ok = prop["latitude"].(float64); ok {
			if lng, ok = prop["longitude"].(float64); ok {
				latlng = true
			}
		}
		if latlng == true {
			fmt.Printf("Value for key '%s': %v %v\n", "latitude", lat, lng)
		}

		if err != nil {
			fmt.Println("Error marshaling JSON:", err)
		} else {
			//fmt.Println(string(propString))
		}
		// ST_GeogPoint(ST_X(ST_GeogFromText('POINT (longitude latitude)')), ST_Y(ST_GeogFromText('POINT (longitude latitude)')))
		bigquerySQL := fmt.Sprintf(`INSERT INTO masterplanner.logs.shipments (uid, org_uid, properties,geog) VALUES (
			'%s',
			'%s',
			JSON '%s',
			ST_GEOGPOINT( %f, %f) 
			)`, result["uid"].(string), result["org_uid"].(string), string(propString), lng, lat)

		q := client.Query(bigquerySQL)

		quryResult, err := q.Run(ctx)
		if err != nil {
			log.Fatalf("Failed to execute query: %v", err)
		}

		queryStatus, err := quryResult.Wait(ctx)
		if err != nil {
			log.Fatalf("Failed to wait for job completion: %v", err)
		}

		if err := queryStatus.Err(); err != nil {
			log.Fatalf("Query execution error: %v", err)
		}

		fmt.Println("Successfully transferred " + result["uid"].(string))
		trasnfered_shipment = append(trasnfered_shipment, result["uid"].(string))
	}

	deletedList, err := deleteRows(trasnfered_shipment, pgConn)

	if reflect.DeepEqual(trasnfered_shipment, deletedList) {
		fmt.Println("Success verifed")
	} else {
		fmt.Println("Partial success identified")
	}
	serviceDB()

}
