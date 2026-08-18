package main

import (
	"context"
	"fmt"
	"os"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func required(name string) string {
	value := os.Getenv(name)
	if value == "" {
		panic(name + " is required")
	}
	return value
}

func main() {
	ctx := context.Background()
	uri := os.Getenv("QCLI_FLIGHT_URI")
	if uri == "" {
		uri = "grpc://127.0.0.1:32010"
	}
	target := os.Getenv("QCLI_FLIGHT_TARGET")
	if target == "" {
		target = "demo"
	}
	query := os.Getenv("QCLI_FLIGHT_QUERY")
	if query == "" {
		query = "select * from sample"
	}
	expectedRows := int64(2)
	if os.Getenv("QCLI_FLIGHT_EXPECTED_ROWS") == "1" {
		expectedRows = 1
	}
	driver := flightsql.NewDriver(memory.DefaultAllocator)
	database, err := driver.NewDatabase(map[string]string{
		adbc.OptionKeyURI:                                   uri,
		flightsql.OptionAuthorizationHeader:                 "Bearer " + required("QCLI_FLIGHT_TOKEN"),
		flightsql.OptionRPCCallHeaderPrefix + "qcli-target": target,
		flightsql.OptionTimeoutQuery:                        "10",
		flightsql.OptionTimeoutFetch:                        "10",
	})
	if err != nil {
		panic(err)
	}
	defer database.Close()
	connection, err := database.Open(ctx)
	if err != nil {
		panic(err)
	}
	defer connection.Close()
	statement, err := connection.NewStatement()
	if err != nil {
		panic(err)
	}
	defer statement.Close()
	if err := statement.SetSqlQuery(query); err != nil {
		panic(err)
	}
	reader, _, err := statement.ExecuteQuery(ctx)
	if err != nil {
		panic(err)
	}
	rows := int64(0)
	for reader.Next() {
		rows += reader.RecordBatch().NumRows()
	}
	if err := reader.Err(); err != nil {
		panic(err)
	}
	reader.Release()
	if rows != expectedRows {
		panic(fmt.Sprintf("expected %d rows, got %d", expectedRows, rows))
	}
	objects, err := connection.GetObjects(ctx, adbc.ObjectDepthTables, nil, nil, nil, nil, nil)
	if err != nil {
		panic(err)
	}
	if !objects.Next() || objects.RecordBatch().NumRows() == 0 {
		panic("metadata returned no catalogs")
	}
	objects.Release()
	fmt.Println("go-adbc: PASS")
}
