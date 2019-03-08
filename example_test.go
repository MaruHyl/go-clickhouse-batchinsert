package batchinsert_test

import batchinsert "github.com/MaruHyl/go-clickhouse-batchinsert"

func ExampleBatchInsert() {
	//
	createTable := `
			CREATE TABLE IF NOT EXISTS test (
				x        UInt8
			) engine=Memory`
	insertSql := "INSERT INTO test (x) values (?)"
	//
	b, err := batchinsert.New(
		insertSql,
		batchinsert.WithHost("127.0.0.1:9000"),
	)
	if err != nil {
		panic(err)
	}
	//
	_, err = b.DB().Exec(createTable)
	if err != nil {
		panic(err)
	}
	//
	_ = b.Insert(1)
	_ = b.Insert(2)
	_ = b.Insert(3)
	//
	_, err = b.DB().Exec(`DROP TABLE IF EXISTS test`)
	if err != nil {
		panic(err)
	}
	err = b.Close()
	if err != nil {
		panic(err)
	}

	// Output:
}
