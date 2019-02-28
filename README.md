# go-clickhouse-batchinsert
go client (batch insert) base on https://github.com/kshvakov/clickhouse/
### Clickhouse client for batch insert
#### Why need this
1. clickhouse-client can do bulk insert like this
```go
    var (
      tx, _   = connect.Begin()
      stmt, _ = tx.Prepare("...")
    )
    defer stmt.Close()

    for _, obj := objs {
      if _, err := stmt.Exec(...); err != nil {
        log.Fatal(err)
      }
    }

    if err := tx.Commit(); err != nil {
      log.Fatal(err)
    }
```
So we have to cache the object ourselves

2. Ref to https://clickhouse.yandex/docs/en/introduction/performance/

`We recommend inserting data in packets of at least 1000 rows, or no more than a single request per second. `

So we preferably do a batch insert at a fixed time interval (e.g. 1s) or a fixed number (e.g. 1000000) of inserts.

#### Example
```go
    opts := Options{
      Hosts:        []string{"127.0.0.1"},
      Debug:        true,
      WriteTimeOut: 20,
      ReadTimeout:  10,
      Database:     "test",
    }

    db, _ := opts.Open()

    db.Exec(`
      CREATE TABLE IF NOT EXISTS test (
        x        UInt8
      ) engine=Memory
    `)

    b := NewBatchInsert(db,
      "INSERT INTO test (x) values (?)",
      WithMaxCache(100),
      WithErrorHandler(func(err error) {
        fmt.Println(err)
      }))

    b.Insert(1)
    b.Insert(2)
    b.Insert(3)
    
    db.Exec(`DROP TABLE IF EXISTS test`)
```
