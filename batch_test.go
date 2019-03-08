package batchinsert_test

import (
	"database/sql"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	batchinsert "github.com/MaruHyl/go-clickhouse-batchinsert"
	"github.com/stretchr/testify/require"
)

type testingLogger struct {
	tb testing.TB
}

func (l testingLogger) Log(keyAndValues ...interface{}) {
	l.tb.Log(keyAndValues...)
}

func Test(t *testing.T) {
	//
	testTable := `
			CREATE TABLE IF NOT EXISTS test (
				x        UInt8
			) engine=Memory`
	testSql := "INSERT INTO test (x) values (?)"
	//
	logger := &testingLogger{t}
	b, err := batchinsert.New(
		testSql,
		batchinsert.WithHost("127.0.0.1:9000"),
		batchinsert.WithDebug(false),
		batchinsert.WithLogger(logger),
		batchinsert.WithWriteTimeOut(1),
		batchinsert.WithFlushPeriod(time.Second),
		batchinsert.WithMaxBatchSize(1000000),
	)
	require.NoError(t, err)
	//
	_, err = b.DB().Exec(testTable)
	require.NoError(t, err)
	//
	t.Log("NumCPU", runtime.NumCPU())
	var wg sync.WaitGroup
	var count int64
	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			i := 0
			for {
				err := b.Insert(i)
				if err != nil {
					require.EqualError(t, err, batchinsert.ErrClosed.Error())
					atomic.AddInt64(&count, int64(i))
					return
				}
				i++
			}
		}()
	}
	//
	time.Sleep(5 * time.Second)
	require.NoError(t, b.Close())
	require.Equal(t, 0, b.Len())
	wg.Wait()
	//
	b, err = batchinsert.New(
		testSql,
		batchinsert.WithHost("127.0.0.1:9000"),
	)
	require.NoError(t, err)
	require.Equal(t, count, int64(selectCount(t, b.DB(), "test")))
	_, err = b.DB().Exec(`DROP TABLE IF EXISTS test`)
	require.NoError(t, err)
	require.NoError(t, b.Close())
	//
	t.Logf("insert count %d\n", count)
}

//
func selectCount(t *testing.T, db *sql.DB, table string) int {
	rows, err := db.Query("SELECT count() FROM " + table)
	require.NoError(t, err)
	defer rows.Close()

	for rows.Next() {
		var count int
		require.NoError(t, rows.Scan(&count))
		return count
	}
	return -1
}
