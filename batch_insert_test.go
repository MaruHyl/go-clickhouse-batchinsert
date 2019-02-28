package batchinsert

import (
	"database/sql"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func TestBatchInsert_Close(t *testing.T) {
	opts := Options{
		Hosts:        []string{testUrl},
		Debug:        true,
		WriteTimeOut: 20,
		ReadTimeout:  10,
		Database:     "test",
	}
	db, err := opts.Open()
	require.NoError(t, err)
	b := NewBatchInsert(db, "")
	require.NoError(t, b.Close())
	require.NoError(t, b.Close())
	require.NoError(t, db.Close())
}

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

func TestBatchInsert_Insert_Slow(t *testing.T) {
	opts := Options{
		Hosts:        []string{testUrl},
		Debug:        true,
		WriteTimeOut: 20,
		ReadTimeout:  10,
		Database:     "test",
	}

	db, err := opts.Open()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS test (
			x        UInt8
		) engine=Memory
	`)
	require.NoError(t, err)

	b := NewBatchInsert(db,
		"INSERT INTO test (x) values (?)",
		WithMaxCache(100),
		WithErrorHandler(func(err error) {
			require.NoError(t, err)
		}))

	var count = 10
	var wg sync.WaitGroup
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				time.Sleep(time.Second)
				require.NoError(t, b.Insert(i))
			}
		}(i)
	}
	wg.Wait()

	require.NoError(t, b.Close())

	require.Equal(t, 50, selectCount(t, db, "test"))

	_, err = db.Exec(`DROP TABLE IF EXISTS test`)
	require.NoError(t, err)
}

func TestBatchInsert_Insert_Fast(t *testing.T) {
	opts := Options{
		Hosts:        []string{testUrl},
		Debug:        true,
		WriteTimeOut: 20,
		ReadTimeout:  10,
		Database:     "test",
	}

	db, err := opts.Open()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	_, err = db.Exec(`DROP TABLE IF EXISTS test`)
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS test (
			x        UInt8
		) engine=Memory
	`)
	require.NoError(t, err)

	b := NewBatchInsert(db,
		"INSERT INTO test (x) values (?)",
		WithMaxCache(100),
		WithErrorHandler(func(err error) {
			require.NoError(t, err)
		}))

	var count = 10
	var wg sync.WaitGroup
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				time.Sleep(time.Second)
				for k := 0; k < 20; k++ {
					require.NoError(t, b.Insert(i))
				}
			}
		}(i)
	}
	wg.Wait()

	require.NoError(t, b.Close())

	require.Equal(t, 1000, selectCount(t, db, "test"))

	_, err = db.Exec(`DROP TABLE IF EXISTS test`)
	require.NoError(t, err)
}
