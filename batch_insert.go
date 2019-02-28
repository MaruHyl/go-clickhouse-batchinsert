package batchinsert

import (
	"container/list"
	"database/sql"
	"errors"
	"sync"
	"time"
)

type ErrorHandler func(err error)

type BatchInsertOption func(*BatchInsert)

func WithErrorHandler(handler ErrorHandler) BatchInsertOption {
	return func(insert *BatchInsert) {
		insert.onError = handler
	}
}

func WithMaxCache(max int) BatchInsertOption {
	return func(insert *BatchInsert) {
		insert.maxCache = max
	}
}

type BatchInsert struct {
	db       *sql.DB
	closed   bool
	mu       sync.Mutex
	kick     chan struct{}
	cache    *list.List
	sqlStr   string
	maxCache int
	onError  func(err error)
	wg       sync.WaitGroup
}

func NewBatchInsert(db *sql.DB, sql string, opts ...BatchInsertOption) *BatchInsert {
	b := new(BatchInsert)
	b.db = db
	b.sqlStr = sql
	b.kick = make(chan struct{}, 1)
	b.cache = list.New()
	b.maxCache = 1000000
	b.onError = func(err error) {}
	for _, opt := range opts {
		opt(b)
	}
	b.wg.Add(1)
	go b.flusher()
	return b
}

func (b *BatchInsert) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	b.kickFlush()
	b.mu.Unlock()
	b.wg.Wait()
	return nil
}

func (b *BatchInsert) kickFlush() {
	select {
	case b.kick <- struct{}{}:
	default:
	}
}

func (b *BatchInsert) Insert(values ...interface{}) error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return errors.New("batch insert closed")
	}
	b.cache.PushBack(values)
	if b.cache.Len() >= b.maxCache {
		b.kickFlush()
	}
	b.mu.Unlock()
	return nil
}

func (b *BatchInsert) flusher() {
	defer b.wg.Done()
	var t = time.NewTimer(time.Second)
	var l *list.List
	var err error
	var closed bool
	for {
		select {
		case <-t.C:
		case <-b.kick:
		}
		b.mu.Lock()
		if b.cache.Len() > 0 {
			l = b.cache
			b.cache = list.New()
		}
		closed = b.closed
		b.mu.Unlock()
		if l != nil {
			err = batchInsert(b.db, b.sqlStr, l)
			if err != nil {
				b.onError(err)
			}
			l = nil
		}
		if !t.Stop() {
			select {
			case <-t.C:
			default:
			}
		}
		if closed {
			return
		}
		t.Reset(time.Second)
	}
}

func batchInsert(db *sql.DB, sqlStr string, values *list.List) (err error) {
	if db == nil {
		return errors.New("nil db")
	}

	var tx *sql.Tx
	tx, err = db.Begin()
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	stmt, err := tx.Prepare(sqlStr)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for e := values.Front(); e != nil; e = e.Next() {
		value := e.Value.([]interface{})
		if _, err := stmt.Exec(value...); err != nil {
			return err
		}
	}

	return tx.Commit()
}
