package batchinsert

import (
	"container/list"
	"database/sql"
	"errors"
	"net/url"
	"strconv"
	"sync"
	"time"

	_ "github.com/kshvakov/clickhouse"
)

type ConnectionOpenStrategy string

const OpenStrategyRandom ConnectionOpenStrategy = "random"
const OpenStrategyInOrder ConnectionOpenStrategy = "in_order"

var ErrClosed = errors.New("batch insert is closed")

type options struct {
	//
	host string
	// Auth credentials
	userName, password string
	// Select the current default database
	database string
	// Timeout in second
	readTimeout, writeTimeOut int
	// Disable/enable the Nagle Algorithm for tcp socket (default is 'true' - disable)
	noDelay bool
	// Comma separated list of single address host for load-balancing
	altHosts string
	// Random/in_order (default random)
	connectionOpenStrategy ConnectionOpenStrategy
	// Maximum rows in block (default is 1000000).
	// If the rows are larger then the data will be split into several blocks to send them
	// to the server
	blockSize int
	// Maximum amount of pre-allocated byte chunks used in queries (default is 100).
	// Decrease this if you experience memory problems at the expense of more GC pressure
	// and vice versa.
	poolSize int
	// Enable debug output
	debug bool
	//
	logger Logger
	//
	maxBatchSize int
	//
	flushPeriod time.Duration
}

var defaultOptions = options{
	host:                   "127.0.0.1:9000",
	noDelay:                true,
	connectionOpenStrategy: OpenStrategyRandom,
	blockSize:              1000000,
	poolSize:               100,
	logger:                 nopLogger{},
	maxBatchSize:           10000,
	flushPeriod:            1 * time.Second,
}

type Option func(opt *options)

func WithHost(host string) Option {
	return func(opt *options) {
		opt.host = host
	}
}

// Auth credentials
func WithUserInfo(userName, password string) Option {
	return func(opt *options) {
		opt.userName, opt.password = userName, password
	}
}

// Select the current default database
func WithDatabase(db string) Option {
	return func(opt *options) {
		opt.database = db
	}
}

// Timeout in second
func WithReadTimeout(readTimeout int) Option {
	return func(opt *options) {
		opt.readTimeout = readTimeout
	}
}

// Timeout in second
func WithWriteTimeOut(writeTimeOut int) Option {
	return func(opt *options) {
		opt.writeTimeOut = writeTimeOut
	}
}

// Disable/enable the Nagle Algorithm for tcp socket (default is 'true' - disable)
func WithNoDelay(noDelay bool) Option {
	return func(opt *options) {
		opt.noDelay = noDelay
	}
}

// Comma separated list of single address host for load-balancing
func WithAltHosts(altHosts string) Option {
	return func(opt *options) {
		opt.altHosts = altHosts
	}
}

// Random/in_order (default random)
func WithConnectionOpenStrategy(connectionOpenStrategy ConnectionOpenStrategy) Option {
	return func(opt *options) {
		opt.connectionOpenStrategy = connectionOpenStrategy
	}
}

// Maximum rows in block (default is 1000000).
// If the rows are larger then the data will be split into several blocks to send them
// to the server
func WithBlockSize(blockSize int) Option {
	return func(opt *options) {
		opt.blockSize = blockSize
	}
}

// Maximum amount of pre-allocated byte chunks used in queries (default is 100).
// Decrease this if you experience memory problems at the expense of more GC pressure
// and vice versa.
func WithPoolSize(poolSize int) Option {
	return func(opt *options) {
		opt.poolSize = poolSize
	}
}

// Enable debug output
func WithDebug(debug bool) Option {
	return func(opt *options) {
		opt.debug = debug
	}
}

//
func WithLogger(logger Logger) Option {
	return func(opt *options) {
		opt.logger = logger
	}
}

//
func WithMaxBatchSize(maxBatchSize int) Option {
	return func(opt *options) {
		opt.maxBatchSize = maxBatchSize
	}
}

//
func WithFlushPeriod(flushPeriod time.Duration) Option {
	return func(opt *options) {
		opt.flushPeriod = flushPeriod
	}
}

func GetUrl(opts options) string {
	u := new(url.URL)
	u.Scheme = "tcp"
	u.Host = opts.host

	v := url.Values{}
	if opts.userName != "" {
		v.Set("username", opts.userName)
	}
	if opts.password != "" {
		v.Set("password", opts.password)
	}
	if opts.database != "" {
		v.Set("database", opts.database)
	}
	if opts.readTimeout > 0 {
		v.Set("read_timeout", strconv.Itoa(opts.readTimeout))
	}
	if opts.writeTimeOut > 0 {
		v.Set("write_timeout", strconv.Itoa(opts.writeTimeOut))
	}
	if !opts.noDelay {
		v.Set("no_delay", "false")
	}
	if opts.altHosts != "" {
		v.Set("alt_hosts", opts.altHosts)
	}
	if opts.connectionOpenStrategy != OpenStrategyRandom {
		v.Set("connection_open_strategy", string(opts.connectionOpenStrategy))
	}
	if opts.blockSize != 1000000 {
		v.Set("block_size", strconv.Itoa(opts.blockSize))
	}
	if opts.poolSize != 100 {
		v.Set("pool_size", strconv.Itoa(opts.poolSize))
	}
	if opts.debug {
		v.Set("debug", "true")
	}
	u.RawQuery = v.Encode()
	return u.String()
}

//
type BatchInsert struct {
	opts      options
	mu        sync.Mutex
	wg        sync.WaitGroup
	db        *sql.DB
	cache     *list.List
	kick      chan struct{}
	exit      chan struct{}
	insertSql string
	exited    bool
}

//
func New(insertSql string, opts ...Option) (*BatchInsert, error) {
	//
	options := defaultOptions
	for _, opt := range opts {
		opt(&options)
	}
	//
	url := GetUrl(options)
	options.logger.Log("open url", url)
	//
	db, err := sql.Open("clickhouse", url)
	if err != nil {
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	//
	b := &BatchInsert{
		opts:      options,
		db:        db,
		cache:     list.New(),
		kick:      make(chan struct{}),
		exit:      make(chan struct{}),
		insertSql: insertSql,
	}
	b.wg.Add(1)
	go func() {
		b.flusher()
		b.wg.Done()
	}()
	return b, nil
}

//
func (b *BatchInsert) Insert(values ...interface{}) (err error) {
	b.mu.Lock()

	if b.exited {
		b.mu.Unlock()
		return ErrClosed
	}

	b.cache.PushBack(values)
	if b.cache.Len() >= b.opts.maxBatchSize {
		select {
		case b.kick <- struct{}{}:
		default:
		}
	}

	b.mu.Unlock()
	return
}

func (b *BatchInsert) Close() error {
	b.mu.Lock()
	if b.exited {
		b.mu.Unlock()
		return nil
	}
	b.exited = true
	close(b.exit)
	b.mu.Unlock()
	b.wg.Wait()
	return b.db.Close()
}

//
func (b *BatchInsert) flusher() {
	ticker := time.NewTicker(b.opts.flushPeriod)
	defer ticker.Stop()
	exited := false
	for {
		select {
		case <-ticker.C:
		case <-b.kick:
		case <-b.exit:
			exited = true
		}
		b.mu.Lock()
		l := b.cache
		b.cache = list.New()
		b.mu.Unlock()
		startTime := time.Now()
		if l.Len() > 0 {
			err := batchInsert(b.db, b.insertSql, l)
			if err != nil {
				b.opts.logger.Log("flush error", err)
			} else {
				b.opts.logger.Log("flushed", l.Len(), "cost", time.Since(startTime))
			}
		}
		if exited {
			close(b.kick)
			b.opts.logger.Log("flusher", "stop")
			return
		}
	}
}

//
func batchInsert(db *sql.DB, insertSql string, values *list.List) (err error) {
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

	stmt, err := tx.Prepare(insertSql)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for e := values.Front(); e != nil; e = e.Next() {
		_, err = stmt.Exec(e.Value.([]interface{})...)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (b *BatchInsert) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.cache.Len()
}

func (b *BatchInsert) DB() *sql.DB {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.db
}
