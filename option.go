package batchinsert

import (
	"database/sql"
	"errors"
	_ "github.com/kshvakov/clickhouse"
	"net/url"
	"strconv"
	"strings"
)

type Options struct {
	// auth credentials
	UserName, Password string
	// select the current default database
	Database string
	// timeout in second
	ReadTimeout, WriteTimeOut int
	// disable/enable the Nagle Algorithm for tcp socket
	Delay bool
	// list of single address host for load-balancing
	Hosts []string
	// maximum rows in block (default is 1000000).
	// If the rows are larger then the data will be split into several blocks
	// to send them to the server
	BlockSize int
	// maximum amount of pre-allocated byte chunks used in queries (default is 100).
	// Decrease this if you experience memory problems at the expense of more GC
	// pressure and vice versa.
	PoolSize int
	// enable debug output
	Debug bool
	// establish secure connection
	Secure bool
	// skip certificate verification
	NotSkipVerify bool
}

func (opts Options) GetUrl() (string, error) {
	if opts.Hosts == nil || len(opts.Hosts) == 0 {
		return "", errors.New("empty hosts")
	}
	u := new(url.URL)
	u.Scheme = "tcp"
	u.Host = opts.Hosts[0]
	v := url.Values{}
	if opts.UserName != "" {
		v.Set("username", opts.UserName)
	}
	if opts.Password != "" {
		v.Set("password", opts.Password)
	}
	if opts.Database != "" {
		v.Set("database", opts.Database)
	}
	if opts.ReadTimeout > 0 {
		v.Set("read_timeout", strconv.Itoa(opts.ReadTimeout))
	}
	if opts.WriteTimeOut > 0 {
		v.Set("write_timeout", strconv.Itoa(opts.WriteTimeOut))
	}
	if opts.Delay {
		v.Set("no_delay", strconv.FormatBool(!opts.Delay))
	}
	if len(opts.Hosts) > 1 {
		altHost := make([]string, 0)
		for i := 1; i < len(opts.Hosts); i++ {
			altHost = append(altHost, opts.Hosts[i])
		}
		v.Set("alt_hosts", strings.Join(altHost, ","))
	}
	if opts.BlockSize > 0 {
		v.Set("block_size", strconv.Itoa(opts.BlockSize))
	}
	if opts.PoolSize > 0 {
		v.Set("pool_size", strconv.Itoa(opts.PoolSize))
	}
	if opts.Debug {
		v.Set("debug", strconv.FormatBool(opts.Debug))
	}
	if opts.Secure {
		v.Set("secure", strconv.FormatBool(opts.Secure))
	}
	if opts.NotSkipVerify {
		v.Set("skip_verify", strconv.FormatBool(!opts.NotSkipVerify))
	}
	u.RawQuery = v.Encode()
	return u.String(), nil
}

func (opts Options) Open() (*sql.DB, error) {
	url, err := opts.GetUrl()
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("clickhouse", url)
	if err != nil {
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}
