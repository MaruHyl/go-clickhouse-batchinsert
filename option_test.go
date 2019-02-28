package batchinsert

import (
	"github.com/stretchr/testify/require"
	"testing"
)

const testUrl = "127.0.0.1:9000"

func TestOptions_GetUrl(t *testing.T) {
	opts := Options{
		Hosts: []string{"127.0.0.1:9000"},
	}
	url, err := opts.GetUrl()
	require.NoError(t, err)
	require.Equal(t, url, "tcp://127.0.0.1:9000")
	t.Log(url)
	opts.UserName = "user"
	opts.Password = "pwd"
	opts.NotSkipVerify = true
	opts.Secure = true
	opts.Debug = true
	opts.PoolSize = 1
	opts.BlockSize = 2
	opts.Hosts = append(opts.Hosts, "127.0.0.1:9001", "127.0.0.1:9002")
	opts.Delay = true
	opts.WriteTimeOut = 3
	opts.ReadTimeout = 4
	opts.Database = "db"
	url, err = opts.GetUrl()
	require.NoError(t, err)
	require.Equal(t, url, "tcp://127.0.0.1:9000?"+
		"alt_hosts=127.0.0.1%3A9001%2C127.0.0.1%3A9002&"+
		"block_size=2&database=db&debug=true&"+
		"no_delay=false&password=pwd&pool_size=1&"+
		"read_timeout=4&secure=true&skip_verify=false&"+
		"username=user&write_timeout=3")
	t.Log(url)
}

func TestOptions_Open(t *testing.T) {
	opts := Options{
		Hosts:        []string{testUrl},
		Debug:        true,
		WriteTimeOut: 20,
		ReadTimeout:  10,
		Database:     "test",
	}
	db, err := opts.Open()
	require.NoError(t, err)
	require.NoError(t, db.Close())
}
